"""Circuit breaker, outlier ejection, and the balancer's behaviour across a Redis outage."""

import asyncio
import random
import time
from collections import Counter

import pytest
import redis.asyncio as aioredis
import redis.exceptions as rexc
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

from ddr.balancer import LoadBalancer
from ddr.resilience import OutlierDetector, RedisCircuit, RedisUnavailable


async def wait_until(cond, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not cond():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.02)


class FakeRedis:
    def __init__(self) -> None:
        self.up = True
        self.calls = 0

    async def ping(self):
        if not self.up:
            raise rexc.ConnectionError("down")
        return True

    async def op(self):
        self.calls += 1
        if not self.up:
            raise rexc.ConnectionError("down")
        return "ok"


class Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


# -- circuit breaker ------------------------------------------------------------
async def test_circuit_trips_fails_fast_and_recovers():
    fake = FakeRedis()
    circuit = RedisCircuit(fake.ping, probe_interval_s=0.01)
    recovered = []

    async def hook():
        recovered.append(True)

    circuit.on_recover(hook)
    assert await circuit.call(fake.op) == "ok"
    fake.up = False
    with pytest.raises(RedisUnavailable):
        await circuit.call(fake.op)
    assert not circuit.available and circuit.outages == 1
    calls = fake.calls
    with pytest.raises(RedisUnavailable):
        await circuit.call(fake.op)
    assert fake.calls == calls  # open circuit: Redis isn't even tried
    fake.up = True
    await wait_until(lambda: circuit.available)
    assert recovered == [True]
    assert await circuit.call(fake.op) == "ok"
    status = circuit.status()
    assert status["state"] == "up" and status["outages"] == 1 and status["downtime_s"] > 0


async def test_failed_recovery_hook_keeps_circuit_open():
    fake = FakeRedis()
    circuit = RedisCircuit(fake.ping, probe_interval_s=0.01)
    attempts = 0

    async def half_up():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise rexc.ConnectionError("still loading")

    circuit.on_recover(half_up)
    fake.up = False
    with pytest.raises(RedisUnavailable):
        await circuit.call(fake.op)
    fake.up = True
    await wait_until(lambda: circuit.available)
    assert attempts == 3


async def test_disabled_circuit_fails_closed():
    fake = FakeRedis()
    circuit = RedisCircuit(fake.ping, enabled=False)
    fake.up = False
    with pytest.raises(rexc.ConnectionError):
        await circuit.call(fake.op)
    assert circuit.available  # never opens: errors reach the caller


async def test_command_errors_do_not_trip():
    circuit = RedisCircuit(FakeRedis().ping)

    async def bad_command():
        raise rexc.ResponseError("ERR wrong number of arguments")

    with pytest.raises(rexc.ResponseError):
        await circuit.call(bad_command)
    assert circuit.available


# -- outlier ejection -------------------------------------------------------------
def test_ejection_backs_off_and_success_clears():
    clock = Clock()
    od = OutlierDetector(3, base_ejection_s=1.0, max_ejection_s=4.0, clock=clock)
    od.record("n", False)
    od.record("n", False)
    assert not od.is_ejected("n")
    od.record("n", False)
    assert od.snapshot() == {"n": 1.0}
    od.record("n", False)               # late failure from a request sent earlier: ignored
    assert od.snapshot() == {"n": 1.0}
    clock.t = 1.0
    assert not od.is_ejected("n")       # on probation: gets one more chance
    od.record("n", False)
    assert od.snapshot() == {"n": 2.0}  # failed again: twice as long
    clock.t = 3.0
    od.record("n", False)
    clock.t = 7.0
    od.record("n", False)
    assert od.snapshot() == {"n": 4.0}  # capped
    clock.t = 11.0
    od.record("n", True)                # one success clears the record
    assert od.snapshot() == {} and not od.is_ejected("n")
    od.record("n", False)
    assert not od.is_ejected("n")       # back to needing 3 in a row
    assert od.total_ejections == 4


def test_success_breaks_a_failure_streak():
    od = OutlierDetector(3, clock=Clock())
    for ok in (False, False, True, False, False):
        od.record("n", ok)
    assert not od.is_ejected("n")


def test_filter_never_returns_nothing():
    od = OutlierDetector(1, clock=Clock())
    od.record("a", False)
    assert od.filter(["a", "b"]) == ["b"]
    od.record("b", False)
    assert od.filter(["a", "b"]) == ["a", "b"]


# -- balancer across a real Redis outage ----------------------------------------------
def _client(url: str) -> aioredis.Redis:
    # Same client settings as the router.
    return aioredis.from_url(url, socket_connect_timeout=0.5, socket_timeout=1.0, retry=Retry(NoBackoff(), 1))


async def test_balancer_falls_back_to_local_counts(restartable_redis):
    r = _client(restartable_redis.url)
    circuit = RedisCircuit(r.ping, probe_interval_s=0.05)
    lb = LoadBalancer(r, "ddr:fo", "least_conn", explore=0.0, rng=random.Random(1), circuit=circuit)
    await lb.publish()
    restartable_redis.kill()
    picks = [await lb.acquire(0, ["x", "y", "z"]) for _ in range(6)]
    assert not circuit.available
    assert Counter(picks) == {"x": 2, "y": 2, "z": 2}  # least_conn on this router's own counts
    assert lb.local_picks == 6
    for p in picks:
        lb.release_nowait(p)
    assert lb.local == {}
    await circuit.close()
    await r.aclose()


@pytest.mark.parametrize("strategy", ["random", "round_robin", "latency_aware", "p2c"])
async def test_every_strategy_works_without_redis(restartable_redis, strategy):
    r = _client(restartable_redis.url)
    circuit = RedisCircuit(r.ping, probe_interval_s=60)
    lb = LoadBalancer(r, "ddr:fo", strategy, circuit=circuit)
    restartable_redis.kill()
    picks = [await lb.acquire(0, ["x", "y"]) for _ in range(20)]
    assert set(picks) <= {"x", "y"} and lb.local_picks == 20
    await circuit.close()
    await r.aclose()


async def test_recovery_restores_exact_shared_counts(restartable_redis):
    r = _client(restartable_redis.url)
    circuit = RedisCircuit(r.ping, probe_interval_s=0.05)
    lb = LoadBalancer(r, "ddr:fo", "least_conn", explore=0.0, circuit=circuit)
    circuit.on_recover(lb.publish)
    for _ in range(3):
        await lb.acquire(0, ["x"])                 # shared: Redis says x=3
    restartable_redis.kill()
    lb.release_nowait("x")                         # finishes during the outage
    assert await lb.acquire(0, ["x", "y"]) == "y"  # chosen locally: x=2, y=0
    await wait_until(lambda: not circuit.available)
    restartable_redis.start()                      # comes back empty
    await wait_until(lambda: circuit.available)
    shared = {k.decode(): int(v) for k, v in (await r.hgetall(lb.slice_key)).items()}
    assert shared == dict(lb.local) == {"x": 2, "y": 1}
    assert circuit.outages == 1
    await circuit.close()
    await r.aclose()
