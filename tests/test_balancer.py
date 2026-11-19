import asyncio
import random
from collections import Counter

import pytest
import redis.asyncio as aioredis

from ddr.balancer import LoadBalancer

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def lb(redis_url):
    r = aioredis.from_url(redis_url)
    await r.flushdb()
    yield LoadBalancer(r, "ddr:test", "least_conn", explore=0.0, rng=random.Random(0))
    await r.aclose()


async def test_least_conn_prefers_idle_replica(lb):
    await lb.r.hset(lb.inflight_key, mapping={"a": 5, "b": 0, "c": 3})
    assert await lb.acquire(0, ["a", "b", "c"], "least_conn") == "b"
    assert (await lb.inflight())["b"] == 1


async def test_least_conn_spreads_concurrent_load(lb):
    picks = [await lb.acquire(0, ["a", "b", "c"], "least_conn") for _ in range(9)]
    assert Counter(picks) == {"a": 3, "b": 3, "c": 3}
    for p in picks:
        await lb.release(p)
    assert set((await lb.inflight()).values()) == {0}


async def test_round_robin_cycles_via_shared_cursor(lb):
    picks = [await lb.acquire(7, ["a", "b", "c"], "round_robin") for _ in range(6)]
    assert picks == ["a", "b", "c", "a", "b", "c"]
    other = LoadBalancer(lb.r, lb.ns, "round_robin")  # a second router shares the cursor
    assert await other.acquire(7, ["a", "b", "c"]) == "a"


async def test_latency_aware_prefers_fast_replica(lb):
    for _ in range(5):
        lb.observe("slow", 50.0, ok=True)
        lb.observe("fast", 5.0, ok=True)
    picks = Counter([await lb.acquire(0, ["slow", "fast"], "latency_aware") for _ in range(8)])
    # fast keeps winning until fast*(inflight+1) exceeds slow: 5*(n+1) < 50 -> ~9 in flight
    assert picks["fast"] == 8


async def test_latency_aware_balances_by_expected_completion(lb):
    lb.observe("slow", 20.0, ok=True)
    lb.observe("fast", 10.0, ok=True)
    picks = Counter([await lb.acquire(0, ["slow", "fast"], "latency_aware") for _ in range(30)])
    assert 18 <= picks["fast"] <= 22  # ~2:1 split, matching the speed ratio


async def test_errors_penalise_replica(lb):
    lb.observe("a", 5.0, ok=True)
    lb.observe("b", 5.0, ok=True)
    lb.observe("b", 5.0, ok=False)
    assert lb.weight("b", ["a", "b"]) > lb.weight("a", ["a", "b"]) * 3
    assert lb.stats["b"].errors == 1


async def test_p2c_only_picks_from_sample(lb):
    picks = Counter([await lb.acquire(0, list("abcdef"), "p2c") for _ in range(300)])
    assert len(picks) == 6


async def test_acquire_many_is_one_call_for_all_shards(lb, monkeypatch):
    calls = 0
    orig = lb._acquire

    async def counting(**kw):
        nonlocal calls
        calls += 1
        return await orig(**kw)

    monkeypatch.setattr(lb, "_acquire", counting)
    groups = [(s, [f"s{s}r0", f"s{s}r1"]) for s in range(8)]
    picks = await lb.acquire_many(groups, "round_robin")
    assert calls == 1
    assert [p.split("r")[0] for p in picks] == [f"s{s}" for s in range(8)]


async def test_unknown_strategy_and_empty_replicas(lb):
    with pytest.raises(ValueError):
        await lb.acquire(0, ["a", "b"], "nope")
    with pytest.raises(LookupError):
        await lb.acquire(0, [], "random")


async def test_concurrent_routers_do_not_herd(redis_url):
    """Two routers racing must still split load evenly: select+increment is atomic."""
    r = aioredis.from_url(redis_url)
    await r.flushdb()
    a = LoadBalancer(r, "ddr:herd", "least_conn")
    b = LoadBalancer(r, "ddr:herd", "least_conn")
    picks = await asyncio.gather(*[(a if i % 2 else b).acquire(0, ["x", "y"]) for i in range(40)])
    assert Counter(picks) == {"x": 20, "y": 20}
    await r.aclose()


async def test_release_nowait_coalesces_into_one_round_trip(lb, monkeypatch):
    nodes = [f"n{i}" for i in range(8)]
    for n in nodes:
        await lb.r.hset(lb.inflight_key, n, 3)
    pipelines = 0
    orig = lb.r.pipeline

    def counting(*a, **kw):
        nonlocal pipelines
        pipelines += 1
        return orig(*a, **kw)

    monkeypatch.setattr(lb.r, "pipeline", counting)
    for n in nodes:
        lb.release_nowait(n)
        lb.release_nowait(n)
    await lb.drain()
    assert pipelines == 1
    assert set((await lb.inflight()).values()) == {1}


async def test_release_nowait_retries_instead_of_leaking(lb, monkeypatch):
    await lb.r.hset(lb.inflight_key, "a", 1)
    orig = lb.r.pipeline
    failures = 1

    def flaky(*a, **kw):
        nonlocal failures
        if failures:
            failures -= 1
            raise ConnectionError("boom")
        return orig(*a, **kw)

    monkeypatch.setattr(lb.r, "pipeline", flaky)
    lb.release_nowait("a")
    await lb.drain()
    assert (await lb.inflight())["a"] == 0
