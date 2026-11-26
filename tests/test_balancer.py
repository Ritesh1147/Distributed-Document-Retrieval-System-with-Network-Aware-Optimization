import asyncio
import random
from collections import Counter

import pytest
import redis.asyncio as aioredis

from ddr.balancer import SLICE_TTL_S, LoadBalancer

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def lb(redis_url):
    r = aioredis.from_url(redis_url)
    await r.flushdb()
    yield LoadBalancer(r, "ddr:test", "least_conn", explore=0.0, rng=random.Random(0))
    await r.aclose()


async def test_least_conn_prefers_idle_replica(lb):
    await lb.r.hset(lb.slice_key, mapping={"a": 5, "b": 0, "c": 3})
    assert await lb.acquire(0, ["a", "b", "c"], "least_conn") == "b"
    assert (await lb.inflight())["b"] == 1


async def test_least_conn_spreads_concurrent_load(lb):
    picks = [await lb.acquire(0, ["a", "b", "c"], "least_conn") for _ in range(9)]
    assert Counter(picks) == {"a": 3, "b": 3, "c": 3}
    assert lb.local == {"a": 3, "b": 3, "c": 3}
    for p in picks:
        await lb.release(p)
    assert set((await lb.inflight()).values()) == {0}
    assert lb.local == {}


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


async def test_failing_replica_is_ejected(lb):
    for _ in range(3):
        lb.observe("bad", 1.0, ok=False)
    assert lb.ejector.is_ejected("bad")
    assert {await lb.acquire(0, ["bad", "good"]) for _ in range(5)} == {"good"}
    assert await lb.acquire(1, ["bad"]) == "bad"  # the only option: try it anyway
    lb.forget("bad")                              # e.g. the node re-registered
    assert not lb.ejector.is_ejected("bad") and "bad" not in lb.stats


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


async def _pair(redis_url: str, ns: str) -> tuple[LoadBalancer, LoadBalancer]:
    r = aioredis.from_url(redis_url)
    await r.flushdb()
    a = LoadBalancer(r, ns, "least_conn", router_id="A")
    b = LoadBalancer(r, ns, "least_conn", router_id="B")
    await a.publish()
    await b.publish()
    await a.publish()  # now each knows the other
    return a, b


async def test_routers_see_each_others_load(redis_url):
    a, b = await _pair(redis_url, "ddr:peers")
    assert a.peer_keys == [b.slice_key] and b.peer_keys == [a.slice_key]
    for _ in range(4):
        await a.acquire(0, ["x"])  # router A now has 4 requests in flight on x
    assert [await b.acquire(0, ["x", "y"]) for _ in range(4)] == ["y"] * 4
    assert await b.inflight() == {"x": 4, "y": 4}
    await a.r.aclose()


async def test_concurrent_routers_do_not_herd(redis_url):
    """Two routers racing must still split load evenly: select+increment is atomic."""
    a, b = await _pair(redis_url, "ddr:herd")
    picks = await asyncio.gather(*[(a if i % 2 else b).acquire(0, ["x", "y"]) for i in range(40)])
    assert Counter(picks) == {"x": 20, "y": 20}
    await a.r.aclose()


async def test_crashed_router_is_forgotten(lb):
    await lb.acquire(0, ["a"])
    assert 0 < await lb.r.ttl(lb.slice_key) <= SLICE_TTL_S  # expires unless re-published
    now = int((await lb.r.time())[0])
    await lb.r.zadd(lb.routers_key, {"ghost": now - 100})   # stopped publishing long ago
    await lb.r.hset(lb._slice_key("ghost"), "a", 50)
    await lb.publish()
    assert lb.peer_keys == []
    assert await lb.r.zscore(lb.routers_key, "ghost") is None
    assert await lb.acquire(0, ["a", "b"]) == "b"  # a's in-flight count is 1, not 51


async def test_release_nowait_coalesces_into_one_round_trip(lb, monkeypatch):
    nodes = [f"n{i}" for i in range(8)]
    for n in nodes:
        await lb.r.hset(lb.slice_key, n, 3)
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


async def test_publish_corrects_lost_updates(lb, monkeypatch):
    await lb.acquire(0, ["a"])
    orig = lb.r.pipeline

    def broken(*a, **kw):
        raise ConnectionError("boom")

    monkeypatch.setattr(lb.r, "pipeline", broken)
    lb.release_nowait("a")  # the Redis decrement is lost
    await lb.drain()
    monkeypatch.setattr(lb.r, "pipeline", orig)
    assert int(await lb.r.hget(lb.slice_key, "a")) == 1  # stale shared count
    await lb.publish()
    assert await lb.r.hget(lb.slice_key, "a") is None   # exact again: nothing in flight
    assert lb.local == {}
