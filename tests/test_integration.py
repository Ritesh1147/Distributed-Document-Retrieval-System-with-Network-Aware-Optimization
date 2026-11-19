"""End-to-end: real router + node processes + Redis on a small corpus."""

import httpx
import pytest

from bench.cluster import LocalCluster
from ddr.corpus import make_queries
from ddr.index import ShardIndex


@pytest.fixture(scope="module")
def cluster(corpus_path, redis_url):
    c = LocalCluster(3, 2, cluster="itest", corpus=corpus_path, redis_url=redis_url,
                     router_env={"CACHE_TTL_S": 60, "LB_STRATEGY": "latency_aware"})
    with c:
        yield c


@pytest.fixture(scope="module")
def central_index(corpus):
    return ShardIndex(corpus, 0, 1)


def search(c, **params):
    r = httpx.get(c.router_url + "/search", params=params, timeout=10)
    r.raise_for_status()
    return r.json()


def netem(c, nodes, **conditions):
    httpx.put(c.router_url + "/admin/netem", json={"nodes": nodes, "conditions": conditions}).raise_for_status()


def test_topology(cluster):
    topo = httpx.get(cluster.router_url + "/topology").json()
    assert topo["num_shards"] == 3 and topo["missing_shards"] == []
    assert all(len(v) == 2 for v in topo["shards"].values())


def test_dfs_matches_centralized_index(cluster, corpus, central_index):
    for q in make_queries(corpus, 25, seed=21):
        got = search(cluster, q=q, k=10, mode="dfs", cache="false")
        want = central_index.search(central_index.parse(q), 10)
        assert [r["doc_id"] for r in got["results"]] == [r["doc_id"] for r in want]
        assert len(got["nodes"]) == 3 and not got["partial"]


@pytest.mark.parametrize("strategy", ["random", "round_robin", "least_conn", "latency_aware", "p2c"])
def test_every_strategy_serves_queries(cluster, corpus, strategy):
    q = make_queries(corpus, 1, seed=1)[0]
    assert search(cluster, q=q, strategy=strategy, cache="false")["results"]


def test_cache_hit(cluster, corpus):
    q = make_queries(corpus, 1, seed=99)[0]
    first = search(cluster, q=q)
    second = search(cluster, q=q)
    assert not first["cached"] and second["cached"]
    assert first["results"] == second["results"]


def test_failover_to_healthy_replica(cluster, corpus):
    netem(cluster, ["s0r0", "s1r0", "s2r0"], fail_rate=1.0)
    try:
        for q in make_queries(corpus, 10, seed=31):
            r = search(cluster, q=q, strategy="round_robin", cache="false")
            assert not r["partial"]
            assert all(n.endswith("r1") for n in r["nodes"])
    finally:
        netem(cluster, ["s0r0", "s1r0", "s2r0"], fail_rate=0.0)


def test_partial_results_when_whole_shard_fails(cluster, corpus):
    netem(cluster, ["s1r0", "s1r1"], fail_rate=1.0)
    try:
        r = search(cluster, q=make_queries(corpus, 1, seed=41)[0], cache="false")
        assert r["partial"] and r["failed_shards"] == [1]
        assert all(x["shard"] != 1 for x in r["results"])
    finally:
        netem(cluster, ["s1r0", "s1r1"], fail_rate=0.0)


def test_hedging_masks_slow_replica(cluster, corpus):
    netem(cluster, ["s0r0", "s1r0", "s2r0"], delay_ms=150)
    try:
        r = search(cluster, q=make_queries(corpus, 1, seed=51)[0], strategy="round_robin",
                   hedge_ms=20, cache="false")
        assert r["took_ms"] < 150
    finally:
        netem(cluster, ["s0r0", "s1r0", "s2r0"], delay_ms=0)


def test_bad_params(cluster):
    assert httpx.get(cluster.router_url + "/search", params={"q": "x", "strategy": "nope"}).status_code == 400
    assert httpx.get(cluster.router_url + "/search", params={"q": "x", "mode": "nope"}).status_code == 400


def test_admission_control_sheds_excess_load(cluster, corpus):
    import asyncio

    async def burst():
        async with httpx.AsyncClient(timeout=10) as client:
            qs = make_queries(corpus, 40, seed=61)
            return await asyncio.gather(*(client.get(cluster.router_url + "/search",
                                                     params={"q": q, "cache": "false"}) for q in qs))

    httpx.put(cluster.router_url + "/admin/config", json={"max_inflight": 2}).raise_for_status()
    try:
        codes = [r.status_code for r in asyncio.run(burst())]
    finally:
        httpx.put(cluster.router_url + "/admin/config", json={"max_inflight": 0}).raise_for_status()
    assert 503 in codes and 200 in codes
    assert set(codes) <= {200, 503}
