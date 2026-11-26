"""End-to-end: a real router and nodes surviving a Redis outage and a node crash."""

import time

import httpx
import pytest

from bench.cluster import LocalCluster
from ddr.corpus import make_queries


@pytest.fixture(scope="module")
def cluster(corpus_path):
    # This cluster runs its own Redis so the test can crash and restart it.
    c = LocalCluster(2, 2, cluster="failover", corpus=corpus_path, router_env={"LB_STRATEGY": "least_conn"})
    with c:
        yield c


@pytest.fixture(scope="module")
def queries(corpus):
    return make_queries(corpus, 60, seed=71)


def search(c: LocalCluster, q: str) -> httpx.Response:
    return httpx.get(c.router_url + "/search", params={"q": q, "cache": "false"}, timeout=10)


def get(c: LocalCluster, path: str) -> dict:
    return httpx.get(c.router_url + path, timeout=5).json()


def wait_for(cond, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while not cond():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        time.sleep(0.1)


def complete(r: httpx.Response) -> bool:
    return r.status_code == 200 and not r.json()["partial"]


def test_survives_redis_outage_and_node_crash(cluster, queries):
    assert all(complete(search(cluster, q)) for q in queries[:5])

    cluster.kill_redis()
    during = [search(cluster, q) for q in queries[5:20]]
    assert all(complete(r) for r in during), [r.status_code for r in during]
    assert all(r.json()["degraded"] for r in during)
    health = get(cluster, "/health")
    assert health["redis"] == "down" and health["status"] == "degraded"

    # A node dies while Redis is down: the registry can't report it, so the router
    # has to notice by itself. Queries must still complete via the other replica.
    cluster.kill_node("s0r0")
    after_crash = [search(cluster, q) for q in queries[20:35]]
    assert all(complete(r) for r in after_crash), [r.status_code for r in after_crash]
    assert "s0r0" in get(cluster, "/metrics")["ejected"]

    cluster.start_redis()  # comes back empty
    wait_for(lambda: get(cluster, "/health")["redis"] == "up")
    back = [search(cluster, q) for q in queries[35:45]]
    assert all(complete(r) and not r.json()["degraded"] for r in back)
    redis_status = get(cluster, "/metrics")["redis"]
    assert redis_status["outages"] == 1 and redis_status["downtime_s"] > 0

    # Live nodes re-register; the dead one leaves the topology after the hold window.
    wait_for(lambda: [n["node_id"] for n in get(cluster, "/topology")["shards"]["0"]] == ["s0r1"])
    assert [n["node_id"] for n in get(cluster, "/topology")["shards"]["1"]] == ["s1r0", "s1r1"]

    # The crashed node restarts, re-registers, and gets traffic again.
    cluster.start_node("s0r0")
    wait_for(lambda: len(get(cluster, "/topology")["shards"]["0"]) == 2, timeout=60)
    served = {n for q in queries[45:60] for n in search(cluster, q).json()["nodes"]}
    assert "s0r0" in served


def test_without_failover_queries_fail_while_redis_is_down(cluster, queries):
    httpx.put(cluster.router_url + "/admin/config", json={"redis_failover": False}).raise_for_status()
    try:
        cluster.kill_redis()
        assert {search(cluster, q).status_code for q in queries[:5]} == {500}
        assert get(cluster, "/health")["redis"] == "down"
        cluster.start_redis()
        wait_for(lambda: complete(search(cluster, queries[5])))  # recovers once nodes re-register
    finally:
        httpx.put(cluster.router_url + "/admin/config", json={"redis_failover": True}).raise_for_status()
