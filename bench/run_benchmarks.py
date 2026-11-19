"""Benchmark suite: centralized vs distributed retrieval under varying load and network conditions.

Experiments
-----------
scaling     throughput & latency as nodes increase (sharding and replication)
network     centralized vs distributed across simulated network profiles,
            with and without adaptive hedging
balancing   load-balancing strategies on heterogeneous replicas
            (slow hardware + a far-away zone) under steady and bursty load
loadcurve   open-loop latency vs offered load (where each architecture saturates)
cache       Redis result cache under uniform vs Zipf-skewed query popularity
quality     query-then-fetch (local stats) vs DFS (global stats): ranking
            agreement with a centralized index, and its latency cost

    python -m bench.run_benchmarks                  # everything (~20 min)
    python -m bench.run_benchmarks -e scaling -e network --duration 5
    python -m bench.run_benchmarks --quick          # smoke run (~4 min)

Results go to ``results/results.json``; run ``python -m bench.plot`` for charts
and the markdown summary.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import time

import httpx
import numpy as np

from ddr.corpus import load, make_queries
from ddr.index import GlobalStats, ShardIndex, merge_topk

from .cluster import ROOT, LocalCluster
from .loadgen import run_load

RESULTS = os.path.join(ROOT, "results", "results.json")


class Bench:
    def __init__(self, duration: float, quick: bool):
        self.duration = duration
        self.quick = quick
        self.corpus = load(os.path.join(ROOT, "data", "corpus"))
        self.queries = make_queries(self.corpus, 5000)
        self.results: dict = _load_results()

    # -- helpers ---------------------------------------------------------------
    def load(self, url: str, **kw) -> dict:
        kw.setdefault("duration", self.duration)
        kw.setdefault("warmup", 1.5)
        return asyncio.run(run_load(url, self.queries, **kw))

    def prime(self, c: LocalCluster) -> LocalCluster:
        """Unmeasured load right after start-up: JIT-free Python still has cold
        caches, lazily-imported paths and fresh connection pools."""
        asyncio.run(run_load(c.router_url, self.queries, duration=3, concurrency=8, warmup=0,
                             params={"cache": "false"}))
        return c

    @staticmethod
    def configure(c: LocalCluster, **cfg) -> None:
        httpx.put(c.router_url + "/admin/config", json={"reset_stats": True, **cfg}).raise_for_status()

    @staticmethod
    def netem(c: LocalCluster, conditions: dict, nodes: list[str] | None = None) -> None:
        httpx.put(c.router_url + "/admin/netem", json={"conditions": conditions, "nodes": nodes},
                  timeout=10).raise_for_status()

    @staticmethod
    def router_metrics(c: LocalCluster) -> dict:
        return httpx.get(c.router_url + "/metrics").json()

    def save(self, name: str, rows: list[dict], **meta) -> None:
        self.results[name] = {"rows": rows, **meta, "duration_s": self.duration,
                              "finished": time.strftime("%Y-%m-%d %H:%M:%S")}
        self.results["_env"] = {"cpus": os.cpu_count(), "python": platform.python_version(),
                                "platform": platform.platform(), "corpus_docs": self.corpus.num_docs,
                                "corpus_tokens": int(len(self.corpus.tokens))}
        os.makedirs(os.path.dirname(RESULTS), exist_ok=True)
        with open(RESULTS, "w") as f:
            json.dump(self.results, f, indent=2)

    @staticmethod
    def log(*a) -> None:
        print(time.strftime("%H:%M:%S"), *a, flush=True)

    # -- experiments -----------------------------------------------------------
    def scaling(self) -> None:
        topologies = [(1, 1), (2, 1), (4, 1), (8, 1), (2, 2), (4, 2)]
        concurrencies = [1, 8, 32, 64] if not self.quick else [8, 32]
        rows = []
        for shards, replicas in topologies:
            arch = "centralized" if shards == 1 and replicas == 1 else "distributed"
            with LocalCluster(shards, replicas, cluster="scale", node_env={"NET_PROFILE": "datacenter"},
                              router_env={"LB_STRATEGY": "least_conn"}) as c:
                self.prime(c)
                for conc in concurrencies:
                    r = self.load(c.router_url, concurrency=conc, params={"cache": "false"})
                    rows.append({"arch": arch, "shards": shards, "replicas": replicas,
                                 "nodes": shards * replicas, **r})
                    self.log(f"scaling {shards}x{replicas} c={conc}: {r['throughput_rps']} rps "
                             f"p50={r['p50']} p99={r['p99']}")
        self.save("scaling", rows, network="datacenter", strategy="least_conn")

    def network(self) -> None:
        # Open-loop Poisson load at ~50% of centralized capacity, so latency reflects
        # service + network time rather than saturation queueing. Hedging is compared
        # against the SAME 4x2 topology without hedging, so replica count is not a
        # confounder.
        profiles = ["ideal", "lan", "datacenter", "cross_zone", "wan", "lossy"]
        variants = [
            ("centralized", 1, 1, {"hedge_ms": 0}),
            ("distributed 4x1", 4, 1, {"hedge_ms": 0}),
            ("distributed 4x2", 4, 2, {"hedge_ms": 0}),
            ("distributed 4x2 + hedging", 4, 2, {"hedge_ms": -1}),
        ]
        rate = 100
        rows = []
        for name, shards, replicas, cfg in variants:
            with LocalCluster(shards, replicas, cluster="net", router_env={"LB_STRATEGY": "latency_aware"}) as c:
                self.prime(c)
                for prof in profiles:
                    self.netem(c, {"profile": prof})
                    self.configure(c, **cfg)
                    r = self.load(c.router_url, rate=rate, params={"cache": "false"})
                    m = self.router_metrics(c)
                    rows.append({"variant": name, "profile": prof, "shards": shards, "replicas": replicas,
                                 "hedges": m["router"]["hedges"], "hedge_ms": m["adaptive_hedge_ms"],
                                 "hedge_rate": round(m["router"]["hedges"] / max(m["router"]["requests"], 1)
                                                     / shards, 4), **r})
                    self.log(f"network {name} {prof}: {r['throughput_rps']} rps p50={r['p50']} "
                             f"p99={r['p99']} hedges={m['router']['hedges']} trigger={m['adaptive_hedge_ms']}ms")
        self.save("network", rows, offered_rps=rate)

    def balancing(self) -> None:
        # 2 shards x 3 replicas. Per shard: r0 healthy, r1 on 3x slower hardware,
        # r2 healthy hardware but in a far zone (cross-zone link).
        per_node = {}
        for s in range(2):
            per_node[f"s{s}r1"] = {"CPU_SLOWDOWN": 3}
            per_node[f"s{s}r2"] = {"NET_PROFILE": "cross_zone"}
        workloads = {
            "steady_closed": dict(concurrency=24),
            "poisson_200rps": dict(rate=200),
            "bursty_80_400rps": dict(rate_fn=lambda t: 400 if int(t / 2) % 2 else 80),
        }
        rows = []
        with LocalCluster(2, 3, cluster="lb", node_env={"NET_PROFILE": "datacenter"},
                          per_node_env=per_node) as c:
            self.prime(c)
            for wl, kw in workloads.items():
                for strat in ("random", "round_robin", "least_conn", "p2c", "latency_aware"):
                    self.configure(c, strategy=strat, hedge_ms=0)
                    r = self.load(c.router_url, params={"cache": "false"}, **kw)
                    m = self.router_metrics(c)
                    share = {n: v["requests"] for n, v in m["nodes"].items()}
                    tot = max(sum(share.values()), 1)
                    rows.append({"workload": wl, "strategy": strat, **r,
                                 "share_healthy": round(sum(v for n, v in share.items() if n.endswith("r0")) / tot, 3),
                                 "share_slow_cpu": round(sum(v for n, v in share.items() if n.endswith("r1")) / tot, 3),
                                 "share_far_zone": round(sum(v for n, v in share.items() if n.endswith("r2")) / tot, 3)})
                    self.log(f"balancing {wl} {strat}: {r['throughput_rps']} rps p50={r['p50']} p99={r['p99']} "
                             f"share(h/slow/far)={rows[-1]['share_healthy']}/{rows[-1]['share_slow_cpu']}/"
                             f"{rows[-1]['share_far_zone']}")
        self.save("balancing", rows, topology="2 shards x 3 replicas",
                  replicas={"r0": "healthy", "r1": "3x slower CPU", "r2": "cross-zone network (5ms +/-2ms, 0.1% loss)"})

    def loadcurve(self) -> None:
        rates = [50, 100, 150, 200, 300, 400, 500, 600] if not self.quick else [100, 300, 500]
        variants = [("centralized", 1, {}), ("distributed", 4, {}),
                    ("distributed + admission control", 4, {"MAX_INFLIGHT": 32})]
        rows = []
        for name, shards, env in variants:
            with LocalCluster(shards, 1, cluster="curve", node_env={"NET_PROFILE": "datacenter"},
                              router_env=env) as c:
                self.prime(c)
                for rate in rates:
                    r = self.load(c.router_url, rate=rate, params={"cache": "false"}, timeout=5)
                    rows.append({"arch": name, "shards": shards, **r})
                    self.log(f"loadcurve {name} {rate} rps: goodput {r['throughput_rps']} p50={r['p50']} "
                             f"p99={r['p99']} shed={r['shed_rate']} partial={r['partial_rate']} err={r['error_rate']}")
        self.save("loadcurve", rows, network="datacenter", latency_note="latency of complete answers only")

    def cache(self) -> None:
        rows = []
        with LocalCluster(4, 1, cluster="cache", node_env={"NET_PROFILE": "datacenter"},
                          router_env={"CACHE_TTL_S": 300}) as c:
            self.prime(c)
            for dist, s in (("uniform", 0.0), ("zipf_1.1", 1.1)):
                for cache in (False, True):
                    httpx.post(c.router_url + "/admin/cache/flush").raise_for_status()
                    r = self.load(c.router_url, concurrency=32, zipf_s=s,
                                  params={"cache": str(cache).lower()})
                    rows.append({"popularity": dist, "cache": cache, **r})
                    self.log(f"cache {dist} cache={cache}: {r['throughput_rps']} rps p50={r['p50']} "
                             f"hit={r['cache_hit_rate']}")
        self.save("cache", rows, ttl_s=300, query_pool=len(self.queries))

    def quality(self) -> None:
        qs = make_queries(self.corpus, 500, seed=99)
        central = ShardIndex(self.corpus, 0, 1)
        truth = {q: [r["doc_id"] for r in central.search(central.parse(q), 10)] for q in qs}
        rows = []
        for shards in (2, 4, 8, 16):
            idx = [ShardIndex(self.corpus, i, shards) for i in range(shards)]
            ov_local, ov_dfs, exact_local, exact_dfs = [], [], 0, 0
            for q in qs:
                tids = central.parse(q)
                loc = [r["doc_id"] for r in merge_topk([s.search(tids, 10) for s in idx], 10)]
                gs = GlobalStats.merge([s.term_stats(tids) for s in idx])
                dfs = [r["doc_id"] for r in merge_topk([s.search(tids, 10, gs) for s in idx], 10)]
                t = truth[q]
                ov_local.append(len(set(loc) & set(t)) / max(len(t), 1))
                ov_dfs.append(len(set(dfs) & set(t)) / max(len(t), 1))
                exact_local += loc == t
                exact_dfs += dfs == t
            rows.append({"shards": shards, "overlap10_qtf": round(float(np.mean(ov_local)), 4),
                         "overlap10_dfs": round(float(np.mean(ov_dfs)), 4),
                         "exact_order_qtf": round(exact_local / len(qs), 4),
                         "exact_order_dfs": round(exact_dfs / len(qs), 4)})
            self.log(f"quality {shards} shards: {rows[-1]}")
            del idx
        lat = []
        with LocalCluster(4, 1, cluster="quality", node_env={"NET_PROFILE": "datacenter"}) as c:
            self.prime(c)
            for prof in ("datacenter", "wan"):
                self.netem(c, {"profile": prof})
                for mode in ("qtf", "dfs"):
                    self.configure(c)
                    r = self.load(c.router_url, concurrency=16, params={"cache": "false", "mode": mode})
                    lat.append({"profile": prof, "mode": mode, **r})
                    self.log(f"quality latency {prof} {mode}: p50={r['p50']} p99={r['p99']}")
        self.save("quality", rows, latency=lat, queries=len(qs))


EXPERIMENTS = ["scaling", "network", "balancing", "loadcurve", "cache", "quality"]


def _load_results() -> dict:
    if os.path.exists(RESULTS):
        with open(RESULTS) as f:
            return json.load(f)
    return {}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-e", "--experiment", action="append", choices=EXPERIMENTS)
    ap.add_argument("--duration", type=float, default=8.0, help="measured seconds per data point")
    ap.add_argument("--quick", action="store_true", help="short runs, fewer points")
    args = ap.parse_args()
    if not os.path.exists(os.path.join(ROOT, "data", "corpus", "vocab.txt")):
        raise SystemExit("corpus missing: run `python -m ddr.corpus build` first")
    b = Bench(3.0 if args.quick else args.duration, args.quick)
    for name in args.experiment or EXPERIMENTS:
        t0 = time.time()
        b.log(f"=== {name} ===")
        getattr(b, name)()
        b.log(f"=== {name} done in {time.time() - t0:.0f}s ===")
    print(f"results written to {RESULTS}")


if __name__ == "__main__":
    main()
