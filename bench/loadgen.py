"""Async HTTP load generator.

Two arrival models:

* **closed loop** (``concurrency=N``): N virtual users, each issuing the next
  query as soon as the previous one returns -- measures max throughput.
* **open loop** (``rate=λ`` or ``rate_fn(t)``): Poisson arrivals independent of
  response times, latency measured from the *scheduled* send time so queueing
  is not hidden (avoids coordinated omission). ``rate_fn`` enables bursty /
  diurnal workloads.

Query popularity is uniform or Zipf(s) over a fixed query pool, which models
the head-heavy distribution of real search traffic.

    python -m bench.loadgen http://127.0.0.1:8080 --concurrency 32 --duration 10
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import time
from typing import Callable

import aiohttp
import numpy as np
import orjson


def percentiles(lat_ms: list[float]) -> dict:
    if not lat_ms:
        return {"p50": None, "p90": None, "p95": None, "p99": None, "max": None, "mean": None}
    a = np.asarray(lat_ms)
    p = np.percentile(a, [50, 90, 95, 99])
    return {"p50": round(float(p[0]), 3), "p90": round(float(p[1]), 3), "p95": round(float(p[2]), 3),
            "p99": round(float(p[3]), 3), "max": round(float(a.max()), 3), "mean": round(float(a.mean()), 3)}


class QueryPicker:
    def __init__(self, queries: list[str], zipf_s: float = 0.0, seed: int = 0):
        self.queries = queries
        self.rng = random.Random(seed)
        if zipf_s > 0:
            w = 1.0 / np.power(np.arange(1, len(queries) + 1), zipf_s)
            self.cum = np.cumsum(w / w.sum())
        else:
            self.cum = None

    def __call__(self) -> str:
        if self.cum is None:
            return self.rng.choice(self.queries)
        return self.queries[int(np.searchsorted(self.cum, self.rng.random()))]


async def run_load(
    base_url: str,
    queries: list[str],
    duration: float = 10.0,
    concurrency: int | None = None,
    rate: float | None = None,
    rate_fn: Callable[[float], float] | None = None,
    zipf_s: float = 0.0,
    params: dict | None = None,
    warmup: float = 1.0,
    timeout: float = 10.0,
    seed: int = 0,
    max_outstanding: int = 4000,
) -> dict:
    if (concurrency is None) == (rate is None and rate_fn is None):
        raise ValueError("specify exactly one of concurrency or rate/rate_fn")
    pick = QueryPicker(queries, zipf_s, seed)
    params = dict(params or {})
    lat: list[float] = []
    stats = {"ok": 0, "errors": 0, "partial": 0, "cached": 0, "dropped": 0, "shed": 0}
    connector = aiohttp.TCPConnector(limit=0, keepalive_timeout=60)
    async with aiohttp.ClientSession(connector=connector,
                                     timeout=aiohttp.ClientTimeout(total=timeout)) as client:
        t_start = time.perf_counter()
        t_measure = t_start + warmup
        t_end = t_measure + duration

        async def one(sched: float) -> None:
            try:
                async with client.get(base_url + "/search", params={"q": pick(), **params}) as r:
                    raw = await r.read()
                    done = time.perf_counter()
                    if r.status == 503 and b"overloaded" in raw:
                        if sched >= t_measure:
                            stats["shed"] += 1
                        return
                    if r.status != 200:
                        raise RuntimeError(r.status)
                body = orjson.loads(raw)
                if sched >= t_measure and done <= t_end:
                    if body.get("partial"):
                        stats["partial"] += 1      # answered, but incomplete: not goodput
                        return
                    lat.append((done - sched) * 1e3)
                    stats["ok"] += 1
                    stats["cached"] += bool(body.get("cached"))
            except Exception:
                if sched >= t_measure:
                    stats["errors"] += 1

        if concurrency is not None:
            async def user() -> None:
                while (now := time.perf_counter()) < t_end:
                    await one(now)
            await asyncio.gather(*(user() for _ in range(concurrency)))
        else:
            rng = random.Random(seed + 1)
            tasks: set[asyncio.Task] = set()
            t = t_start
            while t < t_end:
                lam = rate_fn(t - t_start) if rate_fn else rate
                t += rng.expovariate(max(lam, 1e-6))
                delay = t - time.perf_counter()
                if delay > 0:
                    await asyncio.sleep(delay)
                if len(tasks) >= max_outstanding:
                    stats["dropped"] += 1
                    continue
                task = asyncio.create_task(one(t))
                tasks.add(task)
                task.add_done_callback(tasks.discard)
            if tasks:
                await asyncio.wait(tasks, timeout=timeout)

    n = stats["ok"]
    attempted = n + stats["errors"] + stats["partial"] + stats["shed"]
    out = {"requests": n,
           "throughput_rps": round(n / duration, 2),   # goodput: complete answers only
           "error_rate": round(stats["errors"] / max(attempted, 1), 5),
           "partial_rate": round(stats["partial"] / max(attempted, 1), 5),
           "shed_rate": round(stats["shed"] / max(attempted, 1), 5),
           "cache_hit_rate": round(stats["cached"] / max(n, 1), 5),
           "dropped": stats["dropped"], **percentiles(lat)}
    if concurrency is not None:
        out["concurrency"] = concurrency
    else:
        out["offered_rps"] = rate
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("url")
    ap.add_argument("--duration", type=float, default=10)
    ap.add_argument("--concurrency", type=int)
    ap.add_argument("--rate", type=float)
    ap.add_argument("--zipf", type=float, default=0.0)
    ap.add_argument("--queries", type=int, default=5000)
    ap.add_argument("--corpus", default="data/corpus")
    ap.add_argument("--param", action="append", default=[], help="extra query param, e.g. strategy=p2c")
    args = ap.parse_args()
    if args.concurrency is None and args.rate is None:
        args.concurrency = 16
    from ddr.corpus import load, make_queries
    qs = make_queries(load(args.corpus), args.queries)
    params = dict(p.split("=", 1) for p in args.param)
    res = asyncio.run(run_load(args.url, qs, args.duration, args.concurrency, args.rate,
                               zipf_s=args.zipf, params=params))
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
