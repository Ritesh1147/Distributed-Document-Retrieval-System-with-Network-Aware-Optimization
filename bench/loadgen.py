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

For failure experiments, ``events`` runs actions (e.g. crash Redis) at set
times during the measurement, and ``timeline_bucket_s`` adds a per-interval
breakdown of outcomes and latency to the result.

    python -m bench.loadgen http://127.0.0.1:8080 --concurrency 32 --duration 10
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
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


def window_stats(records: list[tuple], t0: float, t1: float) -> dict:
    """Outcome rates and latency for requests *scheduled* in ``[t0, t1)`` seconds
    after the start of measurement (records come from ``run_load(..., timeline_bucket_s=...)``)."""
    sel = [r for r in records if t0 <= r[0] < t1]
    n = len(sel)
    count = {k: sum(1 for r in sel if r[1] == k) for k in ("ok", "partial", "error", "shed")}
    lat = [r[2] for r in sel if r[1] == "ok"]
    p = percentiles(lat)
    return {"requests": n, "success_rate": round(count["ok"] / n, 5) if n else None,
            "error_rate": round(count["error"] / n, 5) if n else None,
            "partial_rate": round(count["partial"] / n, 5) if n else None,
            "degraded_share": round(sum(1 for r in sel if r[3]) / n, 5) if n else None,
            "p50": p["p50"], "p99": p["p99"]}


def _timeline(records: list[tuple], bucket_s: float, duration: float) -> list[dict]:
    rows = []
    for i in range(int(math.ceil(duration / bucket_s - 1e-9))):
        rows.append({"t": round(i * bucket_s, 3), **window_stats(records, i * bucket_s, (i + 1) * bucket_s)})
    return rows


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
    events: list[tuple[float, str, Callable[[], None]]] | None = None,
    timeline_bucket_s: float | None = None,
) -> dict:
    """Run one load test and summarise it.

    ``events``: ``(seconds after measurement starts, label, action)``; each action
    runs in a worker thread so a slow one can't stall request sending.
    ``timeline_bucket_s``: also return ``timeline`` (per-interval stats),
    ``events`` (when each action actually started) and the raw ``records``.
    """
    if (concurrency is None) == (rate is None and rate_fn is None):
        raise ValueError("specify exactly one of concurrency or rate/rate_fn")
    pick = QueryPicker(queries, zipf_s, seed)
    params = dict(params or {})
    lat: list[float] = []
    stats = {"ok": 0, "errors": 0, "partial": 0, "cached": 0, "dropped": 0, "shed": 0}
    # (seconds after measurement start, outcome, latency ms or None, served degraded)
    records: list[tuple] | None = [] if timeline_bucket_s else None
    event_log: list[dict] = []
    connector = aiohttp.TCPConnector(limit=0, keepalive_timeout=60)
    async with aiohttp.ClientSession(connector=connector,
                                     timeout=aiohttp.ClientTimeout(total=timeout)) as client:
        t_start = time.perf_counter()
        t_measure = t_start + warmup
        t_end = t_measure + duration

        def note(sched: float, outcome: str, latency_ms: float | None = None, degraded: bool = False) -> None:
            if records is not None:
                records.append((sched - t_measure, outcome, latency_ms, degraded))

        async def one(sched: float) -> None:
            try:
                async with client.get(base_url + "/search", params={"q": pick(), **params}) as r:
                    raw = await r.read()
                    done = time.perf_counter()
                    if r.status == 503 and b"overloaded" in raw:
                        if sched >= t_measure:
                            stats["shed"] += 1
                            note(sched, "shed")
                        return
                    if r.status != 200:
                        raise RuntimeError(r.status)
                body = orjson.loads(raw)
                if sched >= t_measure and done <= t_end:
                    degraded = bool(body.get("degraded"))
                    if body.get("partial"):
                        stats["partial"] += 1      # answered, but incomplete: not goodput
                        note(sched, "partial", degraded=degraded)
                        return
                    lat.append((done - sched) * 1e3)
                    stats["ok"] += 1
                    stats["cached"] += bool(body.get("cached"))
                    note(sched, "ok", (done - sched) * 1e3, degraded)
            except Exception:
                if sched >= t_measure:
                    stats["errors"] += 1
                    note(sched, "error")

        async def fire_events() -> None:
            loop = asyncio.get_running_loop()
            for at, label, action in sorted(events or [], key=lambda e: e[0]):
                delay = t_measure + at - time.perf_counter()
                if delay > 0:
                    await asyncio.sleep(delay)
                event_log.append({"t": round(time.perf_counter() - t_measure, 3), "event": label})
                await loop.run_in_executor(None, action)

        events_task = asyncio.create_task(fire_events()) if events else None

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
        if events_task is not None:
            await events_task

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
    if records is not None:
        out["timeline"] = _timeline(records, timeline_bucket_s, duration)
        out["events"] = event_log
        out["records"] = records
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
