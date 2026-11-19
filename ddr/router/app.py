"""Query router: scatter-gather over shards with Redis-backed load balancing.

For each query the router:

1. checks the Redis result cache,
2. (``dfs`` mode only) fans out ``/termstats`` to every shard and merges global
   BM25 statistics so shard scores are exactly comparable,
3. fans out ``/search`` to one replica per shard, chosen by the configured
   load-balancing strategy (:mod:`ddr.balancer`),
4. protects tail latency with **hedged requests** (after ``hedge_ms`` without
   an answer, send the same request to a second replica and take whichever
   returns first) and **retries** on a different replica after errors. With
   ``hedge_ms = -1`` the hedge delay adapts to the network: it tracks the p95
   of recent shard round-trip times, so only the slowest ~5% of calls are
   duplicated whatever the link latency is,
5. merges per-shard top-k into a global top-k, flags ``partial`` results if a
   shard was unreachable, and writes complete results back to the cache.

Environment
-----------
REDIS_URL       required
CLUSTER         registry namespace to route over (default ``distributed``)
LB_STRATEGY     random | round_robin | least_conn | latency_aware | p2c
SEARCH_MODE     qtf (query-then-fetch, local stats) | dfs (global stats)
HEDGE_MS        0 disables hedging, -1 = adaptive (p95 of recent shard RTTs)
MAX_ATTEMPTS    per-shard attempts incl. hedges/retries (default 2)
SHARD_TIMEOUT_MS
CACHE_TTL_S     0 disables the result cache
MAX_INFLIGHT    admission control: shed queries (fast 503) beyond this many
                concurrent queries; 0 = unlimited
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import os
import time
from dataclasses import asdict, dataclass

import collections

import aiohttp
import numpy as np
import orjson
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query, Response
from pydantic import BaseModel

from ..balancer import STRATEGIES, LoadBalancer
from ..index import GlobalStats, merge_topk
from ..registry import Registry, Topology

log = logging.getLogger("ddr.router")

MODES = ("qtf", "dfs")


@dataclass
class RouterConfig:
    strategy: str = os.getenv("LB_STRATEGY", "latency_aware")
    mode: str = os.getenv("SEARCH_MODE", "qtf")
    hedge_ms: float = float(os.getenv("HEDGE_MS", "0"))
    max_attempts: int = int(os.getenv("MAX_ATTEMPTS", "2"))
    shard_timeout_ms: float = float(os.getenv("SHARD_TIMEOUT_MS", "2000"))
    cache_ttl_s: int = int(os.getenv("CACHE_TTL_S", "0"))
    max_inflight: int = int(os.getenv("MAX_INFLIGHT", "0"))


class ConfigUpdate(BaseModel):
    strategy: str | None = None
    mode: str | None = None
    hedge_ms: float | None = None
    max_attempts: int | None = None
    shard_timeout_ms: float | None = None
    cache_ttl_s: int | None = None
    max_inflight: int | None = None
    reset_stats: bool = False


class NetemBroadcast(BaseModel):
    conditions: dict
    nodes: list[str] | None = None   # None = every alive node


class Metrics:
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.requests = self.cache_hits = self.partial = self.hedges = self.retries = self.shard_failures = 0
        self.shed = 0

    def to_dict(self) -> dict:
        return dict(vars(self))


class ShardUnavailable(Exception):
    pass


class AdaptiveHedge:
    """Rolling p95 of successful shard RTTs, used as the hedge trigger."""

    def __init__(self, window: int = 1000, quantile: float = 95.0, every: int = 50, floor_ms: float = 1.0):
        self.samples: collections.deque[float] = collections.deque(maxlen=window)
        self.quantile = quantile
        self.every = every
        self.floor_ms = floor_ms
        self.threshold_ms = 0.0   # 0 until warmed up -> no hedging
        self._n = 0

    def observe(self, ms: float) -> None:
        self.samples.append(ms)
        self._n += 1
        if self._n % self.every == 0 and len(self.samples) >= 100:
            self.threshold_ms = max(self.floor_ms, float(np.percentile(self.samples, self.quantile)))

    def reset(self) -> None:
        self.samples.clear()
        self.threshold_ms = 0.0
        self._n = 0


class Router:
    def __init__(self, r: aioredis.Redis, cluster: str, cfg: RouterConfig):
        self.r = r
        self.cfg = cfg
        self.registry = Registry(r, cluster)
        self.lb = LoadBalancer(r, self.registry.ns, cfg.strategy)
        self.topo = Topology()
        self.metrics = Metrics()
        self.hedge = AdaptiveHedge()
        self.client: aiohttp.ClientSession | None = None  # created inside the event loop
        self.inflight = 0  # queries currently being served by this router process

    async def start(self) -> None:
        self.client = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=0, limit_per_host=0, keepalive_timeout=60),
            json_serialize=lambda o: orjson.dumps(o).decode(),
        )
        self.topo = await self.registry.topology()

    async def refresh_loop(self, interval: float = 0.5) -> None:
        while True:
            try:
                self.topo = await self.registry.topology()
            except Exception as e:
                log.warning("topology refresh failed: %s", e)
            await asyncio.sleep(interval)

    # -- one request to one node ------------------------------------------------
    async def _attempt(self, node_id: str, path: str, payload: dict) -> dict:
        url = self.topo.nodes[node_id].url + path
        t0 = time.perf_counter()
        try:
            timeout = aiohttp.ClientTimeout(total=self.cfg.shard_timeout_ms / 1e3)
            async with self.client.post(url, data=orjson.dumps(payload), timeout=timeout,
                                        headers={"content-type": "application/json"}) as resp:
                resp.raise_for_status()
                data = orjson.loads(await resp.read())
            ms = (time.perf_counter() - t0) * 1e3
            self.lb.observe(node_id, ms, ok=True)
            self.hedge.observe(ms)
            return data
        except asyncio.CancelledError:
            # Lost a hedge race: says nothing about the node's health, but its elapsed
            # time is a lower bound on a real RTT. Recording it keeps the p95 trigger
            # from drifting down to only-the-winners (survivorship bias -> hedge storm).
            self.hedge.observe((time.perf_counter() - t0) * 1e3)
            raise
        except Exception:
            self.lb.observe(node_id, (time.perf_counter() - t0) * 1e3, ok=False)
            raise
        finally:
            self.lb.release_nowait(node_id)  # batched; safe even if this task was cancelled

    # -- one shard: replica choice, hedging, retries ------------------------------
    async def shard_call(self, shard: int, first: str, path: str, payload: dict, strategy: str,
                         hedge_ms: float) -> dict:
        """Call one shard starting with the already-acquired replica ``first``."""
        replicas = list(self.topo.shards.get(shard, []))
        tried: set[str] = {first}
        pending: set[asyncio.Task] = {asyncio.create_task(self._attempt(first, path, payload))}
        budget = max(1, self.cfg.max_attempts)
        if hedge_ms < 0:
            hedge_ms = self.hedge.threshold_ms

        async def launch() -> None:
            node = await self.lb.acquire(shard, [n for n in replicas if n not in tried], strategy)
            tried.add(node)
            pending.add(asyncio.create_task(self._attempt(node, path, payload)))

        try:
            while pending:
                can_spawn = len(tried) < min(budget, len(replicas))
                timeout = hedge_ms / 1e3 if (hedge_ms > 0 and can_spawn) else None
                done, _ = await asyncio.wait(pending, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
                pending -= done
                failed = False
                for t in done:
                    if t.exception() is None:
                        return t.result()
                    failed = True
                if not done and can_spawn:           # hedge: primary is slow
                    self.metrics.hedges += 1
                    await launch()
                elif failed and not pending and can_spawn:  # retry elsewhere
                    self.metrics.retries += 1
                    await launch()
            raise ShardUnavailable(shard)
        finally:
            for t in pending:
                t.cancel()

    async def fan_out(self, path: str, payload: dict, strategy: str, hedge_ms: float) -> tuple[list[dict], list[int]]:
        topo = self.topo
        shards = [s for s in range(topo.num_shards) if topo.shards.get(s)]
        failed = topo.missing_shards
        firsts = await self.lb.acquire_many([(s, topo.shards[s]) for s in shards], strategy)
        outs = await asyncio.gather(*(self.shard_call(s, n, path, payload, strategy, hedge_ms)
                                      for s, n in zip(shards, firsts)), return_exceptions=True)
        ok = []
        for s, o in zip(shards, outs):
            if isinstance(o, BaseException):
                failed.append(s)
            else:
                ok.append(o)
        self.metrics.shard_failures += len(failed)
        return ok, failed

    # -- full query ----------------------------------------------------------
    async def search(self, q: str, k: int, strategy: str, mode: str, hedge_ms: float, use_cache: bool) -> dict:
        t0 = time.perf_counter()
        self.metrics.requests += 1
        if self.topo.num_shards == 0:
            raise HTTPException(503, "no nodes registered")
        cache_key = None
        if use_cache and self.cfg.cache_ttl_s > 0:
            norm = " ".join(sorted(set(q.lower().split())))
            cache_key = f"{self.registry.ns}:cache:{mode}:{k}:{hashlib.sha1(norm.encode()).hexdigest()}"
            hit = await self.r.get(cache_key)
            if hit is not None:
                self.metrics.cache_hits += 1
                out = orjson.loads(hit)
                out.update(cached=True, took_ms=round((time.perf_counter() - t0) * 1e3, 3))
                return out

        failed_shards: list[int] = []
        payload: dict = {"q": q, "k": k}
        if mode == "dfs":
            parts, failed_shards = await self.fan_out("/termstats", {"q": q}, strategy, hedge_ms)
            if parts:
                payload["stats"] = GlobalStats.merge(parts).to_json()
        replies, failed = await self.fan_out("/search", payload, strategy, hedge_ms)
        failed_shards = sorted(set(failed_shards) | set(failed))
        results = merge_topk([rep["results"] for rep in replies], k)
        out = {
            "query": q, "k": k, "mode": mode, "strategy": strategy,
            "num_shards": self.topo.num_shards, "failed_shards": failed_shards,
            "partial": bool(failed_shards), "cached": False,
            "nodes": sorted(rep["node_id"] for rep in replies), "results": results,
        }
        if out["partial"]:
            self.metrics.partial += 1
        elif cache_key:
            await self.r.set(cache_key, orjson.dumps(out), ex=self.cfg.cache_ttl_s)
        out["took_ms"] = round((time.perf_counter() - t0) * 1e3, 3)
        return out


R: Router | None = None


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    global R
    # Blocking pool: under bursts callers wait briefly for a connection instead of
    # failing with MaxConnectionsError.
    pool = aioredis.BlockingConnectionPool.from_url(
        os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        max_connections=int(os.getenv("REDIS_POOL_SIZE", "64")), timeout=5)
    r = aioredis.Redis(connection_pool=pool)
    R = Router(r, os.getenv("CLUSTER", "distributed"), RouterConfig())
    await R.start()
    task = asyncio.create_task(R.refresh_loop())
    yield
    task.cancel()
    await R.lb.drain()
    await R.client.close()
    await r.aclose()


app = FastAPI(title="DDR router", lifespan=lifespan)


@app.get("/search")
async def search(
    q: str = Query(..., min_length=1),
    k: int = Query(10, ge=1, le=100),
    strategy: str | None = None,
    mode: str | None = None,
    hedge_ms: float | None = None,
    cache: bool = True,
) -> Response:
    strategy = strategy or R.cfg.strategy
    mode = mode or R.cfg.mode
    if strategy not in STRATEGIES:
        raise HTTPException(400, f"strategy must be one of {STRATEGIES}")
    if mode not in MODES:
        raise HTTPException(400, f"mode must be one of {MODES}")
    # Admission control: past capacity, queueing more work only makes every query
    # slower (and eventually time out). Rejecting the excess immediately keeps
    # goodput at capacity and latency bounded for the queries we do accept.
    if R.cfg.max_inflight and R.inflight >= R.cfg.max_inflight:
        R.metrics.shed += 1
        raise HTTPException(503, "overloaded, retry later", headers={"Retry-After": "1"})
    R.inflight += 1
    try:
        out = await R.search(q, k, strategy, mode, R.cfg.hedge_ms if hedge_ms is None else hedge_ms, cache)
    finally:
        R.inflight -= 1
    return Response(orjson.dumps(out), media_type="application/json")


@app.get("/health")
async def health() -> dict:
    missing = R.topo.missing_shards
    return {"status": "ok" if R.topo.num_shards and not missing else "degraded",
            "num_shards": R.topo.num_shards, "missing_shards": missing, "nodes": len(R.topo.nodes)}


@app.get("/topology")
async def topology() -> dict:
    return R.topo.to_dict()


@app.get("/metrics")
async def metrics() -> dict:
    return {"router": R.metrics.to_dict(), "config": asdict(R.cfg),
            "adaptive_hedge_ms": round(R.hedge.threshold_ms, 3),
            "nodes": {n: s.to_dict() for n, s in sorted(R.lb.stats.items())},
            "inflight": await R.lb.inflight()}


@app.put("/admin/config")
async def set_config(u: ConfigUpdate) -> dict:
    if u.strategy is not None and u.strategy not in STRATEGIES:
        raise HTTPException(400, f"strategy must be one of {STRATEGIES}")
    if u.mode is not None and u.mode not in MODES:
        raise HTTPException(400, f"mode must be one of {MODES}")
    for k, v in u.model_dump(exclude_none=True, exclude={"reset_stats"}).items():
        setattr(R.cfg, k, v)
    R.lb.strategy = R.cfg.strategy
    if u.reset_stats:
        R.metrics.reset()
        R.lb.stats.clear()
        R.hedge.reset()
    return asdict(R.cfg)


@app.post("/admin/cache/flush")
async def flush_cache() -> dict:
    n = 0
    async for key in R.r.scan_iter(f"{R.registry.ns}:cache:*", count=1000):
        n += await R.r.delete(key)
    return {"deleted": n}


@app.put("/admin/netem")
async def broadcast_netem(b: NetemBroadcast) -> dict:
    """Apply network conditions to some or all nodes (used by benchmarks)."""
    targets = b.nodes if b.nodes is not None else list(R.topo.nodes)
    unknown = [n for n in targets if n not in R.topo.nodes]
    if unknown:
        raise HTTPException(404, f"unknown nodes {unknown}")
    async def put(n: str) -> dict:
        async with R.client.put(R.topo.nodes[n].url + "/admin/netem", json=b.conditions) as resp:
            resp.raise_for_status()
            return await resp.json()
    return dict(zip(targets, await asyncio.gather(*(put(n) for n in targets))))
