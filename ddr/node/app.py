"""Shard node: a FastAPI service that owns one shard of the inverted index.

Environment
-----------
NODE_ID         unique id (default: ``shard<SHARD_ID>-<hostname>``)
SHARD_ID        which shard this node serves (0-based)
NUM_SHARDS      total shards in the cluster (1 = centralized, full index)
CLUSTER         registry namespace, e.g. ``distributed`` / ``centralized``
ADVERTISE_URL   URL the router should use to reach this node
ZONE            free-form locality label (reported in topology)
REDIS_URL       registry; if unset the node runs standalone (no registration)
CORPUS_PATH     corpus directory (built on first start if missing)
CPU_SLOWDOWN    >1 simulates slower hardware (multiplies compute time)
NET_*           initial network conditions, see :mod:`ddr.netsim`
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket
import time

import orjson
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field

from ..corpus import load_or_build
from ..index import GlobalStats, ShardIndex
from ..netsim import NetConditions, NetworkSimulator
from ..registry import HEARTBEAT_INTERVAL, NodeInfo, Registry

log = logging.getLogger("ddr.node")


class SearchRequest(BaseModel):
    q: str
    k: int = Field(10, ge=1, le=1000)
    stats: dict | None = None  # GlobalStats JSON for dfs mode


class TermStatsRequest(BaseModel):
    q: str


class NetemRequest(BaseModel):
    profile: str | None = None
    delay_ms: float | None = None
    jitter_ms: float | None = None
    loss: float | None = None
    rto_ms: float | None = None
    bandwidth_mbps: float | None = None
    fail_rate: float | None = None
    cpu_slowdown: float | None = None


class NodeState:
    def __init__(self) -> None:
        self.shard_id = int(os.getenv("SHARD_ID", "0"))
        self.num_shards = int(os.getenv("NUM_SHARDS", "1"))
        self.cluster = os.getenv("CLUSTER", "distributed")
        self.node_id = os.getenv("NODE_ID") or f"shard{self.shard_id}-{socket.gethostname()}"
        self.url = os.getenv("ADVERTISE_URL", f"http://{socket.gethostname()}:8000")
        self.zone = os.getenv("ZONE", "default")
        self.cpu_slowdown = float(os.getenv("CPU_SLOWDOWN", "1.0"))
        self.net = NetworkSimulator(NetConditions.from_env())
        self.index: ShardIndex | None = None
        self.registry: Registry | None = None
        self.inflight = 0
        self.served = 0
        self.ewma_service_ms = 0.0
        self.started = time.time()

    def load_report(self) -> dict:
        return {"inflight": self.inflight, "served": self.served,
                "ewma_service_ms": round(self.ewma_service_ms, 3), "cpu_slowdown": self.cpu_slowdown}


S = NodeState()


async def _heartbeat_loop() -> None:
    """Register and heartbeat every interval. Search keeps working without Redis, and
    re-registering on every beat refills the registry as soon as Redis is back."""
    healthy = True
    while True:
        try:
            await S.registry.register(NodeInfo(S.node_id, S.url, S.shard_id, S.num_shards, S.zone))
            await S.registry.heartbeat(S.node_id, S.load_report())
            if not healthy:
                log.warning("Redis reachable again: node %s re-registered", S.node_id)
                healthy = True
        except Exception as e:
            if healthy:  # log once per outage, not every second
                log.warning("heartbeat failed (%s); retrying every %.0fs", e, HEARTBEAT_INTERVAL)
                healthy = False
        await asyncio.sleep(HEARTBEAT_INTERVAL)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    t0 = time.perf_counter()
    corpus = load_or_build(os.getenv("CORPUS_PATH", "data/corpus"))
    S.index = ShardIndex(corpus, S.shard_id, S.num_shards)
    log.warning("node %s: shard %d/%d, %d docs, %d postings, built in %.1fs",
                S.node_id, S.shard_id, S.num_shards, S.index.num_docs, S.index.num_postings,
                time.perf_counter() - t0)
    hb = None
    r = None
    if os.getenv("REDIS_URL"):
        r = aioredis.from_url(os.environ["REDIS_URL"], socket_connect_timeout=0.5, socket_timeout=1.0)
        S.registry = Registry(r, S.cluster)
        hb = asyncio.create_task(_heartbeat_loop())
    yield
    if hb:
        hb.cancel()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(S.registry.deregister(S.node_id), 1.0)
        await r.aclose()


app = FastAPI(title="DDR shard node", lifespan=lifespan)


def _json(data: dict) -> tuple[bytes, Response]:
    body = orjson.dumps(data)
    return body, Response(content=body, media_type="application/json")


async def _serve(fn) -> Response:
    """Wrap a handler with simulated network legs, fault injection and load accounting."""
    await S.net.leg()                       # request travels router -> node
    if S.net.should_fail():
        raise HTTPException(503, "injected failure")
    S.inflight += 1
    try:
        t0 = time.perf_counter()
        data = fn()
        compute = time.perf_counter() - t0
        if S.cpu_slowdown > 1.0:            # slower hardware: hold the CPU longer
            time.sleep(compute * (S.cpu_slowdown - 1.0))
        ms = (time.perf_counter() - t0) * 1e3
        S.ewma_service_ms = 0.2 * ms + 0.8 * S.ewma_service_ms
        S.served += 1
    finally:
        S.inflight -= 1
    body, resp = _json(data)
    await S.net.leg(len(body))              # response travels node -> router
    return resp


@app.post("/search")
async def search(req: SearchRequest) -> Response:
    def run() -> dict:
        tids = S.index.parse(req.q)
        stats = GlobalStats.from_json(req.stats) if req.stats else None
        return {"node_id": S.node_id, "shard": S.shard_id, "results": S.index.search(tids, req.k, stats)}
    return await _serve(run)


@app.post("/termstats")
async def termstats(req: TermStatsRequest) -> Response:
    return await _serve(lambda: S.index.term_stats(S.index.parse(req.q)))


@app.get("/health")
async def health() -> dict:
    return {"status": "ok" if S.index else "starting", "node_id": S.node_id}


@app.get("/info")
async def info() -> dict:
    return {"node_id": S.node_id, "cluster": S.cluster, "shard_id": S.shard_id, "num_shards": S.num_shards,
            "zone": S.zone, "url": S.url, "num_docs": S.index.num_docs if S.index else 0,
            "num_postings": S.index.num_postings if S.index else 0, "uptime_s": round(time.time() - S.started, 1),
            "net": S.net.cond.to_dict(), **S.load_report()}


@app.get("/admin/netem")
async def get_netem() -> dict:
    return {**S.net.cond.to_dict(), "cpu_slowdown": S.cpu_slowdown}


@app.put("/admin/netem")
async def set_netem(req: NetemRequest) -> dict:
    d = req.model_dump(exclude_none=True)
    if "cpu_slowdown" in d:
        S.cpu_slowdown = max(1.0, float(d.pop("cpu_slowdown")))
    try:
        S.net.cond.update(d)
    except KeyError as e:
        raise HTTPException(400, f"unknown profile {e}")
    return await get_netem()
