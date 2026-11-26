"""Replica selection (load balancing) backed by Redis, with a local fallback.

Shared load view
----------------
Each router owns a *slice* of the cluster-wide in-flight counts: a Redis hash
``{ns}:inflight:{router_id}`` mapping node -> requests this router has in
flight. Selection runs inside a Lua script that sums every live router's slice,
picks a replica, and increments the caller's slice atomically, so concurrent
routers can't all pile onto the same "least loaded" node (the herd effect you
get when you read load, then act on it). One script call picks replicas for
*every* shard of a query, so routing costs one round trip per query.

The router also keeps its own counts locally, and those are exact. Every
refresh interval it re-publishes them over its slice and refreshes the slice's
TTL. Lost updates therefore can't accumulate: after a Redis outage or restart
the shared view is correct again within one publish, and a crashed router's
slice simply expires.

If Redis is unreachable (see :mod:`ddr.resilience`), the same strategies run
against the router's local counts, and replicas that keep failing are skipped.

Strategies
----------
``random``          uniform random replica.
``round_robin``     shared Redis cursor per shard (global RR across routers).
``least_conn``      fewest in-flight requests cluster-wide.
``latency_aware``   network-aware: minimises ``ewma_rtt * (inflight + 1)``,
                    i.e. expected completion time. The EWMA is the round-trip
                    time *observed by the router*, so it captures network
                    distance, jitter and loss as well as server load.
``p2c``             power-of-two-choices: sample two replicas, take the one
                    with the lower latency-aware score. Near-optimal balance
                    with O(1) state reads and less herding.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import socket
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field

import redis.asyncio as aioredis

from .resilience import OutlierDetector, RedisCircuit, RedisUnavailable

log = logging.getLogger("ddr.balancer")

STRATEGIES = ("random", "round_robin", "least_conn", "latency_aware", "p2c")

# A router's slice expires this long after its last publish (router crashed).
SLICE_TTL_S = 3

# Batched selection for a whole fan-out in ONE round trip.
# KEYS[1]            this router's slice (incremented)
# KEYS[2 .. n]       other live routers' slices (read only), n = ARGV[1]
# KEYS[n + 1 ..]     round-robin cursors
# ARGV[2]            TTL for this router's slice, in seconds
# ARGV[3 ..]         groups: kind, rr_key_index, count, id1, w1, ..., idk, wk
#   kind "rr"   -> advance the shared cursor KEYS[rr_key_index], pick modulo count
#   kind "pick" -> argmin w * (cluster-wide in-flight + 1)   (least_conn: all w = 1)
_ACQUIRE_LUA = """
local n_slices = tonumber(ARGV[1])
local function inflight(id)
  local total = 0
  for k = 1, n_slices do
    local v = tonumber(redis.call('HGET', KEYS[k], id) or '0')
    if v > 0 then total = total + v end
  end
  return total
end
local out = {}
local i = 3
while i <= #ARGV do
  local kind = ARGV[i]
  local kidx = tonumber(ARGV[i + 1])
  local n = tonumber(ARGV[i + 2])
  i = i + 3
  local chosen
  if kind == 'rr' then
    local c = redis.call('INCR', KEYS[kidx])
    chosen = ARGV[i + 2 * ((c - 1) % n)]
  elseif n == 1 then
    chosen = ARGV[i]
  else
    local best
    for j = 0, n - 1 do
      local id = ARGV[i + 2 * j]
      local score = tonumber(ARGV[i + 2 * j + 1]) * (inflight(id) + 1)
      if best == nil or score < best then chosen, best = id, score end
    end
  end
  redis.call('HINCRBY', KEYS[1], chosen, 1)
  out[#out + 1] = chosen
  i = i + 2 * n
end
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return out
"""

# Overwrite this router's slice with its exact counts and list the live routers.
# KEYS[1] this router's slice, KEYS[2] live-router index (sorted set, scored by
# Redis server time so router clocks don't need to agree).
# ARGV[1] router id, ARGV[2] TTL seconds, ARGV[3 ..] node, count pairs.
_PUBLISH_LUA = """
redis.call('DEL', KEYS[1])
for i = 3, #ARGV, 2 do
  redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
end
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
local now = tonumber(redis.call('TIME')[1])
redis.call('ZADD', KEYS[2], now, ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now - tonumber(ARGV[2]))
return redis.call('ZRANGE', KEYS[2], 0, -1)
"""


@dataclass
class NodeStats:
    ewma_ms: float | None = None
    requests: int = 0
    errors: int = 0
    last_ms: float = 0.0
    last_seen: float = field(default_factory=time.monotonic)

    def to_dict(self) -> dict:
        return {"ewma_ms": None if self.ewma_ms is None else round(self.ewma_ms, 3),
                "requests": self.requests, "errors": self.errors, "last_ms": round(self.last_ms, 3)}


class LoadBalancer:
    def __init__(self, r: aioredis.Redis, ns: str, strategy: str = "latency_aware",
                 alpha: float = 0.3, explore: float = 0.02, error_penalty_ms: float = 50.0,
                 rng: random.Random | None = None, router_id: str | None = None,
                 circuit: RedisCircuit | None = None, ejector: OutlierDetector | None = None):
        if strategy not in STRATEGIES:
            raise ValueError(f"unknown strategy {strategy!r}; choose from {STRATEGIES}")
        self.r = r
        self.ns = ns
        self.strategy = strategy
        self.alpha = alpha
        self.explore = explore
        self.error_penalty_ms = error_penalty_ms
        self.rng = rng or random.Random()
        self.router_id = router_id or f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        # Without a circuit, Redis errors propagate (fail closed).
        self.circuit = circuit or RedisCircuit(r.ping, enabled=False)
        self.ejector = ejector or OutlierDetector()
        self.stats: dict[str, NodeStats] = {}
        self.local: Counter[str] = Counter()   # this router's exact in-flight counts
        self.peer_keys: list[str] = []         # other live routers' slices, learned on publish
        self.local_picks = 0                   # replicas chosen without Redis
        self._rr_local: Counter[int] = Counter()
        self._pending_release: Counter[str] = Counter()
        self._flush_task: asyncio.Task | None = None
        self._acquire = r.register_script(_ACQUIRE_LUA)
        self._publish = r.register_script(_PUBLISH_LUA)

    @property
    def slice_key(self) -> str:
        return self._slice_key(self.router_id)

    @property
    def routers_key(self) -> str:
        return f"{self.ns}:routers"

    def _slice_key(self, router_id: str) -> str:
        return f"{self.ns}:inflight:{router_id}"

    # -- feedback ------------------------------------------------------------
    def observe(self, node_id: str, latency_ms: float, ok: bool) -> None:
        s = self.stats.setdefault(node_id, NodeStats())
        s.requests += 1
        s.last_seen = time.monotonic()
        s.last_ms = latency_ms
        if not ok:
            s.errors += 1
            latency_ms = max(latency_ms, (s.ewma_ms or 0.0)) + self.error_penalty_ms
        s.ewma_ms = latency_ms if s.ewma_ms is None else self.alpha * latency_ms + (1 - self.alpha) * s.ewma_ms
        self.ejector.record(node_id, ok)

    def forget(self, node_id: str) -> None:
        """Drop what we learned about a node, e.g. when it (re)joins the cluster."""
        self.stats.pop(node_id, None)
        self.ejector.forget(node_id)

    def weight(self, node_id: str, peers: list[str]) -> float:
        """Latency weight for scoring. Unseen nodes get the best peer EWMA (optimistic)."""
        s = self.stats.get(node_id)
        if s is not None and s.ewma_ms is not None:
            return max(s.ewma_ms, 0.01)
        known = [self.stats[p].ewma_ms for p in peers if p in self.stats and self.stats[p].ewma_ms is not None]
        return max(min(known), 0.01) if known else 1.0

    # -- selection -----------------------------------------------------------
    def _prepare(self, groups: list[tuple[int, list[str]]], strategy: str) -> list[tuple[int, list[str]]]:
        if strategy not in STRATEGIES:
            raise ValueError(f"unknown strategy {strategy!r}")
        out = []
        for shard, replicas in groups:
            if not replicas:
                raise LookupError(f"no replicas for shard {shard}")
            out.append((shard, self.ejector.filter(replicas)))
        return out

    def _explore(self, strategy: str) -> bool:
        return strategy in ("latency_aware", "p2c") and self.rng.random() < self.explore

    def _plan(self, shard: int, replicas: list[str], strategy: str, rr_keys: list[str], key_offset: int) -> list:
        """Translate a strategy into one Lua argument group (decided client-side where possible)."""
        if len(replicas) == 1:
            return ["pick", 0, 1, replicas[0], 0]
        if strategy == "random" or self._explore(strategy):
            return ["pick", 0, 1, self.rng.choice(replicas), 0]
        if strategy == "round_robin":
            rr_keys.append(f"{self.ns}:rr:{shard}")
            return ["rr", key_offset + len(rr_keys), len(replicas), *[x for c in replicas for x in (c, 0)]]
        cands = self.rng.sample(replicas, 2) if strategy == "p2c" else list(replicas)
        self.rng.shuffle(cands)  # random tie-breaking
        if strategy == "least_conn":
            return ["pick", 0, len(cands), *[x for c in cands for x in (c, 1)]]
        return ["pick", 0, len(cands), *[x for c in cands for x in (c, self.weight(c, replicas))]]

    def _choose_local(self, shard: int, replicas: list[str], strategy: str) -> str:
        """The same strategies, scored on this router's own in-flight counts."""
        if len(replicas) == 1:
            return replicas[0]
        if strategy == "random" or self._explore(strategy):
            return self.rng.choice(replicas)
        if strategy == "round_robin":
            self._rr_local[shard] += 1
            return replicas[(self._rr_local[shard] - 1) % len(replicas)]
        cands = self.rng.sample(replicas, 2) if strategy == "p2c" else list(replicas)
        self.rng.shuffle(cands)
        if strategy == "least_conn":
            return min(cands, key=lambda c: self.local[c])
        return min(cands, key=lambda c: self.weight(c, replicas) * (self.local[c] + 1))

    async def _acquire_shared(self, groups: list[tuple[int, list[str]]], strategy: str) -> list[str]:
        slices = [self.slice_key, *self.peer_keys]
        rr_keys: list[str] = []
        args: list = [len(slices), SLICE_TTL_S]
        for shard, replicas in groups:
            args += self._plan(shard, replicas, strategy, rr_keys, len(slices))
        return [_s(x) for x in await self._acquire(keys=[*slices, *rr_keys], args=args)]

    async def acquire_many(self, groups: list[tuple[int, list[str]]], strategy: str | None = None) -> list[str]:
        """Choose one replica per ``(shard, replicas)`` group, in a single Redis round trip
        when Redis is up and from local state when it isn't.

        Every returned node is counted as in-flight; pair each with :meth:`release_nowait`.
        """
        strategy = strategy or self.strategy
        groups = self._prepare(groups, strategy)
        if not groups:
            return []
        try:
            nodes = await self.circuit.call(self._acquire_shared, groups, strategy)
        except RedisUnavailable:
            nodes = [self._choose_local(shard, replicas, strategy) for shard, replicas in groups]
            self.local_picks += len(nodes)
        for n in nodes:
            self.local[n] += 1
        return nodes

    async def acquire(self, shard: int, replicas: list[str], strategy: str | None = None) -> str:
        return (await self.acquire_many([(shard, replicas)], strategy))[0]

    # -- completion ------------------------------------------------------------
    def _drop_local(self, node_id: str) -> None:
        if self.local[node_id] > 1:
            self.local[node_id] -= 1
        else:
            self.local.pop(node_id, None)

    async def release(self, node_id: str) -> None:
        self._drop_local(node_id)
        try:
            await self.circuit.call(self.r.hincrby, self.slice_key, node_id, -1)
        except RedisUnavailable:
            pass  # re-published on recovery

    def release_nowait(self, node_id: str) -> None:
        """Count a request as finished. Redis decrements queued in the same event-loop
        tick go out in ONE pipelined round trip, so a high fan-out query finishing N
        shard calls at once costs one Redis write, not N.

        Safe to call from ``finally`` blocks of cancelled tasks.
        """
        self._drop_local(node_id)
        if not self.circuit.available:
            return  # Redis is down; publish() restores the exact count on recovery
        self._pending_release[node_id] += 1
        if self._flush_task is None:
            self._flush_task = asyncio.get_running_loop().create_task(self._flush())

    async def _flush(self) -> None:
        await asyncio.sleep(0)  # let the rest of this tick's releases accumulate
        batch, self._pending_release = self._pending_release, Counter()
        try:
            if batch:
                await self.circuit.call(self._write_releases, batch)
        except Exception as e:
            # Don't retry: the next publish() overwrites the slice with exact counts.
            log.debug("release flush failed (%s); the next publish corrects it", e)
        finally:
            self._flush_task = None
            if self._pending_release:
                self._flush_task = asyncio.get_running_loop().create_task(self._flush())

    async def _write_releases(self, batch: Counter[str]) -> None:
        async with self.r.pipeline(transaction=False) as p:
            for node, n in batch.items():
                p.hincrby(self.slice_key, node, -n)
            await p.execute()

    async def drain(self) -> None:
        """Wait until every queued release has been written (tests, shutdown)."""
        while self._flush_task is not None:
            await self._flush_task

    # -- shared state ------------------------------------------------------------
    async def publish(self) -> None:
        """Overwrite this router's slice with its exact local counts, refresh its TTL,
        and learn which other routers are alive. Uses the raw client: the router calls
        it through the circuit, and the circuit calls it directly while recovering."""
        snapshot = [x for node, n in self.local.items() if n > 0 for x in (node, n)]
        self._pending_release.clear()  # the snapshot already reflects them
        routers = await self._publish(keys=[self.slice_key, self.routers_key],
                                      args=[self.router_id, SLICE_TTL_S, *snapshot])
        self.peer_keys = [self._slice_key(_s(r)) for r in routers if _s(r) != self.router_id]

    async def inflight(self) -> dict[str, int]:
        """Cluster-wide in-flight counts: the sum of every live router's slice."""
        async with self.r.pipeline(transaction=False) as p:
            for key in (self.slice_key, *self.peer_keys):
                p.hgetall(key)
            slices = await p.execute()
        total: Counter[str] = Counter()
        for h in slices:
            for node, n in h.items():
                total[_s(node)] += max(int(n), 0)
        return dict(total)


def _s(x) -> str:
    return x.decode() if isinstance(x, bytes) else str(x)
