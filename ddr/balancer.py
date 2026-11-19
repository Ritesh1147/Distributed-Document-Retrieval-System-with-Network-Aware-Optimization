"""Replica selection (load balancing) backed by Redis.

Every router instance shares one view of in-flight requests per node, kept in
a Redis hash. Selection and the in-flight increment happen atomically inside a
Lua script, so concurrent routers cannot all pile onto the same "least loaded"
replica (the herd effect you get when you read load, then act on it). The
script picks replicas for *every* shard of a query in one call, so routing
costs one Redis round trip per query regardless of the number of shards.

Strategies
----------
``random``          uniform random replica.
``round_robin``     shared Redis cursor per shard (global RR across routers).
``least_conn``      fewest in-flight requests cluster-wide (Redis counters).
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
import random
import time
from collections import Counter
from dataclasses import dataclass, field

import redis.asyncio as aioredis

log = logging.getLogger("ddr.balancer")

STRATEGIES = ("random", "round_robin", "least_conn", "latency_aware", "p2c")

# Batched selection for a whole fan-out in ONE round trip.
# KEYS[1] = inflight hash, KEYS[2..] = round-robin cursors.
# ARGV is a sequence of groups: kind, rr_key_index, n, id1, w1, ..., idn, wn
#   kind "rr"   -> advance the shared cursor KEYS[rr_key_index], pick modulo n
#   kind "pick" -> argmin w * (inflight + 1)   (least_conn: all w = 1)
# Each chosen node's in-flight counter is incremented atomically with the choice.
_ACQUIRE_LUA = """
local out = {}
local i = 1
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
      local w = tonumber(ARGV[i + 2 * j + 1])
      local inflight = tonumber(redis.call('HGET', KEYS[1], id) or '0')
      if inflight < 0 then inflight = 0 end
      local score = w * (inflight + 1)
      if best == nil or score < best then chosen, best = id, score end
    end
  end
  redis.call('HINCRBY', KEYS[1], chosen, 1)
  out[#out + 1] = chosen
  i = i + 2 * n
end
return out
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
                 rng: random.Random | None = None):
        if strategy not in STRATEGIES:
            raise ValueError(f"unknown strategy {strategy!r}; choose from {STRATEGIES}")
        self.r = r
        self.ns = ns
        self.strategy = strategy
        self.alpha = alpha
        self.explore = explore
        self.error_penalty_ms = error_penalty_ms
        self.rng = rng or random.Random()
        self.stats: dict[str, NodeStats] = {}
        self._pending_release: Counter[str] = Counter()
        self._flush_task: asyncio.Task | None = None
        self._acquire = r.register_script(_ACQUIRE_LUA)

    @property
    def inflight_key(self) -> str:
        return f"{self.ns}:inflight"

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

    def weight(self, node_id: str, peers: list[str]) -> float:
        """Latency weight for scoring. Unseen nodes get the best peer EWMA (optimistic)."""
        s = self.stats.get(node_id)
        if s is not None and s.ewma_ms is not None:
            return max(s.ewma_ms, 0.01)
        known = [self.stats[p].ewma_ms for p in peers if p in self.stats and self.stats[p].ewma_ms is not None]
        return max(min(known), 0.01) if known else 1.0

    # -- selection -----------------------------------------------------------
    def _plan(self, shard: int, replicas: list[str], strategy: str, rr_keys: list[str]) -> list:
        """Translate a strategy into one Lua argument group (decided client-side where possible)."""
        if not replicas:
            raise LookupError(f"no replicas for shard {shard}")
        if strategy not in STRATEGIES:
            raise ValueError(f"unknown strategy {strategy!r}")
        if len(replicas) == 1:
            return ["pick", 0, 1, replicas[0], 0]
        if strategy == "random" or (strategy in ("latency_aware", "p2c") and self.rng.random() < self.explore):
            return ["pick", 0, 1, self.rng.choice(replicas), 0]
        if strategy == "round_robin":
            rr_keys.append(f"{self.ns}:rr:{shard}")
            return ["rr", len(rr_keys) + 1, len(replicas), *[x for c in replicas for x in (c, 0)]]
        cands = self.rng.sample(replicas, 2) if strategy == "p2c" else list(replicas)
        self.rng.shuffle(cands)  # random tie-breaking
        if strategy == "least_conn":
            return ["pick", 0, len(cands), *[x for c in cands for x in (c, 1)]]
        return ["pick", 0, len(cands), *[x for c in cands for x in (c, self.weight(c, replicas))]]

    async def acquire_many(self, groups: list[tuple[int, list[str]]], strategy: str | None = None) -> list[str]:
        """Choose one replica per ``(shard, replicas)`` group in a single Redis round trip.

        Every returned node is counted as in-flight; pair each with :meth:`release`.
        """
        strategy = strategy or self.strategy
        rr_keys: list[str] = []
        args: list = []
        for shard, replicas in groups:
            args += self._plan(shard, replicas, strategy, rr_keys)
        if not args:
            return []
        return [_s(x) for x in await self._acquire(keys=[self.inflight_key, *rr_keys], args=args)]

    async def acquire(self, shard: int, replicas: list[str], strategy: str | None = None) -> str:
        return (await self.acquire_many([(shard, replicas)], strategy))[0]

    async def release(self, node_id: str) -> None:
        await self.r.hincrby(self.inflight_key, node_id, -1)

    def release_nowait(self, node_id: str) -> None:
        """Queue an in-flight decrement; all decrements queued in the same event-loop
        tick are written in ONE pipelined round trip.

        Safe to call from ``finally`` blocks of cancelled tasks. A high fan-out
        query finishing N shard calls at once costs one Redis write, not N.
        """
        self._pending_release[node_id] += 1
        if self._flush_task is None:
            self._flush_task = asyncio.get_running_loop().create_task(self._flush())

    async def _flush(self) -> None:
        await asyncio.sleep(0)  # let the rest of this tick's releases accumulate
        batch, self._pending_release = self._pending_release, Counter()
        try:
            async with self.r.pipeline(transaction=False) as p:
                for node, n in batch.items():
                    p.hincrby(self.inflight_key, node, -n)
                await p.execute()
        except Exception as e:
            # Never drop decrements: a lost one would leave the node looking busy forever.
            log.warning("release flush failed (%s); retrying", e)
            self._pending_release.update(batch)
            await asyncio.sleep(0.05)
        finally:
            self._flush_task = None
            if self._pending_release:
                self._flush_task = asyncio.get_running_loop().create_task(self._flush())

    async def drain(self) -> None:
        """Wait until every queued release has been written (tests, shutdown)."""
        while self._flush_task is not None:
            await self._flush_task

    async def inflight(self) -> dict[str, int]:
        raw = await self.r.hgetall(self.inflight_key)
        return {_s(k): int(v) for k, v in raw.items()}


def _s(x) -> str:
    return x.decode() if isinstance(x, bytes) else str(x)
