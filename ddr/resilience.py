"""Failure handling: a circuit breaker for Redis and passive health checks for nodes.

Redis coordinates the cluster (registry, shared load counts, result cache) but
holds no search data, so losing it should cost some optimisation, not
availability.

:class:`RedisCircuit` makes the router *fail open*. The first Redis error trips
the circuit. While it is open, every Redis call is skipped instantly, so no
request waits on a dead server, and the router runs on its own local state. A
background probe pings Redis; once it answers, recovery hooks run (re-publish
this router's load counts, re-read the registry) and the circuit closes.

:class:`OutlierDetector` is the router's own view of node health. While Redis is
down the registry can't report dead nodes, so the router ejects a node after a
few consecutive failures, then lets a single probe request through after a
back-off that doubles each time the node fails again.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections import Counter
from typing import Awaitable, Callable

import redis.exceptions as rexc

log = logging.getLogger("ddr.resilience")

# Errors that mean "Redis is unreachable", as opposed to a bad command or script.
REDIS_DOWN_ERRORS = (rexc.ConnectionError, rexc.TimeoutError, OSError)


class RedisUnavailable(Exception):
    """Raised instead of calling Redis while the circuit is open."""


class RedisCircuit:
    """Circuit breaker around every Redis call the router makes.

    With ``enabled=False`` it is a pass-through: Redis errors propagate and the
    router fails closed, which is the behaviour before failover existed.
    """

    def __init__(self, ping: Callable[[], Awaitable[object]], enabled: bool = True,
                 probe_interval_s: float = 0.5, clock: Callable[[], float] = time.monotonic):
        self._ping = ping
        self.enabled = enabled
        self.probe_interval_s = probe_interval_s
        self.clock = clock
        self.is_open = False
        self.opened_at = 0.0
        self.outages = 0
        self.last_error = ""
        self._downtime = 0.0
        self._hooks: list[Callable[[], Awaitable[None]]] = []
        self._probe: asyncio.Task | None = None

    @property
    def available(self) -> bool:
        return not self.is_open

    def on_recover(self, hook: Callable[[], Awaitable[None]]) -> None:
        """Register a coroutine to run, in order, before the circuit closes again.
        Hooks run while the circuit is still open, so they must use the raw client."""
        self._hooks.append(hook)

    async def call(self, fn: Callable[..., Awaitable], *args, **kwargs):
        """Run one Redis operation, or raise :class:`RedisUnavailable` without trying."""
        if not self.enabled:
            return await fn(*args, **kwargs)
        if self.is_open:
            raise RedisUnavailable(self.last_error)
        try:
            return await fn(*args, **kwargs)
        except REDIS_DOWN_ERRORS as e:
            self.trip(e)
            raise RedisUnavailable(self.last_error) from e

    def trip(self, exc: BaseException) -> None:
        if self.is_open:
            return
        self.is_open = True
        self.opened_at = self.clock()
        self.outages += 1
        self.last_error = f"{type(exc).__name__}: {exc}"
        log.warning("Redis unavailable (%s): serving from local state", self.last_error)
        self._probe = asyncio.get_running_loop().create_task(self._probe_until_recovered())

    async def _probe_until_recovered(self) -> None:
        while self.is_open:
            await asyncio.sleep(self.probe_interval_s)
            try:
                await self._ping()
                for hook in self._hooks:
                    await hook()
            except Exception as e:  # still down, or recovery failed half-way: keep probing
                self.last_error = f"{type(e).__name__}: {e}"
                continue
            down_for = self.clock() - self.opened_at
            self._downtime += down_for
            self.is_open = False
            log.warning("Redis is back after %.1fs: shared state restored", down_for)
        self._probe = None

    def downtime_s(self) -> float:
        return self._downtime + (self.clock() - self.opened_at if self.is_open else 0.0)

    def status(self) -> dict:
        return {"state": "down" if self.is_open else "up", "failover": self.enabled,
                "outages": self.outages, "downtime_s": round(self.downtime_s(), 3),
                "last_error": self.last_error or None}

    async def close(self) -> None:
        if self._probe is not None:
            self._probe.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._probe


class OutlierDetector:
    """Eject nodes that keep failing, based only on what this router observes.

    A node is ejected after ``consecutive_failures`` failures in a row. When the
    ejection ends it is on probation: one more failure ejects it again for twice
    as long (capped at ``max_ejection_s``), and one success clears its record.
    Failures reported while a node is already ejected come from requests sent
    before the ejection and are ignored.
    """

    def __init__(self, consecutive_failures: int = 3, base_ejection_s: float = 1.0,
                 max_ejection_s: float = 15.0, clock: Callable[[], float] = time.monotonic):
        self.threshold = consecutive_failures
        self.base_ejection_s = base_ejection_s
        self.max_ejection_s = max_ejection_s
        self.clock = clock
        self.total_ejections = 0
        self._fails: Counter[str] = Counter()
        self._ejections: Counter[str] = Counter()
        self._until: dict[str, float] = {}

    def record(self, node: str, ok: bool) -> None:
        if ok:
            self.forget(node)
            return
        now = self.clock()
        until = self._until.get(node)
        if until is not None and now < until:
            return
        self._fails[node] += 1
        on_probation = until is not None
        if on_probation or self._fails[node] >= self.threshold:
            self._ejections[node] += 1
            backoff = min(self.base_ejection_s * 2 ** (self._ejections[node] - 1), self.max_ejection_s)
            self._until[node] = now + backoff
            self._fails[node] = 0
            self.total_ejections += 1
            log.warning("ejecting node %s for %.1fs after repeated failures", node, backoff)

    def is_ejected(self, node: str) -> bool:
        return self._until.get(node, 0.0) > self.clock()

    def filter(self, replicas: list[str]) -> list[str]:
        """Replicas that aren't ejected; all of them if every one is (better to try)."""
        healthy = [n for n in replicas if not self.is_ejected(n)]
        return healthy or list(replicas)

    def forget(self, node: str) -> None:
        self._fails.pop(node, None)
        self._ejections.pop(node, None)
        self._until.pop(node, None)

    def snapshot(self) -> dict[str, float]:
        """Currently ejected nodes -> seconds until they get another chance."""
        now = self.clock()
        return {n: round(u - now, 2) for n, u in sorted(self._until.items()) if u > now}
