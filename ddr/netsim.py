"""Application-level network condition simulation.

Each node wraps its HTTP handlers in :class:`NetworkSimulator`, which models the
link between the router and that node:

* one-way propagation ``delay_ms`` plus Gaussian ``jitter_ms`` on both the
  request and the response leg (so RTT ~= 2 * delay),
* packet loss: with probability ``loss`` a leg suffers a TCP retransmission
  timeout (``rto_ms``), which is how loss actually shows up to an application,
* finite ``bandwidth_mbps`` (serialisation delay proportional to payload size),
* ``fail_rate``: fraction of requests answered with HTTP 503 (fault injection).

Conditions can be set via environment variables at start-up or changed at
runtime through the node's ``/admin/netem`` endpoint, which lets the benchmark
sweep network profiles without restarting the cluster. Because this works in
plain Python it behaves the same in Docker and in local processes; in Docker
you can additionally use ``tc netem`` (see ``docker/netem.sh``).
"""

from __future__ import annotations

import asyncio
import os
import random
from dataclasses import asdict, dataclass, fields

# Named profiles used by the benchmarks.
PROFILES: dict[str, dict] = {
    "ideal": dict(delay_ms=0.0, jitter_ms=0.0, loss=0.0),
    "lan": dict(delay_ms=0.25, jitter_ms=0.05, loss=0.0),
    "datacenter": dict(delay_ms=1.0, jitter_ms=0.5, loss=0.0),
    "cross_zone": dict(delay_ms=5.0, jitter_ms=2.0, loss=0.001),
    "wan": dict(delay_ms=20.0, jitter_ms=8.0, loss=0.01),
    "lossy": dict(delay_ms=2.0, jitter_ms=1.0, loss=0.03),
}


@dataclass
class NetConditions:
    delay_ms: float = 0.0
    jitter_ms: float = 0.0
    loss: float = 0.0
    rto_ms: float = 200.0
    bandwidth_mbps: float = 0.0   # 0 = unlimited
    fail_rate: float = 0.0

    @classmethod
    def from_env(cls, prefix: str = "NET_") -> "NetConditions":
        c = cls()
        profile = os.getenv(prefix + "PROFILE")
        if profile:
            c.update(PROFILES[profile])
        for f in fields(cls):
            v = os.getenv(prefix + f.name.upper())
            if v is not None:
                setattr(c, f.name, float(v))
        return c

    def update(self, d: dict) -> None:
        if "profile" in d and d["profile"]:
            for k, v in PROFILES[d["profile"]].items():
                setattr(self, k, v)
        names = {f.name for f in fields(self)}
        for k, v in d.items():
            if k in names and v is not None:
                setattr(self, k, float(v))

    def to_dict(self) -> dict:
        return asdict(self)


class NetworkSimulator:
    def __init__(self, cond: NetConditions | None = None, rng: random.Random | None = None):
        self.cond = cond or NetConditions()
        self.rng = rng or random.Random()

    def leg_delay_s(self, payload_bytes: int = 0) -> float:
        c = self.cond
        d = c.delay_ms
        if c.jitter_ms:
            d += self.rng.gauss(0.0, c.jitter_ms)
        if c.loss and self.rng.random() < c.loss:
            d += c.rto_ms
        if c.bandwidth_mbps:
            d += payload_bytes * 8 / (c.bandwidth_mbps * 1e6) * 1e3
        return max(d, 0.0) / 1e3

    def should_fail(self) -> bool:
        return bool(self.cond.fail_rate) and self.rng.random() < self.cond.fail_rate

    async def leg(self, payload_bytes: int = 0) -> None:
        d = self.leg_delay_s(payload_bytes)
        if d > 0:
            await asyncio.sleep(d)
