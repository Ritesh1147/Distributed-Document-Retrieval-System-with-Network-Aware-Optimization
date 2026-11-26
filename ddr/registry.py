"""Redis-backed service registry.

Key layout (``ns`` = ``ddr:<cluster>``)::

    {ns}:nodes                 HASH  node_id -> JSON {url, shard_id, num_shards, zone}
    {ns}:hb:{node_id}          STR   JSON load report, expires after HEARTBEAT_TTL
    {ns}:inflight:{router_id}  HASH  node_id -> requests that router has in flight
                                     (see ddr.balancer; expires if the router dies)
    {ns}:routers               ZSET  live router ids, scored by last publish time
    {ns}:rr:{shard}            INT   shared round-robin cursor
    {ns}:cache:{...}           STR   cached query results

A node is *alive* while its heartbeat key exists, so crashed nodes drop out of
routing within ``HEARTBEAT_TTL`` seconds without any explicit deregistration.
Nodes re-register on every heartbeat, so after a Redis restart the registry
refills within one heartbeat interval. Several clusters (e.g. ``distributed``
and ``centralized``) can share one Redis.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import redis.asyncio as aioredis

HEARTBEAT_INTERVAL = 1.0
HEARTBEAT_TTL = 3


def namespace(cluster: str) -> str:
    return f"ddr:{cluster}"


@dataclass
class NodeInfo:
    node_id: str
    url: str
    shard_id: int
    num_shards: int
    zone: str = "default"
    load: dict = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps({"node_id": self.node_id, "url": self.url, "shard_id": self.shard_id,
                           "num_shards": self.num_shards, "zone": self.zone})


@dataclass
class Topology:
    num_shards: int = 0
    nodes: dict[str, NodeInfo] = field(default_factory=dict)
    shards: dict[int, list[str]] = field(default_factory=dict)  # shard -> alive replica ids

    def merged(self, newer: "Topology") -> "Topology":
        """Union of two views, preferring ``newer`` for nodes in both. Used right after
        Redis comes back, while nodes are still re-registering."""
        t = Topology(num_shards=max(self.num_shards, newer.num_shards), nodes={**self.nodes, **newer.nodes})
        for node_id in sorted(t.nodes):
            t.shards.setdefault(t.nodes[node_id].shard_id, []).append(node_id)
        return t

    @property
    def missing_shards(self) -> list[int]:
        return [s for s in range(self.num_shards) if not self.shards.get(s)]

    def to_dict(self) -> dict:
        return {
            "num_shards": self.num_shards,
            "missing_shards": self.missing_shards,
            "shards": {str(s): [{"node_id": n, "url": self.nodes[n].url, "zone": self.nodes[n].zone,
                                 "load": self.nodes[n].load} for n in ids]
                       for s, ids in sorted(self.shards.items())},
        }


class Registry:
    def __init__(self, r: aioredis.Redis, cluster: str):
        self.r = r
        self.ns = namespace(cluster)

    # -- node side -----------------------------------------------------------
    async def register(self, info: NodeInfo) -> None:
        await self.r.hset(f"{self.ns}:nodes", info.node_id, info.to_json())

    async def heartbeat(self, node_id: str, load: dict) -> None:
        await self.r.set(f"{self.ns}:hb:{node_id}", json.dumps(load), ex=HEARTBEAT_TTL)

    async def deregister(self, node_id: str) -> None:
        async with self.r.pipeline(transaction=False) as p:
            p.hdel(f"{self.ns}:nodes", node_id)
            p.delete(f"{self.ns}:hb:{node_id}")
            await p.execute()

    # -- router side -----------------------------------------------------------
    async def topology(self) -> Topology:
        raw = await self.r.hgetall(f"{self.ns}:nodes")
        if not raw:
            return Topology()
        ids = sorted(k.decode() if isinstance(k, bytes) else k for k in raw)
        hbs = await self.r.mget([f"{self.ns}:hb:{i}" for i in ids])
        topo = Topology()
        for node_id, hb in zip(ids, hbs):
            if hb is None:
                continue  # heartbeat expired -> treat as dead
            d = json.loads(raw[node_id] if node_id in raw else raw[node_id.encode()])
            info = NodeInfo(node_id=node_id, url=d["url"], shard_id=int(d["shard_id"]),
                            num_shards=int(d["num_shards"]), zone=d.get("zone", "default"),
                            load=json.loads(hb))
            topo.nodes[node_id] = info
            topo.shards.setdefault(info.shard_id, []).append(node_id)
            topo.num_shards = max(topo.num_shards, info.num_shards)
        return topo
