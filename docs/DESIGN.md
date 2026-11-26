# Design and API

How the system is put together, what the router does to keep latency low, and
how to configure and operate it. For measurements, see
[BENCHMARKS.md](BENCHMARKS.md).

## Request lifecycle

1. **Cache.** The router checks the Redis result cache, keyed on the normalised
   query terms, the search mode, and `k`.
2. **Global statistics (DFS mode only).** The router asks every shard for its
   term statistics and merges them, so all shards score with the same BM25
   numbers.
3. **Replica selection.** One Redis Lua call picks a replica for every shard and
   increments each chosen node's in-flight count.
4. **Scatter.** The query goes to all shards in parallel. A slow call can be
   hedged to a second replica, and a failed call is retried on another one.
5. **Gather.** The per-shard top-k lists are merged into a global top-k. If a
   whole shard was unreachable, the response is marked `partial: true` and is
   not cached. Complete responses are written back to the cache.

If Redis is unreachable, steps 1 and 3 run without it: no cache, and replicas
are chosen from the router's own load counts. See
[Failure handling](#failure-handling).

## Components

| Component | File | What it does |
|---|---|---|
| Corpus | [`ddr/corpus.py`](../ddr/corpus.py) | Deterministic synthetic corpus (default 500k docs / 66M tokens): Zipf vocabulary, topical documents, log-normal lengths. Stored as memory-mapped `.npy` files so co-located nodes share one copy. |
| Index | [`ddr/index.py`](../ddr/index.py) | Vectorised BM25 inverted index over one hash partition (`doc_id % num_shards`). Scores with **local** statistics (query-then-fetch) or with **global** statistics (DFS) passed in by the router. |
| Shard node | [`ddr/node/app.py`](../ddr/node/app.py) | FastAPI service that owns one shard. It registers in Redis, sends a heartbeat every second with its load (in-flight requests, EWMA service time), and can be reconfigured at runtime (`/admin/netem`) to simulate network and hardware conditions. |
| Router | [`ddr/router/app.py`](../ddr/router/app.py) | Runs the request lifecycle above: cache, optional DFS round, replica selection, fan-out with hedging and retries, top-k merge, partial results, admission control. |
| Load balancer | [`ddr/balancer.py`](../ddr/balancer.py) | `random`, `round_robin`, `least_conn`, `p2c`, `latency_aware`. Selection and the in-flight increment run **atomically in one Redis Lua call for all shards of a query**, so many routers share one view of load without herding. Falls back to the router's own counts when Redis is down. |
| Failure handling | [`ddr/resilience.py`](../ddr/resilience.py) | Circuit breaker around Redis, and per-node outlier ejection based on the router's own observations. |
| Registry | [`ddr/registry.py`](../ddr/registry.py) | Node table and TTL heartbeats in Redis. A node whose heartbeat expires drops out of routing within 3 s. |
| Network simulator | [`ddr/netsim.py`](../ddr/netsim.py) | Per-node delay, jitter, packet loss (modelled as a TCP retransmission timeout), bandwidth, and fault injection. Kernel `tc netem` is also available in Docker ([`docker/netem.sh`](../docker/netem.sh)). |

## Network-aware optimizations

- **Latency-aware replica selection.** Picks the replica that minimises
  `EWMA(RTT) × (in-flight + 1)`, which estimates when the request would finish.
  The round-trip time is measured *by the router*, so it reflects network
  distance, jitter, and loss as well as server load. Errors add a latency
  penalty, and 2% of picks go to a random replica so a recovered node can earn
  traffic back.
- **Atomic, batched Redis routing.** One Lua call chooses a replica for every
  shard and increments its in-flight count. Completions are coalesced, so all
  decrements from one event-loop tick go out in a single pipeline. Routing costs
  about two Redis round trips per query whatever the shard count, and concurrent
  routers can't all pick the same "least loaded" node. A failed decrement is
  retried rather than dropped, so a node never looks busy forever.
- **Adaptive hedging** (`HEDGE_MS=-1`). If a shard hasn't answered by the
  rolling 95th percentile of recent shard round-trip times, the router sends the
  same request to a second replica and keeps whichever answer arrives first.
  About 5% of calls get duplicated, and the trigger follows the network on its
  own: about 12 ms in the simulated data center, about 66 ms over the simulated
  WAN. Attempts that lose the race are still recorded, so the trigger can't
  drift down to only the fastest answers.
- **Retries and graceful degradation.** A failed replica is retried on another
  replica. If every replica of a shard is down, the router returns
  `partial: true` results instead of an error.
- **Admission control** (`MAX_INFLIGHT`). Once a router has this many queries in
  flight, it rejects new ones right away with `503` and `Retry-After` instead of
  queueing them. Past saturation, extra queueing only makes every query slower
  until requests time out; shedding the excess keeps goodput at capacity and
  latency bounded.
- **Redis result cache.** Serves the popular head of the query distribution
  without touching the shards.

## Failure handling

Redis coordinates the cluster but holds no search data, so the router treats it
as optional: losing Redis costs some optimisation, not availability.

**Redis outage.** Every Redis call goes through a circuit breaker
([`ddr/resilience.py`](../ddr/resilience.py)). The first connection error or
timeout trips it, and from then on Redis calls are skipped instantly instead of
each request waiting on a dead server. While the circuit is open, the router:

- keeps routing on the **last-known topology**,
- balances on **its own in-flight counts**, with the same strategy (round-robin
  uses a local cursor),
- **skips the result cache**, and
- marks responses `degraded: true`.

A background probe pings Redis every 0.5 s. When Redis answers, the router drops
its stale pooled connections, re-publishes its exact in-flight counts, and
re-reads the registry before closing the circuit. For 3 s after that it keeps
nodes the registry hasn't heard from yet, since a restarted Redis is empty until
each node's next heartbeat (nodes re-register every second). Setting
`REDIS_FAILOVER=0` restores the old fail-closed behaviour, where every query
fails while Redis is down.

**Shared load counts that repair themselves.** Counting in-flight requests with
increments and decrements in one shared hash breaks across failures. Decrements
sent during an outage are lost, a Redis restart wipes the counts while requests
are still in flight, and a router that crashes leaves its increments behind
forever. Instead, each router owns a *slice*: a hash of its own in-flight
counts, which the selection script sums across all live routers. Every 0.5 s the
router overwrites its slice with its exact local counts and refreshes a 3 s
expiry, so any drift is corrected within one interval and a crashed router's
slice simply expires. Live routers are tracked in a sorted set scored by Redis
server time, so router clocks don't need to agree.

**Node failures.** While Redis is up, a dead node drops out of the registry once
its heartbeat expires (within 3 s). The router doesn't wait for that, and while
Redis is down it can't, so it also ejects nodes on its own: after 3 consecutive
failed requests a node is skipped for 1 s. When the ejection ends the node gets
one trial request; another failure ejects it for twice as long (up to 15 s), and
one success clears its record. A shard's last replicas are never all skipped,
since trying a suspect node beats failing outright. A node that re-joins the
registry starts with a clean record.

**Limits.** Redis itself runs as a single instance; production use would add
replication and failover on the Redis side (for example Redis Sentinel or a
managed Redis). While degraded, each router sees only its own load, so several
routers can briefly pick the same replica, and cached results are unavailable.

## Redis key layout

Each deployment uses its own namespace, `ddr:<cluster>`, so the distributed and
centralized clusters can share one Redis.

| Key | Type | Contents |
|---|---|---|
| `ddr:<cluster>:nodes` | hash | node id → JSON `{url, shard_id, num_shards, zone}` |
| `ddr:<cluster>:hb:<node>` | string | latest load report; expires after 3 s |
| `ddr:<cluster>:inflight:<router>` | hash | node id → requests that router has in flight; expires 3 s after the router's last publish |
| `ddr:<cluster>:routers` | sorted set | live router ids, scored by last publish time (Redis server clock) |
| `ddr:<cluster>:rr:<shard>` | integer | shared round-robin cursor |
| `ddr:<cluster>:cache:...` | string | cached query results |

## Network profiles

Delay and jitter apply to each direction, so the round-trip time is about twice
the delay. A lost packet costs a 200 ms retransmission timeout.

| Profile | One-way delay | Jitter | Loss |
|---|---:|---:|---:|
| `ideal` | 0 ms | 0 ms | 0% |
| `lan` | 0.25 ms | 0.05 ms | 0% |
| `datacenter` | 1 ms | 0.5 ms | 0% |
| `cross_zone` | 5 ms | 2 ms | 0.1% |
| `wan` | 20 ms | 8 ms | 1% |
| `lossy` | 2 ms | 1 ms | 3% |

## Configuration

**Router**

| Variable | Default | Meaning |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379/0` | Redis for the registry, routing state, and cache |
| `CLUSTER` | `distributed` | Registry namespace to route over |
| `LB_STRATEGY` | `latency_aware` | `random`, `round_robin`, `least_conn`, `p2c`, or `latency_aware` |
| `SEARCH_MODE` | `qtf` | `qtf` (local statistics, one round trip) or `dfs` (global statistics, exact ranking) |
| `HEDGE_MS` | `0` | `0` = off, `-1` = adaptive (rolling p95), positive = fixed delay in ms |
| `MAX_ATTEMPTS` | `2` | Attempts per shard, counting hedges and retries |
| `SHARD_TIMEOUT_MS` | `2000` | Timeout for one request to a node |
| `CACHE_TTL_S` | `0` | Result cache lifetime; `0` = off |
| `MAX_INFLIGHT` | `0` | Admission control limit per router process; `0` = off |
| `REDIS_FAILOVER` | `1` | `1` keeps serving while Redis is down; `0` fails closed |
| `REDIS_POOL_SIZE` | `64` | Maximum Redis connections per router process |

**Shard node**

| Variable | Default | Meaning |
|---|---|---|
| `SHARD_ID` | `0` | Which shard this node serves |
| `NUM_SHARDS` | `1` | Total shards; `1` means a centralized, full index |
| `NODE_ID` | `shard<SHARD_ID>-<hostname>` | Unique node id |
| `CLUSTER` | `distributed` | Registry namespace |
| `ADVERTISE_URL` | `http://<hostname>:8000` | Address routers use to reach the node |
| `ZONE` | `default` | Locality label shown in the topology |
| `REDIS_URL` | unset | Registry; if unset, the node runs standalone |
| `CORPUS_PATH` | `data/corpus` | Corpus directory; built on first start if missing |
| `CPU_SLOWDOWN` | `1.0` | Values above 1 simulate slower hardware |
| `NET_PROFILE`, `NET_DELAY_MS`, `NET_JITTER_MS`, `NET_LOSS`, `NET_RTO_MS`, `NET_BANDWIDTH_MBPS`, `NET_FAIL_RATE` | none | Initial network conditions; see the profiles above |

## API

**Router**

| Endpoint | Description |
|---|---|
| `GET /search?q=&k=10` | Search. Optional `strategy=`, `mode=qtf\|dfs`, `hedge_ms=` (`-1` = adaptive), `cache=false`. The response flags `partial` (a shard was unreachable) and `degraded` (served without Redis) |
| `GET /topology` | Live nodes per shard, with each node's latest load report |
| `GET /metrics` | Router counters, per-node EWMA RTT and errors, in-flight counts, current hedge trigger, Redis circuit state (outages, time degraded), ejected nodes |
| `GET /health` | `ok`, or `degraded` when a shard has no live replica or Redis is down; includes `redis: up\|down` |
| `PUT /admin/config` | Change `strategy`, `mode`, `hedge_ms`, `max_attempts`, `shard_timeout_ms`, `cache_ttl_s`, `max_inflight`, or `redis_failover` at runtime |
| `PUT /admin/netem` | Apply network conditions (`profile`, `delay_ms`, `jitter_ms`, `loss`, `bandwidth_mbps`, `fail_rate`, `cpu_slowdown`) to some or all nodes |
| `POST /admin/cache/flush` | Clear the result cache |

**Shard node:** `POST /search`, `POST /termstats`, `GET /health`, `GET /info`,
`GET` and `PUT /admin/netem`.

## Operating the Docker stack

`docker/gen_compose.py` writes a Compose file for any layout. It starts Redis,
the distributed nodes behind `router-distributed` on port **8080**, and one
full-index node behind `router-centralized` on port **8081**. Options include
`--profile` (network profile for every node), `--far-zone-profile` (a slower
link for the last replica of each shard), `--strategy`, `--hedge-ms`,
`--cache-ttl`, `--max-inflight`, and `--no-centralized`.

```bash
python docker/gen_compose.py --shards 4 --replicas 2 --far-zone-profile cross_zone > docker-compose.yml
docker compose up -d --build --wait
```

```bash
curl -s 'localhost:8080/topology' | jq   # live shards and replicas, with load reports
curl -s 'localhost:8080/metrics' | jq    # per-node RTT, hedges, retries, cache hits
```

Degrade one replica's link and watch `latency_aware` route around it:

```bash
curl -s -X PUT localhost:8080/admin/netem -H 'content-type: application/json' \
     -d '{"nodes":["dist-s0r0"],"conditions":{"profile":"wan"}}'
```

Or use kernel-level network emulation inside a container:

```bash
docker compose exec dist-s0r1 netem.sh 20ms 5ms 1%
```

Stop Redis and watch the router keep serving in degraded mode, then recover:

```bash
docker compose stop redis
curl -s 'localhost:8080/search?q=dairkdark+geasjil' | jq '.degraded, .partial'   # true, false
curl -s 'localhost:8080/health' | jq '.redis'                                    # "down"
docker compose start redis
```

## Testing

`pytest -q` runs 67 tests:

- index correctness, including that sharded DFS scoring reproduces the
  centralized ranking exactly
- network simulator statistics
- load-balancer semantics: atomic selection, routers seeing each other's load,
  no herding, coalesced releases, crashed routers' counts expiring, lost updates
  corrected by the next publish
- failure handling: the circuit breaker trips, fails fast and recovers; node
  ejection and back-off; every strategy working without Redis; exact shared
  counts restored after a real Redis crash and restart
- the Compose generator
- end-to-end runs that start real router and node processes plus Redis, and
  check failover to a healthy replica, partial results when a shard is down,
  hedging around a slow replica, caching, and admission control
- an end-to-end failure scenario: Redis crashes, then a node crashes while Redis
  is down, then both restart. Every query must still complete, and the router
  must recover on its own. A second test confirms that with `REDIS_FAILOVER=0`
  the same outage fails every query.
