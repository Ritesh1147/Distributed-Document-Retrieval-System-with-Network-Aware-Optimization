"""Launch a cluster as local processes (Redis + shard nodes + router).

This mirrors the Docker Compose deployment (same images/commands, same env
vars) but lets the benchmark harness tear down and rebuild topologies quickly.

    python -m bench.cluster --shards 4 --replicas 2          # runs until Ctrl-C
"""

from __future__ import annotations

import argparse
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field

import httpx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@dataclass
class LocalCluster:
    shards: int
    replicas: int = 1
    cluster: str = "bench"
    corpus: str = os.path.join(ROOT, "data", "corpus")
    redis_url: str | None = None
    router_env: dict = field(default_factory=dict)
    node_env: dict = field(default_factory=dict)       # applied to every node
    per_node_env: dict = field(default_factory=dict)   # node_id -> env overrides
    router_workers: int = 1
    procs: list[subprocess.Popen] = field(default_factory=list)
    router_url: str = ""
    node_urls: dict = field(default_factory=dict)

    def _spawn(self, args: list[str], env: dict, name: str) -> subprocess.Popen:
        full_env = {**os.environ, "PYTHONPATH": ROOT, **{k: str(v) for k, v in env.items()}}
        log = open(os.path.join(ROOT, "results", "logs", f"{name}.log"), "w")
        p = subprocess.Popen(args, env=full_env, stdout=log, stderr=subprocess.STDOUT, cwd=ROOT,
                             start_new_session=True)
        self.procs.append(p)
        return p

    def start(self, timeout: float = 120.0) -> "LocalCluster":
        os.makedirs(os.path.join(ROOT, "results", "logs"), exist_ok=True)
        if self.redis_url is None:
            port = free_port()
            self._spawn([shutil.which("redis-server") or "redis-server", "--port", str(port), "--save", "",
                         "--appendonly", "no"], {}, f"redis-{self.cluster}")
            self.redis_url = f"redis://127.0.0.1:{port}/0"
            _wait(lambda: socket.create_connection(("127.0.0.1", port), timeout=0.2).close(), 10)
        import redis
        r = redis.from_url(self.redis_url)
        for key in r.scan_iter(f"ddr:{self.cluster}:*"):
            r.delete(key)

        for s in range(self.shards):
            for rep in range(self.replicas):
                node_id = f"s{s}r{rep}"
                port = free_port()
                url = f"http://127.0.0.1:{port}"
                self.node_urls[node_id] = url
                env = {"NODE_ID": node_id, "SHARD_ID": s, "NUM_SHARDS": self.shards, "CLUSTER": self.cluster,
                       "ADVERTISE_URL": url, "REDIS_URL": self.redis_url, "CORPUS_PATH": self.corpus,
                       "ZONE": f"zone{rep}", **self.node_env, **self.per_node_env.get(node_id, {})}
                self._spawn([sys.executable, "-m", "uvicorn", "ddr.node.app:app", "--host", "127.0.0.1",
                             "--port", str(port), "--log-level", "warning", "--no-access-log"],
                            env, f"{self.cluster}-{node_id}")

        port = free_port()
        self.router_url = f"http://127.0.0.1:{port}"
        self._spawn([sys.executable, "-m", "uvicorn", "ddr.router.app:app", "--host", "127.0.0.1",
                     "--port", str(port), "--log-level", "warning", "--no-access-log",
                     "--workers", str(self.router_workers)],
                    {"REDIS_URL": self.redis_url, "CLUSTER": self.cluster, **self.router_env},
                    f"{self.cluster}-router")

        def ready() -> None:
            for p in self.procs:
                if p.poll() is not None:
                    raise RuntimeError(f"process exited early: {p.args} (see results/logs)")
            h = httpx.get(self.router_url + "/health", timeout=1).json()
            if h["status"] != "ok" or h["nodes"] != self.shards * self.replicas:
                raise AssertionError(h)
        _wait(ready, timeout, fatal=RuntimeError)
        return self

    def stop(self) -> None:
        for p in reversed(self.procs):
            if p.poll() is None:
                with _suppress():
                    os.killpg(p.pid, signal.SIGTERM)
        for p in self.procs:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(p.pid, signal.SIGKILL)
        self.procs.clear()

    def __enter__(self) -> "LocalCluster":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()


class _suppress:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return True


def _wait(fn, timeout: float, fatal: type[BaseException] | None = None) -> None:
    deadline = time.monotonic() + timeout
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            fn()
            return
        except Exception as e:
            if fatal and isinstance(e, fatal):
                raise
            last = e
            time.sleep(0.25)
    raise TimeoutError(f"timed out waiting: {last}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shards", type=int, default=4)
    ap.add_argument("--replicas", type=int, default=1)
    ap.add_argument("--strategy", default="latency_aware")
    args = ap.parse_args()
    c = LocalCluster(args.shards, args.replicas, cluster="local", router_env={"LB_STRATEGY": args.strategy})
    with c:
        print(f"router: {c.router_url}")
        for n, u in c.node_urls.items():
            print(f"  {n}: {u}")
        try:
            signal.pause()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
