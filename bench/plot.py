"""Render charts and a markdown summary from ``results/results.json``.

    python -m bench.plot
"""

from __future__ import annotations

import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker  # noqa: E402
import numpy as np  # noqa: E402

from .cluster import ROOT  # noqa: E402

RES = os.path.join(ROOT, "results")
PLOTS = os.path.join(RES, "plots")

# Validated categorical palette (fixed order), light surface.
C = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]
INK, INK2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
    "text.color": INK, "axes.titlecolor": INK, "axes.titlesize": 11, "axes.titleweight": "bold",
    "axes.titlelocation": "left", "axes.labelsize": 9, "xtick.labelsize": 8.5, "ytick.labelsize": 8.5,
    "legend.fontsize": 8.5, "legend.frameon": False, "axes.grid": True, "axes.grid.axis": "y",
    "grid.color": GRID, "grid.linewidth": 0.8, "axes.spines.top": False, "axes.spines.right": False,
    "axes.spines.left": False, "font.family": "DejaVu Sans", "lines.linewidth": 2,
})


def _bars(ax, groups: list[str], series: dict[str, list[float]], fmt: str = "{:.0f}", label_series=None):
    n = len(series)
    width = 0.8 / n
    x = np.arange(len(groups))
    for i, (name, vals) in enumerate(series.items()):
        pos = x - 0.4 + width * (i + 0.5)
        bars = ax.bar(pos, vals, width - 0.03, color=C[i], label=name, zorder=2, edgecolor=SURFACE, linewidth=1)
        if label_series is None or name in label_series:
            for b, v in zip(bars, vals):
                if v is not None and not np.isnan(v):
                    ax.annotate(fmt.format(v), (b.get_x() + b.get_width() / 2, b.get_height()),
                                xytext=(0, 2), textcoords="offset points", ha="center", va="bottom",
                                fontsize=7, color=INK2)
    ax.set_xticks(x, groups)
    ax.tick_params(axis="x", length=0)


def _save(fig, name: str) -> str:
    os.makedirs(PLOTS, exist_ok=True)
    path = os.path.join(PLOTS, name)
    fig.tight_layout()
    fig.savefig(path, dpi=160)
    plt.close(fig)
    return path


def _get(rows, **match):
    for r in rows:
        if all(r.get(k) == v for k, v in match.items()):
            return r
    return None


# -- charts ---------------------------------------------------------------------
def plot_scaling(d) -> None:
    rows = d["rows"]
    topos = []
    for r in rows:
        t = (r["shards"], r["replicas"])
        if t not in topos:
            topos.append(t)
    labels = ["central\n1 node" if t == (1, 1) else f"{t[0]}×{t[1]}\n{t[0] * t[1]} nodes" for t in topos]
    concs = sorted({r["concurrency"] for r in rows})
    cmax, cmin = max(concs), min(concs)
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.8))
    thr = [_get(rows, shards=s, replicas=rp, concurrency=cmax)["throughput_rps"] for s, rp in topos]
    _bars(axes[0], labels, {f"throughput @ {cmax} concurrent clients": thr})
    axes[0].set_title("Throughput by topology (shards × replicas)")
    axes[0].set_ylabel("queries / second")
    axes[0].legend(loc="upper left")
    p50 = [_get(rows, shards=s, replicas=rp, concurrency=cmin)["p50"] for s, rp in topos]
    p99 = [_get(rows, shards=s, replicas=rp, concurrency=cmin)["p99"] for s, rp in topos]
    _bars(axes[1], labels, {"p50": p50, "p99": p99}, fmt="{:.1f}")
    axes[1].set_title(f"Per-query latency at {cmin} client{'s' if cmin > 1 else ''} (scatter-gather speedup)")
    axes[1].set_ylabel("ms")
    axes[1].legend(loc="upper right")
    _save(fig, "scaling.png")


def plot_network(d) -> None:
    rows = d["rows"]
    profiles = list(dict.fromkeys(r["profile"] for r in rows))
    variants = list(dict.fromkeys(r["variant"] for r in rows))
    fig, axes = plt.subplots(1, 3, figsize=(15, 3.9), sharey=True)
    for ax, metric in zip(axes, ("p50", "p95", "p99")):
        _bars(ax, [p.replace("_", " ") for p in profiles],
              {v: [_get(rows, variant=v, profile=p)[metric] for p in profiles] for v in variants},
              fmt="{:.0f}", label_series=[variants[-1]] if metric != "p50" else [])
        ax.set_yscale("log")
        ax.set_title(f"{metric} latency by network profile")
        ax.grid(True, which="major", axis="y")
        ax.grid(False, which="minor")
    axes[0].set_ylabel(f"ms, log scale ({d.get('offered_rps', '?')} rps Poisson load)")
    axes[0].legend(loc="upper left")
    _save(fig, "network.png")


def plot_balancing(d) -> None:
    rows = d["rows"]
    wls = list(dict.fromkeys(r["workload"] for r in rows))
    strats = list(dict.fromkeys(r["strategy"] for r in rows))
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.8), gridspec_kw={"width_ratios": [3, 2]})
    _bars(axes[0], [w.replace("_", " ") for w in wls],
          {s: [_get(rows, workload=w, strategy=s)["p99"] for w in wls] for s in strats}, fmt="{:.0f}")
    axes[0].set_yscale("log")
    axes[0].grid(False, which="minor")
    axes[0].set_ylabel("p99 ms (log scale)")
    axes[0].set_title("p99 latency on heterogeneous replicas")
    axes[0].legend(loc="upper center", bbox_to_anchor=(0.5, -0.1), ncol=5)
    # traffic share under the Poisson workload
    wl = "poisson_200rps" if "poisson_200rps" in wls else wls[0]
    ax = axes[1]
    left = np.zeros(len(strats))
    parts = [("share_healthy", "healthy"), ("share_slow_cpu", "3× slower CPU"), ("share_far_zone", "far zone")]
    for i, (key, name) in enumerate(parts):
        vals = np.array([_get(rows, workload=wl, strategy=s)[key] for s in strats])
        ax.barh(strats, vals, left=left, color=C[i], label=name, height=0.6, edgecolor=SURFACE, linewidth=2)
        for y, (l, v) in enumerate(zip(left, vals)):
            if v > 0.08:
                ax.text(l + v / 2, y, f"{v:.0%}", ha="center", va="center", fontsize=7.5, color="white")
        left += vals
    ax.set_xlim(0, 1)
    ax.invert_yaxis()
    ax.grid(False)
    ax.set_xticks([])
    ax.tick_params(axis="y", length=0)
    ax.set_title(f"Where traffic went ({wl.replace('_', ' ')})")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.02), ncol=3)
    _save(fig, "balancing.png")


def plot_loadcurve(d) -> None:
    rows = d["rows"]
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.6))
    for i, arch in enumerate(dict.fromkeys(r["arch"] for r in rows)):
        rs = [r for r in rows if r["arch"] == arch]
        x = [r["offered_rps"] for r in rs]
        axes[0].plot(x, [r["throughput_rps"] for r in rs], color=C[i], marker="o", ms=4, label=arch)
        axes[1].plot(x, [r["p99"] for r in rs], color=C[i], marker="o", ms=4, label=arch)
    lim = max(r["offered_rps"] for r in rows)
    axes[0].plot([0, lim], [0, lim], color=INK2, lw=1, ls="--", label="ideal (served = offered)")
    axes[0].set_title("Served vs offered load (open loop)")
    axes[0].set_xlabel("offered queries / second")
    axes[0].set_ylabel("served queries / second")
    axes[1].set_yscale("log")
    axes[1].grid(False, which="minor")
    axes[1].set_title("p99 latency vs offered load")
    axes[1].set_xlabel("offered queries / second")
    axes[1].set_ylabel("p99 ms (log scale)")
    for ax in axes:
        ax.legend(loc="upper left")
    _save(fig, "loadcurve.png")


def plot_quality(d) -> None:
    rows = d["rows"]
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.6))
    x = [r["shards"] for r in rows]
    axes[0].plot(x, [r["overlap10_dfs"] for r in rows], color=C[0], marker="o", ms=4, label="DFS (global stats)")
    axes[0].plot(x, [r["overlap10_qtf"] for r in rows], color=C[1], marker="o", ms=4, label="QTF (local stats)")
    axes[0].set_xscale("log", base=2)
    axes[0].set_xticks(x, [str(v) for v in x])
    axes[0].set_xlabel("shards")
    axes[0].set_ylabel("top-10 overlap with centralized")
    axes[0].set_title("Ranking agreement with a single index")
    axes[0].legend(loc="lower left")
    lat = d.get("latency", [])
    profs = list(dict.fromkeys(r["profile"] for r in lat))
    if profs:
        _bars(axes[1], profs, {m.upper(): [_get(lat, profile=p, mode=m)["p50"] for p in profs] for m in ("qtf", "dfs")},
              fmt="{:.1f}")
        axes[1].set_ylabel("p50 ms")
        axes[1].set_title("Cost of the extra DFS round trip")
        axes[1].legend(loc="upper left")
    _save(fig, "quality.png")


def plot_chaos(d) -> None:
    timeline = d["timeline"]
    variants = ["with failover", "without failover"]
    events = d["events"][variants[0]]
    at = {e["event"]: e["t"] for e in events}
    fig, (top, bottom) = plt.subplots(2, 1, figsize=(11, 5.8), sharex=True)
    for i, v in enumerate(variants):
        rows = [r for r in timeline if r["variant"] == v]
        if not rows:
            continue
        step = rows[1]["t"] - rows[0]["t"] if len(rows) > 1 else 1.0
        x = [r["t"] for r in rows] + [rows[-1]["t"] + step]
        ok = [100 * (r["success_rate"] or 0) for r in rows]
        p99 = [r["p99"] if r["p99"] is not None else np.nan for r in rows]
        z = 4 if i == 0 else 3  # "with failover" on top where the lines coincide
        top.step(x, ok + ok[-1:], where="post", color=C[i], label=v, zorder=z)
        bottom.step(x, p99 + p99[-1:], where="post", color=C[i], label=v, zorder=z)
    for ax in (top, bottom):
        if "Redis crashes" in at and "Redis restarts (empty)" in at:
            ax.axvspan(at["Redis crashes"], at["Redis restarts (empty)"], color=GRID, alpha=0.55, lw=0, zorder=1)
        for name in ("node s1r0 crashes", "node s1r0 restarts"):
            if name in at:
                ax.axvline(at[name], color=INK2, lw=1, ls="--", zorder=2)
    if "Redis crashes" in at:
        top.text(at["Redis crashes"] + 0.2, 104, "Redis down", ha="left", va="bottom", fontsize=8.5, color=INK2)
    for name, label in (("node s1r0 crashes", "node crashes"), ("node s1r0 restarts", "node restarts")):
        if name in at:
            top.text(at[name] + 0.2, 104, label, ha="left", va="bottom", fontsize=8.5, color=INK2)
    top.set_ylim(0, 112)
    top.set_yticks([0, 25, 50, 75, 100])
    top.set_ylabel("% answered completely")
    top.set_title("Queries answered completely, per second", pad=18)
    top.legend(loc="lower left")
    bottom.set_yscale("log")
    bottom.grid(False, which="minor")
    bottom.yaxis.set_major_locator(matplotlib.ticker.LogLocator(subs=(1, 2, 5)))
    bottom.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:g}"))
    bottom.yaxis.set_minor_locator(matplotlib.ticker.NullLocator())
    bottom.set_ylim(bottom=10)
    bottom.set_ylabel("p99 ms (log scale)")
    bottom.set_xlabel("seconds into the run")
    bottom.set_title(f"p99 latency of complete answers ({d.get('offered_rps', '?')} queries/s, open loop)")
    _save(fig, "chaos.png")


# -- markdown -------------------------------------------------------------------
def _table(rows: list[dict], cols: list[tuple[str, str]]) -> str:
    out = ["| " + " | ".join(h for _, h in cols) + " |", "|" + "|".join("---:" for _ in cols) + "|"]
    for r in rows:
        cells = []
        for k, _ in cols:
            v = r.get(k)
            if v is None:
                cells.append("—")
                continue
            cells.append(f"{v:.3g}" if isinstance(v, float) and abs(v) < 1 else
                         f"{v:,.1f}" if isinstance(v, float) else str(v))
        out.append("| " + " | ".join(cells) + " |")
    return "\n".join(out)


def _host_note(res: dict, name: str, env: dict) -> list[str]:
    """A note for a section measured on a different host than the main run."""
    e = res.get(name, {}).get("env")
    if not e or e.get("platform") == env.get("platform"):
        return []
    return [f"*Measured separately, later, on a different host ({e.get('cpus')} vCPUs, {e.get('platform')}); "
            "compare these numbers within this section.*", ""]


def markdown(res: dict) -> str:
    env = res.get("scaling", {}).get("env") or res.get("_env", {})
    md = ["# Benchmark results", "",
          f"Generated by `python -m bench.run_benchmarks` + `python -m bench.plot`. Host: {env.get('cpus')} vCPUs, "
          f"Python {env.get('python')}; corpus {env.get('corpus_docs', 0):,} docs / {env.get('corpus_tokens', 0):,} tokens. "
          "All services, Redis and the load generator share the same host, so absolute numbers are "
          "CPU-bound by that host; relative comparisons are the point.", ""]
    lat = [("p50", "p50 ms"), ("p95", "p95 ms"), ("p99", "p99 ms")]
    if "scaling" in res:
        md += ["## Scaling with nodes", "", "![scaling](plots/scaling.png)", "",
               _table(res["scaling"]["rows"], [("shards", "shards"), ("replicas", "replicas"), ("nodes", "nodes"),
                                               ("concurrency", "clients"), ("throughput_rps", "rps"), *lat]), ""]
    if "network" in res:
        md += ["## Network conditions", "", "![network](plots/network.png)", "",
               _table(res["network"]["rows"], [("variant", "variant"), ("profile", "profile"),
                                               ("throughput_rps", "rps"), *lat, ("hedge_ms", "hedge trigger ms"),
                                               ("hedge_rate", "hedged shard calls")]), ""]
    if "balancing" in res:
        md += ["## Load balancing on heterogeneous replicas", "",
               "2 shards × 3 replicas; per shard r0 = healthy, r1 = 3× slower CPU, r2 = far zone "
               "(5 ms ± 2 ms, 0.1% loss).", "", "![balancing](plots/balancing.png)", "",
               _table(res["balancing"]["rows"], [("workload", "workload"), ("strategy", "strategy"),
                                                 ("throughput_rps", "rps"), *lat, ("share_healthy", "→healthy"),
                                                 ("share_slow_cpu", "→slow CPU"), ("share_far_zone", "→far zone")]), ""]
    if "loadcurve" in res:
        md += ["## Latency vs offered load", "", "![loadcurve](plots/loadcurve.png)", "",
               _table(res["loadcurve"]["rows"], [("arch", "arch"), ("offered_rps", "offered rps"),
                                                 ("throughput_rps", "served rps"), *lat]), ""]
    if "cache" in res:
        md += ["## Redis result cache", "",
               _table(res["cache"]["rows"], [("popularity", "query popularity"), ("cache", "cache"),
                                             ("throughput_rps", "rps"), ("cache_hit_rate", "hit rate"), *lat]), ""]
    if "quality" in res:
        md += ["## Ranking quality: local vs global statistics", "", "![quality](plots/quality.png)", "",
               _table(res["quality"]["rows"], [("shards", "shards"), ("overlap10_qtf", "QTF overlap@10"),
                                               ("exact_order_qtf", "QTF exact top-10"),
                                               ("overlap10_dfs", "DFS overlap@10"),
                                               ("exact_order_dfs", "DFS exact top-10")]), "",
               _table(res["quality"].get("latency", []), [("profile", "profile"), ("mode", "mode"),
                                                          ("throughput_rps", "rps"), *lat]), ""]
    if "chaos" in res:
        c = res["chaos"]
        md += ["## Failure handling: Redis outage and node crash", "", *_host_note(res, "chaos", env),
               f"{c.get('topology')}, {c.get('offered_rps')} queries/s open-loop load. Redis crashes, then a node "
               "crashes while Redis is still down; both restart. Same scenario for a router that fails closed "
               "(`REDIS_FAILOVER=0`) and one that fails open.", "", "![chaos](plots/chaos.png)", "",
               _table(c["rows"], [("variant", "router"), ("window", "window"), ("requests", "requests"),
                                  ("success_rate", "answered completely"), ("error_rate", "errors"),
                                  ("partial_rate", "partial"), ("degraded_share", "served degraded"),
                                  ("p50", "p50 ms"), ("p99", "p99 ms")]), ""]
        for name, m in c.get("routers", {}).items():
            md.append(f"- **{name}**: Redis outages seen {m['redis']['outages']}, time degraded "
                      f"{m['redis']['downtime_s']} s, node ejections {m['ejections']}, retries {m['retries']}")
        md.append("")
    return "\n".join(md)


def main() -> None:
    with open(os.path.join(RES, "results.json")) as f:
        res = json.load(f)
    for name, fn in (("scaling", plot_scaling), ("network", plot_network), ("balancing", plot_balancing),
                     ("loadcurve", plot_loadcurve), ("quality", plot_quality), ("chaos", plot_chaos)):
        if name in res:
            fn(res[name])
            print(f"plots/{name}.png")
    with open(os.path.join(RES, "RESULTS.md"), "w") as f:
        f.write(markdown(res))
    print("results/RESULTS.md")


if __name__ == "__main__":
    main()
