"""Two-panel stacked-latency figure:
A) makespan at g=0 relative to baseline (%), cumulative across the 3 opts, with a
   same-session Ray reference marker — shows the gains stacking up.
B) incremental µs attribution per opt per shape — shows which idea dominates which
   topology (eager->sequential chains, pickle->fan-out, proxy->critical path).
"""

import json
import os
import statistics

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

RESULTS = "benchmarks/results"
CONFIGS = ["baseline", "+eager", "+eager+pickle", "+eager+pickle+proxy"]
CLABEL = {
    "baseline": "baseline",
    "+eager": "+#274 eager",
    "+eager+pickle": "+#273 pickle-memo",
    "+eager+pickle+proxy": "+proxy-memo",
}
BARC = {
    "baseline": "#9AA0A6",
    "+eager": "#C58A3A",
    "+eager+pickle": "#B4531F",
    "+eager+pickle+proxy": "#7A2E10",
}
SHAPES = ["s1", "s2", "s3", "s4", "s5", "s6", "s7"]
NAME = {
    "s1": "point-to-point",
    "s2": "fan-out",
    "s3": "scatter-gather",
    "s4": "pipeline",
    "s5": "recursive-tree",
    "s6": "diamond",
    "s7": "streaming",
}
G = 0.0


def med_wool():
    agg = {}
    for line in open(f"{RESULTS}/stacked_latency.jsonl"):
        r = json.loads(line)
        if r["granularity_s"] == G and r["makespan_p50_s"] is not None:
            agg.setdefault((r["config"], r["shape"]), []).append(
                r["makespan_p50_s"] * 1e6
            )
    return {k: statistics.median(v) for k, v in agg.items()}


def ray_ref():
    path = f"{RESULTS}/ray_stacked_ref.jsonl"
    if not os.path.exists(path):
        return {}
    out = {}
    for line in open(path):
        r = json.loads(line)
        if r.get("granularity_s") == G and r.get("makespan_p50_s") is not None:
            out[r["shape"]] = r["makespan_p50_s"] * 1e6
    return out


def main():
    w = med_wool()
    ray = ray_ref()
    fig, (axA, axB) = plt.subplots(1, 2, figsize=(15, 6.2))

    # Panel A: normalized to wool baseline = 100%
    x = np.arange(len(SHAPES))
    bw = 0.2
    for j, c in enumerate(CONFIGS):
        ys = [100 * w[(c, s)] / w[("baseline", s)] for s in SHAPES]
        axA.bar(x + (j - 1.5) * bw, ys, bw, color=BARC[c], label=CLABEL[c])
    # total-Δ labels on the last bar
    for i, s in enumerate(SHAPES):
        full = 100 * w[("+eager+pickle+proxy", s)] / w[("baseline", s)]
        axA.text(
            x[i] + 1.5 * bw,
            full - 3,
            f"-{100 - full:.0f}%",
            ha="center",
            va="top",
            fontsize=7.5,
            fontweight="bold",
            color="white",
        )
    if ray:
        ry = [100 * ray[s] / w[("baseline", s)] for s in SHAPES if s in ray]
        rx = [x[i] for i, s in enumerate(SHAPES) if s in ray]
        axA.scatter(
            rx,
            ry,
            marker="D",
            color="#0E6E8C",
            s=55,
            zorder=5,
            label="Ray 2.56 (ref)",
            edgecolor="white",
            linewidth=0.6,
        )
    axA.axhline(100, color="0.3", lw=1)
    axA.set_xticks(x)
    axA.set_xticklabels([f"{s}\n{NAME[s]}" for s in SHAPES], fontsize=8)
    axA.set_ylabel("makespan @ g=0, % of wool baseline (lower = faster)", fontsize=9)
    axA.set_title(
        "Stacked dispatch opts — cumulative latency (W=4, p50, same session)",
        fontsize=11,
        fontweight="bold",
    )
    axA.legend(fontsize=8, loc="lower left", ncol=1)
    axA.grid(True, axis="y", alpha=0.25)
    axA.set_ylim(0, 118)

    # Panel B: incremental µs attribution, stacked
    eager = [w[("baseline", s)] - w[("+eager", s)] for s in SHAPES]
    pickle = [w[("+eager", s)] - w[("+eager+pickle", s)] for s in SHAPES]
    proxy = [w[("+eager+pickle", s)] - w[("+eager+pickle+proxy", s)] for s in SHAPES]
    ypos = np.arange(len(SHAPES))[::-1]
    axB.barh(ypos, eager, color="#C58A3A", label="#274 eager-first-next")
    left = np.array(eager, dtype=float)
    axB.barh(ypos, pickle, left=left, color="#B4531F", label="#273 pickle-memo")
    left = left + np.array(pickle, dtype=float)
    axB.barh(ypos, proxy, left=left, color="#7A2E10", label="worker proxy-memo")
    totals = [e + p + q for e, p, q in zip(eager, pickle, proxy)]
    for y, t in zip(ypos, totals):
        axB.text(
            max(t, 0) + max(totals) * 0.01,
            y,
            f"{t:,.0f} µs",
            va="center",
            fontsize=8,
            fontweight="bold",
        )
    axB.axvline(0, color="0.3", lw=1)
    axB.set_yticks(ypos)
    axB.set_yticklabels([f"{s} {NAME[s]}" for s in SHAPES], fontsize=8)
    axB.set_xlabel("latency saved at g=0 (µs), by opt", fontsize=9)
    axB.set_title("Which idea wins which topology", fontsize=11, fontweight="bold")
    axB.legend(fontsize=8, loc="lower right")
    axB.grid(True, axis="x", alpha=0.25)

    fig.suptitle(
        "Wool 0.12.0-rc0 — stacked cancellation-safe dispatch-latency opts "
        "(shapebench, W=4, g=0)",
        fontsize=12.5,
        fontweight="bold",
    )
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    out = f"{RESULTS}/stacked_latency.png"
    fig.savefig(out, dpi=140)
    print(f"wrote {out}")
    for s in SHAPES:
        base = w[("baseline", s)]
        full = w[("+eager+pickle+proxy", s)]
        rref = f"  ray={ray[s]:.0f}us" if s in ray else ""
        print(
            f"  {s}: {base:.0f} -> {full:.0f} us (-{100 * (base - full) / base:.0f}%){rref}"
        )


if __name__ == "__main__":
    main()
