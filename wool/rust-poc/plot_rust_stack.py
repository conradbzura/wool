"""Full Rust stack (Rust caller + Rust worker) vs Ray — canonical shape ladder,
same layout as v12_combo_vs_ray.png: 7 R-vs-granularity panels + a g=0
dispatch-overhead gap panel.
"""

import json

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

RESULTS = "benchmarks/results"
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
WOOL_C = "#2E7D32"  # full Rust stack (green)
RAY_C = "#0E6E8C"  # teal


def load(fw):
    by = {}
    for line in open(f"{RESULTS}/rust_stack_sweep.jsonl"):
        r = json.loads(line)
        if r["framework"] == fw:
            by[(r["shape"], r["granularity_s"])] = r
    return by


def main():
    wool = load("wool-full-rust")
    ray = load("ray")
    gs = sorted({g for (_s, g) in wool if g > 0})

    fig, axes = plt.subplots(2, 4, figsize=(15, 7.2))
    axes = axes.flatten()

    for i, s in enumerate(SHAPES):
        ax = axes[i]
        gg = [g * 1e6 for g in gs]
        wR = [
            wool[(s, g)]["overhead_ratio"]
            for g in gs
            if (s, g) in wool and wool[(s, g)]["overhead_ratio"]
        ]
        wx = [g * 1e6 for g in gs if (s, g) in wool and wool[(s, g)]["overhead_ratio"]]
        rR = [
            ray[(s, g)]["overhead_ratio"]
            for g in gs
            if (s, g) in ray and ray[(s, g)]["overhead_ratio"]
        ]
        rx = [g * 1e6 for g in gs if (s, g) in ray and ray[(s, g)]["overhead_ratio"]]
        ax.plot(wx, wR, "o-", color=WOOL_C, lw=2, ms=6, label="Wool full Rust stack")
        ax.plot(rx, rR, "s--", color=RAY_C, lw=2, ms=6, label="Ray 2.56.0")
        ax.axhline(2.0, color="0.4", ls=":", lw=1.2)
        ax.text(
            gg[0], 2.05, "R=2 (METG, 50% eff.)", fontsize=7, color="0.4", va="bottom"
        )
        ax.set_xscale("log")
        ax.set_yscale("log")
        title = f"{s}  {NAME[s]}" + ("  (caller=Py)" if s == "s7" else "")
        ax.set_title(title, fontsize=11, fontweight="bold")
        ax.set_xlabel("granularity g (µs)", fontsize=8)
        ax.set_ylabel("overhead ratio R", fontsize=8)
        ax.grid(True, which="both", alpha=0.22)
        ax.tick_params(labelsize=8)
        if i == 0:
            ax.legend(fontsize=8, loc="upper right")

    ax = axes[7]
    ratios, shown = [], []
    for s in SHAPES:
        if (
            (s, 0.0) in wool
            and (s, 0.0) in ray
            and wool[(s, 0.0)]["makespan_p50_s"]
            and ray[(s, 0.0)]["makespan_p50_s"]
        ):
            ratios.append(
                wool[(s, 0.0)]["makespan_p50_s"] / ray[(s, 0.0)]["makespan_p50_s"]
            )
            shown.append(s)
    ypos = np.arange(len(shown))[::-1]
    colors = [WOOL_C if r <= 1.0 else "#B4531F" for r in ratios]
    ax.barh(ypos, ratios, color=colors, alpha=0.9, height=0.62)
    ax.axvline(1.0, color="k", lw=1.2)
    ax.text(1.02, len(shown) - 0.3, "Ray parity", fontsize=7, rotation=90, va="top")
    for y, r, s in zip(ypos, ratios, shown):
        ax.text(r + 0.05, y, f"{r:.2f}×", va="center", fontsize=8, fontweight="bold")
    ax.set_yticks(ypos)
    ax.set_yticklabels([f"{s} {NAME[s]}" for s in shown], fontsize=8)
    ax.set_xlim(0, max(ratios) * 1.18)
    ax.set_xlabel("g=0 makespan, × slower than Ray", fontsize=8)
    ax.set_title("Dispatch-overhead gap (g=0)", fontsize=11, fontweight="bold")
    ax.grid(True, axis="x", alpha=0.22)
    ax.tick_params(labelsize=8)

    fig.suptitle(
        "Wool full Rust stack (Rust caller + Rust worker) vs Ray 2.56.0 — "
        "canonical shape ladder (W=4, p50, warm)",
        fontsize=12.5,
        fontweight="bold",
    )
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    out = f"{RESULTS}/rust_stack_vs_ray.png"
    fig.savefig(out, dpi=140)
    print(f"wrote {out}")
    print("g=0 wool-full-rust / Ray:", {s: round(r, 2) for s, r in zip(shown, ratios)})


if __name__ == "__main__":
    main()
