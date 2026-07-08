"""Compare Wool (base / per-worker-lock) against Ray on the same knob sweep.

Produces:
  results/scaling_compare.png  self-speedup vs workers for base/pwl/ray (S2,S3),
                               with each series' fitted Amdahl ceiling.
  results/gap_heatmap.png      Wool(+PWL) / Ray makespan ratio across (shape,size,W).
and prints the Amdahl serial-fraction / ceiling table.
"""

import json

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

RESULTS = "benchmarks/results"
W = [1, 2, 4, 8]
SHAPE_NAME = {
    "s1": "point-to-point",
    "s2": "fan-out",
    "s3": "scatter-gather",
    "s4": "pipeline",
    "s6": "diamond",
    "s7": "streaming",
}


def load(path):
    d = {}
    for line in open(path):
        r = json.loads(line)
        if r.get("p50_s"):
            d[(r["shape"], r["size"], r["workers"])] = r["p50_s"]
    return d


def amdahl(ws, speeds):
    ws, speeds = np.array(ws, float), np.array(speeds, float)
    best_s, best_err = 1.0, np.inf
    for s in np.linspace(0.001, 0.99, 990):
        pred = 1.0 / (s + (1 - s) / ws)
        err = float(np.sum((pred - speeds) ** 2))
        if err < best_err:
            best_s, best_err = s, err
    return best_s, 1.0 / best_s


def self_speedup(data, shape, size):
    m1 = data[(shape, size, 1)]
    return [m1 / data[(shape, size, w)] for w in W]


def main():
    base = load(f"{RESULTS}/sweep_base.jsonl")
    pwl = load(f"{RESULTS}/sweep_pwl.jsonl")
    ray = load(f"{RESULTS}/sweep_ray.jsonl")
    series = {
        "Wool base": (base, "tab:red"),
        "Wool +PWL": (pwl, "tab:orange"),
        "Ray": (ray, "tab:blue"),
    }

    # --- Figure 1: self-scaling overlay ---
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.6))
    for ax, shape in zip(axes, ["s2", "s3"]):
        for name, (data, color) in series.items():
            try:
                sp = self_speedup(data, shape, 128)
            except KeyError:
                continue
            _, ceiling = amdahl(W, sp)
            ax.plot(W, sp, "o-", color=color, label=f"{name} (ceil≈{ceiling:.1f}×)")
        ax.plot(W, W, "k--", alpha=0.4, label="ideal ∝W")
        ax.set_xscale("log", base=2)
        ax.set_yscale("log", base=2)
        ax.set_xlabel("workers W")
        ax.set_ylabel("self-speedup  makespan(W=1) / makespan(W)")
        ax.set_title(f"{shape} {SHAPE_NAME[shape]} (n=128): scaling with workers")
        ax.legend(fontsize=8)
        ax.grid(True, which="both", alpha=0.3)
    fig.tight_layout()
    fig.savefig(f"{RESULTS}/scaling_compare.png", dpi=130)

    # --- Figure 2: Wool(+PWL) / Ray makespan-ratio heatmap ---
    shapes = ["s1", "s2", "s3", "s4", "s6", "s7"]
    rows, labels = [], []
    for shape in shapes:
        sizes = sorted(
            {k[1] for k in ray if k[0] == shape}, key=lambda s: (s is None, s)
        )
        for size in sizes:
            row = []
            for w in W:
                a, b = pwl.get((shape, size, w)), ray.get((shape, size, w))
                row.append(a / b if a and b else np.nan)
            rows.append(row)
            sz = "" if size is None else f" {'d' if shape == 's4' else 'n'}={size}"
            labels.append(f"{shape} {SHAPE_NAME[shape]}{sz}")
    mat = np.array(rows)
    fig2, ax = plt.subplots(figsize=(6, 0.34 * len(labels) + 1.2))
    im = ax.imshow(mat, aspect="auto", cmap="Reds", vmin=1.0, vmax=np.nanmax(mat))
    ax.set_xticks(range(len(W)), [f"W={w}" for w in W])
    ax.set_yticks(range(len(labels)), labels, fontsize=7)
    for i in range(mat.shape[0]):
        for j in range(mat.shape[1]):
            v = mat[i, j]
            if not np.isnan(v):
                ax.text(j, i, f"{v:.1f}", ha="center", va="center", fontsize=6.5)
    ax.set_title("Wool(+PWL) / Ray makespan ratio, g=0  (>1 = Wool slower)", fontsize=10)
    fig2.colorbar(im, ax=ax, label="× slower than Ray", shrink=0.6)
    fig2.tight_layout()
    fig2.savefig(f"{RESULTS}/gap_heatmap.png", dpi=130)

    # --- Amdahl comparison table ---
    print(f"\n{'series':<14}{'shape':<18}{'serial s':>10}{'ceiling':>10}{'peak@W8':>10}")
    print("-" * 62)
    for name, (data, _) in series.items():
        for shape in ["s2", "s3"]:
            try:
                sp = self_speedup(data, shape, 128)
            except KeyError:
                continue
            s, ceiling = amdahl(W, sp)
            print(
                f"{name:<14}{shape + ' ' + SHAPE_NAME[shape]:<18}"
                f"{s:>10.2f}{ceiling:>9.1f}×{sp[-1]:>9.1f}×"
            )
    print(f"\nwrote {RESULTS}/scaling_compare.png and {RESULTS}/gap_heatmap.png")


if __name__ == "__main__":
    main()
