"""Join the base and per-worker-lock sweeps into speedups, render a heatmap, and
fit the functional form of the speedup along each knob (log-log exponents).

    ./.venv/bin/python benchmarks/analyze.py

Writes results/heatmap.png and results/scaling.png; prints a fits table.
"""

import collections
import json

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

RESULTS = "benchmarks/results"
SHAPE_ORDER = ["s1", "s2", "s3", "s4", "s5", "s6", "s7"]
SHAPE_NAME = {
    "s1": "point-to-point",
    "s2": "fan-out",
    "s3": "scatter-gather",
    "s4": "pipeline",
    "s5": "recursive-tree",
    "s6": "diamond",
    "s7": "streaming",
}


def load(path):
    by = {}
    for line in open(path):
        r = json.loads(line)
        by[(r["shape"], r["size"], r["workers"])] = r
    return by


def classify_power(exp):
    if exp >= 1.7:
        return "quadratic+" if exp < 2.3 else f"~n^{exp:.1f}"
    if 0.85 <= exp < 1.7:
        return "linear" if exp < 1.35 else "super-linear"
    if 0.4 <= exp < 0.85:
        return "sub-linear"
    return "flat"


def powfit(xs, ys):
    """Fit y ~ x^a in log-log; return (exponent, r2)."""
    lx, ly = np.log(np.array(xs, float)), np.log(np.array(ys, float))
    a, b = np.polyfit(lx, ly, 1)
    pred = a * lx + b
    ss_res = np.sum((ly - pred) ** 2)
    ss_tot = np.sum((ly - ly.mean()) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 1.0
    return a, r2


def amdahl_fit(ws, speeds):
    """Fit speedup(W) = 1/(s + (1-s)/W); return (serial_fraction s, ceiling 1/s).

    Speedup saturating below linear is Amdahl's law: s is the un-parallelizable
    fraction (here, the single caller's serial per-dispatch CPU), and 1/s is the
    hard ceiling no worker count can beat.
    """
    ws = np.array(ws, float)
    speeds = np.array(speeds, float)
    grid = np.linspace(0.001, 0.99, 990)
    best_s, best_err = 1.0, np.inf
    for s in grid:
        pred = 1.0 / (s + (1 - s) / ws)
        err = float(np.sum((pred - speeds) ** 2))
        if err < best_err:
            best_s, best_err = s, err
    return best_s, 1.0 / best_s


def main():
    base = load(f"{RESULTS}/sweep_base.jsonl")
    pwl = load(f"{RESULTS}/sweep_pwl.jsonl")
    workers = sorted({k[2] for k in base})

    # --- build (shape,size) rows and the speedup matrix ---
    rows, labels = [], []
    for shape in SHAPE_ORDER:
        sizes = sorted(
            {k[1] for k in base if k[0] == shape}, key=lambda s: (s is None, s)
        )
        for size in sizes:
            speeds = []
            for w in workers:
                b = base.get((shape, size, w))
                p = pwl.get((shape, size, w))
                if b and p and b["p50_s"] and p["p50_s"]:
                    speeds.append(b["p50_s"] / p["p50_s"])
                else:
                    speeds.append(np.nan)
            rows.append(speeds)
            sz = (
                ""
                if size is None
                else f" {'d' if shape in ('s4', 's5') else 'n'}={size}"
            )
            labels.append(f"{shape} {SHAPE_NAME[shape]}{sz}")
    mat = np.array(rows)

    # --- Figure 1: speedup heatmap ---
    fig, ax = plt.subplots(figsize=(6.2, 0.34 * len(labels) + 1.2))
    im = ax.imshow(
        mat, aspect="auto", cmap="RdYlGn", vmin=1.0, vmax=max(2.0, np.nanmax(mat))
    )
    ax.set_xticks(range(len(workers)), [f"W={w}" for w in workers])
    ax.set_yticks(range(len(labels)), labels, fontsize=7)
    for i in range(mat.shape[0]):
        for j in range(mat.shape[1]):
            v = mat[i, j]
            if not np.isnan(v):
                ax.text(
                    j,
                    i,
                    f"{v:.1f}",
                    ha="center",
                    va="center",
                    fontsize=6.5,
                    color="black",
                )
    ax.set_title("Per-worker-lock speedup (base / PWL makespan), g=0", fontsize=10)
    fig.colorbar(im, ax=ax, label="speedup ×", shrink=0.6)
    fig.tight_layout()
    fig.savefig(f"{RESULTS}/heatmap.png", dpi=130)

    # --- Figure 2: scaling line plots for the fan-out shape (s2) ---
    fig2, (axA, axB) = plt.subplots(1, 2, figsize=(11, 4.2))
    s2sizes = sorted({k[1] for k in base if k[0] == "s2"})
    # (A) speedup vs W, one line per n
    for n in s2sizes:
        ys = [base[("s2", n, w)]["p50_s"] / pwl[("s2", n, w)]["p50_s"] for w in workers]
        axA.plot(workers, ys, "o-", label=f"n={n}")
    axA.plot(workers, workers, "k--", alpha=0.5, label="ideal ∝W")
    axA.set_xscale("log", base=2)
    axA.set_yscale("log", base=2)
    axA.set_xlabel("workers W")
    axA.set_ylabel("speedup ×")
    axA.set_title("S2 fan-out: speedup vs workers")
    axA.legend(fontsize=8)
    axA.grid(True, which="both", alpha=0.3)
    # (B) makespan vs n, base vs pwl at W=4
    w4 = 4 if 4 in workers else workers[-1]
    nb = [base[("s2", n, w4)]["p50_s"] * 1e3 for n in s2sizes]
    npwl = [pwl[("s2", n, w4)]["p50_s"] * 1e3 for n in s2sizes]
    axB.plot(s2sizes, nb, "o-", label="base")
    axB.plot(s2sizes, npwl, "s-", label="per-worker lock")
    axB.set_xscale("log", base=2)
    axB.set_yscale("log", base=2)
    axB.set_xlabel("fan-out width n")
    axB.set_ylabel(f"makespan (ms), W={w4}")
    axB.set_title("S2 fan-out: makespan vs n")
    axB.legend(fontsize=8)
    axB.grid(True, which="both", alpha=0.3)
    fig2.tight_layout()
    fig2.savefig(f"{RESULTS}/scaling.png", dpi=130)

    # --- fits table ---
    print(
        f"\n{'shape':<24}{'speedup vs W (Amdahl, max size)':<34}{'makespan~size^b base|pwl (W=%d)' % w4}"
    )
    print("-" * 96)
    for shape in SHAPE_ORDER:
        sizes = sorted(
            {k[1] for k in base if k[0] == shape}, key=lambda s: (s is None, s)
        )
        maxsize = sizes[-1]
        # speedup vs W at max size, fit as Amdahl
        sp = [
            base[(shape, maxsize, w)]["p50_s"] / pwl[(shape, maxsize, w)]["p50_s"]
            for w in workers
        ]
        s, ceiling = amdahl_fit(workers, sp)
        peak = max(sp)
        wcol = f"s={s:.2f} ceiling≈{ceiling:.1f}x (peak {peak:.1f}x@W{workers[sp.index(peak)]})"
        # makespan vs size at W=4 (only if shape has a real size knob)
        if maxsize is not None and len(sizes) > 2:
            szs = [s2 for s2 in sizes if s2 is not None]
            bb, _ = powfit(szs, [base[(shape, s2, w4)]["p50_s"] for s2 in szs])
            pp, _ = powfit(szs, [pwl[(shape, s2, w4)]["p50_s"] for s2 in szs])
            ncol = f"b={bb:.2f}|{pp:.2f} [{classify_power(bb)}|{classify_power(pp)}]"
        else:
            ncol = "(no size knob)"
        print(f"{shape + ' ' + SHAPE_NAME[shape]:<24}{wcol:<34}{ncol}")
    print(f"\nwrote {RESULTS}/heatmap.png and {RESULTS}/scaling.png")


if __name__ == "__main__":
    main()
