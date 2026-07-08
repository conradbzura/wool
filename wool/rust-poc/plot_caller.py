"""Caller-side result: py-wool vs Rust-caller vs Rust-both (both sides Rust) vs
Ray at g=0. Shows both-sides-Rust beating/matching Ray, and the caller-alone
bridge tax on fan-out."""

import json

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

RESULTS = "benchmarks/results"
SHAPES = ["s1", "s2", "s3", "s4", "s5", "s6"]
NAME = {
    "s1": "point-to-point",
    "s2": "fan-out",
    "s3": "scatter-gather",
    "s4": "pipeline",
    "s5": "recursive-tree",
    "s6": "diamond",
}


def load():
    d = {}
    for line in open(f"{RESULTS}/rust_caller_vs_ray.jsonl"):
        r = json.loads(line)
        if r["makespan_p50_s"] is not None:
            d[(r["config"], r["shape"])] = r["makespan_p50_s"] * 1e6
    return d


def main():
    d = load()
    fig, ax = plt.subplots(figsize=(13, 5.4))
    x = np.arange(len(SHAPES))
    bw = 0.26
    series = [
        ("py-wool", "Python-wool", "#9AA0A6"),
        ("rust-caller", "Rust caller + Python worker", "#C58A3A"),
        ("rust-both", "Rust caller + Rust worker", "#2E7D32"),
    ]
    for j, (cfg, label, color) in enumerate(series):
        ys, xs = [], []
        for i, s in enumerate(SHAPES):
            if (cfg, s) in d and ("ray", s) in d:
                ys.append(d[(cfg, s)] / d[("ray", s)])
                xs.append(x[i] + (j - 1) * bw)
        ax.bar(xs, ys, bw, color=color, label=label)
        for xi, yi in zip(xs, ys):
            ax.text(
                xi,
                yi + 0.06,
                f"{yi:.1f}",
                ha="center",
                fontsize=7,
                fontweight="bold" if cfg == "rust-both" else "normal",
                color="#2E7D32" if cfg == "rust-both" else "0.3",
            )
    ax.axhline(1.0, color="#0E6E8C", lw=1.4, ls="--")
    ax.text(
        len(SHAPES) - 0.4, 1.05, "Ray parity", fontsize=8, color="#0E6E8C", ha="right"
    )
    ax.set_xticks(x)
    ax.set_xticklabels([f"{s}\n{NAME[s]}" for s in SHAPES], fontsize=8.5)
    ax.set_ylabel("makespan @ g=0, × slower than Ray (lower = closer)", fontsize=9)
    ax.set_title(
        "Rustifying the caller: both-sides-Rust wool beats/matches Ray on s1/s4/s5/s6\n"
        "(shapebench, W=4, g=0). Caller-alone hurts fan-out — the embedded bridge's per-dispatch "
        "GIL cost only pays with a Rust worker.",
        fontsize=11,
        fontweight="bold",
    )
    ax.legend(fontsize=9, loc="upper left")
    ax.grid(True, axis="y", alpha=0.25)
    fig.tight_layout()
    out = f"{RESULTS}/rust_caller_vs_ray.png"
    fig.savefig(out, dpi=140)
    print(f"wrote {out}")
    for s in SHAPES:
        row = "  ".join(
            f"{c}={d[(c, s)] / d[('ray', s)]:.2f}x" if (c, s) in d else f"{c}=n/a"
            for c in ("py-wool", "rust-caller", "rust-both")
        )
        print(f"{s}: {row}")


if __name__ == "__main__":
    main()
