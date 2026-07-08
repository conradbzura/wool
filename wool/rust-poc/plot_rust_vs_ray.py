"""Three-way shapebench result: Python-wool vs Rust-wool vs Ray at g=0. Shows the
Rust worker plane closing the wool->Ray latency gap (makespan relative to Ray)."""

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
G = 0.0


def load():
    d = {}
    for line in open(f"{RESULTS}/rust_vs_ray.jsonl"):
        r = json.loads(line)
        if r["granularity_s"] == G and r["makespan_p50_s"] is not None:
            d[(r["framework"], r["shape"])] = r["makespan_p50_s"] * 1e6
    return d


def main():
    d = load()
    fig, ax = plt.subplots(figsize=(12.5, 5.2))
    x = np.arange(len(SHAPES))
    bw = 0.38
    wool = [d[("wool", s)] / d[("ray", s)] for s in SHAPES]
    rust = [d[("rust-wool", s)] / d[("ray", s)] for s in SHAPES]
    ax.bar(x - bw / 2, wool, bw, color="#9AA0A6", label="Python-wool worker")
    ax.bar(
        x + bw / 2,
        rust,
        bw,
        color="#2E7D32",
        label="Rust-wool worker (tonic + executor)",
    )
    for i, s in enumerate(SHAPES):
        ax.text(
            x[i] - bw / 2, wool[i] + 0.08, f"{wool[i]:.1f}×", ha="center", fontsize=7.5
        )
        ax.text(
            x[i] + bw / 2,
            rust[i] + 0.08,
            f"{rust[i]:.1f}×",
            ha="center",
            fontsize=8,
            fontweight="bold",
            color="#2E7D32",
        )
    ax.axhline(1.0, color="#0E6E8C", lw=1.4, ls="--")
    ax.text(
        len(SHAPES) - 0.5,
        1.05,
        "Ray parity",
        fontsize=8,
        color="#0E6E8C",
        va="bottom",
        ha="right",
    )
    ax.set_xticks(x)
    ax.set_xticklabels([f"{s}\n{NAME[s]}" for s in SHAPES], fontsize=8.5)
    ax.set_ylabel(
        "makespan @ g=0, × slower than Ray (lower = closer to Ray)", fontsize=9
    )
    ax.set_title(
        "Rust worker plane closes the wool→Ray gap\n"
        "shapebench, W=4, g=0, same session — the Rust worker makes wool 1.6–2.9× faster "
        "and near-parity with Ray on s1/s4/s5",
        fontsize=11.5,
        fontweight="bold",
    )
    ax.legend(fontsize=9, loc="upper left")
    ax.grid(True, axis="y", alpha=0.25)
    ax.set_ylim(0, max(wool) * 1.12)
    fig.tight_layout()
    out = f"{RESULTS}/rust_vs_ray.png"
    fig.savefig(out, dpi=140)
    print(f"wrote {out}")
    print("\nshape   pywool   rustwool   ray   |  wool/ray  rust/ray  rust-vs-pywool")
    for s in SHAPES:
        pw, rw, ry = d[("wool", s)], d[("rust-wool", s)], d[("ray", s)]
        print(
            f"{s:6} {pw:8.0f} {rw:9.0f} {ry:7.0f}  |  {pw / ry:7.2f}  {rw / ry:7.2f}   {pw / rw:.2f}x"
        )


if __name__ == "__main__":
    main()
