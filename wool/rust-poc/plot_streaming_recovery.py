"""Streaming per-yield RTT: the executor-push bridge recovers (and beats) the
per-yield bridge tax that the naive pull bridge incurred."""

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RESULTS = "benchmarks/results"

# (label, ns/yield, color, note)
BARS = [
    ("Python worker\n(drives gen in-process)", 211, "#B4531F", "baseline"),
    (
        "Rust worker — PULL bridge\n(IPC round-trip per yield)",
        234,
        "#9AA0A6",
        "+11% (tax)",
    ),
    (
        "Rust worker — PUSH bridge\n(executor produces ahead, forwards per Next)",
        136,
        "#2E7D32",
        "-35%",
    ),
]


def main():
    fig, ax = plt.subplots(figsize=(10.5, 4.2))
    labels = [b[0] for b in BARS]
    vals = [b[1] for b in BARS]
    colors = [b[2] for b in BARS]
    y = list(range(len(BARS)))
    ax.barh(y, vals, color=colors, height=0.6)
    for i, (_l, v, _c, note) in enumerate(BARS):
        ax.text(
            v + 3,
            i,
            f"{v} µs/yield  ({note})",
            va="center",
            fontsize=9,
            fontweight="bold" if note in ("-35%", "+11% (tax)") else "normal",
        )
    ax.axvline(211, color="#B4531F", ls=":", lw=1, alpha=0.6)
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=8.5)
    ax.invert_yaxis()
    ax.set_xlim(0, 290)
    ax.set_xlabel(
        "streaming per-yield RTT (µs, n=8, same grpc-aio client, same session)",
        fontsize=9,
    )
    ax.set_title(
        "Streaming bridge: pull vs push\n"
        "Executor produces yields ahead → IPC overlaps the gRPC round-trip → tax recovered",
        fontsize=11,
        fontweight="bold",
    )
    ax.grid(True, axis="x", alpha=0.25)
    fig.tight_layout()
    out = f"{RESULTS}/streaming_recovery.png"
    fig.savefig(out, dpi=140)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
