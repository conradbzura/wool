"""Capstone: the s1 dispatch RTT decomposition — the diagnosis that reframed the
whole exploration. The worker orchestration (the named target) is ~7%; the
transport is the cost."""

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RESULTS = "benchmarks/results"

# (segment, µs, color, group)
SEG = [
    ("gRPC stream setup\n(per dispatch)", 200, "#0E6E8C"),
    ("gRPC warm exchanges (×2)", 280, "#1C88A6"),
    ("caller pickle + cross-process\n+ two-sided asyncio", 300, "#4FB0C6"),
    ("worker orchestration\n(queues/FSM) — the target", 60, "#B4531F"),
    ("proxy unpickle", 12, "#7A2E10"),
]


def main():
    fig, ax = plt.subplots(figsize=(12, 2.9))
    left = 0
    for label, v, c in SEG:
        ax.barh(0, v, left=left, color=c, edgecolor="white", height=0.5)
        if v >= 40:
            ax.text(
                left + v / 2,
                0,
                f"{label}\n{v}µs",
                ha="center",
                va="center",
                fontsize=7.5,
                color="white",
                fontweight="bold",
            )
        left += v
    ax.text(
        240,
        0.42,
        "TRANSPORT ≈ 65%",
        ha="center",
        fontsize=11,
        fontweight="bold",
        color="#0E6E8C",
    )
    ax.annotate(
        "orchestration ≈ 7%\n(the named refactor target)",
        xy=(810, 0),
        xytext=(700, -0.5),
        fontsize=8.5,
        color="#B4531F",
        fontweight="bold",
        ha="center",
        arrowprops=dict(arrowstyle="->", color="#B4531F", lw=1.2),
    )
    ax.set_xlim(0, 900)
    ax.set_ylim(-0.7, 0.7)
    ax.set_yticks([])
    ax.set_xlabel("s1 dispatch RTT = 852µs (real wool worker, g=0, warm)", fontsize=9.5)
    ax.set_title(
        "Diagnosis: where the dispatch latency actually is\n"
        "The transport is the cost — not the worker orchestration the refactor was aimed at",
        fontsize=11.5,
        fontweight="bold",
    )
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    fig.tight_layout()
    out = f"{RESULTS}/rtt_decomposition.png"
    fig.savefig(out, dpi=140)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
