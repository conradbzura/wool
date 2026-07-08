"""Worker-plane s1 RTT comparison (vertical slice v0): the Rust tonic worker +
Python executor vs Python worker planes, same grpc-aio client + same coroutine."""

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RESULTS = "benchmarks/results"

# (label, µs, color, note)
BARS = [
    ("Real WorkerService\n(dual-loop + chain, wool client)", 852, "#9AA0A6", "current"),
    ("Minimal Python worker\n(single-loop, coroutine inline)", 582, "#B4531F", ""),
    ("Bare grpc-aio echo\n(no routine — transport floor)", 530, "#C9C9C9", "ref"),
    (
        "Rust tonic worker + Python executor\n(+ process-boundary IPC hop)",
        398,
        "#2E7D32",
        "-32%",
    ),
]


def main():
    fig, ax = plt.subplots(figsize=(11, 4.6))
    labels = [b[0] for b in BARS]
    vals = [b[1] for b in BARS]
    colors = [b[2] for b in BARS]
    y = range(len(BARS))
    ax.barh(list(y), vals, color=colors, height=0.62)
    for i, (lbl, v, c, note) in enumerate(BARS):
        ax.text(
            v + 8,
            i,
            f"{v} µs" + (f"  ({note})" if note else ""),
            va="center",
            fontsize=9,
            fontweight="bold" if note == "-32%" else "normal",
        )
    # bridge annotation on the Rust bar
    ax.text(
        398 / 2,
        3,
        "grpc-aio client ≈300µs  |  tonic+bridge ≈98µs\n(bridge/IPC ≈80µs)",
        va="center",
        ha="center",
        fontsize=7.5,
        color="white",
        fontweight="bold",
    )
    ax.set_yticks(list(y))
    ax.set_yticklabels(labels, fontsize=8.5)
    ax.invert_yaxis()
    ax.set_xlabel(
        "s1 dispatch RTT (µs, min; same grpc-aio client, same coroutine, same session)",
        fontsize=9,
    )
    ax.set_xlim(0, 960)
    ax.set_title(
        "Rustified worker plane — vertical slice v0 (coroutine dispatch)\n"
        "Rust owns transport+serdes+FSM; Python executor keeps the GIL (cloudpickle + loop)",
        fontsize=11,
        fontweight="bold",
    )
    ax.grid(True, axis="x", alpha=0.25)
    fig.tight_layout()
    out = f"{RESULTS}/worker_plane_v0.png"
    fig.savefig(out, dpi=140)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
