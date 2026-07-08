"""Re-plot the nested vs flat experiment at matched N=256, using the saved Wool
data and fresh Ray probes (subprocess-isolated, so a Ray hang can't stall us).

Mechanism, established by magnitude from the Wool data: nested at W=1 costs ~15ms
= ~16 dispatches, not the ~195ms (272 dispatches) it would cost if every leaf
round-tripped. So a nested routine's inner calls execute LOCALLY on the worker.
Nesting therefore coarsens N fine-grained driver dispatches into `branch` coarse
ones (each running N/branch leaves in-process) — the driver's serial dispatch
count, i.e. the Amdahl serial fraction, drops from N to branch. Ray keeps every
leaf a distributed task and blocks a worker slot per internal node, so the same
restructuring makes Ray far slower (and deadlock-prone at depth).
"""

import json
import subprocess
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RESULTS = "benchmarks/results"
WORKERS = [1, 2, 4, 8]
N, BRANCH = 256, 16


def ray_probe(mode, n, branch, w, timeout=35.0):
    try:
        r = subprocess.run(
            [
                sys.executable,
                "benchmarks/ray_nested_probe.py",
                mode,
                str(n),
                str(branch),
                str(w),
            ],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        for line in r.stdout.splitlines():
            if line.startswith("OK"):
                return float(line.split()[1])
        return None
    except subprocess.TimeoutExpired:
        return "DEADLOCK"


def main():
    wool = json.load(open(f"{RESULTS}/nested.json"))["wool"]
    wf = {int(k): v * 1e3 for k, v in wool["flat"].items()}  # -> ms
    wn = {int(k): v * 1e3 for k, v in wool["nested"].items()}

    print(f"Ray probes at matched N={N}, branch={BRANCH}:")
    rf = {w: ray_probe("flat", N, BRANCH, w) for w in WORKERS}
    print("  flat:   " + "  ".join(f"W{w}={rf[w]}" for w in WORKERS))
    rn = {w: ray_probe("nested", N, BRANCH, w) for w in (4, 8)}
    print(f"  nested: W4={rn[4]}  W8={rn[8]}")

    base = wf[1]  # flat @ W=1 — common speedup baseline
    fig, (axA, axB) = plt.subplots(1, 2, figsize=(12, 4.6))

    axA.plot(WORKERS, [wf[w] for w in WORKERS], "o-", color="tab:red", label="Wool flat")
    axA.plot(
        WORKERS, [wn[w] for w in WORKERS], "s-", color="tab:green", label="Wool nested"
    )
    rfx = [(w, rf[w]) for w in WORKERS if isinstance(rf[w], float)]
    if rfx:
        axA.plot(
            [w for w, _ in rfx],
            [v for _, v in rfx],
            "^-",
            color="tab:blue",
            label="Ray flat",
        )
    rnx = [(w, rn[w]) for w in (4, 8) if isinstance(rn[w], float)]
    if rnx:
        axA.plot(
            [w for w, _ in rnx],
            [v for _, v in rnx],
            "x--",
            color="tab:purple",
            label="Ray nested",
        )
    axA.set_xscale("log", base=2)
    axA.set_yscale("log", base=2)
    axA.set_xlabel("workers W")
    axA.set_ylabel("makespan (ms)  —  lower is better")
    axA.set_title(f"Fan-out of N={N} tasks: makespan vs workers")
    axA.legend(fontsize=8)
    axA.grid(True, which="both", alpha=0.3)

    axB.plot(
        WORKERS,
        [base / wf[w] for w in WORKERS],
        "o-",
        color="tab:red",
        label="Wool flat",
    )
    axB.plot(
        WORKERS,
        [base / wn[w] for w in WORKERS],
        "s-",
        color="tab:green",
        label="Wool nested",
    )
    axB.plot(WORKERS, WORKERS, "k--", alpha=0.4, label="ideal ∝W")
    axB.annotate(
        "flat plateaus ~2×\n(single-caller Amdahl wall)",
        xy=(8, base / wf[8]),
        xytext=(1.6, 3.4),
        fontsize=8,
        color="tab:red",
        arrowprops=dict(arrowstyle="->", color="tab:red", alpha=0.6),
    )
    axB.set_xscale("log", base=2)
    axB.set_yscale("log", base=2)
    axB.set_xlabel("workers W")
    axB.set_ylabel("speedup vs flat @ W=1")
    axB.set_title("Nesting coarsens dispatch and clears the flat ceiling")
    axB.legend(fontsize=8)
    axB.grid(True, which="both", alpha=0.3)
    fig.tight_layout()
    fig.savefig(f"{RESULTS}/nested.png", dpi=130)

    peak_nested = base / min(wn.values())
    print(
        f"\nWool flat plateau {base / wf[8]:.1f}×  |  Wool nested peak {peak_nested:.1f}× vs flat@W1"
        f"  |  nested/flat @W8: {wf[8] / wn[8]:.1f}× faster"
    )
    if isinstance(rf[8], float) and isinstance(rn[8], float):
        print(
            f"Ray nested/flat @W8: {rn[8] / rf[8]:.0f}× SLOWER (opposite response to nesting)"
        )
    print(f"wrote {RESULTS}/nested.png")


if __name__ == "__main__":
    main()
