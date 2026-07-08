"""Nested fan-out vs flat fan-out — clearing the single-caller Amdahl ceiling.

Flat fan-out issues all N dispatches from one driver process: one event loop, a
serial per-dispatch CPU cost that caps self-speedup near ~2x no matter how many
workers (the Amdahl wall from the earlier sweep). Restructured as a shallow tree,
the driver issues only `branch` coarse dispatches — and because a Wool routine
runs its inner calls LOCALLY on the worker (confirmed by magnitude: nested at W=1
costs ~16 dispatches, not ~272), the remaining leaves never round-trip. The
serial dispatch count the driver pays drops from N to branch, so the wall lifts.

This is dispatch-overhead collapse via local execution, measured at g=0 — not
added compute parallelism. Ray responds to the same restructuring in the opposite
direction: it keeps every leaf a distributed task and blocks a worker slot per
internal node, so nested fan-out runs far slower (and deadlocks at depth). Run
with the per-worker lock on:

    WOOL_PER_WORKER_LOCK=1 ./.venv/bin/python benchmarks/nested.py
"""

import asyncio
import json
import subprocess
import sys
from functools import partial

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import wool  # noqa: E402
from wool.runtime.worker.base import (
    ChannelOptions,  # noqa: E402
    WorkerOptions,  # noqa: E402
)
from wool.runtime.worker.local import LocalWorker  # noqa: E402

from shapebench.stats import (
    median,  # noqa: E402
    sample,  # noqa: E402
)
from shapebench.workloads import spin  # noqa: E402

RESULTS = "benchmarks/results"
WORKERS = [1, 2, 4, 8]
N = 256
BRANCH = 16
G = 0.0
# Raise the per-channel concurrency limit above the N=256 fan-out so the client
# semaphore never gates — this sidesteps the v0.12 gated-dispatch
# NoWorkersAvailable regression (default max_concurrent_streams=100). It is a
# config deviation from the default-100 canonical/v0.11 runs, noted in the
# comparison writeup. The value flows to both the worker's grpcio limit and the
# client's semaphore (advertised via worker metadata).
CAP = 512
_hicap_worker = partial(
    LocalWorker,
    options=WorkerOptions(channel=ChannelOptions(max_concurrent_streams=CAP)),
)


@wool.routine
async def _leaf(g: float) -> None:
    spin(g)


@wool.routine
async def _subfanout(k: int, g: float) -> None:
    # Runs on a worker: issues k leaf dispatches from that worker's process.
    await asyncio.gather(*(_leaf(g) for _ in range(k)))


def _split(n: int, parts: int) -> list[int]:
    per, rem = divmod(n, parts)
    return [per + (1 if i < rem else 0) for i in range(parts)]


async def flat(n: int, g: float) -> None:
    await asyncio.gather(*(_leaf(g) for _ in range(n)))


async def nested(n: int, branch: int, g: float) -> None:
    sizes = [s for s in _split(n, branch) if s > 0]
    await asyncio.gather(*(_subfanout(s, g) for s in sizes))


async def measure_wool():
    out = {"flat": {}, "nested": {}}
    for w in WORKERS:
        async with wool.WorkerPool(spawn=w, worker=_hicap_worker):
            await flat(N, G)  # warm the whole path
            fs = await sample(lambda: flat(N, G), warmup=3, iters=10)
            ns = await sample(lambda: nested(N, BRANCH, G), warmup=3, iters=10)
        out["flat"][w] = median(fs)
        out["nested"][w] = median(ns)
        print(f"  W={w}: flat={median(fs) * 1e3:.1f}ms  nested={median(ns) * 1e3:.1f}ms")
    return out


def ray_probe(mode: str, n: int, branch: int, workers: int, timeout: float = 20.0):
    """Run the Ray probe in a subprocess; return makespan ms, or 'DEADLOCK'."""
    try:
        r = subprocess.run(
            [
                sys.executable,
                "benchmarks/ray_nested_probe.py",
                mode,
                str(n),
                str(branch),
                str(workers),
            ],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        for line in r.stdout.splitlines():
            if line.startswith("OK"):
                return float(line.split()[1])
        return f"ERROR ({r.stderr.strip()[-80:]})"
    except subprocess.TimeoutExpired:
        return "DEADLOCK"


def main():
    print(
        "Wool flat vs nested fan-out (N=%d, branch=%d, g=0, max_concurrent_streams=%d):"
        % (N, BRANCH, CAP)
    )
    wool_data = asyncio.run(measure_wool())

    print("\nRay probes (n=64, branch=8):")
    ray_flat = {w: ray_probe("flat", 64, 8, w) for w in WORKERS}
    print("  flat:   " + "  ".join(f"W{w}={ray_flat[w]}" for w in WORKERS))
    ray_nested_4 = ray_probe("nested", 64, 8, 4)
    ray_nested_8 = ray_probe("nested", 64, 8, 8)
    print(f"  nested: W4={ray_nested_4}   W8={ray_nested_8}")

    # baseline = flat at W=1, common to both curves so nested visibly exceeds
    # flat's ceiling.
    base = wool_data["flat"][1]
    fig, (axA, axB) = plt.subplots(1, 2, figsize=(12, 4.6))
    axA.plot(
        WORKERS,
        [wool_data["flat"][w] * 1e3 for w in WORKERS],
        "o-",
        color="tab:red",
        label="Wool flat fan-out",
    )
    axA.plot(
        WORKERS,
        [wool_data["nested"][w] * 1e3 for w in WORKERS],
        "s-",
        color="tab:green",
        label="Wool nested fan-out",
    )
    rf = [ray_flat[w] for w in WORKERS if isinstance(ray_flat[w], float)]
    rw = [w for w in WORKERS if isinstance(ray_flat[w], float)]
    if rf:
        axA.plot(rw, rf, "^-", color="tab:blue", label="Ray flat (n=64)")
    axA.set_xscale("log", base=2)
    axA.set_yscale("log", base=2)
    axA.set_xlabel("workers W")
    axA.set_ylabel("makespan (ms)")
    axA.set_title(f"Fan-out makespan vs workers (N={N})")
    axA.legend(fontsize=8)
    axA.grid(True, which="both", alpha=0.3)

    axB.plot(
        WORKERS,
        [base / wool_data["flat"][w] for w in WORKERS],
        "o-",
        color="tab:red",
        label="Wool flat",
    )
    axB.plot(
        WORKERS,
        [base / wool_data["nested"][w] for w in WORKERS],
        "s-",
        color="tab:green",
        label="Wool nested",
    )
    axB.plot(WORKERS, WORKERS, "k--", alpha=0.4, label="ideal ∝W")
    axB.axhline(3.0, color="tab:red", ls=":", alpha=0.5, label="flat Amdahl ceiling ~3×")
    axB.set_xscale("log", base=2)
    axB.set_yscale("log", base=2)
    axB.set_xlabel("workers W")
    axB.set_ylabel("speedup vs flat @ W=1")
    axB.set_title("Speedup (common baseline): nested breaks the flat ceiling")
    axB.legend(fontsize=8)
    axB.grid(True, which="both", alpha=0.3)
    fig.tight_layout()
    fig.savefig(f"{RESULTS}/nested_v12.png", dpi=130)

    result = {
        "wool": {k: {str(w): v for w, v in d.items()} for k, d in wool_data.items()},
        "ray_flat": {str(w): ray_flat[w] for w in WORKERS},
        "ray_nested": {"W4": ray_nested_4, "W8": ray_nested_8},
        "N": N,
        "branch": BRANCH,
        "max_concurrent_streams": CAP,
    }
    with open(f"{RESULTS}/nested_v12.json", "w") as fh:
        json.dump(result, fh, indent=2)
    peak_flat = base / min(wool_data["flat"].values())
    peak_nested = base / min(wool_data["nested"].values())
    print(
        f"\nflat peak speedup {peak_flat:.1f}x  |  nested peak speedup {peak_nested:.1f}x"
        f"  |  nested/flat at W=8: {wool_data['flat'][8] / wool_data['nested'][8]:.1f}x faster"
    )
    print(f"wrote {RESULTS}/nested_v12.png")


if __name__ == "__main__":
    main()
