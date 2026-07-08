"""Nesting-depth sweep: fan out a fixed N leaves through trees of decreasing
branch (increasing depth) and measure makespan at g=0, to see how deeply nesting
can expand the fan-out ceiling before critical-path latency takes over.

    WOOL_PER_WORKER_LOCK=1 PYTHONPATH=benchmarks ./.venv/bin/python benchmarks/depth_sweep.py
"""

import asyncio
import os
import sys

import wool

from shapebench.stats import median, sample
from treemod import leaf, tree

N = 256
G = 0.0
W = 8
BRANCHES = [256, 16, 8, 4, 2]


async def flat() -> None:
    # The single-caller baseline: the driver issues all N dispatches itself.
    await asyncio.gather(*(leaf(G) for _ in range(N)))


def depth_of(n: int, branch: int) -> int:
    d = 1
    while n > branch:
        n = -(-n // branch)
        d += 1
    return d


async def main() -> None:
    rows = []
    async with wool.WorkerPool(spawn=W):
        await flat()  # warm the whole path
        flat_ms = median(await sample(flat, warmup=3, iters=8)) * 1e3
        for b in BRANCHES:
            try:
                m = (
                    median(await sample(lambda b=b: tree(N, b, G), warmup=3, iters=8))
                    * 1e3
                )
                rows.append((b, depth_of(N, b), m, None))
            except Exception as exc:
                rows.append((b, depth_of(N, b), None, f"{type(exc).__name__}"))

    print(
        f"WLDBG flat (driver issues {N}): {flat_ms:.1f} ms", file=sys.stderr, flush=True
    )
    for b, d, m, err in rows:
        if m is None:
            print(
                f"WLDBG tree branch={b:>3} depth={d}: FAILED {err}",
                file=sys.stderr,
                flush=True,
            )
        else:
            print(
                f"WLDBG tree branch={b:>3} depth={d}: {m:7.1f} ms  speedup-vs-flat={flat_ms / m:.2f}x",
                file=sys.stderr,
                flush=True,
            )
    os._exit(0)


if __name__ == "__main__":
    asyncio.run(main())
