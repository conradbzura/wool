"""Caller-side throughput decomposition. wool is caller-bound (one asyncio driver
serializes dispatch), so s2 throughput ≈ 1 / (caller CPU per dispatch). The worker
is a subprocess, so cProfile here captures ONLY caller-side CPU — the bottleneck.

Reports: s2 fan-out throughput (tasks/s) and the caller's top self-time functions.
"""

import asyncio
import cProfile
import io
import os
import pstats
import sys
import time

os.environ["GRPC_VERBOSITY"] = "NONE"
sys.path.insert(0, "wool/src")
sys.path.insert(0, "benchmarks")

from shapebench.frameworks.wool import WoolAdapter  # noqa: E402

import wool  # noqa: E402,F401

W = 4
N = 256
ROUNDS = 12


async def main() -> None:
    a = WoolAdapter()
    await a.setup(W)
    try:
        # warm
        for _ in range(3):
            await a.s2_fanout(0.0, N)

        # throughput
        t0 = time.perf_counter()
        for _ in range(ROUNDS):
            await a.s2_fanout(0.0, N)
        dt = time.perf_counter() - t0
        tput = ROUNDS * N / dt
        print(
            f"s2 throughput (W={W}, N={N}): {tput:,.0f} tasks/s "
            f"({1e6 * dt / (ROUNDS * N):.1f} us/task caller-serialized)"
        )

        # profile the caller only
        pr = cProfile.Profile()
        pr.enable()
        for _ in range(ROUNDS):
            await a.s2_fanout(0.0, N)
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats("tottime")
        ps.print_stats(18)
        print("\n=== caller self-time (tottime) top functions ===")
        for line in s.getvalue().splitlines():
            if line.strip() and (
                "function calls" in line
                or "tottime" in line
                or "/" in line
                or "{" in line
            ):
                print(line)
    finally:
        await a.teardown()


if __name__ == "__main__":
    asyncio.run(main())
