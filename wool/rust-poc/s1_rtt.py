"""Real end-to-end s1 dispatch RTT (g=0, 1 worker), warm. Grounds the microbench
fractions in an actual makespan."""

import asyncio
import os
import sys
import time

os.environ["GRPC_VERBOSITY"] = "NONE"
sys.path.insert(0, "wool/src")
sys.path.insert(0, "benchmarks")

from shapebench.frameworks.wool import WoolAdapter  # noqa: E402
from shapebench.frameworks.wool import _leaf  # noqa: E402

import wool  # noqa: E402,F401


async def main() -> None:
    a = WoolAdapter()
    await a.setup(1)
    try:
        for _ in range(300):
            await _leaf(0.0)
        times = []
        for _ in range(7):
            t0 = time.perf_counter_ns()
            for _ in range(500):
                await _leaf(0.0)
            times.append((time.perf_counter_ns() - t0) / 500)
        times.sort()
        print(
            f"s1 real RTT (g=0, 1 worker): min={times[0]:.0f}ns "
            f"median={times[len(times) // 2]:.0f}ns"
        )
    finally:
        await a.teardown()


if __name__ == "__main__":
    asyncio.run(main())
