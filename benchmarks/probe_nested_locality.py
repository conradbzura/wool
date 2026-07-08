"""Definitive check: when a routine running on a worker calls another routine,
does that inner call DISPATCH to the pool (runs on some pool worker) or run
LOCALLY (in the same worker process)?

Each leaf returns its own os.getpid(); each subfanout returns its pid plus its
leaves' pids. If leaves are local, every leaf pid equals its subfanout's pid. If
leaves dispatch, leaf pids spread across all worker pids independent of parent.

Prints inside the pool scope and os._exit(0) to sidestep the flaky teardown hang.
"""

import asyncio
import os

import wool

from shapebench.workloads import spin  # noqa: F401


@wool.routine
async def leaf() -> int:
    return os.getpid()


@wool.routine
async def sub(k: int):
    pids = await asyncio.gather(*(leaf() for _ in range(k)))
    return os.getpid(), list(pids)


async def main() -> None:
    async with wool.WorkerPool(spawn=4):
        await sub(2)  # warm
        results = await asyncio.gather(*(sub(4) for _ in range(6)))
        worker_pids, leaf_pids = set(), set()
        local = dispatched = 0
        for sub_pid, pids in results:
            worker_pids.add(sub_pid)
            for lp in pids:
                leaf_pids.add(lp)
                if lp == sub_pid:
                    local += 1
                else:
                    dispatched += 1
        print(f"driver pid:            {os.getpid()}", flush=True)
        print(f"subfanout worker pids: {sorted(worker_pids)}", flush=True)
        print(f"leaf pids:             {sorted(leaf_pids)}", flush=True)
        print(
            f"leaves LOCAL to parent worker: {local}   dispatched elsewhere: {dispatched}",
            flush=True,
        )
        verdict = (
            "LOCAL (nested calls do NOT dispatch)"
            if dispatched == 0
            else "DISPATCHED to pool"
        )
        print(f"verdict: {verdict}", flush=True)
    os._exit(0)


if __name__ == "__main__":
    asyncio.run(main())
