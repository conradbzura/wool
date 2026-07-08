"""Minimal nested-dispatch trace: 1 subfanout -> 2 leaves on 2 workers.

Prints (via WLDBG markers in the instrumented wool source) show, per process:
what callable the worker actually runs (wrapper vs raw fn), which wrapper branch
each call takes, and the do_dispatch value seen at each call.
"""

import asyncio
import os
import sys

import wool


@wool.routine
async def leaf() -> int:
    return os.getpid()


@wool.routine
async def sub(k: int):
    pids = await asyncio.gather(*(leaf() for _ in range(k)))
    return os.getpid(), list(pids)


async def main() -> None:
    print(f"WLDBG[{os.getpid()}] DRIVER start", file=sys.stderr, flush=True)
    async with wool.WorkerPool(spawn=2):
        sub_pid, leaf_pids = await sub(2)
        print(
            f"WLDBG[{os.getpid()}] RESULT sub_worker={sub_pid} leaf_workers={leaf_pids} "
            f"=> leaves {'LOCAL' if all(p == sub_pid for p in leaf_pids) else 'DISTRIBUTED'}",
            file=sys.stderr,
            flush=True,
        )
    os._exit(0)


if __name__ == "__main__":
    asyncio.run(main())
