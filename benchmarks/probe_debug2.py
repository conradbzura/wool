"""Same trace as probe_debug.py, but the routines live in an importable module
(nrt), so cloudpickle can serialize them by reference if it chooses to."""

import asyncio
import os
import sys

import wool

from nrt import sub


async def main() -> None:
    print(
        f"WLDBG[{os.getpid()}] DRIVER start (routines from importable nrt)",
        file=sys.stderr,
        flush=True,
    )
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
