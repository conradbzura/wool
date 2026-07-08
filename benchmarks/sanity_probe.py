"""Sanity + serialization check for the 'dispatch the wrapper' change:
plain coroutine dispatch, async-generator streaming, nested dispatch, and the
pickled size of the callable (wrapper-by-reference vs raw-fn-by-value)."""

import asyncio
import os
import sys

import cloudpickle
import wool

from nrt import echo, gen, sub


async def main() -> None:
    out = []
    async with wool.WorkerPool(spawn=2):
        r = await echo(42)
        out.append(f"plain dispatch: echo(42) -> {r}  {'OK' if r == 42 else 'FAIL'}")
        collected = [x async for x in gen(4)]
        out.append(
            f"streaming: gen(4) -> {collected}  {'OK' if collected == [0, 1, 2, 3] else 'FAIL'}"
        )
        sub_pid, leaf_pids = await sub(3)
        distributed = any(p != sub_pid for p in leaf_pids)
        out.append(
            f"nested: sub(3) leaves on {sorted(set(leaf_pids))}  {'OK (distributed)' if distributed else 'FAIL (local)'}"
        )

    # serialization: the wrapper (what we now dispatch) vs the raw fn (what we used to)
    wrapper_bytes = len(cloudpickle.dumps(echo))
    raw_bytes = len(cloudpickle.dumps(echo.__wrapped__))
    out.append(
        f"pickle size: wrapper(by-ref)={wrapper_bytes}B  raw-fn(by-value)={raw_bytes}B"
    )

    for line in out:
        print(f"WLDBG {line}", file=sys.stderr, flush=True)
    os._exit(0)


if __name__ == "__main__":
    asyncio.run(main())
