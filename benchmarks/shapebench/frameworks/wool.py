"""Wool adapter — each shape as an idiomatic ``@wool.routine`` construction.

The routines are module-level so they are picklable and importable in the spawned
worker processes. Nested dispatch (S5) is native: a routine awaiting a child
routine yields the worker's event loop rather than blocking a slot, so Wool does
not have the blocking-slot deadlock that limits recursive depth on a compute
fabric. Async generators (S7) are first-class routines consumed with ``async for``.
"""

import asyncio

import wool

from shapebench.workloads import spin


@wool.routine
async def _leaf(g: float) -> None:
    spin(g)


@wool.routine
async def _stage(x: int, g: float) -> int:
    spin(g)
    return x + 1


@wool.routine
async def _reduce(values: list[int], g: float) -> int:
    spin(g)
    return sum(values)


@wool.routine
async def _tree(depth: int, branch: int, g: float) -> int:
    spin(g)
    if depth <= 0:
        return 1
    async with asyncio.TaskGroup() as tg:
        children = [tg.create_task(_tree(depth - 1, branch, g)) for _ in range(branch)]
    return 1 + sum(c.result() for c in children)


@wool.routine
async def _stream(n: int, g: float):
    for i in range(n):
        spin(g)
        yield i


class WoolAdapter:
    name = "wool"

    def __init__(self) -> None:
        self._pool = None
        self._workers = 1

    def version(self) -> str:
        return str(wool.__version__)

    async def setup(self, workers: int) -> None:
        self._workers = workers
        self._pool = wool.WorkerPool(spawn=workers)
        await self._pool.__aenter__()

    async def teardown(self) -> None:
        if self._pool is not None:
            await self._pool.__aexit__(None, None, None)
            self._pool = None

    async def reset(self) -> None:
        # A fresh pool per shape: isolates each shape from another's accumulated
        # worker-loop state and recovers a pool left broken by a failed cell.
        await self.teardown()
        await self.setup(self._workers)

    async def s1_point_to_point(self, g: float) -> None:
        await _leaf(g)

    async def s2_fanout(self, g: float, n: int) -> None:
        await asyncio.gather(*(_leaf(g) for _ in range(n)))

    async def s3_scatter_gather(self, g: float, n: int) -> None:
        results = await asyncio.gather(*(_stage(0, g) for _ in range(n)))
        await _reduce(list(results), g)

    async def s4_pipeline(self, g: float, depth: int) -> None:
        x = 0
        for _ in range(depth):
            x = await _stage(x, g)

    async def s5_recursive_tree(self, g: float, depth: int, branch: int) -> None:
        await _tree(depth, branch, g)

    async def s6_diamond(self, g: float) -> None:
        a = await _stage(0, g)
        b, c = await asyncio.gather(_stage(a, g), _stage(a, g))
        await _reduce([b, c], g)

    async def s7_streaming(self, g: float, n: int) -> None:
        async for _ in _stream(n, g):
            pass
