"""Importable recursive fan-out for the nesting-depth sweep. `tree(n, branch, g)`
fans n leaf tasks out through a branching tree: at each level it splits the work
into <=branch subtrees until a node holds <=branch leaves, which it dispatches
directly. Smaller branch => deeper tree. Importable so the dispatch fix pickles
the wrappers by reference and nested calls actually distribute."""

import asyncio

import wool

from shapebench.workloads import spin


@wool.routine
async def leaf(g: float) -> None:
    spin(g)


@wool.routine
async def tree(n: int, branch: int, g: float) -> None:
    if n <= branch:
        if n > 0:
            await asyncio.gather(*(leaf(g) for _ in range(n)))
        return
    per = -(-n // branch)  # ceil division
    sizes, r = [], n
    while r > 0:
        s = min(per, r)
        sizes.append(s)
        r -= s
    await asyncio.gather(*(tree(s, branch, g) for s in sizes))
