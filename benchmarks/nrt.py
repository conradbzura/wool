"""Importable routines (NOT __main__) to test how cloudpickle serializes a
@wool.routine when its qualname is importable — by reference (-> the wrapper) or
by value (-> the raw fn)."""

import asyncio
import os

import wool


@wool.routine
async def leaf() -> int:
    return os.getpid()


@wool.routine
async def sub(k: int):
    pids = await asyncio.gather(*(leaf() for _ in range(k)))
    return os.getpid(), list(pids)


@wool.routine
async def echo(x):
    return x


@wool.routine
async def gen(n: int):
    for i in range(n):
        yield i
