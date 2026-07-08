"""The shape-level interface.

This is the crux of the methodology (METHODOLOGY §2, §4.1): the common interface
across frameworks is the *shape*, not a ``submit()``/``map()`` executor API. A
lowest-common-denominator futures API is exactly what forces orchestrators and
async models to flatten into a compute fabric; instead, every framework
implements every shape in its own idiom behind these methods.

Each ``sN_*`` method must *construct and drive the shape to completion* (fan out,
resolve dependencies, and block/await until every task has finished). The runner
times the call to obtain the shape's makespan; the method returns nothing.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class Adapter(Protocol):
    name: str

    def version(self) -> str:
        """Version string of the underlying framework, recorded per run."""
        ...

    async def setup(self, workers: int) -> None:
        """Stand up a pool of ``workers`` and any warm endpoints the shapes need."""
        ...

    async def teardown(self) -> None:
        """Tear the pool down and release all resources."""
        ...

    async def reset(self) -> None:
        """Return the framework to a clean state between shapes / after a failure.

        Used to isolate each shape from another's accumulated runtime state (so a
        break in one shape does not taint the next) and to recover a pool left in
        a bad state by a failed cell. May be a no-op where a framework needs no
        per-shape isolation.
        """
        ...

    async def s1_point_to_point(self, g: float) -> None:
        """1 -> 1: a single task, awaited. The irreducible round-trip."""
        ...

    async def s2_fanout(self, g: float, n: int) -> None:
        """1 -> N independent tasks, all gathered. Embarrassingly parallel."""
        ...

    async def s3_scatter_gather(self, g: float, n: int) -> None:
        """N map tasks -> 1 reduce task. The join/barrier cost."""
        ...

    async def s4_pipeline(self, g: float, depth: int) -> None:
        """A -> B -> C ...: a linear chain, each stage consuming the prior."""
        ...

    async def s5_recursive_tree(self, g: float, depth: int, branch: int) -> None:
        """A task that recursively spawns sub-tasks. Nested / decentralized dispatch."""
        ...

    async def s6_diamond(self, g: float) -> None:
        """A -> {B, C} -> D: the canonical data-dependent DAG unit."""
        ...

    async def s7_streaming(self, g: float, n: int) -> None:
        """One call -> N streamed results. The warm/streaming path."""
        ...
