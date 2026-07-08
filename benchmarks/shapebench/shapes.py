"""The shape ladder registry (METHODOLOGY §4).

Each ``ShapeSpec`` binds a shape id to its default parameters, its ``work`` (total
task count) and ``span`` (critical-path task count) functions used for Brent's
ideal makespan, and the ``Adapter`` method that constructs it.

S5's default is intentionally small (depth=2, branch=2 -> 7 tasks). A blocking
compute fabric holds a worker slot for every internal node while it waits on its
children, so a deep tree can exhaust ``W`` workers and deadlock (Ray's documented
nested-task footgun). Keeping internal-node count <= default W=4 keeps the PoC
robust on every framework; the deadlock boundary itself is a finding to explore
deliberately later, not to trip over here.
"""

from dataclasses import dataclass, field
from typing import Any, Callable

from shapebench.adapter import Adapter


def _tree_work(p: dict[str, Any]) -> int:
    branch, depth = p["branch"], p["depth"]
    if branch <= 1:
        return depth + 1
    return (branch ** (depth + 1) - 1) // (branch - 1)


def _tree_span(p: dict[str, Any]) -> int:
    return p["depth"] + 1


@dataclass(frozen=True)
class ShapeSpec:
    id: str
    name: str
    method: str
    params: dict[str, Any] = field(default_factory=dict)
    work: Callable[[dict[str, Any]], int] = lambda p: 1
    span: Callable[[dict[str, Any]], int] = lambda p: 1

    async def run(self, adapter: Adapter, g: float, params: dict[str, Any]) -> None:
        await getattr(adapter, self.method)(g, **params)


SHAPES: dict[str, ShapeSpec] = {
    "s1": ShapeSpec("s1", "point-to-point", "s1_point_to_point"),
    "s2": ShapeSpec(
        "s2",
        "fan-out",
        "s2_fanout",
        params={"n": 64},
        work=lambda p: p["n"],
        span=lambda p: 1,
    ),
    "s3": ShapeSpec(
        "s3",
        "scatter-gather",
        "s3_scatter_gather",
        params={"n": 64},
        work=lambda p: p["n"] + 1,
        span=lambda p: 2,
    ),
    "s4": ShapeSpec(
        "s4",
        "pipeline",
        "s4_pipeline",
        params={"depth": 16},
        work=lambda p: p["depth"],
        span=lambda p: p["depth"],
    ),
    "s5": ShapeSpec(
        "s5",
        "recursive-tree",
        "s5_recursive_tree",
        params={"depth": 2, "branch": 2},
        work=_tree_work,
        span=_tree_span,
    ),
    "s6": ShapeSpec(
        "s6",
        "diamond",
        "s6_diamond",
        params={},
        work=lambda p: 4,
        span=lambda p: 3,
    ),
    "s7": ShapeSpec(
        "s7",
        "streaming",
        "s7_streaming",
        params={"n": 64},
        work=lambda p: p["n"],
        span=lambda p: p["n"],
    ),
}
