"""Ideal (zero-overhead) makespan, the overhead ratio R, and per-shape METG.

``T_ideal`` uses Brent's bound: on ``W`` workers a task graph with ``work``
total tasks and ``span`` tasks on its critical path cannot finish in fewer than
``max(span, ceil(work / W))`` task-durations, and a greedy scheduler achieves
that within a factor of two. Multiplying by the per-task granularity ``g`` gives
the makespan a perfect, overhead-free scheduler would achieve for that shape.

The reported metric is the dimensionless overhead ratio ``R = measured / ideal``
(see METHODOLOGY §6.2): ``R -> inf`` as ``g -> 0`` (pure plumbing) and ``R -> 1``
as ``g -> inf`` (overhead amortized away). Because R is wall-clock based rather
than FLOP based, it stays defined for orchestrators and streaming models where
"useful-FLOP/s efficiency" is not.
"""

import math


def brent_ideal(work: int, span: int, workers: int, g: float) -> float:
    """Zero-overhead makespan for ``work``/``span`` tasks on ``workers`` at granularity ``g``."""
    steps = max(span, math.ceil(work / workers))
    return steps * g


def overhead_ratio(measured: float, ideal: float) -> float | None:
    """``measured / ideal``; ``None`` when ``ideal == 0`` (the g=0 pure-plumbing point)."""
    if ideal <= 0:
        return None
    return measured / ideal


def find_metg(
    points: list[tuple[float, float | None]], threshold: float = 2.0
) -> float | None:
    """Smallest granularity ``g`` whose overhead ratio ``R <= threshold``.

    ``points`` is ``[(g, R), ...]``; entries with ``R is None`` (g=0) are skipped.
    ``R <= 2`` corresponds to >=50% efficiency, matching Task Bench's METG.
    Returns ``None`` if no measured granularity reaches the threshold.
    """
    for g, r in sorted(points, key=lambda pr: pr[0]):
        if r is not None and r <= threshold:
            return g
    return None
