"""Sampling with start-up / steady-state separation (METHODOLOGY §7).

Warmup iterations are run and discarded before any measured sample is kept, so
cold-start effects (JIT of the dispatch path, channel establishment, worker
loop spin-up) never contaminate the steady-state numbers. This PoC uses a fixed
warmup + fixed sample count; the CoV<2% adaptive stopping criterion from the
methodology is a later refinement.
"""

import statistics
import time
from typing import Awaitable, Callable


async def sample(
    once: Callable[[], Awaitable[None]], warmup: int, iters: int
) -> list[float]:
    """Run ``once`` ``warmup`` times (discarded) then ``iters`` times (timed).

    Returns the sorted list of per-iteration wall-clock makespans in seconds.
    """
    for _ in range(warmup):
        await once()
    samples: list[float] = []
    for _ in range(iters):
        start = time.perf_counter()
        await once()
        samples.append(time.perf_counter() - start)
    samples.sort()
    return samples


def percentile(sorted_samples: list[float], q: float) -> float:
    """The ``q``-quantile (0..1) of an already-sorted sample list."""
    if not sorted_samples:
        return 0.0
    idx = int(q * (len(sorted_samples) - 1) + 0.5)
    return sorted_samples[min(idx, len(sorted_samples) - 1)]


def median(sorted_samples: list[float]) -> float:
    return statistics.median(sorted_samples) if sorted_samples else 0.0
