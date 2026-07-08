"""The task body under test: a tunable-duration busy no-op.

We *spin* (busy-wait) rather than sleep so sub-millisecond granularities stay
honest. ``asyncio.sleep``/``time.sleep`` yield to the scheduler and would let a
framework overlap "work" with its own plumbing, under-reporting overhead. A
busy-spin holds a core for ``g`` seconds of genuine CPU, which is what a real
CPU-bound task of that granularity would do. The spin runs inside the *worker*
process (Wool ``LocalWorker`` and Ray workers are separate processes), so it
does not contend with the driver's measurement loop.
"""

import time


def spin(seconds: float) -> None:
    """Busy-wait for ``seconds`` of wall-clock. ``<= 0`` returns immediately."""
    if seconds <= 0:
        return
    end = time.perf_counter() + seconds
    while time.perf_counter() < end:
        pass
