"""Line-delimited JSON run records.

One record per (framework, shape, granularity) measurement. The schema mirrors
the fields we can reconstruct at the driver; the finer-grained TaPS per-task
schema (submit/start/complete/transform/resolve) is a later refinement once the
adapters expose task-level hooks (METHODOLOGY §6.3).
"""

import json
from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class RunRecord:
    framework: str
    framework_version: str
    shape: str
    shape_name: str
    workers: int
    granularity_s: float
    params: dict[str, Any]
    work: int
    span: int
    ideal_s: float
    makespan_p50_s: float | None
    makespan_p99_s: float | None
    overhead_ratio: float | None
    samples: int
    error: str | None = None


class RecordLogger:
    """Appends ``RunRecord``s as JSON lines to ``path`` (or nowhere if ``path`` is None)."""

    def __init__(self, path: str | None):
        self._fh = open(path, "a") if path else None

    def write(self, record: RunRecord) -> None:
        if self._fh is None:
            return
        self._fh.write(json.dumps(asdict(record)) + "\n")
        self._fh.flush()

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None
