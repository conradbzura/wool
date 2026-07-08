"""Capture the exact wire bytes of a real s1 dispatch Task from a live 1-worker
pool, so the microbenchmark can decode a representative payload without needing to
hand-construct a WorkerProxy. Also captures an s7 (streaming) armed dispatch to
get a chain-manifest-bearing frame for mount-cost measurement.
"""

import asyncio
import os
import sys

os.environ["GRPC_VERBOSITY"] = "NONE"
sys.path.insert(0, "wool/src")
sys.path.insert(0, "benchmarks")

from shapebench.frameworks.wool import WoolAdapter  # noqa: E402

import wool  # noqa: E402
from wool.runtime.routine.task import Task  # noqa: E402

OUT_DIR = os.path.dirname(os.path.abspath(__file__))
_orig = Task.to_protobuf
captured: dict[str, bytes] = {}


def patched(self):
    pb = _orig(self)
    key = getattr(self.callable, "__name__", "?")
    if key not in captured:
        captured[key] = pb.SerializeToString()
    return pb


Task.to_protobuf = patched


async def main() -> None:
    a = WoolAdapter()
    await a.setup(1)
    try:
        await a.s1_point_to_point(0.0)  # -> _leaf task
        await a.s7_streaming(0.0, 4)  # -> _stream task
    finally:
        await a.teardown()
    for name, data in captured.items():
        path = os.path.join(OUT_DIR, f"task_{name.strip('_')}.bin")
        with open(path, "wb") as f:
            f.write(data)
        print(f"captured {name}: {len(data)} bytes -> {path}")


if __name__ == "__main__":
    asyncio.run(main())
