"""De-risk: drive REAL wool routines through wool's REAL caller (WorkerPool) to a
RustWorker. Tests the full chain: pool -> proxy -> loadbalancer -> connection ->
Rust tonic worker -> IPC -> executor(routine_scope) -> real routine -> result back.
"""

import asyncio
import os
import sys

os.environ["GRPC_VERBOSITY"] = "NONE"
sys.path.insert(0, "wool/src")
sys.path.insert(0, "benchmarks")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rust_worker_adapter import RustWorker  # noqa: E402
from shapebench.frameworks.wool import _leaf  # noqa: E402
from shapebench.frameworks.wool import _stage  # noqa: E402
from shapebench.frameworks.wool import _stream  # noqa: E402

import wool  # noqa: E402


async def main():
    async with wool.WorkerPool(spawn=1, worker=RustWorker):
        r1 = await _leaf(0.0)
        print(f"s1 leaf -> {r1!r}  (expect None)")
        r2 = await _stage(5, 0.0)
        print(f"s4 stage(5) -> {r2!r}  (expect 6)")
        vals = [x async for x in _stream(4, 0.0)]
        print(f"s7 stream(4) -> {vals}  (expect [0,1,2,3])")
        import asyncio as _a

        fan = await _a.gather(*(_leaf(0.0) for _ in range(8)))
        print(f"s2 fan-out(8) -> {len(fan)} results  (expect 8)")
    print("POOL EXITED CLEANLY")


if __name__ == "__main__":
    asyncio.run(main())
