"""Isolate the asyncio loop-iteration floor and the cross-loop tax under default
asyncio vs uvloop. Determines whether the worker-side plumbing cost is (a) an
artifact of default-asyncio on macOS that uvloop erases (cheap lever), or (b)
fundamental cross-thread wakeup latency (where a Rust bridge could help).
"""

import asyncio
import concurrent.futures
import os
import sys
import threading
import time

os.environ["GRPC_VERBOSITY"] = "NONE"
sys.path.insert(0, "wool/src")
sys.path.insert(0, "benchmarks")

import wool  # noqa: E402
from wool.runtime.worker.frame import NextRequestFrame  # noqa: E402
from wool.runtime.worker.frame import ResultResponseFrame  # noqa: E402
from wool.runtime.worker.session import _RequestQueue  # noqa: E402
from wool.runtime.worker.session import _ResponseQueue  # noqa: E402

S = wool.__serializer__
N = 20000


def best_of(fn, reps=5):
    xs = sorted(fn() for _ in range(reps))
    return xs[0], xs[len(xs) // 2]


def measure_sleep0(new_loop):
    loop = new_loop()

    async def run(n):
        t0 = time.perf_counter_ns()
        for _ in range(n):
            await asyncio.sleep(0)
        return (time.perf_counter_ns() - t0) / n

    loop.run_until_complete(run(2000))
    mn, md = best_of(lambda: loop.run_until_complete(run(N)))
    loop.close()
    return mn, md


def measure_crossloop(new_loop, label):
    dummy = ResultResponseFrame.for_send(None, serializer=S, wire_chain_manifest=None)
    prime = NextRequestFrame.for_send(
        serializer=S, wire_chain_manifest=None
    ).to_protobuf()

    worker_loop = new_loop()
    wt = threading.Thread(target=worker_loop.run_forever, daemon=True)
    wt.start()
    main_loop = new_loop()
    wd: concurrent.futures.Future = concurrent.futures.Future()
    rq = _RequestQueue(worker_loop, serializer=S)
    respq = _ResponseQueue(main_loop, wd)

    async def worker_side():
        while True:
            item = await rq.get()
            if item is None:
                break
            respq.put(dummy)

    wfut = asyncio.run_coroutine_threadsafe(worker_side(), worker_loop)

    async def drive(n):
        t0 = time.perf_counter_ns()
        for _ in range(n):
            rq.put(prime)
            await respq.get()
        return (time.perf_counter_ns() - t0) / n

    main_loop.run_until_complete(drive(500))
    mn, md = best_of(lambda: main_loop.run_until_complete(drive(N)))

    rq.close()
    wfut.result(timeout=5)
    worker_loop.call_soon_threadsafe(worker_loop.stop)
    wt.join(timeout=5)
    main_loop.close()
    return mn, md


def main():
    import uvloop

    def asyncio_loop():
        return asyncio.new_event_loop()

    def uvloop_loop():
        return uvloop.new_event_loop()

    rows = []
    rows.append(("sleep0 iteration  [asyncio]", *measure_sleep0(asyncio_loop)))
    rows.append(("sleep0 iteration  [uvloop] ", *measure_sleep0(uvloop_loop)))
    rows.append(
        ("cross-loop RTT    [asyncio]", *measure_crossloop(asyncio_loop, "asyncio"))
    )
    rows.append(
        ("cross-loop RTT    [uvloop] ", *measure_crossloop(uvloop_loop, "uvloop"))
    )

    print(f"\n{'stage':<32}{'min ns':>12}{'median ns':>12}")
    print("-" * 56)
    for name, mn, md in rows:
        print(f"{name:<32}{mn:>12.0f}{md:>12.0f}")


if __name__ == "__main__":
    main()
