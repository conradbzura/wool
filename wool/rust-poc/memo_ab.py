"""Direct A/B of the worker-side proxy-memo (rec #2): parse cost of the real
captured Task frame with the memo off vs on. Deterministic, in-process, no drift.
"""

import os
import sys
import time

os.environ["GRPC_VERBOSITY"] = "NONE"
sys.path.insert(0, "wool/src")

import wool  # noqa: E402,F401
from wool import protocol  # noqa: E402
from wool.runtime.routine import task as task_mod  # noqa: E402
from wool.runtime.worker.frame import TaskRequestFrame  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))


def bench(fn, iters=10000, warmup=300):
    for _ in range(warmup):
        fn()
    xs = []
    for _ in range(5):
        t0 = time.perf_counter_ns()
        for _ in range(iters):
            fn()
        xs.append((time.perf_counter_ns() - t0) / iters)
    xs.sort()
    return xs[0]


def main():
    with open(os.path.join(HERE, "task_leaf.bin"), "rb") as f:
        req = protocol.Request(task=protocol.Task.FromString(f.read()))

    task_mod._PROXY_MEMO = False
    off = bench(lambda: TaskRequestFrame.from_protobuf(req))

    task_mod._PROXY_MEMO = True
    task_mod._proxy_loads_cache.clear()
    TaskRequestFrame.from_protobuf(req)  # warm the cache
    on = bench(lambda: TaskRequestFrame.from_protobuf(req))

    print(f"TaskRequestFrame.from_protobuf  memo OFF: {off:8.0f} ns")
    print(f"TaskRequestFrame.from_protobuf  memo ON : {on:8.0f} ns")
    print(f"saved: {off - on:.0f} ns/dispatch ({100 * (off - on) / off:.0f}%)")


if __name__ == "__main__":
    main()
