"""Decompose worker-side per-dispatch cost into movable (plumbing Rust could
replace) vs unmovable (CPython: cloudpickle / protobuf / contextvars / routine).

Uses wool's own _RequestQueue/_ResponseQueue for a faithful cross-loop measurement
and a real captured Task payload for decode cost. Prints ns/op for each stage.
"""

import asyncio
import concurrent.futures
import contextvars
import os
import sys
import threading
import time

os.environ["GRPC_VERBOSITY"] = "NONE"
sys.path.insert(0, "wool/src")
sys.path.insert(0, "benchmarks")

import cloudpickle  # noqa: E402

import wool  # noqa: E402
from wool import protocol  # noqa: E402
from wool.runtime.routine.task import Task  # noqa: E402
from wool.runtime.worker.frame import AckResponseFrame  # noqa: E402
from wool.runtime.worker.frame import NextRequestFrame  # noqa: E402
from wool.runtime.worker.frame import ResultResponseFrame  # noqa: E402
from wool.runtime.worker.frame import TaskRequestFrame  # noqa: E402
from wool.runtime.worker.session import _RequestQueue  # noqa: E402
from wool.runtime.worker.session import _ResponseQueue  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
S = wool.__serializer__


def _bench(fn, iters, warmup=200):
    for _ in range(warmup):
        fn()
    best = float("inf")
    # min-of-reps to reject scheduler noise; report median-ish via best-of-5 batches
    reps = 5
    times = []
    for _ in range(reps):
        t0 = time.perf_counter_ns()
        for _ in range(iters):
            fn()
        dt = (time.perf_counter_ns() - t0) / iters
        times.append(dt)
        best = min(best, dt)
    times.sort()
    return best, times[len(times) // 2]  # (min, median) ns/op


def load_task_request() -> protocol.Request:
    with open(os.path.join(HERE, "task_leaf.bin"), "rb") as f:
        task_pb = protocol.Task.FromString(f.read())
    return protocol.Request(task=task_pb)


def main() -> None:
    req = load_task_request()
    req_bytes = req.SerializeToString()
    task_pb = req.task

    # Pre-extract the individual pickled fields for component decode timing.
    callable_b = task_pb.callable
    args_b = task_pb.args
    kwargs_b = task_pb.kwargs
    proxy_b = task_pb.proxy
    rc_present = task_pb.HasField("runtime_context")

    results: list[tuple[str, float, float]] = []

    def rec(name, fn, iters):
        mn, md = _bench(fn, iters)
        results.append((name, mn, md))

    # ---- DECODE (worker receives Task request) : UNMOVABLE (protobuf+pickle) ----
    rec(
        "protobuf parse Request envelope",
        lambda: protocol.Request.FromString(req_bytes),
        20000,
    )
    rec("cloudpickle.loads callable", lambda: cloudpickle.loads(callable_b), 20000)
    rec("cloudpickle.loads args", lambda: cloudpickle.loads(args_b), 20000)
    rec("cloudpickle.loads kwargs", lambda: cloudpickle.loads(kwargs_b), 20000)
    rec("cloudpickle.loads proxy", lambda: cloudpickle.loads(proxy_b), 20000)
    rec("Task.from_protobuf (all fields)", lambda: Task.from_protobuf(task_pb), 10000)
    rec(
        "TaskRequestFrame.from_protobuf (full parse)",
        lambda: TaskRequestFrame.from_protobuf(req),
        10000,
    )

    # ---- ENCODE (worker sends frames) : mostly UNMOVABLE (pickle+protobuf) ----
    rec(
        "ResultResponseFrame build+encode (None, unarmed)",
        lambda: ResultResponseFrame.for_send(
            None, serializer=S, wire_chain_manifest=None
        ).to_protobuf(),
        20000,
    )
    rec(
        "AckResponseFrame build+encode",
        lambda: AckResponseFrame.for_send(serializer=S).to_protobuf(),
        20000,
    )
    rec(
        "NextRequestFrame build+encode (#274 prime)",
        lambda: NextRequestFrame.for_send(
            serializer=S, wire_chain_manifest=None
        ).to_protobuf(),
        20000,
    )

    # ---- PLUMBING : MOVABLE (Rust-amenable) ----
    rec("contextvars.copy_context()", contextvars.copy_context, 50000)

    # per-step task construction + await (bypasses factory, like _create_step_task)
    loop_solo = asyncio.new_event_loop()

    async def _trivial():
        return None

    def _mk_task_await():
        ctx = contextvars.copy_context()
        t = asyncio.Task(_trivial(), loop=loop_solo, context=ctx)
        loop_solo.run_until_complete(t)

    rec("asyncio.Task(coro,ctx) construct+await", _mk_task_await, 20000)
    loop_solo.close()

    # ---- CROSS-LOOP HOP (the dual-loop tax) : MOVABLE ----
    # Faithful: wool's own _RequestQueue/_ResponseQueue across two real loops on
    # two threads. One "step" = main->worker request hop + worker->main response hop.
    dummy_resp = ResultResponseFrame.for_send(
        None, serializer=S, wire_chain_manifest=None
    )

    worker_loop = asyncio.new_event_loop()
    wt = threading.Thread(target=worker_loop.run_forever, daemon=True)
    wt.start()

    main_loop = asyncio.new_event_loop()
    worker_done: concurrent.futures.Future = concurrent.futures.Future()
    rq = _RequestQueue(worker_loop, serializer=S)
    respq = _ResponseQueue(main_loop, worker_done)

    async def _worker_side():
        while True:
            item = await rq.get()
            if item is None:
                break
            respq.put(dummy_resp)

    # schedule the worker-side consumer on the worker loop
    worker_fut = asyncio.run_coroutine_threadsafe(_worker_side(), worker_loop)

    ITER_HOP = 20000
    prime = NextRequestFrame.for_send(
        serializer=S, wire_chain_manifest=None
    ).to_protobuf()

    async def _drive_hops(n):
        t0 = time.perf_counter_ns()
        for _ in range(n):
            rq.put(prime)
            await respq.get()
        return (time.perf_counter_ns() - t0) / n

    # warmup + measure on main_loop
    main_loop.run_until_complete(_drive_hops(500))
    hop_times = [main_loop.run_until_complete(_drive_hops(ITER_HOP)) for _ in range(5)]
    hop_times.sort()
    results.append(
        (
            "CROSS-LOOP step RTT (2 hops, wool queues)",
            min(hop_times),
            hop_times[len(hop_times) // 2],
        )
    )

    # single-loop asyncio.Queue baseline (no cross-thread wakeup)
    q_in: asyncio.Queue = asyncio.Queue()
    q_out: asyncio.Queue = asyncio.Queue()

    async def _solo_worker():
        while True:
            x = await q_in.get()
            if x is None:
                break
            q_out.put_nowait(dummy_resp)

    async def _solo_drive(n):
        sw = asyncio.ensure_future(_solo_worker())
        t0 = time.perf_counter_ns()
        for _ in range(n):
            q_in.put_nowait(prime)
            await q_out.get()
        dt = (time.perf_counter_ns() - t0) / n
        q_in.put_nowait(None)
        await sw
        return dt

    solo = asyncio.new_event_loop()
    solo.run_until_complete(asyncio.sleep(0))
    solo_times = [solo.run_until_complete(_solo_drive(ITER_HOP)) for _ in range(5)]
    solo_times.sort()
    results.append(
        (
            "SINGLE-LOOP step RTT (asyncio.Queue only)",
            min(solo_times),
            solo_times[len(solo_times) // 2],
        )
    )
    solo.close()

    # teardown worker loop
    rq.close()
    worker_fut.result(timeout=5)
    worker_loop.call_soon_threadsafe(worker_loop.stop)
    wt.join(timeout=5)
    main_loop.close()

    # ---- report ----
    print(
        f"\nCaptured Task payload: {len(req_bytes)} bytes wire; "
        f"proxy pickle {len(proxy_b)}B, callable {len(callable_b)}B, "
        f"args {len(args_b)}B, kwargs {len(kwargs_b)}B, rc={rc_present}\n"
    )
    print(f"{'stage':<52}{'min ns':>12}{'median ns':>12}")
    print("-" * 76)
    for name, mn, md in results:
        print(f"{name:<52}{mn:>12.0f}{md:>12.0f}")


if __name__ == "__main__":
    main()
