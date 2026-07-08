"""Caller-transport A/B: same Python worker + same trivial coroutine, dispatched
via (A) the grpc-aio client and (B) the Rust tonic client (wool_client_rs). Holds
the server constant to isolate the CALLER-side transport.
"""

import asyncio
import os
import sys
import threading
import time

os.environ["GRPC_VERBOSITY"] = "NONE"
sys.path.insert(0, "wool/src")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import cloudpickle  # noqa: E402
import grpc.aio  # noqa: E402
import wool_client_rs  # noqa: E402
from worker_plane_ab import MiniWorker  # noqa: E402
from worker_plane_ab import make_task_pb  # noqa: E402

import wool  # noqa: E402,F401
from wool import protocol  # noqa: E402


def start_mini(box, ready):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def serve():
        server = grpc.aio.server()
        port = server.add_insecure_port("127.0.0.1:0")
        protocol.add_to_server[protocol.WorkerServicer](MiniWorker(), server)
        await server.start()
        box["addr"] = f"127.0.0.1:{port}"
        ready.set()
        await server.wait_for_termination()

    loop.run_until_complete(serve())


async def bench_grpcaio(addr, task_pb):
    channel = grpc.aio.insecure_channel(addr)
    stub = protocol.WorkerStub(channel)

    async def one():
        call = stub.dispatch()
        await call.write(protocol.Request(task=task_pb))
        await call.read()  # Ack
        await call.write(protocol.Request(next=protocol.Void()))
        await call.read()  # Result
        await call.done_writing()

    await one()
    for _ in range(300):
        await one()
    times = []
    for _ in range(7):
        t0 = time.perf_counter_ns()
        for _ in range(500):
            await one()
        times.append((time.perf_counter_ns() - t0) / 500)
    times.sort()
    print(
        f"(A) grpc-aio client       s1 RTT: min={times[0]:.0f}ns median={times[len(times) // 2]:.0f}ns"
    )
    await channel.close()


async def bench_rust(addr, task_bytes):
    async def one():
        kind, payload = await wool_client_rs.dispatch(addr, task_bytes)
        return kind, payload

    kind, payload = await one()
    result = cloudpickle.loads(payload) if payload else None
    print(f"(B) rust client correctness: kind={kind} result={result!r} (expect 0, None)")
    for _ in range(300):
        await one()
    times = []
    for _ in range(7):
        t0 = time.perf_counter_ns()
        for _ in range(500):
            await one()
        times.append((time.perf_counter_ns() - t0) / 500)
    times.sort()
    print(
        f"(B) Rust tonic client     s1 RTT: min={times[0]:.0f}ns median={times[len(times) // 2]:.0f}ns"
    )


async def main():
    ready = threading.Event()
    box = {}
    threading.Thread(target=start_mini, args=(box, ready), daemon=True).start()
    ready.wait(timeout=10)
    addr = box["addr"]
    task_pb = make_task_pb()
    task_bytes = task_pb.SerializeToString()
    await bench_grpcaio(addr, task_pb)
    await bench_rust(addr, task_bytes)


if __name__ == "__main__":
    asyncio.run(main())
