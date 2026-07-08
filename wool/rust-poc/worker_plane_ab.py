"""Worker-plane A/B: identical grpc-aio client + identical trivial pickled
coroutine, measured against
  (A) a minimal single-loop Python grpc.aio worker (runs the coroutine inline), and
  (B) the Rust tonic worker + Python executor (must be started separately, listening
      on 127.0.0.1:$WOOL_RUST_PORT).
Isolates the worker-plane cost (transport + coordination + routine bridge). The real
full WorkerService (dual-loop, chain machinery) is heavier — see s1_rtt.py (852µs).
"""

import asyncio
import os
import sys
import threading
import time
import uuid

os.environ["GRPC_VERBOSITY"] = "NONE"
sys.path.insert(0, "wool/src")

import cloudpickle  # noqa: E402
import grpc.aio  # noqa: E402

import wool  # noqa: E402,F401
from wool import protocol  # noqa: E402

RUST_PORT = int(os.environ.get("WOOL_RUST_PORT", "50088"))


async def _noop():
    return None


def make_task_pb() -> protocol.Task:
    return protocol.Task(
        version=protocol.__version__,
        id=str(uuid.uuid4()),
        callable=cloudpickle.dumps(_noop),
        args=cloudpickle.dumps(()),
        kwargs=cloudpickle.dumps({}),
        proxy=b"",
        proxy_id="",
    )


# ---------- (A) minimal single-loop Python worker ----------
class MiniWorker(protocol.WorkerServicer):
    async def dispatch(self, request_iterator, context):
        first = await anext(aiter(request_iterator))
        task = first.task
        yield protocol.Response(ack=protocol.Ack(version=protocol.__version__))
        async for _req in request_iterator:  # the prime Next
            break
        fn = cloudpickle.loads(task.callable)
        args = cloudpickle.loads(task.args)
        kwargs = cloudpickle.loads(task.kwargs)
        result = await fn(*args, **kwargs)
        yield protocol.Response(result=protocol.Message(dump=cloudpickle.dumps(result)))

    async def stop(self, request, context):
        return protocol.Void()


def start_mini(box, ready):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def serve():
        server = grpc.aio.server()
        port = server.add_insecure_port("127.0.0.1:0")
        protocol.add_to_server[protocol.WorkerServicer](MiniWorker(), server)
        await server.start()
        box["addr"] = f"127.0.0.1:{port}"
        box["server"] = server
        box["loop"] = loop
        ready.set()
        await server.wait_for_termination()

    loop.run_until_complete(serve())


async def bench(addr: str, label: str, task_pb) -> None:
    channel = grpc.aio.insecure_channel(addr)
    stub = protocol.WorkerStub(channel)

    async def one():
        call = stub.dispatch()
        await call.write(protocol.Request(task=task_pb))
        await call.read()  # Ack
        await call.write(protocol.Request(next=protocol.Void()))
        await call.read()  # Result
        await call.done_writing()

    # correctness check on first call
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
        f"{label:<42} s1 RTT: min={times[0]:.0f}ns median={times[len(times) // 2]:.0f}ns"
    )
    await channel.close()


async def main() -> None:
    task_pb = make_task_pb()

    ready = threading.Event()
    box: dict = {}
    threading.Thread(target=start_mini, args=(box, ready), daemon=True).start()
    ready.wait(timeout=10)
    await bench(box["addr"], "(A) Python grpc.aio worker (single-loop)", task_pb)

    # (B) Rust worker — only if reachable
    rust_addr = f"127.0.0.1:{RUST_PORT}"
    try:
        ch = grpc.aio.insecure_channel(rust_addr)
        await asyncio.wait_for(ch.channel_ready(), timeout=3)
        await ch.close()
        await bench(rust_addr, "(B) Rust tonic worker + Python executor", task_pb)
    except Exception as exc:  # noqa: BLE001
        print(
            f"(B) Rust worker at {rust_addr} not reachable ({type(exc).__name__}); "
            f"start executor.py + rust_worker first"
        )


if __name__ == "__main__":
    asyncio.run(main())
