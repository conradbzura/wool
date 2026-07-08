"""Slice v1 validation: streaming (async-gen) correctness + RTT through the Rust
worker plane, plus a coroutine regression. Same grpc-aio client against (A) a
streaming-capable Python worker and (B) the Rust worker (port $WOOL_RUST_PORT).
"""

import asyncio
import os
import sys
import threading
import time
import uuid
from inspect import isasyncgenfunction

os.environ["GRPC_VERBOSITY"] = "NONE"
sys.path.insert(0, "wool/src")

import cloudpickle  # noqa: E402
import grpc  # noqa: E402
import grpc.aio  # noqa: E402

import wool  # noqa: E402,F401
from wool import protocol  # noqa: E402

RUST_PORT = int(os.environ.get("WOOL_RUST_PORT", "50089"))


async def _noop():
    return None


async def _stream_gen(n):
    for i in range(n):
        yield i


def make_task(fn, *args) -> protocol.Task:
    return protocol.Task(
        version=protocol.__version__,
        id=str(uuid.uuid4()),
        callable=cloudpickle.dumps(fn),
        args=cloudpickle.dumps(args),
        kwargs=cloudpickle.dumps({}),
        proxy=b"",
        proxy_id="",
    )


# ---------- streaming-capable minimal Python worker ----------
class MiniWorker(protocol.WorkerServicer):
    async def dispatch(self, request_iterator, context):
        first = await anext(aiter(request_iterator))
        task = first.task
        yield protocol.Response(ack=protocol.Ack(version=protocol.__version__))
        fn = cloudpickle.loads(task.callable)
        args = cloudpickle.loads(task.args)
        kwargs = cloudpickle.loads(task.kwargs)
        if isasyncgenfunction(fn):
            gen = fn(*args, **kwargs)
            async for _req in request_iterator:
                try:
                    v = await gen.asend(None)
                except StopAsyncIteration:
                    break
                yield protocol.Response(
                    result=protocol.Message(dump=cloudpickle.dumps(v))
                )
        else:
            async for _req in request_iterator:
                break
            result = await fn(*args, **kwargs)
            yield protocol.Response(
                result=protocol.Message(dump=cloudpickle.dumps(result))
            )

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
        box["loop"] = loop
        ready.set()
        await server.wait_for_termination()

    loop.run_until_complete(serve())


async def consume_stream(stub, task_pb):
    call = stub.dispatch()
    await call.write(protocol.Request(task=task_pb))
    await call.read()  # Ack
    values = []
    while True:
        await call.write(protocol.Request(next=protocol.Void()))
        resp = await call.read()
        if resp == grpc.aio.EOF:
            break
        if resp.HasField("result"):
            values.append(cloudpickle.loads(resp.result.dump))
        elif resp.HasField("exception"):
            raise cloudpickle.loads(resp.exception.dump)
        else:
            break
    await call.done_writing()
    return values


async def coro_once(stub, task_pb):
    call = stub.dispatch()
    await call.write(protocol.Request(task=task_pb))
    await call.read()  # Ack
    await call.write(protocol.Request(next=protocol.Void()))
    r = await call.read()  # Result
    await call.done_writing()
    return cloudpickle.loads(r.result.dump)


async def bench_stream(addr, label, n):
    channel = grpc.aio.insecure_channel(addr)
    stub = protocol.WorkerStub(channel)
    task = make_task(_stream_gen, n)
    got = await consume_stream(stub, task)
    ok = got == list(range(n))
    print(
        f"{label:<42} stream(n={n}) correct={ok} got={got[:6]}{'...' if n > 6 else ''}"
    )
    for _ in range(200):
        await consume_stream(stub, task)
    times = []
    for _ in range(7):
        t0 = time.perf_counter_ns()
        for _ in range(300):
            await consume_stream(stub, task)
        times.append((time.perf_counter_ns() - t0) / 300)
    times.sort()
    print(
        f"{label:<42} stream(n={n}) RTT: min={times[0]:.0f}ns "
        f"({times[0] / n:.0f}ns/yield)"
    )
    await channel.close()


async def main():
    ready = threading.Event()
    box = {}
    threading.Thread(target=start_mini, args=(box, ready), daemon=True).start()
    ready.wait(timeout=10)

    rust_addr = f"127.0.0.1:{RUST_PORT}"
    ch = grpc.aio.insecure_channel(rust_addr)
    try:
        await asyncio.wait_for(ch.channel_ready(), timeout=5)
        rust_ok = True
    except Exception as e:  # noqa: BLE001
        print(f"Rust worker not reachable: {type(e).__name__}")
        rust_ok = False
    await ch.close()

    # coroutine regression
    for addr, lbl in [(box["addr"], "(A) Python worker")] + (
        [(rust_addr, "(B) Rust worker")] if rust_ok else []
    ):
        channel = grpc.aio.insecure_channel(addr)
        stub = protocol.WorkerStub(channel)
        val = await coro_once(stub, make_task(_noop))
        print(f"{lbl:<42} coroutine result={val!r} (expect None)")
        await channel.close()

    # streaming correctness + RTT
    for addr, lbl in [(box["addr"], "(A) Python worker")] + (
        [(rust_addr, "(B) Rust worker")] if rust_ok else []
    ):
        await bench_stream(addr, lbl, n=8)


if __name__ == "__main__":
    asyncio.run(main())
