"""Cancellation validation: a coroutine that sleeps 4s and records whether it was
interrupted (CANCELLED) or ran to completion (COMPLETED). The client starts it,
then cancels the gRPC call mid-flight (RST_STREAM). Correct workers propagate the
cancel into the routine so it writes CANCELLED within ~1s (not still sleeping).

Tests the Rust plane's RST_STREAM -> CancelGuard -> executor CANCEL -> Task.cancel
path against a naive in-process Python worker.
"""

import asyncio
import os
import sys
import threading
import uuid

os.environ["GRPC_VERBOSITY"] = "NONE"
sys.path.insert(0, "wool/src")

import cloudpickle  # noqa: E402
import grpc.aio  # noqa: E402

import wool  # noqa: E402,F401
from wool import protocol  # noqa: E402

RUST_PORT = int(os.environ.get("WOOL_RUST_PORT", "50089"))


async def _cancel_probe(path):
    try:
        await asyncio.sleep(4.0)
    except asyncio.CancelledError:
        with open(path, "w") as f:
            f.write("CANCELLED")
        raise
    with open(path, "w") as f:
        f.write("COMPLETED")


def make_task(fn, *args):
    return protocol.Task(
        version=protocol.__version__,
        id=str(uuid.uuid4()),
        callable=cloudpickle.dumps(fn),
        args=cloudpickle.dumps(args),
        kwargs=cloudpickle.dumps({}),
        proxy=b"",
        proxy_id="",
    )


class MiniWorker(protocol.WorkerServicer):
    async def dispatch(self, request_iterator, context):
        first = await anext(aiter(request_iterator))
        task = first.task
        yield protocol.Response(ack=protocol.Ack(version=protocol.__version__))
        async for _req in request_iterator:
            break
        fn = cloudpickle.loads(task.callable)
        args = cloudpickle.loads(task.args)
        result = await fn(*args)
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
        ready.set()
        await server.wait_for_termination()

    loop.run_until_complete(serve())


async def run_cancel(addr, label, probe_path):
    if os.path.exists(probe_path):
        os.unlink(probe_path)
    channel = grpc.aio.insecure_channel(addr)
    stub = protocol.WorkerStub(channel)
    call = stub.dispatch()
    await call.write(protocol.Request(task=make_task(_cancel_probe, probe_path)))
    await call.read()  # Ack
    await call.write(protocol.Request(next=protocol.Void()))  # start the coroutine
    await asyncio.sleep(0.4)  # let it reach the sleep
    call.cancel()  # RST_STREAM
    await asyncio.sleep(1.2)  # allow cancel to propagate + finally to run
    try:
        await channel.close()
    except Exception:
        pass
    content = (
        open(probe_path).read()
        if os.path.exists(probe_path)
        else "<absent-still-sleeping>"
    )
    verdict = (
        "PASS (routine interrupted)"
        if content == "CANCELLED"
        else "FAIL (not cancelled)"
    )
    print(f"{label:<42} probe={content!r:<28} -> {verdict}")


async def main():
    ready = threading.Event()
    box = {}
    threading.Thread(target=start_mini, args=(box, ready), daemon=True).start()
    ready.wait(timeout=10)

    await run_cancel(box["addr"], "(A) naive Python worker", "/tmp/wool_cancel_A.txt")

    rust_addr = f"127.0.0.1:{RUST_PORT}"
    try:
        ch = grpc.aio.insecure_channel(rust_addr)
        await asyncio.wait_for(ch.channel_ready(), timeout=5)
        await ch.close()
        await run_cancel(
            rust_addr, "(B) Rust worker + executor", "/tmp/wool_cancel_B.txt"
        )
    except Exception as e:  # noqa: BLE001
        print(f"(B) Rust worker not reachable: {type(e).__name__}")


if __name__ == "__main__":
    asyncio.run(main())
