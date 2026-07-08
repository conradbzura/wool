"""Bare grpc-aio bidi-streaming RTT, no wool orchestration — isolates the
transport + protobuf + asyncio-integration cost per message exchange. Server runs
on its own loop/thread (mimicking the worker's separate loop) over a real socket.

Measures both a minimal 1-exchange RTT and a 2-exchange RTT (closer to s1's
task->ack, next->result frame pattern), over TCP loopback and UDS.
"""

import asyncio
import os
import sys
import threading
import time

os.environ["GRPC_VERBOSITY"] = "NONE"
sys.path.insert(0, "wool/src")

import grpc.aio  # noqa: E402

import wool  # noqa: E402,F401
from wool import protocol  # noqa: E402


class EchoServicer(protocol.WorkerServicer):
    async def dispatch(self, request_iterator, context):
        # Mirror the dispatch frame pattern: ack on first frame, result on second.
        n = 0
        async for _req in request_iterator:
            n += 1
            if n == 1:
                yield protocol.Response(ack=protocol.Ack(version=protocol.__version__))
            else:
                yield protocol.Response(result=protocol.Message(dump=b""))
                return

    async def stop(self, request, context):
        return protocol.Void()


def start_server(bind: str, ready: threading.Event, box: dict) -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def serve():
        server = grpc.aio.server()
        port = server.add_insecure_port(bind)
        protocol.add_to_server[protocol.WorkerServicer](EchoServicer(), server)
        await server.start()
        box["addr"] = bind if bind.startswith("unix:") else f"127.0.0.1:{port}"
        box["server"] = server
        box["loop"] = loop
        ready.set()
        await server.wait_for_termination()

    loop.run_until_complete(serve())


async def bench_channel(addr: str, label: str) -> None:
    channel = grpc.aio.insecure_channel(addr)
    stub = protocol.WorkerStub(channel)
    task_req = protocol.Request(task=protocol.Task(version=protocol.__version__, id="x"))
    next_req = protocol.Request(next=protocol.Void())

    async def one_exchange():
        # 1 write, 1 read — a single message round trip
        call = stub.dispatch()
        await call.write(task_req)
        await call.read()
        call.cancel()

    async def two_exchange():
        # task->ack, next->result — the s1 frame pattern
        call = stub.dispatch()
        await call.write(task_req)
        await call.read()
        await call.write(next_req)
        await call.read()
        await call.done_writing()
        await call.read()

    for fn, name in ((one_exchange, "1-exchange"), (two_exchange, "2-exchange")):
        for _ in range(200):
            await fn()
        times = []
        for _ in range(7):
            t0 = time.perf_counter_ns()
            for _ in range(500):
                await fn()
            times.append((time.perf_counter_ns() - t0) / 500)
        times.sort()
        print(
            f"grpc-aio {label} {name} RTT: min={times[0]:.0f}ns "
            f"median={times[len(times) // 2]:.0f}ns"
        )
    await channel.close()


async def main() -> None:
    targets = [("127.0.0.1:0", "TCP")]
    uds_path = "/tmp/wool_echo_rtt.sock"
    if hasattr(__import__("socket"), "AF_UNIX"):
        try:
            os.unlink(uds_path)
        except OSError:
            pass
        targets.append((f"unix:{uds_path}", "UDS"))

    for bind, label in targets:
        ready = threading.Event()
        box: dict = {}
        t = threading.Thread(target=start_server, args=(bind, ready, box), daemon=True)
        t.start()
        ready.wait(timeout=10)
        await bench_channel(box["addr"], label)
        box["loop"].call_soon_threadsafe(
            lambda: asyncio.ensure_future(box["server"].stop(0))
        )


if __name__ == "__main__":
    asyncio.run(main())
