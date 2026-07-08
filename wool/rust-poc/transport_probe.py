"""Decompose the grpc-aio transport cost:
  (1) raw asyncio socket ping-pong on a persistent connection  -> syscall/transport floor
  (2) grpc-aio per-exchange on a PERSISTENT bidi stream        -> warm per-message cost
Compare against the per-CALL stream-setup numbers from grpc_echo_rtt.py to split
per-call-setup (Python-protocol, fixable by stream reuse) from per-message
integration overhead (grpc-aio asyncio bridge, only a native stack escapes).
"""

import asyncio
import os
import socket
import sys
import threading
import time

os.environ["GRPC_VERBOSITY"] = "NONE"
sys.path.insert(0, "wool/src")

import grpc.aio  # noqa: E402

import wool  # noqa: E402,F401
from wool import protocol  # noqa: E402


def stats(fn_times):
    fn_times.sort()
    return fn_times[0], fn_times[len(fn_times) // 2]


# ---------- (1) raw asyncio socket ping-pong, persistent connection ----------
async def raw_socket_rtt():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    srv.setblocking(False)
    addr = srv.getsockname()
    loop = asyncio.get_running_loop()

    async def server():
        conn, _ = await loop.sock_accept(srv)
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        conn.setblocking(False)
        while True:
            data = await loop.sock_recv(conn, 64)
            if not data:
                break
            await loop.sock_sendall(conn, data)

    st = asyncio.ensure_future(server())
    cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    cli.setblocking(False)
    await loop.sock_connect(cli, addr)
    cli.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    async def ping():
        await loop.sock_sendall(cli, b"ping")
        await loop.sock_recv(cli, 64)

    for _ in range(500):
        await ping()
    times = []
    for _ in range(7):
        t0 = time.perf_counter_ns()
        for _ in range(1000):
            await ping()
        times.append((time.perf_counter_ns() - t0) / 1000)
    mn, md = stats(times)
    print(f"raw asyncio socket ping-pong (persistent): min={mn:.0f}ns median={md:.0f}ns")
    cli.close()
    st.cancel()
    srv.close()


# ---------- (2) grpc-aio persistent bidi stream, per-exchange ----------
class PersistentEcho(protocol.WorkerServicer):
    async def dispatch(self, request_iterator, context):
        async for _req in request_iterator:
            yield protocol.Response(result=protocol.Message(dump=b""))

    async def stop(self, request, context):
        return protocol.Void()


def start_server(bind, ready, box):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def serve():
        server = grpc.aio.server()
        port = server.add_insecure_port(bind)
        protocol.add_to_server[protocol.WorkerServicer](PersistentEcho(), server)
        await server.start()
        box["addr"] = f"127.0.0.1:{port}"
        box["server"] = server
        box["loop"] = loop
        ready.set()
        await server.wait_for_termination()

    loop.run_until_complete(serve())


async def grpc_persistent_rtt():
    ready = threading.Event()
    box = {}
    t = threading.Thread(
        target=start_server, args=("127.0.0.1:0", ready, box), daemon=True
    )
    t.start()
    ready.wait(timeout=10)

    channel = grpc.aio.insecure_channel(box["addr"])
    stub = protocol.WorkerStub(channel)
    next_req = protocol.Request(next=protocol.Void())

    call = stub.dispatch()

    async def exchange():
        await call.write(next_req)
        await call.read()

    for _ in range(500):
        await exchange()
    times = []
    for _ in range(7):
        t0 = time.perf_counter_ns()
        for _ in range(1000):
            await exchange()
        times.append((time.perf_counter_ns() - t0) / 1000)
    mn, md = stats(times)
    print(f"grpc-aio PERSISTENT-stream per-exchange: min={mn:.0f}ns median={md:.0f}ns")
    await channel.close()


async def main():
    await raw_socket_rtt()
    await grpc_persistent_rtt()


if __name__ == "__main__":
    asyncio.run(main())
