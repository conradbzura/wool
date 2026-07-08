"""Python executor process (rustification slice v3: REAL wool routines).

Now a proper wool worker minus the gRPC/frame transport: it replicates
WorkerProcess.run's setup (proxy_pool + task factory) and drives routines through
wool's own `routine_scope` (which binds wool.__proxy__ for nested dispatch, enters
the runtime context, and unwraps the callable). Rust forwards the whole opaque
Task message; the executor rebuilds it with Task.from_protobuf. PUSH streaming.

IPC framing (length-prefixed, little-endian):
  request : u32 total | u64 disp_id | u8 op | <op fields>
    op 0 START : u32 tlen | task_protobuf_bytes
    op 4 CANCEL: (none)
  push    : u32 total | u64 disp_id | u8 status | u32 plen | payload
    status 0 VALUE (pickled) | 1 EXC (pickled) | 2 STOP
"""

import asyncio
import os
import struct
import sys
from inspect import isasyncgen

sys.path.insert(0, "wool/src")
sys.path.insert(0, "benchmarks")  # so cloudpickle can resolve by-reference routines

import cloudpickle  # noqa: E402

import wool  # noqa: E402
from wool import protocol  # noqa: E402
from wool.runtime.context.factory import install_task_factory  # noqa: E402
from wool.runtime.resourcepool import ResourcePool  # noqa: E402
from wool.runtime.routine.task import Task  # noqa: E402
from wool.runtime.routine.task import routine_scope  # noqa: E402
from wool.runtime.worker.process import _proxy_factory  # noqa: E402
from wool.runtime.worker.process import _proxy_finalizer  # noqa: E402

SOCK = os.environ.get("WOOL_EXEC_SOCK", "/tmp/wool_executor.sock")
OP_START, OP_CANCEL = 0, 4
VALUE, EXC, STOP = 0, 1, 2


async def _read_exact(reader, n):
    return await reader.readexactly(n)


def _pack_push(disp_id, status, payload=b""):
    body = struct.pack("<QBI", disp_id, status, len(payload)) + payload
    return struct.pack("<I", len(body)) + body


async def _handle_conn(reader, writer):
    tasks: dict[int, asyncio.Task] = {}
    write_lock = asyncio.Lock()

    async def push(disp_id, status, payload=b""):
        async with write_lock:
            writer.write(_pack_push(disp_id, status, payload))
            await writer.drain()

    async def drive(disp_id, task_bytes):
        try:
            task = Task.from_protobuf(protocol.Task.FromString(task_bytes))
            async with routine_scope(task) as routine:
                if isasyncgen(routine):
                    while True:
                        try:
                            v = await routine.asend(None)
                        except StopAsyncIteration:
                            break
                        await push(disp_id, VALUE, cloudpickle.dumps(v))
                    await push(disp_id, STOP)
                else:
                    result = await routine
                    await push(disp_id, VALUE, cloudpickle.dumps(result))
                    await push(disp_id, STOP)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            try:
                await push(disp_id, EXC, cloudpickle.dumps(exc))
            except Exception:
                await push(disp_id, EXC, cloudpickle.dumps(RuntimeError(repr(exc))))
        finally:
            tasks.pop(disp_id, None)

    try:
        while True:
            (total,) = struct.unpack("<I", await _read_exact(reader, 4))
            buf = await _read_exact(reader, total)
            off = 0
            (disp_id, op) = struct.unpack_from("<QB", buf, off)
            off += 9
            if op == OP_START:
                (tlen,) = struct.unpack_from("<I", buf, off)
                off += 4
                task_bytes = buf[off : off + tlen]
                tasks[disp_id] = asyncio.create_task(drive(disp_id, task_bytes))
            elif op == OP_CANCEL:
                t = tasks.get(disp_id)
                if t is not None and not t.done():
                    t.cancel()
    except asyncio.IncompleteReadError:
        pass


async def main():
    if os.path.exists(SOCK):
        os.unlink(SOCK)
    # Replicate WorkerProcess.run's worker-side setup so routine_scope works
    # (nested dispatch, runtime context, chain fork-on-task semantics).
    wool.__proxy_pool__.set(
        ResourcePool(factory=_proxy_factory, finalizer=_proxy_finalizer, ttl=60.0)
    )
    install_task_factory()
    server = await asyncio.start_unix_server(_handle_conn, path=SOCK)
    print(f"EXECUTOR_READY {SOCK}", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
