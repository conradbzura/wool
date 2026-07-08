"""Isolate the Rust↔executor bridge cost: connect to the executor's unix socket
directly and measure the IPC round-trip (send pickled noop, get pickled result).
Bounds how much of the 398µs worker-plane RTT is the bridge vs the gRPC transport.
(Python-side asyncio socket here; Rust's tokio side is at least as fast.)
"""

import asyncio
import os
import struct
import sys
import time

sys.path.insert(0, "wool/src")
import cloudpickle  # noqa: E402

SOCK = os.environ.get("WOOL_EXEC_SOCK", "/tmp/wool_exec_ab.sock")


async def _noop():
    return None


def build_request(req_id, c, a, k):
    body = struct.pack("<Q", req_id)
    for f in (c, a, k):
        body += struct.pack("<I", len(f)) + f
    return struct.pack("<I", len(body)) + body


async def main():
    reader, writer = await asyncio.open_unix_connection(SOCK)
    c = cloudpickle.dumps(_noop)
    a = cloudpickle.dumps(())
    k = cloudpickle.dumps({})

    async def one(i):
        writer.write(build_request(i, c, a, k))
        await writer.drain()
        (total,) = struct.unpack("<I", await reader.readexactly(4))
        await reader.readexactly(total)

    for i in range(300):
        await one(i)
    times = []
    for r in range(7):
        t0 = time.perf_counter_ns()
        for i in range(1000):
            await one(i)
        times.append((time.perf_counter_ns() - t0) / 1000)
    times.sort()
    print(
        f"executor IPC RTT (unpickle noop + run + pickle + socket x2): "
        f"min={times[0]:.0f}ns median={times[len(times) // 2]:.0f}ns"
    )
    writer.close()


if __name__ == "__main__":
    asyncio.run(main())
