"""Premise check for recommendation #2 (worker-side proxy loads-memo): across an
s2 fan-out, are the per-task proxy pickle bytes identical (so a worker cache keyed
by proxy_id / bytes would hit ~N-1 of N times)? Also across DIFFERENT routines.
"""

import asyncio
import hashlib
import os
import sys

os.environ["GRPC_VERBOSITY"] = "NONE"
sys.path.insert(0, "wool/src")
sys.path.insert(0, "benchmarks")

from shapebench.frameworks.wool import WoolAdapter  # noqa: E402

import wool  # noqa: E402,F401
from wool.runtime.routine.task import Task  # noqa: E402

_orig = Task.to_protobuf
proxies: list[tuple[str, str]] = []  # (proxy_id, sha1(proxy_bytes))


def patched(self):
    pb = _orig(self)
    proxies.append((pb.proxy_id, hashlib.sha1(pb.proxy).hexdigest()[:12]))
    return pb


Task.to_protobuf = patched


async def main() -> None:
    a = WoolAdapter()
    await a.setup(2)
    try:
        await a.s2_fanout(0.0, 16)  # 16 tasks, one routine
        await a.s3_scatter_gather(0.0, 8)  # 8 _stage + 1 _reduce, mixed routines
    finally:
        await a.teardown()
    ids = {p[0] for p in proxies}
    byte_hashes = {p[1] for p in proxies}
    print(f"total tasks dispatched: {len(proxies)}")
    print(f"distinct proxy_id: {len(ids)}")
    print(f"distinct proxy-byte hashes: {len(byte_hashes)}")
    print(
        f"cache would hit: {len(proxies) - len(byte_hashes)}/{len(proxies)} "
        f"({100 * (len(proxies) - len(byte_hashes)) / len(proxies):.0f}%)"
    )


if __name__ == "__main__":
    asyncio.run(main())
