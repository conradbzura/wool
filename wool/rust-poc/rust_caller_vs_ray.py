"""Four-way shapebench @ g=0: Python-wool vs Rust-caller (Rust client + Python
worker) vs Rust-both (Rust client + Rust worker) vs Ray. Tests whether rustifying
the CALLER closes the caller-bound fan-out gap. Coroutine shapes s1-s6 (s7 streaming
falls back to the Python caller in the prototype).
"""

import asyncio
import json
import logging
import os
import sys
import warnings

os.environ["GRPC_VERBOSITY"] = "NONE"
warnings.filterwarnings("ignore")
logging.disable(logging.CRITICAL)
sys.path.insert(0, "wool/src")
sys.path.insert(0, "benchmarks")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import rust_caller_patch  # noqa: E402,F401  (applies the WorkerConnection.dispatch patch)
from shapebench.shapes import SHAPES  # noqa: E402
from shapebench.stats import median  # noqa: E402
from shapebench.stats import sample  # noqa: E402

SHAPE_IDS = ["s1", "s2", "s3", "s4", "s5", "s6"]
G = 0.0
W = 4
WARMUP, ITERS = 4, 20
OUT = "benchmarks/results/rust_caller_vs_ray.jsonl"


def make_adapter(name):
    from rust_worker_adapter import RustWoolAdapter
    from shapebench.frameworks.wool import WoolAdapter

    if name in ("py-wool", "rust-caller"):
        return WoolAdapter()
    if name == "rust-both":
        return RustWoolAdapter()
    if name == "ray":
        from shapebench.frameworks.ray import RayAdapter

        return RayAdapter()
    raise ValueError(name)


CONFIGS = [
    ("py-wool", "0"),  # Python caller + Python worker
    ("rust-caller", "1"),  # Rust caller + Python worker
    ("rust-both", "1"),  # Rust caller + Rust worker
    ("ray", "0"),
]


async def run(name, rust_caller, out):
    os.environ["WOOL_RUST_CALLER"] = rust_caller
    adapter = make_adapter(name)
    await adapter.setup(W)
    try:
        await SHAPES["s1"].run(adapter, 0.0, {})
        for sid in SHAPE_IDS:
            spec = SHAPES[sid]

            async def once(spec=spec):
                await spec.run(adapter, G, spec.params)

            try:
                p50 = median(await asyncio.wait_for(sample(once, WARMUP, ITERS), 45))
            except (Exception, asyncio.TimeoutError) as exc:  # noqa: BLE001
                p50 = None
                print(f"  [{name}] {sid}: ERR {type(exc).__name__}: {exc}"[:110])
            out.write(
                json.dumps({"config": name, "shape": sid, "makespan_p50_s": p50}) + "\n"
            )
            out.flush()
            if p50:
                print(f"  [{name:<11}] {sid}: {p50 * 1e6:8.0f} us")
    finally:
        try:
            await adapter.teardown()
        except FileNotFoundError:
            pass


async def main():
    # Run one config per process (sys.argv[1]) to isolate cross-config state
    # (channel cache, bridge loop, pool teardown). Append to OUT.
    which = sys.argv[1] if len(sys.argv) > 1 else None
    configs = [(n, rc) for (n, rc) in CONFIGS if which is None or n == which]
    mode = "a" if which else "w"
    with open(OUT, mode) as out:
        for name, rc in configs:
            print(f"=== {name} ===")
            await run(name, rc, out)


if __name__ == "__main__":
    asyncio.run(main())
