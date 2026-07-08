"""Granularity sweep for the FULL RUST STACK (Rust caller + Rust worker) vs Ray,
same session, W=4 — to chart the shape ladder in the canonical R-vs-g format.
s7 streaming's caller falls back to Python (the Rust client is coroutine-only), so
s7 = Python caller + Rust worker (noted on the chart).
"""

import asyncio
import json
import logging
import os
import sys
import warnings

os.environ["GRPC_VERBOSITY"] = "NONE"
os.environ["WOOL_RUST_CALLER"] = "1"
os.environ["WOOL_MEMOIZE_PICKLE"] = os.environ.get("WOOL_MEMOIZE_PICKLE", "1")
warnings.filterwarnings("ignore")
logging.disable(logging.CRITICAL)
sys.path.insert(0, "wool/src")
sys.path.insert(0, "benchmarks")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import memoize_patch  # noqa: E402,F401  (#273 pickle-memoize on Task.to_protobuf)
import rust_caller_patch  # noqa: E402,F401  (patches WorkerConnection.dispatch)
from shapebench.ideal import brent_ideal  # noqa: E402
from shapebench.ideal import overhead_ratio  # noqa: E402
from shapebench.shapes import SHAPES  # noqa: E402
from shapebench.stats import median  # noqa: E402
from shapebench.stats import sample  # noqa: E402

SHAPE_IDS = ["s1", "s2", "s3", "s4", "s5", "s6", "s7"]
GRANS = [0.0, 1e-4, 1e-3, 1e-2]
W = 4
WARMUP, ITERS = 4, 15
OUT = "benchmarks/results/rust_stack_memo_sweep.jsonl"


def load_adapters():
    from rust_worker_adapter import RustWoolAdapter
    from shapebench.frameworks.ray import RayAdapter

    return [("wool-full-rust-memo", RustWoolAdapter()), ("ray", RayAdapter())]


async def run_adapter(name, adapter, out):
    await adapter.setup(W)
    try:
        await SHAPES["s1"].run(adapter, 0.0, {})
        for sid in SHAPE_IDS:
            spec = SHAPES[sid]
            for g in GRANS:
                work, span = spec.work(spec.params), spec.span(spec.params)
                ideal = brent_ideal(work, span, W, g)

                async def once(spec=spec, g=g):
                    await spec.run(adapter, g, spec.params)

                try:
                    p50 = median(await asyncio.wait_for(sample(once, WARMUP, ITERS), 60))
                    r = overhead_ratio(p50, ideal)
                except (Exception, asyncio.TimeoutError) as exc:  # noqa: BLE001
                    p50 = r = None
                    print(f"  [{name}] {sid} g={g:g}: ERR {type(exc).__name__}")
                out.write(
                    json.dumps(
                        {
                            "framework": name,
                            "shape": sid,
                            "granularity_s": g,
                            "makespan_p50_s": p50,
                            "overhead_ratio": r,
                            "ideal_s": ideal,
                        }
                    )
                    + "\n"
                )
                out.flush()
                if p50:
                    rr = f"R={r:.1f}" if r else "R=inf"
                    print(f"  [{name:<14}] {sid} g={g:g}: {p50 * 1e6:9.0f}us {rr}")
    finally:
        try:
            await adapter.teardown()
        except FileNotFoundError:
            pass


async def main():
    with open(OUT, "w") as out:
        for name, adapter in load_adapters():
            print(f"=== {name} ===")
            await run_adapter(name, adapter, out)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    asyncio.run(main())
