"""Three-way shapebench benchmark, same session: Python-wool worker vs Rust-wool
worker (tonic+executor) vs Ray. Reports makespan p50 + overhead ratio per shape.
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

from shapebench.ideal import brent_ideal  # noqa: E402
from shapebench.ideal import overhead_ratio  # noqa: E402
from shapebench.shapes import SHAPES  # noqa: E402
from shapebench.stats import median  # noqa: E402
from shapebench.stats import sample  # noqa: E402

SHAPE_IDS = ["s1", "s2", "s3", "s4", "s5", "s6", "s7"]
GRANS = [0.0, 1e-4]
W = 4
WARMUP = 4
ITERS = 20
OUT = "benchmarks/results/rust_vs_ray.jsonl"


def load_adapters():
    from rust_worker_adapter import RustWoolAdapter
    from shapebench.frameworks.wool import WoolAdapter

    adapters = [("wool", WoolAdapter()), ("rust-wool", RustWoolAdapter())]
    try:
        from shapebench.frameworks.ray import RayAdapter

        adapters.append(("ray", RayAdapter()))
    except Exception as e:  # noqa: BLE001
        print(f"(ray unavailable: {type(e).__name__})")
    return adapters


async def run_adapter(name, adapter, out):
    await adapter.setup(W)
    try:
        await SHAPES["s1"].run(adapter, 0.0, {})  # warm the path
        # One pool for all shapes (no per-shape reset): avoids a wool
        # LocalDiscovery shared-memory teardown race on rapid respawn, and is
        # faster. Minor cross-shape contamination is acceptable for latency.
        for sid in SHAPE_IDS:
            spec = SHAPES[sid]
            for g in GRANS:
                work, span = spec.work(spec.params), spec.span(spec.params)
                ideal = brent_ideal(work, span, W, g)

                async def once(spec=spec, g=g):
                    await spec.run(adapter, g, spec.params)

                err = None
                try:
                    samples = await sample(once, WARMUP, ITERS)
                    p50 = median(samples)
                    r = overhead_ratio(p50, ideal)
                except Exception as exc:  # noqa: BLE001
                    p50 = r = None
                    err = f"{type(exc).__name__}: {exc}"[:120]
                out.write(
                    json.dumps(
                        {
                            "framework": name,
                            "shape": sid,
                            "granularity_s": g,
                            "makespan_p50_s": p50,
                            "overhead_ratio": r,
                            "ideal_s": ideal,
                            "error": err,
                        }
                    )
                    + "\n"
                )
                out.flush()
                if p50 is None:
                    tag = f"ERR {err}"
                elif r is None:
                    tag = f"{p50 * 1e6:8.0f}us R=inf"
                else:
                    tag = f"{p50 * 1e6:8.0f}us R={r:5.1f}"
                print(f"  [{name:<9}] {sid} g={g:g}: {tag}")
    finally:
        try:
            await adapter.teardown()
        except FileNotFoundError:
            pass  # wool LocalDiscovery shm cleanup race — benign


async def main():
    with open(OUT, "w") as out:
        for name, adapter in load_adapters():
            print(f"=== {name} ===")
            await run_adapter(name, adapter, out)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    asyncio.run(main())
