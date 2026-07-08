"""Knob sweep: measure Wool makespan across (shape, structural size, workers) at
a fixed granularity, for one load-balancer variant.

Run once per variant by toggling the env flag; the variant is read back from the
imported flag so the output is self-labeling:

    ./.venv/bin/python benchmarks/sweep.py --out results/sweep_base.jsonl
    WOOL_PER_WORKER_LOCK=1 ./.venv/bin/python benchmarks/sweep.py --out results/sweep_pwl.jsonl

analyze.py joins the two files into per-cell speedups. Defaults sweep at g=0
(pure dispatch plumbing), where the per-worker-lock effect is undiluted by exec
time and its functional form is cleanest.
"""

import argparse
import asyncio
import json

from shapebench.stats import median, percentile, sample


def _load_adapter(framework: str):
    if framework == "wool":
        from shapebench.frameworks.wool import WoolAdapter

        return WoolAdapter()
    if framework == "ray":
        from shapebench.frameworks.ray import RayAdapter

        return RayAdapter()
    raise ValueError(f"unknown framework: {framework}")


def _variant(framework: str) -> str:
    if framework == "ray":
        return "ray"
    import wool.runtime.loadbalancer.roundrobin as rr

    return "pwl" if rr.PER_WORKER_LOCK else "base"


# shape -> list of structural sizes to sweep (None = shape has no size knob).
SIZES: dict[str, list] = {
    "s1": [None],
    "s2": [8, 16, 32, 64, 128],
    "s3": [8, 16, 32, 64, 128],
    "s4": [4, 8, 16, 32, 64],
    "s5": [1, 2, 3, 4],
    "s6": [None],
    "s7": [8, 16, 32, 64, 128],
}
METHOD = {
    "s1": "s1_point_to_point",
    "s2": "s2_fanout",
    "s3": "s3_scatter_gather",
    "s4": "s4_pipeline",
    "s5": "s5_recursive_tree",
    "s6": "s6_diamond",
    "s7": "s7_streaming",
}


def params_for(shape: str, size) -> dict:
    if shape == "s5":
        return {"depth": size, "branch": 2}
    if size is None:
        return {}
    if shape == "s4":
        return {"depth": size}
    return {"n": size}


async def run(args: argparse.Namespace) -> None:
    variant = _variant(args.framework)
    shapes = [s.strip() for s in args.shapes.split(",") if s.strip()]
    sizes_by_shape = {s: SIZES[s] for s in shapes}
    workers = [int(x) for x in args.workers.split(",")]
    g = args.g
    out = open(args.out, "w")
    print(f"# framework={args.framework} variant={variant} workers={workers} g={g}")
    for w in workers:
        adapter = _load_adapter(args.framework)
        await adapter.setup(w)
        try:
            for i, (shape, sizes) in enumerate(sizes_by_shape.items()):
                if i > 0:
                    await adapter.reset()
                method = getattr(adapter, METHOD[shape])
                for size in sizes:
                    params = params_for(shape, size)

                    async def once(method=method, params=params) -> None:
                        await method(g, **params)

                    try:
                        samples = await sample(once, args.warmup, args.iters)
                        rec = {
                            "variant": variant,
                            "shape": shape,
                            "size": size,
                            "workers": w,
                            "g": g,
                            "p50_s": median(samples),
                            "p99_s": percentile(samples, 0.99),
                            "samples": len(samples),
                            "error": None,
                        }
                    except Exception as exc:
                        rec = {
                            "variant": variant,
                            "shape": shape,
                            "size": size,
                            "workers": w,
                            "g": g,
                            "p50_s": None,
                            "p99_s": None,
                            "samples": 0,
                            "error": f"{type(exc).__name__}: {exc}"[:150],
                        }
                        try:
                            await adapter.reset()
                        except Exception:
                            pass
                    out.write(json.dumps(rec) + "\n")
                    out.flush()
                    tag = rec["p50_s"] and f"{rec['p50_s'] * 1e6:.0f}us" or rec["error"]
                    print(f"  {variant} W={w} {shape} size={size}: {tag}")
        finally:
            await adapter.teardown()
    out.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--framework", default="wool", choices=["wool", "ray"])
    ap.add_argument(
        "--shapes",
        default="s1,s2,s3,s4,s5,s6,s7",
        help="Ray deadlocks on s5 (nested blocking) — exclude it for ray",
    )
    ap.add_argument("--workers", default="1,2,4,8")
    ap.add_argument("--g", type=float, default=0.0)
    ap.add_argument("--iters", type=int, default=10)
    ap.add_argument("--warmup", type=int, default=3)
    asyncio.run(run(ap.parse_args()))


if __name__ == "__main__":
    main()
