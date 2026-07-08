"""PoC runner: sweep granularities across the shape ladder for one framework.

For each (shape, granularity) it warms, samples the makespan, computes the
overhead ratio R against Brent's ideal, writes a JSON-lines record, and prints a
table plus a per-shape METG summary.
"""

import argparse
import asyncio

from shapebench.adapter import Adapter
from shapebench.ideal import brent_ideal, find_metg, overhead_ratio
from shapebench.records import RecordLogger, RunRecord
from shapebench.shapes import SHAPES
from shapebench.stats import median, percentile, sample


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="shapebench PoC runner (Wool/Ray)")
    ap.add_argument("--framework", required=True, choices=["wool", "ray"])
    ap.add_argument("--shapes", default="s1,s2,s3,s4,s5,s6,s7")
    ap.add_argument(
        "--granularities",
        default="0,1e-4,1e-3,1e-2",
        help="comma-separated task durations in seconds",
    )
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--iters", type=int, default=30)
    ap.add_argument("--warmup", type=int, default=5)
    ap.add_argument("--out", default=None, help="JSONL output path (optional)")
    return ap.parse_args()


def _load_adapter(name: str) -> Adapter:
    if name == "wool":
        from shapebench.frameworks.wool import WoolAdapter

        return WoolAdapter()
    if name == "ray":
        from shapebench.frameworks.ray import RayAdapter

        return RayAdapter()
    raise ValueError(f"unknown framework: {name}")


def _fmt_r(r: float | None) -> str:
    return "  inf" if r is None else f"{r:6.1f}"


async def run(args: argparse.Namespace) -> None:
    shape_ids = [s.strip() for s in args.shapes.split(",") if s.strip()]
    granularities = [float(x) for x in args.granularities.split(",")]
    adapter = _load_adapter(args.framework)
    logger = RecordLogger(args.out)

    await adapter.setup(args.workers)
    print(
        f"# framework={adapter.name} v{adapter.version()} "
        f"workers={args.workers} iters={args.iters} warmup={args.warmup}"
    )
    print(
        f"# {'shape':<16} {'g(s)':>8} {'R':>6} {'p50(us)':>10} {'p99(us)':>10} {'ideal(us)':>10}"
    )
    records: list[RunRecord] = []
    try:
        # Warm the whole dispatch path once before any shape is measured.
        await SHAPES["s1"].run(adapter, 0.0, {})

        for i, sid in enumerate(shape_ids):
            spec = SHAPES[sid]
            # Fresh pool per shape so one shape's accumulated state never taints
            # the next (Wool spawns new workers; Ray is a no-op).
            if i > 0:
                await adapter.reset()
            for g in granularities:
                work, span = spec.work(spec.params), spec.span(spec.params)
                ideal = brent_ideal(work, span, args.workers, g)

                async def once(spec=spec, g=g) -> None:
                    await spec.run(adapter, g, spec.params)

                try:
                    samples = await sample(once, args.warmup, args.iters)
                    p50 = median(samples)
                    p99 = percentile(samples, 0.99)
                    r = overhead_ratio(p50, ideal)
                    error = None
                except Exception as exc:  # a shape may fail under load — that is data
                    p50 = p99 = r = None
                    error = f"{type(exc).__name__}: {exc}"[:200]

                rec = RunRecord(
                    framework=adapter.name,
                    framework_version=adapter.version(),
                    shape=spec.id,
                    shape_name=spec.name,
                    workers=args.workers,
                    granularity_s=g,
                    params=dict(spec.params),
                    work=work,
                    span=span,
                    ideal_s=ideal,
                    makespan_p50_s=p50,
                    makespan_p99_s=p99,
                    overhead_ratio=r,
                    samples=len(samples) if error is None else 0,
                    error=error,
                )
                logger.write(rec)
                records.append(rec)
                if error is None:
                    print(
                        f"  {spec.id + ' ' + spec.name:<16} {g:>8.1e} {_fmt_r(r)} "
                        f"{p50 * 1e6:>10.1f} {p99 * 1e6:>10.1f} {ideal * 1e6:>10.1f}"
                    )
                else:
                    print(
                        f"  {spec.id + ' ' + spec.name:<16} {g:>8.1e}  FAILED  {error}"
                    )
                    # Recover the pool so the remaining cells get a fair chance.
                    try:
                        await adapter.reset()
                    except Exception:
                        pass
    finally:
        await adapter.teardown()
        logger.close()

    print("\n# per-shape METG (smallest g with R <= 2, i.e. >=50% efficiency)")
    for sid in shape_ids:
        pts = [(r.granularity_s, r.overhead_ratio) for r in records if r.shape == sid]
        metg = find_metg(pts)
        metg_str = "not reached" if metg is None else f"{metg * 1e6:.0f} us"
        print(f"  {sid} {SHAPES[sid].name:<16} METG = {metg_str}")


def main() -> None:
    asyncio.run(run(parse_args()))
