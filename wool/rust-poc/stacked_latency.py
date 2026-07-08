"""Same-session, interleaved latency benchmark of the stacked dispatch opts,
measured through shapebench's own shapes + sampling. Interleaving configs across
reps cancels slow machine drift so small per-opt gains are resolvable.

Configs (cumulative): baseline -> +#274 eager-first-next -> +#273 pickle-memoize
-> +worker proxy-memo. Caller-side flags toggled as module globals; worker-side
proxy-memo via env before each pool spawn (fresh workers read it at import).

Focus: LATENCY -> low granularities. Reports p50 makespan (us) per config/shape/g.
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
SP = os.path.dirname(os.path.abspath(__file__))
W274 = os.path.abspath(os.path.join(SP, "..", "w274"))
sys.path.insert(0, W274)
sys.path.insert(0, "benchmarks")

from shapebench.frameworks.wool import WoolAdapter  # noqa: E402
from shapebench.shapes import SHAPES  # noqa: E402
from shapebench.stats import median  # noqa: E402
from shapebench.stats import sample  # noqa: E402

import wool  # noqa: E402
import wool.runtime.routine.task as taskmod  # noqa: E402
import wool.runtime.worker.connection as conn  # noqa: E402

assert "w274" in wool.__file__, wool.__file__
assert hasattr(conn, "EAGER_FIRST_NEXT"), "w274 connection.py missing EAGER_FIRST_NEXT"
assert hasattr(taskmod, "MEMOIZE_PICKLE"), "w274 task.py missing MEMOIZE_PICKLE"

SHAPE_IDS = ["s1", "s2", "s3", "s4", "s5", "s6", "s7"]
GRANS = [0.0, 1e-4]
WORKERS = 4
WARMUP = 8
ITERS = 40
REPS = 4  # == len(CONFIGS): with per-rep rotation, each config hits each position once

CONFIGS = [
    ("baseline", dict(eager=False, pickle=False, proxy=False)),
    ("+eager", dict(eager=True, pickle=False, proxy=False)),
    ("+eager+pickle", dict(eager=True, pickle=True, proxy=False)),
    ("+eager+pickle+proxy", dict(eager=True, pickle=True, proxy=True)),
]

OUT = os.path.join("benchmarks", "results", "stacked_latency.jsonl")


async def measure_config(name, cfg, rep, out) -> None:
    conn.EAGER_FIRST_NEXT = cfg["eager"]
    taskmod.MEMOIZE_PICKLE = cfg["pickle"]
    os.environ["WOOL_PROXY_MEMO"] = "1" if cfg["proxy"] else "0"

    adapter = WoolAdapter()
    await adapter.setup(WORKERS)
    try:
        await SHAPES["s1"].run(adapter, 0.0, {})  # warm the dispatch path
        for i, sid in enumerate(SHAPE_IDS):
            if i > 0:
                await adapter.reset()
            spec = SHAPES[sid]
            for g in GRANS:

                async def once(spec=spec, g=g):
                    await spec.run(adapter, g, spec.params)

                try:
                    p50 = median(await sample(once, WARMUP, ITERS))
                except Exception as exc:
                    p50 = None
                    print(f"  [{name}] {sid} g={g}: ERR {type(exc).__name__}: {exc}")
                out.write(
                    json.dumps(
                        {
                            "config": name,
                            "rep": rep,
                            "shape": sid,
                            "granularity_s": g,
                            "makespan_p50_s": p50,
                            "eager": cfg["eager"],
                            "pickle": cfg["pickle"],
                            "proxy": cfg["proxy"],
                        }
                    )
                    + "\n"
                )
                out.flush()
    finally:
        await adapter.teardown()


async def main() -> None:
    print(f"wool {wool.__version__} [{wool.__file__.split('/scratchpad/')[-1]}]")
    with open(OUT, "w") as out:
        for rep in range(REPS):
            # rotate config order per rep so each config is measured in each
            # position once — cancels monotonic within-rep machine drift.
            k = rep % len(CONFIGS)
            order = CONFIGS[k:] + CONFIGS[:k]
            for name, cfg in order:
                await measure_config(name, cfg, rep, out)
                print(f"rep {rep} config {name!r} done")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    asyncio.run(main())
