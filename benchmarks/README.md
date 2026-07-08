# shapebench

A benchmark suite that measures how a task-execution framework's per-task overhead changes across the *shape* of the workload, rather than on a single latency or throughput number. See [`METHODOLOGY.md`](./METHODOLOGY.md) for the full design, the shape ladder (S1–S7), the overhead-ratio metric, and the positioning against prior art (Task Bench, TaPS, the 2026 granularity paper, IISWC'20).

**Status:** Phase 1 proof-of-concept — Wool and Ray only.

## Layout

```
benchmarks/
  METHODOLOGY.md          the design / methodology (source of truth)
  pyproject.toml          the shapebench package
  run.py                  entry point (has the spawn-guard __main__ block)
  shapebench/
    adapter.py            the shape-level Adapter interface (NOT submit/map)
    shapes.py             the S1–S7 registry: params, work/span, adapter method
    workloads.py          the tunable-duration busy-noop task body
    ideal.py              Brent's-bound ideal makespan, overhead ratio R, METG
    stats.py              warmup / steady-state sampling
    records.py            JSON-lines run records
    cli.py                the runner
    frameworks/
      wool.py             Wool adapter (idiomatic @wool.routine per shape)
      ray.py              Ray adapter (idiomatic remote-task / actor per shape)
```

## Setup (isolated venv)

This suite uses its own virtualenv so the benchmarked Wool build is pinned to this clone and never depends on the shared sandbox venv.

```sh
cd /Users/conrad/Projects/wool-labs/wool-staging/.sandbox/wool-benchmark

# create an isolated interpreter for the suite
uv venv .venv --python 3.13

# install: wool from THIS clone (editable), the suite, and ray
VIRTUAL_ENV=.venv uv pip install -e ./wool -e ./benchmarks ray
```

## Run

```sh
# from the benchmarks/ directory, with the isolated venv active
.venv/bin/python benchmarks/run.py --framework wool --workers 4
.venv/bin/python benchmarks/run.py --framework ray  --workers 4

# a quick subset while iterating
.venv/bin/python benchmarks/run.py --framework wool --shapes s1,s2,s5,s7 \
    --granularities 0,1e-3 --iters 20 --out results.jsonl
```

Each run prints a per-(shape, granularity) table of the overhead ratio `R = measured / ideal` and a per-shape METG summary (smallest granularity at which `R ≤ 2`, i.e. ≥50% efficiency), and — with `--out` — appends one JSON record per measurement.

## PoC scope / caveats

- **Metric is run-level, not per-task.** Makespan is timed at the driver; the finer TaPS per-task record schema (submit/start/complete/transform/resolve) is a later refinement.
- **S5 tree is intentionally small** (depth 2, branch 2). A blocking compute fabric holds a worker slot per internal node while awaiting children, so a deep tree can deadlock Ray at low worker counts — that boundary is a deliberate future experiment, not something to trip over in the PoC.
- **Wool baseline is v0.10.0**, which carries the known dispatch regression; published Wool numbers must state the exact build (the optimization work #262/#263/#273/#274 is not merged here).
