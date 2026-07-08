# Rust dispatch-plane proof-of-concept

Exploratory PoC (Claude session capture) rustifying wool's dispatch transport via
tonic / pyo3 / maturin, benchmarked against Ray through the shapebench suite.
**Measurement-only** against the wool package source — nothing in `src/wool` was
changed; every artifact lives here.

**Start with [`REPORT.md`](REPORT.md)** (the synthesized report). [`FINDINGS.md`](FINDINGS.md)
is the full chronological measurement log (R1–R16).

## Headline

- **Diagnosis:** the transport is ~65% of the dispatch RTT; the worker orchestration
  queues are ~7%. (`rtt_decomposition`, `microbench.py`, `transport_probe.py`, `tonic_echo/`)
- **Stacked pure-Python opts** (#274 eager-first-next, #273 pickle-memoize, worker
  proxy-memo): −15…24% dispatch latency on s1–s6. (`stacked_latency.py`)
- **Rust worker plane** (`rust_worker/` tonic+prost + `executor.py`, unix-socket IPC):
  coroutine −32%, streaming −35% (executor-push), cancellation deterministic.
  Integrated as a drop-in wool `Worker` (`rust_worker_adapter.py`).
- **Rust caller client** (`wool_client_rs/` pyo3 tonic client + tokio↔asyncio bridge),
  spliced at `WorkerConnection.dispatch` (`rust_caller_patch.py`).
- **Full Rust stack + #273** beats Ray on s1/s4/s5/s6 (point-to-point, pipeline,
  recursive-tree, diamond); fan-out (~2.6×) and streaming (~2.9×) remain — fan-out is
  the caller's GIL-bound concurrency (batched bridge), streaming needs a Rust streaming
  client. (`rust_stack_memo_vs_ray.png`, `rust_stack_sweep.py`)
- Filed a real wool bug found en route: `LocalDiscovery` shared-memory double-unlink on
  rapid pool respawn — wool-labs/wool#291.

## Layout

- `rust_worker/` — Rust tonic `Worker` service + `executor.py` (Python routine executor).
- `wool_client_rs/` — pyo3 Rust tonic **client** (tokio↔asyncio bridge, channel pool).
- `tonic_echo/` — native gRPC transport-ceiling benchmark. `wool_rs/` — scaffolded pyo3
  cross-loop channel (deprioritized).
- `rust_worker_adapter.py`, `rust_caller_patch.py`, `memoize_patch.py` — wool integration.
- `*_vs_ray.py`, `worker_plane_*.py`, `plot_*.py` — benchmarks + charts.
- `figures/`, `data/` — generated charts and JSONL results.

## Build

Rust crates build with `cargo build --release` (or `maturin develop --release` for
`wool_client_rs`). Benchmarks run against the wool clone via `PYTHONPATH` and the
shapebench suite; see the driver scripts for exact invocations.
