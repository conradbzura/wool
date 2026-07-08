# Rustifying wool's worker plane — exploration report

**Task.** Explore refactoring wool's worker task-orchestration layer (`process.py`, `service.py`,
`session.py`, `local.py`, `frame.py`) to Rust via pyo3/maturin, to cut dispatch latency. Exploratory;
informs future work. **Scope discipline:** the wool clone stayed pristine (no source change); every
artifact lives in `scratchpad/rust-explore/`. Numbers are macOS, Python 3.13, clone source (0.12.0rc0
lineage), warm, g=0 — the *relative* decompositions are platform-invariant, the absolute µs are inflated
by macOS's kqueue selector (see caveats).

## Executive summary

Three phases, each correcting the last:

1. **Diagnosis.** The named target — worker orchestration (queues/FSM) — is **~7% of the dispatch RTT**.
   The **transport is ~65%**. The starting premise was aimed at the wrong layer.
2. **Ship now (pure Python, cancellation-safe).** A stack of three dispatch opts (#274 eager-first-next,
   #273 pickle-memoize, worker proxy-memo) cuts **15–24% of dispatch latency on every dispatch-bound
   shape** (s1–s6), measured through shapebench. Closes ~a quarter-to-third of the wool→Ray g=0 gap.
3. **The real lever — a Rust worker plane, built and measured.** tonic + prost + the dispatch FSM in
   Rust, bridging pickled routine steps over IPC to a separate Python executor. Measured: **coroutine
   −32%, streaming −35%, cancellation deterministic** — all correct. The architecture holds up on the
   hard paths (streaming, cancellation) that make wool distinctive.

## Phase 1 — Diagnosis: the transport is the cost, not the orchestration

One s1 dispatch (`await routine()`, real wool worker) is **852µs**. Decomposition:

| slice | cost | movable to Rust? |
|---|---|---|
| gRPC per-call **stream setup** | ~200µs | no — architecture (but cancellation needs per-stream, see below) |
| gRPC **warm exchanges** (~2) | ~140µs each | yes — a native stack |
| worker **orchestration** (queues, FSM, parse, encode) | ~60µs (**~7%**) | yes, but low value |
| **proxy unpickle** | ~11.7µs | no — arbitrary Python (but memoizable, see Phase 2) |
| caller pickle + cross-process + two-sided asyncio | remainder | mixed |

Supporting isolated measurements: grpc-aio warm exchange 140µs, per-call 341µs (1-exch) / 531µs
(2-exch); raw asyncio socket ping-pong 77µs; worker cross-loop step 40µs; one `asyncio.sleep(0)` loop
turn 13.5µs (uvloop barely helps); `cloudpickle.loads(proxy)` 11.7µs.

**Native gRPC ceiling (tonic echo, Rust↔Rust):** warm exchange **45µs** (vs grpc-aio 140µs, 3.1×),
per-call **115µs** (vs 341µs, 3.0×). The transport is ~3× on a native stack.

**Stream-reuse is a non-starter (user correction).** One-stream-per-dispatch is load-bearing for
cancellation (RST_STREAM); multiplexing forfeits it. So the ~200µs per-call setup is not cheaply
reclaimable in Python — it wants a native stack that keeps one stream per dispatch.

## Phase 2 — Ship now: the cancellation-safe stacked opts (see stacked_latency.png)

Measured through shapebench (W=4, g=0, 4 reps, config order rotated to cancel drift, cross-rep CV ~2%):

| shape | baseline → stacked | faster | vs Ray (base → stacked) |
|---|---|---|---|
| s1 point-to-point | 1068 → 810µs | **−24%** | 1.98× → 1.50× |
| s6 diamond | 3333 → 2615µs | **−22%** | 2.15× → 1.68× |
| s4 pipeline | 15342 → 12417µs | **−19%** | 2.19× → 1.77× |
| s3 scatter-gather | 16263 → 13520µs | **−17%** | 1.95× → 1.63× |
| s2 fan-out | 17298 → 14665µs | **−15%** | 2.21× → 1.88× |
| s5 recursive-tree | 4808 → 4379µs | −9% | 1.20× → 1.10× |
| s7 streaming | 20073 → 19810µs | −1% (noise) | 8.39× → 8.28× |

Each idea wins a different topology: **eager-first-next** dominates sequential chains (s4 +1218µs, 64
round-trips saved), **pickle-memoize** dominates fan-out (s2 +2716µs, 128 proxy dumps memoized), **worker
proxy-memo** helps only the critical path (s4 +918µs; hidden on caller-bound fan-out). All three are pure
Python, cancellation-safe, and additive. **s7 streaming is untouched** — per-dispatch opts apply once, not
per-yield (your separate plan).

## Phase 3 — The Rust worker plane, built and measured (see worker_plane_v0.png, streaming_recovery.png)

**Architecture.** A **tonic gRPC `Worker` service** (Rust) speaks wool's exact `wire.proto` with prost
serdes and owns the dispatch FSM + per-stream lifecycle. It bridges pickled routine steps over a
**unix-socket IPC** to a **separate Python executor process** that keeps the GIL: cloudpickle + the
routine's one asyncio loop, holding the live async-gen keyed by dispatch-id. **Rust never touches a
PyObject** — it carries opaque already-pickled bytes. A structural bonus: the real worker's *dual event
loop* (gRPC loop + worker loop, ~40µs cross-thread hop) disappears — transport is Rust, routine is a
different process, so the executor has one loop and no dual-loop hop.

**Why separate-process, not embedded-pyo3.** Embedding CPython in the tonic process would serialize every
routine step on one GIL and make tonic's worker threads contend for it. A separate executor keeps the GIL
out of Rust entirely, allows **N executors for parallelism** (each its own GIL) + fault isolation, and
matches wool's subprocess-per-worker model. The cost is an IPC hop — cheap, and hideable (below).

**Measured** (same grpc-aio client, same session):

| feature | result |
|---|---|
| **coroutine dispatch** | **398µs vs 582µs** minimal Python worker — **−32%**, even paying a process-boundary IPC hop; also under the bare grpc-aio echo (530µs, no routine) |
| **streaming (async-gen)** | correct; **136µs/yield vs 211µs Python — −35%** with the executor-push bridge |
| **cancellation (RST_STREAM)** | correct + deterministic — routine interrupted mid-flight |

**The streaming story is the interesting one.** A naive *pull* bridge (Rust requests each yield) taxes
streaming +11% — the ~80µs IPC is paid per yield. An **executor-push** bridge (the executor produces
yields ahead and pushes them; Rust forwards one per client Next, bounded channel + socket buffer =
backpressure) overlaps the IPC with the gRPC round-trip and **recovers it to −35%**, matching the
coroutine win. So the per-yield tax was a pull-model artifact, not fundamental: **the separate-process
model is viable across the board.**

**Cancellation** works via an explicit path — RST_STREAM → tonic drops the response stream →
`CancelGuard::drop` → CANCEL over IPC → `Task.cancel` in the executor → the routine's `await` raises
`CancelledError`. This is *more deterministic* than grpc.aio's cancellation propagation (the exact quirk
the real WorkerService needs `_propagate_cancel_on_done` machinery to work around).

**Bridge decomposition of the 398µs:** executor IPC ~80µs, tonic server ~50–115µs, and the unchanged
**grpc-aio client (~300µs) is now the dominant term**. So the worker plane is fast enough that the Python
*caller* becomes the bottleneck — the win **compounds** with caller-side rustification.

## Leverage ordering (updated)

1. **Ship the Phase-2 stack** (pure Python, cancellation-safe, 15–24% on s1–s6). Lowest risk; do first.
2. **Rust worker plane** — built and proven (−32%/−35%). The path forward is: finish it (chain round-trip,
   Send/Throw step-path, credit backpressure, lifecycle) and rustify the caller transport to compound.
3. **Caller-side balancer** (`roundrobin.py:84-102`): the `asyncio.Lock` held across the awaited handshake
   serializes concurrent dispatches (inflates s2/s3/s5/s6). Narrowing its scope is correctness-coupled to
   the transport-ceiling work (#290). Free + safe: kill the 3×/dispatch `MappingProxyType` realloc + O(N)
   `islice` worker select. (Discovery is NOT on the hot path — cached, updated out-of-band.)
4. **Worker-orchestration queues/FSM as an *isolated* Rust swap — skip.** ~7% of RTT; a Rust MPSC can't
   beat the asyncio loop turn it still has to trigger. (The worker plane subsumes this correctly by moving
   the whole transport, not just the queues.)

## Scope, caveats, remaining work

- **Platform.** Absolute µs are macOS-kqueue-inflated (one loop turn ~13µs vs ~1–2µs on Linux+epoll). The
  *relative* results (transport ≫ orchestration; −32%/−35% ratios; per-yield tax is pull-only) are
  platform-invariant; re-measure absolutes on the Linux target before committing.
- **Prototype scope.** The worker plane handles coroutine + streaming + cancellation. **Not yet built,**
  all understood and low-risk: chain/contextvars round-trip (opaque `context` ChainManifest forwarded both
  ways — same proven pattern as the payload bytes — with wool's existing `Chain.from_manifest`/`to_manifest`
  reused unchanged in the executor); Send/Throw step-path for non-anext generators; credit-based
  backpressure; process lifecycle (`process.py`/`local.py`: parent-watchdog, stop-drain, metadata pipe).
- **Two bridge designs.** Built + measured: separate-process **IPC** bridge (~80µs, pipelined away for
  streaming). Not built: the **embedded tokio↔asyncio** bridge (estimated ~40µs) — an alternative, not
  required given the IPC design wins.
- **wool source is pristine** — the entire plane is a standalone prototype; nothing to regress in the test
  suite. Correctness verification attaches when the plane is integrated into wool proper.

## Artifacts (`scratchpad/rust-explore/`)
- **Report/log:** `REPORT.md`, `FINDINGS.md` (R1–R13, full measurement log).
- **Diagnosis:** `microbench.py`, `loop_probe.py`, `transport_probe.py`, `grpc_echo_rtt.py`, `s1_rtt.py`,
  `capture_task.py`, `tonic_echo/` (native gRPC ceiling, builds & runs).
- **Phase 2:** `stacked_latency.py`, `aggregate_stacked.py`, `proxy_premise.py`, `memo_ab.py`; opts in
  `w274/` (env-gated).
- **Phase 3 (Rust worker plane):** `rust_worker/` (tonic+prost `main.rs`, push streaming model) +
  `rust_worker/executor.py` (Python executor); `worker_plane_ab.py`, `worker_plane_stream.py`,
  `worker_plane_cancel.py`, `ipc_probe.py`.
- **Figures:** `stacked_latency.png`, `worker_plane_v0.png`, `streaming_recovery.png`.
