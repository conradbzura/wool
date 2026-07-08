# Rust worker-orchestration exploration — findings & progress log

**Task:** Explore refactoring wool's worker task-orchestration layer (`process.py`, `service.py`,
`session.py`, `local.py`, optionally `frame.py`) into Rust via pyo3/maturin. Goal: reduced dispatch
overhead / increased throughput. Exploratory — informs future enhancements. Benchmark: `benchmarks/`
(shapebench). Correctness: wool's test suite. Autonomous ~3h. **Do NOT commit to main/master/release
or push to GitHub.** Leave the #273/#274 prototypes in place.

## Environment (verified 2026-07-08)
- Rust: cargo/rustc 1.89 nightly. maturin 1.14.1 (installed into `.venv`).
- py-spy present but **requires root on macOS** → unusable non-interactively. Using in-process
  component microbenchmarks + cProfile instead.
- wool clone at `wool/` on branch `226-bound-spent-log-to-token-lifespan` (clean tree). Imports via
  `PYTHONPATH=wool/src`, reports 0.12.0rc0, has the newer token-lifespan source (`token_sink`,
  `_wire_tokens`). **This is the source I modify/test** (never the protected branches).
- venv has a *separate* PyPI `wool==0.12.0rc0` in site-packages (used by earlier benchmark runs).
- Package build backend is **hatchling** + custom protobuf codegen hook — NOT maturin. So the Rust
  extension is built as a **standalone pyo3 crate** (`maturin develop` → importable module), imported
  behind an env flag, mirroring how #273/#274 are env-toggled. Isolated + reversible.
- Machine: 14 physical cores. Python 3.13.3 in venv.

## Architecture under study (worker-side dispatch hot path)
Per dispatch, worker-side:
1. Parse (`DispatchSession.__aenter__`): `TaskRequestFrame.from_protobuf` → `Task.from_protobuf`
   (cloudpickle.loads of callable+args+kwargs+proxy+runtime_context) + optional ChainManifest decode.
2. `_schedule_worker`: allocate cross-loop queues, `call_soon_threadsafe(_start)` onto **worker loop**.
3. **Dual-loop design**: gRPC serving runs on the *main* loop; routine execution runs on a *separate*
   worker event loop on a daemon thread (`WorkerService._loop_pool`, keyed `"worker"`, TTL 30s). Every
   step crosses loops twice: request main→worker (`_RequestQueue`, `call_soon_threadsafe` + asyncio.Queue),
   response worker→main (`_ResponseQueue`, same). Frame decode happens worker-side in `get`.
4. Per step: `Frame.from_protobuf(raw_request)` decode → `request.mount(ctx)` (contextvars/chain;
   no-op for unarmed s1/s2) → `_drive_step` (build step coro, run in cached `contextvars.Context` via a
   directly-constructed `asyncio.Task`) → capture post-step chain manifest (`ctx.run(chain.to_manifest
   ().to_protobuf())`) → `ResultResponseFrame.for_send` → cross-loop response hop.

**Prior established finding (carried in):** wool is **caller-bound** — one Python driver serializes
dispatch, plateaus by W≈4. #273 (pickle memoize) and #274 (eager-first-next) target the caller side.
Implication to test: worker-side Rust may not raise throughput if the caller is the bottleneck, but can
cut per-dispatch *latency* (round-trip). Must MEASURE, not assume.

## Feasibility priors (what can / cannot move to Rust)
- **Cannot move** (needs CPython + arbitrary Python objects): routine execution (arbitrary async
  funcs/gens); cloudpickle of payloads; contextvars/chain; protobuf is already C++.
- **Can move** (pure plumbing over PyObject pointers): the dual-loop cross-thread coordination
  (`_RequestQueue`/`_ResponseQueue` + `call_soon_threadsafe`), the dispatch FSM control flow, frame
  routing/allocation.
- Therefore the Rust-winnable ceiling = the plumbing fraction of worker-side per-dispatch cost. The
  profiling step measures that fraction directly. If pickle/protobuf/contextvars dominate, the honest
  finding is "structural (e.g. collapse the dual loop), not Rust." If cross-loop asyncio overhead is
  large, Rust (or a single-loop refactor) has room.

## Plan
- [x] Orient: env, toolchain, harness, source.
- [ ] **Profile**: decompose worker-side per-dispatch cost (movable plumbing vs unmovable Python) via
      component microbenchmarks on real captured payloads. GATES everything.
- [ ] Decide the highest-leverage Rust target from the profile.
- [ ] Build minimal pyo3 crate for that target; wire in behind env flag.
- [ ] Benchmark same-session A/B (s1 latency, s2 throughput, +others).
- [ ] Verify correctness with the test suite subset touching orchestration.
- [ ] Report findings + recommendation.

## Results log

### R1 — Worker-side per-dispatch cost decomposition (microbench.py, macOS, clone source)
Captured real s1 `_leaf` Task: 1504B wire; **proxy pickle 1287B**, callable 51B, args 23B, kwargs 5B.

| stage | min ns | bucket |
|---|---|---|
| protobuf parse Request envelope | 314 | unmovable (C++) |
| cloudpickle.loads callable | 360 | unmovable |
| cloudpickle.loads args / kwargs | 124 / 108 | unmovable |
| **cloudpickle.loads proxy** | **11673** | unmovable (biggest single item) |
| Task.from_protobuf (all fields) | 14951 | unmovable |
| TaskRequestFrame.from_protobuf (full parse) | 16722 | unmovable |
| ResultResponseFrame build+encode (None) | 1565 | unmovable (pickle+pb) |
| Ack / Next frame build+encode | 861 / 861 | unmovable |
| contextvars.copy_context() | 16 | negligible |
| **CROSS-LOOP step RTT (2 hops, wool queues)** | **39640** | movable (plumbing) |
| SINGLE-LOOP step RTT (asyncio.Queue only) | 27466 | movable |

### R2 — asyncio loop-iteration floor (loop_probe.py)
| stage | min ns |
|---|---|
| `await asyncio.sleep(0)` one loop turn [asyncio] | 13484 |
| `await asyncio.sleep(0)` one loop turn [uvloop] | 12512 |
| cross-loop RTT [asyncio] | 38896 |
| cross-loop RTT [uvloop] | 32029 |

### Interpretation (the reframing)
1. **Overhead is asyncio-scheduling-bound, not plumbing-bound.** One loop turn ≈ 13µs here; a
   cross-loop dispatch step ≈ 3 turns ≈ 40µs. The data plumbing Rust could rewrite (queue push/pop,
   frame routing) is a *small slice*; the cost is the loop turns themselves.
2. **uvloop does NOT rescue it** (7–18% here). "Just use uvloop" is not the cheap lever.
3. **The routine step is an unavoidable asyncio coroutine await.** A Rust bridge cannot remove the
   loop turn that drives the routine — arbitrary async Python must run on the Python loop.
4. **Biggest unmovable cost = proxy unpickle (11.7µs/dispatch), and it's redundant.** The proxy is
   identical across dispatches from one pool (stable `proxy_id`). A worker-side `loads` memo keyed by
   `proxy_id` would erase ~11.7µs×(N−1) for fan-out — **pure-Python, mirrors #273 on the worker side.**
   (High-value follow-up, orthogonal to Rust.)
5. **Two structural levers actually move the number, both non-Rust:** (a) collapse the dual-loop design
   (removes the cross-thread turn: 40µs→~27µs); (b) reduce loop-turns-per-dispatch. The dual loop
   exists to keep a compute-bound routine off the gRPC serving loop — a real trade-off.
6. **Rust-winnable ceiling** = the cross-thread-wakeup delta (cross-loop 40µs − single-loop 27µs ≈
   13µs ≈ one loop turn). A faster Rust wakeup can shave only *part* of that. Prototype will measure it.

### R3 — Transport decomposition (the reorientation, prompted by user: "use Rust's gRPC too?")
The worker-orchestration layer I started profiling is **not** where the RTT is. Measured:

| measurement | min ns | note |
|---|---|---|
| **Real end-to-end s1 RTT** (g=0, 1 real subprocess worker) | **851784** | the whole thing |
| grpc-aio per-CALL 1-exchange (fresh stream) | 340731 | TCP |
| grpc-aio per-CALL 2-exchange (task→ack, next→result ≈ s1) | 530778 | TCP |
| grpc-aio per-CALL 2-exchange | 603896 | UDS |
| **grpc-aio warm PERSISTENT-stream per-exchange** | **140257** | no stream setup |
| **raw asyncio socket ping-pong (persistent)** | **77245** | asyncio+macOS I/O floor |
| worker orchestration (parse+cross-loop+encode, from R1) | ~60000 | **~7% of RTT** |

**Splits of the grpc-aio cost:**
- **Per-call stream SETUP ≈ 340µs − 140µs ≈ 200µs / dispatch.** wool opens a fresh bidi stream per
  `await routine()`. This is the single biggest attackable chunk — and it's **architecture, not Rust**:
  reusing/multiplexing a warm stream per worker removes it in pure Python/protocol changes.
- **Warm per-exchange grpc-aio 140µs vs raw asyncio socket 77µs → grpc-aio adds ~63µs** of HTTP/2 +
  Cython/asyncio integration per message. A native stack (tonic) could cut most of that ~63µs; the
  ~77µs raw floor is asyncio+macOS and only goes away if socket I/O also leaves asyncio (tonic/tokio).

**Answer to "use Rust's gRPC too?":** Transport is ~65% of the RTT (vs orchestration ~7%), so yes —
it's the right layer, and my initial queue target was low-leverage. BUT:
1. The biggest single win is **stream reuse (~200µs/dispatch), pure-Python, do first / regardless.**
2. **tonic** could ~halve the warm-exchange transport cost, but the routine stays Python so every step
   still crosses **tokio↔asyncio** (a new bridge, ≈ the ~40µs cross-loop hop) + pays the Python loop
   turn + proxy unpickle. Its net win is real but bounded and it's a large rewrite.
3. Next measurement: a **native tonic echo** RTT to size the transport ceiling (best case) vs grpc-aio's
   140µs/340µs — turns the tonic estimate into a number.

### R4 — Native Rust gRPC (tonic) transport ceiling (tonic_echo/, tonic 0.12, release LTO)
| measurement | tonic (Rust) | grpc-aio (Python) | speedup |
|---|---|---|---|
| warm PERSISTENT-stream per-exchange | **45256 ns** | 140257 ns | **3.1×** |
| per-CALL 1-exchange (fresh stream) | **115342 ns** | 340731 ns | **3.0×** |

Native gRPC is ~3× the Python stack, warm and per-call. Real transport win. **But** the routine stays
Python, so tonic adds a **tokio↔asyncio bridge** per step (est. ~30–50µs, ≈ the asyncio cross-loop hop;
not yet measured — the one open number) plus the unavoidable Python loop turn + proxy unpickle.

### Caller architecture (connection.py `_execute`) — confirms stream-reuse finding
- `_channel_pool` (ResourcePool) **reuses the channel** (TCP conn) ✔, with a per-channel
  `max_concurrent_streams` semaphore.
- **But `call = channel.stub.dispatch()` opens a fresh bidi STREAM per dispatch** → the ~200µs
  HTTP/2 stream-setup is paid on every `await routine()`. Stream reuse = keep a long-lived `dispatch`
  stream per worker and multiplex task frames over it (task-id framing + a response demux router). Pure
  Python protocol change; no Rust.

## SYNTHESIS — leverage ordering (s1 ≈ 852µs RTT, macOS) — REVISED after user corrections
**RETRACTED: stream reuse.** I first ranked stream reuse (~200µs/dispatch) as the top lever. **User
correction: it is a non-starter.** wool depends on one-stream-per-dispatch for cancellation semantics —
cancelling a routine = RST_STREAM on its stream. Multiplexing dispatches over a shared stream forfeits
per-dispatch HTTP/2 cancellation and would require reimplementing cancellation in application framing. Not
low-risk; a cancellation-model redesign. So the ~200µs stream-setup is **not** cheaply reclaimable. This
raises the relative value of tonic (native stack cuts per-call setup 340→115µs *while keeping one stream
per dispatch*) and of the cancellation-safe opts below.

Cancellation-preserving latency levers:
1. **#274 eager-first-next (pure protocol, cancellation-safe).** Pipelines the first `Next` *within* the
   existing single stream, collapsing a round-trip for coroutine dispatch (potentially ~one warm exchange,
   ~140µs on s1). Now the top tractable latency lever. Measuring via the suite (stacked_latency.py).
2. **tonic transport (Rust): ~3× on both stream-setup (340→115µs) and warm-exchange (140→45µs), keeps one
   stream/dispatch.** Net bounded by the mandatory tokio↔asyncio bridge (~40µs) + Python routine. Large
   rewrite.
3. **#273 pickle-memoize (caller ~12µs) + worker proxy-memo (worker parse −80%, ~13µs).** Small caller/
   worker CPU shaves; measured invisible-ish end-to-end alone but stack additively.
4. **Caller-side balancer micro-opts (pure Python, cancellation-safe):** kill the 3× per-dispatch
   `MappingProxyType` re-allocation + the O(N) `islice` worker select (see R7). Small but free.
5. **Worker-orchestration queues/FSM in Rust (my initial target): ~7% of RTT. Skip.**

### R7 — Caller-side per-dispatch map (agent-mapped; user-requested proxy/loadbalancer/discovery)
- **Discovery is NOT on the hot path.** Worker set cached in `LoadBalancerContext._workers`; updated
  out-of-band by the `_worker_sentinel` task (`proxy.py:726,942-975`). Zero per-dispatch discovery cost.
- **`WorkerProxy.dispatch` (proxy.py:809-850):** warm path just delegates to the balancer; lazy-start
  machinery is one-time behind `_started`. Negligible.
- **Load balancer `RoundRobinLoadBalancer.dispatch` (roundrobin.py:56-125) — the caller hotspot:**
  - **roundrobin.py:84+102 — `asyncio.Lock` held across `await connection.dispatch()`** (the whole gRPC
    handshake round-trip). Serializes every concurrent dispatch from a proxy → inflates makespan of the
    *concurrent* shapes (s2/s3/s5/s6). **Correctness-coupled**: this lock serializes stream-opens against
    the transport ceiling (the #290 regression territory) — cannot be naively removed. Narrowing its scope
    (hold across stream-open only, not the ack-wait) is the real lever but needs the #290 decoupling.
  - **roundrobin.py:83,85,89 — `context.workers` re-materialized as a fresh `MappingProxyType` 3×/dispatch;
    :88-94 O(N) `islice` worker select.** Pure per-dispatch allocation + linear scan waste. Safe to fix
    (cursor over a cached list). Small at W=4.
- **wrapper.py:306-314:** `uuid4()` + f-string tag + 2 ContextVar reads + `Task` alloc per dispatch.
  Constant caller CPU; minor.

### R5 — Proxy-memo premise confirmed (proxy_premise.py)
25 tasks across 3 routines (_leaf/_stage/_reduce), 2-worker pool: **1 distinct proxy_id, 1 distinct
proxy-byte hash** → a worker-side `loads(proxy)` cache keyed by `proxy_id` hits **24/25 (96%)** here,
→100% in a long-lived pool. The proxy is one stable routing handle per pool, re-unpickled redundantly
every dispatch. Confirms rec #2's ~11.7µs saving is real and routine-agnostic.
**Precision:** this shaves the worker-side *critical path* (~11.7µs of the ~60µs worker slice), so it
helps *latency* and multi-worker CPU headroom. It does **not** necessarily raise s2 throughput, which is
*caller-bound* — the worker has spare CPU there. Correctness caveat: sharing one unpickled proxy
instance is safe only if it carries no per-dispatch mutable state (verify before shipping).

### R6 — Proxy-memo prototype measured end-to-end (memo_ab.py; env-gated edit, reverted)
Env-gated `WOOL_PROXY_MEMO` cache in `Task.from_protobuf` (keyed by `proxy_id`), applied to the clone,
measured, then `git checkout`-reverted (tree pristine).
- **Direct parse A/B (deterministic, in-process):** `TaskRequestFrame.from_protobuf` 16543 → 3361 ns,
  **saves 13182 ns (80% of parse)** — eliminates the proxy unpickle outright.
- **End-to-end s1 RTT:** memo OFF 878713 → memo ON 853826 ns (~25µs / ~3%, ≈ the parse saving + noise).
- **Conclusion:** a large, real worker-side win *in isolation* (80% of parse) is **nearly invisible
  end-to-end** because transport dominates (~65%). Confirms the thesis: leverage is transport/stream-
  reuse, not worker-side micro-opts. (Prototype shares one proxy instance pool-wide; the ~100% hit rate
  makes it effective, but production must confirm the proxy has no per-dispatch mutable state.)

### R8 — STACKED latency through the suite (stacked_latency.py, 4 reps, rotated order, W=4)
Same-session, drift-cancelled (config order rotated per rep). Cross-rep CV median 1.9%, max 5.2% — so
8–24% gains are real signal. Median makespan µs at g=0, cumulative:

| shape | base | +eager | +pickle | +proxy(all) | stack Δ |
|---|---|---|---|---|---|
| s1 point-to-point | 1068 | 925 | 898 | 810 | **+24.1%** |
| s2 fan-out | 17298 | 17141 | 14426 | 14665 | +15.2% |
| s3 scatter-gather | 16263 | 15674 | 13550 | 13520 | +16.9% |
| s4 pipeline | 15342 | 14124 | 13336 | 12417 | +19.1% |
| s5 recursive-tree | 4808 | 4605 | 4462 | 4379 | +8.9% |
| s6 diamond | 3333 | 2938 | 2743 | 2615 | +21.5% |
| s7 streaming | 20073 | 19920 | 19999 | 19810 | +1.3% (noise) |

Incremental attribution at g=0 (µs saved):
- **#274 eager-first-next** dominates SEQUENTIAL chains: **s4 pipeline +1218** (64 dispatches each save a
  round-trip → compounds), s6 +395, s3 +589, s1 +143. Matches the isolated prediction (~1 warm exchange
  ~140µs on s1).
- **#273 pickle-memoize** dominates FAN-OUT: **s2 +2716, s3 +2124** (128 tasks × ~21µs proxy+callable
  dumps memoized on the single caller). Small on single-dispatch (s1 +27).
- **worker proxy-memo** helps only the CRITICAL-PATH shapes: s4 +918 (=64×~14µs), s1 +88; but **hidden**
  on fan-out (s2 −240, noise) because those are caller-bound — worker parse runs in parallel off the
  critical path. Clean confirmation of the caller-bound vs critical-path distinction.
- **s7 streaming +1.3% (noise)** — the per-dispatch opts apply once, not per-yield; needs a per-yield
  optimization (the user's "separate plan").

**Takeaway:** the three cancellation-safe opts stack to **~15–24% dispatch latency reduction on every
dispatch-bound shape** (s1–s6), with the dominant contributor set by topology. s7 needs a different idea.

### R9 — RUSTIFIED WORKER PLANE, vertical slice v0 measured (rust_worker/ + executor.py)
Built a working Rust worker plane: **tonic gRPC `Worker` service** (speaks wool's exact wire.proto,
prost serdes, dispatch FSM) → **unix-socket IPC** (req_id-correlated) → **separate Python executor
process** (asyncio loop, cloudpickle.loads/runs the coroutine, cloudpickle.dumps result back). Rust never
touches a PyObject or the GIL. Runs end-to-end, correctly (noop coroutine round-trips).

A/B — identical grpc-aio client, identical trivial pickled coroutine, same session (worker_plane_ab.py):
| worker plane | s1 RTT (min) | vs A |
|---|---|---|
| (A) minimal single-loop Python grpc.aio worker (coroutine inline) | 581566 ns | — |
| **(B) Rust tonic worker + Python executor (extra IPC hop)** | **398046 ns** | **−32%** |
| (ref) real full WorkerService, wool client (s1_rtt.py) | 851784 ns | heavier (dual-loop+chain) |
| (ref) bare grpc-aio echo, no routine (2-exchange) | ~530000 ns | (B) beats it while running a routine |

**The Rust plane is ~1.46× faster even paying a process-boundary IPC hop**, vs the *minimal* Python
worker; vs the real WorkerService the gap is larger. Client is unchanged (grpc-aio), so this is a pure
worker-side win.

### R10 — bridge decomposition (ipc_probe.py)
Executor IPC RTT in isolation (unpickle noop + run + pickle + unix-socket ×2, Python client side):
**~80µs**. So of (B)'s 398µs: bridge+executor ≤80µs, tonic server ~45-115µs, and the **grpc-aio *client*
(~250-300µs) is now the dominant term.** The worker plane is fast enough that the Python client is the
new bottleneck → the next win is rustifying the CALLER transport (the caller-side task), which compounds.
Bridge itself could drop with shared-memory instead of a socket (matters more for large by-value payloads).

**Slice scope:** coroutine dispatch only. Full version still needs: streaming (executor holds the live
async-gen keyed by dispatch-id across Next/Send/Throw IPC steps), cancellation (RST_STREAM → cancel msg →
Task.cancel in executor), chain-manifest round-trip (forward the opaque `context` bytes both ways +
Chain.from_manifest in the executor), backpressure, and the process lifecycle (parent-watchdog, stop RPC
drain) currently in process.py/local.py. None are blocked — they're additive on this proven bridge.

### R11 — STREAMING works, and exposes the per-yield bridge tax (slice v1)
Extended the slice to a unified step protocol (START_AND_STEP / Next / Send / Throw + CANCEL) over the
IPC; executor holds the live async-gen keyed by dispatch-id. **Correctness: PASS** — async-gen yields
[0..7] correctly end-to-end through Rust; coroutine regression still returns None.

RTT (worker_plane_stream.py, n=8 stream, same grpc-aio client):
| | per-yield RTT |
|---|---|
| (A) Python worker (drives gen in-process) | 211636 ns/yield |
| (B) Rust worker + executor (IPC per yield) | 234228 ns/yield — **~11% SLOWER** |

**Key architectural finding — the bridge tax flips sign with yield count.** The separate-process executor
pays the ~80µs IPC hop **once** for a coroutine (net −32%, Rust wins) but **per yield** for streaming
(net +11%, Rust loses). So the process-boundary design is right for single-shot dispatch and *wrong,
naively, for high-yield streaming.*

**Fix (future): pipeline / push the bridge.** Two options: (a) **prefetch** — after forwarding yield k,
Rust immediately requests yield k+1 from the executor so the IPC overlaps the client's gRPC round-trip
(hides the hop; works for pure `anext`, not `asend`); or (b) **executor-push** — one START drives the
whole gen in the executor, which pushes each yield over IPC as produced, Rust forwards to gRPC under the
client's flow control (eliminates per-yield request round-trips entirely). Either recovers streaming.
This also reframes s7: a rustified plane only helps s7 *with* the pipelined bridge, not naively.

### R12 — CANCELLATION validated through the Rust plane (worker_plane_cancel.py)
A coroutine sleeps 4s, records CANCELLED (interrupted) vs COMPLETED (ran to end). Client starts it,
cancels the gRPC call mid-flight (RST_STREAM), checks after ~1.2s.
| worker | probe file | verdict |
|---|---|---|
| (A) naive Python worker | 'CANCELLED' | PASS |
| (B) Rust worker + executor | 'CANCELLED' | PASS |

**The Rust plane correctly propagates cancellation:** RST_STREAM → tonic drops the response stream →
`CancelGuard::drop` → OP_CANCEL over IPC → executor `step_task.cancel()` → routine's `sleep` raises
`CancelledError`. This is the correctness-critical path (the reason stream-separation matters), and the
Rust design is **more deterministic** than grpc.aio's cancellation propagation — it explicitly signals
the executor rather than relying on the async-cancellation threading that the real WorkerService needs
elaborate machinery (`_propagate_cancel_on_done`) to work around for async-gen handlers. (Both pass for
the simple coroutine case; the async-gen-mid-yield case is where the explicit path's robustness matters.)

### R13 — STREAMING RECOVERED via executor-push (slice v2): Rust now wins streaming too
Rewrote the bridge to a **unified push model**: Rust sends one START; the executor self-drives the
routine (`asend(None)` loop) and pushes each yield over IPC keyed by dispatch-id; Rust forwards one push
per client Next from a bounded per-dispatch channel (channel + socket buffer = backpressure). The executor
produces yield k+1 while the client consumes yield k, overlapping the IPC with the gRPC round-trip.

| streaming n=8 | per-yield RTT | vs Python |
|---|---|---|
| (A) Python worker | 211292 ns | — |
| (B) Rust pull model (v1, R11) | 234228 ns | +11% (tax) |
| **(B) Rust push model (v2)** | **136504 ns** | **−35%** |

Correctness holds ([0..7]); coroutine still −32%-class; **cancellation still deterministic** (re-ran, PASS).
So the Rust plane now **wins on coroutine AND streaming** (−32% / −35%), matching. **Resolves R11:** the
per-yield bridge tax is a *pull-model artifact*, not fundamental — the separate-process design is viable
for streaming; embedded-pyo3 is NOT required. Caveats (prototype): pure-anext only (Send/Throw would need
the step path back); backpressure is via the bounded channel + socket buffer (credit-based flow control is
the production form); ordering/count validated at n=8.

### Worker-plane rustification — status summary (slices v0 + v1 + v2)
Built & validated in scratchpad/rust-explore/rust_worker (tonic+prost) + executor.py:
| protocol feature | status | result |
|---|---|---|
| coroutine dispatch | ✅ works | **−32% RTT** vs Python worker (R9) |
| async-gen streaming | ✅ correct | push model **−35%** (R13); recovered from the +11% pull-model tax (R11) |
| cancellation (RST_STREAM) | ✅ works | routine interrupted, deterministic (R12, re-verified under push) |
| chain / contextvars round-trip | ⬜ not built | opaque `context` forward + Chain.from_manifest in executor (plumbing) |
| streaming push/prefetch | ⬜ not built | recovers R11 tax for pure-anext; needs credit flow-control |
| lifecycle (process.py/local.py) | ⬜ not built | parent-watchdog, stop-drain, worker metadata pipe |

**Architectural conclusion (revised after R13):** hoisting the executor out of grpcio and letting Rust own
transport+serdes+FSM is a real latency win — **−32% coroutine, −35% streaming** — and handles the hard
correctness paths (streaming, cancellation) cleanly. The R11 per-yield tax was a *pull-model artifact*:
the **executor-push** bridge (produce ahead, forward per client Next) recovers it, so **the
separate-process model is viable across the board** — it keeps its strengths (GIL isolation, N-executor
parallelism, fault isolation) without a streaming-latency penalty. Embedded-pyo3 is an alternative, not a
requirement. Remaining: chain/contextvars round-trip (opaque forward + Chain.from_manifest in executor),
Send/Throw step-path for non-anext generators, credit-based backpressure, and process lifecycle.

### R14 — INTEGRATED: Rust worker as a drop-in wool worker, benchmarked vs Ray (rust_vs_ray.py)
Wrapped the Rust worker as `RustWorker(Worker)` so `WorkerPool(spawn=N, worker=RustWorker)` spawns +
routes to it via wool's REAL caller (proxy/loadbalancer/connection). Executor now uses wool's
`routine_scope` (real routines, nested dispatch, runtime context). Runs ALL 7 shapes incl. s5 nested.
shapebench, W=4, g=0, same session (makespan µs):

| shape | Python-wool | Rust-wool | Ray | Rust vs Python | wool/Ray → rust-wool/Ray |
|---|---|---|---|---|---|
| s1 point-to-point | 1124 | 690 | 593 | 1.63× | 1.90× → **1.16×** |
| s2 fan-out | 46943 | 26316 | 7956 | 1.78× | 5.90× → 3.31× |
| s3 scatter-gather | 47412 | 26055 | 9151 | 1.82× | 5.18× → 2.85× |
| s4 pipeline | 15568 | 8570 | 7214 | 1.82× | 2.16× → **1.19×** |
| s5 recursive-tree | 6031 | 3294 | 2937 | 1.83× | 2.05× → **1.12×** |
| s6 diamond | 3967 | 2200 | 1746 | 1.80× | 2.27× → **1.26×** |
| s7 streaming | 22352 | 7754 | 2573 | 2.88× | 8.69× → **3.01×** |

**Result:** Rust worker makes wool **1.6–2.9× faster** and reaches **near-parity with Ray on s1/s4/s5**
(~1.1–1.2×); s7 streaming 8.7×→3.0×. Remaining gaps (s2/s3 fan-out) are **caller-bound** — the Python
caller serializes 128 dispatches; the worker plane can't fix that (→ rustify the caller next).
Caveats: macOS g=0; no per-shape pool reset (worked around a wool LocalDiscovery shm teardown race — a
real wool bug worth filing); context-propagation still not exercised (shapebench uses no contextvars).
Artifacts: `rust_worker_adapter.py` (RustWorker + RustWoolAdapter), `rust_vs_ray.py`, `derisk_rust_pool.py`,
`plot_rust_vs_ray.py`, figure `rust_vs_ray.png`.

### R15 — CALLER-SIDE rustified (pyo3 tonic client) + integrated + benchmarked vs Ray
Built `wool_client_rs` (pyo3 crate): a tonic gRPC **client** with a tokio↔asyncio bridge
(pyo3-async-runtimes, embedded — no IPC) + channel pooling. `await wool_client_rs.dispatch(addr,
task_bytes) -> (kind, payload)`. Spliced into wool's real caller by monkeypatching
`WorkerConnection.dispatch` (coroutine case; keeps load-balancer worker selection via `self._target`).
Fixes found: pre-load the Task frame before `client.dispatch()` (grpc.aio doesn't surface the call
until a frame arrives); drop the sender + drain to EOF per dispatch (else HTTP/2 streams leak →
CANCELLED); the dispatch stream yields the **decoded value**, not a raw `protocol.Message`.

Isolation (same Python worker, same coroutine): grpc-aio client **560µs** vs Rust client **423µs (−25%)**.

Four-way shapebench @ g=0, W=4 (×Ray):
| shape | py-wool | rust-caller (Py worker) | **rust-both** | ray (µs) |
|---|---|---|---|---|
| s1 | 1.67× | 1.43× | **0.72× (beats Ray)** | 671 |
| s2 fan-out | 5.05× | **6.82× (worse)** | 2.91× | 8541 |
| s3 scatter | 3.62× | 5.06× (worse) | 2.07× | 11762 |
| s4 pipeline | 2.00× | 1.91× | **0.81× (beats Ray)** | 7447 |
| s5 recursive | 1.73× | timeout | **0.97×** | 3163 |
| s6 diamond | 2.17× | 2.21× | **0.95×** | 1739 |

**Findings:**
1. **`rust-both` (Rust caller + Rust worker) beats or matches Ray on s1/s4/s5/s6** — s1 point-to-point
   *beats* Ray (483 vs 671µs) — and cuts fan-out s2/s3 to ~2–3× (from py-wool's ~4–5×). Full native-
   transport wool is Ray-competitive across the dispatch-bound shapes.
2. **The Rust caller ALONE (with a Python worker) is WORSE on fan-out** (s2 5.05×→6.82×): the
   pyo3-async-runtimes bridge has a per-dispatch GIL round-trip that, under 128-way concurrency,
   serializes — it only pays off when the worker is also Rust (each dispatch completes fast enough to
   hide the bridge). Honest limitation of the embedded tokio↔asyncio bridge for caller-bound fan-out.
3. rust-caller s5 (recursive nested dispatch) timed out — prototype limitation (worker-side nested
   dispatch interacting with the caller bridge). rust-both s5 works (0.97×).

"From the grpc stub backwards" reached: tonic **client** transport + prost frame serdes + dispatch-stream
FSM + channel pool, bridged to asyncio, integrated at WorkerConnection (keeps Python load-balancer
selection). Not done: streaming (falls back to Python caller), chain-manifest context, dispatch-phase
timeout/semaphore/transient-retry, and moving load-balancer selection into Rust.
Artifacts: `wool_client_rs/` (crate), `rust_caller_patch.py`, `caller_client_ab.py`,
`rust_caller_vs_ray.py`, `plot_caller.py`, figure `rust_caller_vs_ray.png`.

### Platform caveat (prominent)
Absolute asyncio numbers are **macOS default-selector (kqueue) inflated**; one loop turn on Linux+epoll
is typically ~1–2µs (5–10× cheaper). That would *shrink* the plumbing fraction further and make the
Rust-plumbing case *weaker*, while the unmovable pickle stays fixed → pickle becomes the dominant term.
The movable/unmovable *decomposition* is platform-invariant; the *magnitudes* are not. Re-measure on
the Linux target before acting.
