# Benchmark Methodology — Generalization Across Problem Shapes

**Status:** Draft v0.1 · **Scope:** methodology only (no results). This document is the single source of truth for the suite's design. Shape definitions, task model, metrics, and statistical protocol here govern all harness code under `benchmarks/`.

## 1. Research question

Task-execution frameworks are almost always compared on a single number — dispatch latency or peak throughput on one workload. That framing rewards whichever framework was tuned for that one shape and tells a user nothing about the framework they will actually run. We ask a different question:

> **How does the per-task overhead of a framework change as the _shape_ of the workload changes?**

Overhead is governed by the workload's dependency **topology**, not its scale. An all-to-all shape stresses a scheduler differently than a flat bag-of-tasks; a recursive fan-out stresses it differently again. A framework that is excellent at one shape can be an order of magnitude worse at another. The deliverable is therefore not a leaderboard but an **overhead-versus-shape profile** for each framework — a fingerprint of where it generalizes and where it degrades.

This reframing is deliberate: rather than "compare latency apples-to-apples," we ask "how do these frameworks generalize across problem shapes?" The former has a winner; the latter has a map.

## 2. Positioning against prior art

The methodology is assembled from established work rather than invented; the novelty is in the combination and the categories covered, not in reinventing metrics.

| Prior work | What we adopt | What it does _not_ cover |
| --- | --- | --- |
| **Task Bench** (Slaughter et al., SC'20) | Parameterized multi-shape design; the **METG** overhead metric (smallest task granularity at which a system still reaches ≥50% efficiency; empirical floor ~100µs) | Excludes Ray, Celery, Prefect; HPC-only; rejects noop tasks |
| **TaPS** (Pauloski, Chard, Foster et al., IEEE eScience'24) | Per-task record schema (submit/start/complete/transform/resolve); synthetic shape taxonomy (sequential, reduce, bag-of-tasks, diamond); "many short tasks to accentuate overhead"; the Transformer/ProxyStore **data dimension** | Plugin interface is `concurrent.futures.Executor` — structurally excludes orchestrators (Celery/Prefect) and async/streaming/nested models; compute-fabrics only |
| **Granularity Characterization** (Taghipour Anvari & Kaeli, arXiv:2602.20561, 2026) | The thesis that **topology governs overhead degradation**; the single granularity number **G = T_kernel / T_overhead** | Single unreplicated preprint; MPI/HPC; some claims are definitional identities — cite as framing, not settled numbers |
| **Rigorous Python Benchmarking** (Crape & Eeckhout, IISWC'20) | Start-up vs steady-state separation; concrete statistical stopping criteria | Not shape-related |
| **Berkeley Dwarfs** (Asanović et al., UCB'06) | Motif vocabulary for naming shapes | Dwarfs are compute/communication motifs, not task-graph topologies — align terminology, do not conflate levels |

**Our contribution over the closest prior art (TaPS).** TaPS proves the same-application-many-frameworks idea works, but it makes that work by adopting the futures API (`submit`/`map`/`shutdown`) as the common denominator. That denominator _is_ the compute-fabric assumption: it cannot represent an orchestrator's DSL/canvas (Celery chords, Prefect state) or an async/streaming/nested model (a generator that yields many results per call) without flattening them to a fabric. Our common interface is therefore the **shape**, not the API — each framework expresses each shape _idiomatically_, and we measure per-shape overhead. Concretely we add: (1) **cross-category coverage** — compute fabrics + orchestrators + an async model in one ladder; (2) a **shape-level rather than API-level** comparison; (3) direct answers to TaPS's own stated open questions ("why are some executors 2–3× faster?", "how does performance correlate to task duration / data volume?", "how do executors deal with nested parallelism?"); (4) **METG/G redefined for orchestrators and async**, where "useful-FLOP/s efficiency" is undefined (see §6).

**Known risk.** Because TaPS already covers Ray/Dask/Parsl cleanly, our justification to build a new suite rests entirely on the cross-category and async claims. If a reviewer collapses the async model into "just another futures executor," the novelty collapses with it. The suite and the write-up must therefore foreground the streaming/nested/warm-endpoint shapes (S5–S7) that do not fit `submit()`.

## 3. Design principles

1. **Shape is the unit of comparison, not the framework's API.** Each framework implements each shape in its own idiomatic style. We never force a framework through a foreign abstraction; doing so would measure the adapter, not the framework.
2. **Isolate plumbing, then re-introduce work.** The default task body is a tunable-duration no-op (a sleep), so the measured cost is scheduling + transport + dependency resolution, not user compute. Task granularity is then swept upward to locate the point where overhead stops dominating (§6). This mirrors TaPS's synthetic app and sidesteps Task Bench's objection to fixed empty tasks.
3. **Overhead is dimensionless.** We report an overhead _ratio_ (measured / ideal), not raw microseconds, so shapes and frameworks with different absolute costs remain comparable and the metric stays defined for orchestrators and async models.
4. **Data movement is a separate axis.** Passing a large object _through_ the control path and passing a _reference_ to it (bring-your-own object store) are measured separately, so a framework's dispatch overhead is never conflated with its (optional) bundled data plane.
5. **Reproducibility is a first-class artifact.** Every run emits its full config, framework versions, and per-task records. Statistical protocol (§7) is fixed before any numbers are collected.

## 4. The shape ladder

Seven shapes, ordered from the irreducible unit to the topologies that break the futures abstraction. Each is a parameterized generator (task count, fan-out width, depth, granularity), not a fixed graph.

| ID | Shape | Topology | Stresses | Maps to |
| --- | --- | --- | --- | --- |
| **S1** | Point-to-point | 1 → 1 → 1 | Irreducible round-trip; single-dispatch latency floor | Task Bench trivial (n=1) |
| **S2** | Fan-out (bag-of-tasks) | 1 → N independent | Scheduler throughput; pool saturation; embarrassingly parallel | TaPS bag-of-tasks; Dwarf: map |
| **S3** | Scatter-gather (reduce) | N → 1 | Result collection; sync barrier; the join cost | TaPS reduce; Dwarf: map-reduce |
| **S4** | Pipeline (chain) | A → B → C → … | Sequential dependency handoff; per-edge data passing | TaPS sequential; Task Bench stencil (1-D) |
| **S5** | Recursive tree (nested) | task spawns sub-tasks, depth d | **Nested parallelism / decentralized dispatch** — the p2p strength; awkward-to-impossible for orchestrators | Task Bench tree; TaPS open question |
| **S6** | Diamond (DAG) | A → {B, C} → D | Data-dependent DAG resolution; the canonical join-after-split | TaPS diamond; Task Bench FFT (unit) |
| **S7** | Warm endpoint (streaming/actor) | one call → many results | **Warm/stateful path; streaming** that `submit()→one Future` cannot represent | Ray actor; Wool async generator |

**Rationale for the ladder's ordering.** S1–S4 and S6 are expressible by every framework, including via a futures API — they establish the shared baseline and let us reproduce/triangulate against TaPS and Task Bench. **S5 (nested) and S7 (streaming/warm) are the differentiators**: they are where compute fabrics, orchestrators, and async models genuinely diverge, and where the shape-level (rather than API-level) interface earns its keep. If a framework cannot express a shape idiomatically (e.g., an orchestrator has no first-class recursive dispatch), that is itself a reportable result, recorded as "not idiomatic" with the adapter cost noted — never silently dropped.

### 4.1 Shape × framework idiom

The core of the shape-level stance is that each cell below is the framework's _native_ construction of the shape, not a wrapper. The entries are provisional sketches of the idiom, to be pinned to exact API calls and verified as each framework's adapter lands (§5); they are recorded up front so the shape-level comparison is legible before any code exists. Cells marked _awkward_ are expressible only by bending a construct to a shape it was not designed for (the adapter cost is measured and reported); a blank `—` marks a shape with no idiomatic construction at all, which is itself evidence for the generalization thesis rather than a gap in the harness.

| Shape | Wool | Ray | Dask | Celery | Prefect |
| --- | --- | --- | --- | --- | --- |
| **S1** point-to-point | `await routine(x)` | `ray.get(f.remote(x))` | `submit(f, x).result()` | `f.delay(x).get()` | `f.submit(x).result()` |
| **S2** fan-out | `gather(*(routine(x) …))` | `ray.get([f.remote(x) …])` | `client.map(f, xs)` | `group(f.s(x) …)` | `f.map(xs)` |
| **S3** scatter-gather | routine awaits fan-out, reduces | `reduce.remote([f.remote(x) …])` | `submit(reduce, map(f, xs))` | `chord(group(f.s …))(reduce.s)` | reduce task over mapped futures |
| **S4** pipeline | `await c(await b(await a(x)))` | `c.remote(b.remote(a.remote(x)))` | `delayed` / chained futures | `chain(a.s, b.s, c.s)` | sequential tasks (return → arg) |
| **S5** recursive tree | routine calls routine (native p2p) | nested `f.remote()` inside a task | `submit` inside a task / recursive `delayed` | subtask `.delay()` in a task (_awkward_) | recursive subflows |
| **S6** diamond | `d(*await gather(b(a), c(a)))` | `d.remote(b.remote(a), c.remote(a))` | `delayed` diamond graph | `chord` over a `group` (_awkward_ join) | futures passed as args (native) |
| **S7** warm/streaming | async-generator routine (`async for`) | actor / streaming-generator return | Dask Actor / queue (non-core) | — | — |

The pattern in the matrix is the thesis in miniature: the shared baseline (S1–S4, S6) is expressible everywhere, but S5 turns awkward for the orchestrator (Celery) and S7 empties out entirely for both orchestrators — precisely the shapes the futures abstraction cannot carry.

## 5. Frameworks under test (phased)

Phasing keeps early iterations cheap and the comparison honest; each phase must reproduce the prior phase's shared-baseline numbers before new frameworks are trusted.

- **Phase 1 — Wool + Ray.** The async/p2p model versus the best-in-class compute fabric. Establishes the harness, the record schema, and S1–S7 for two maximally different architectures.
- **Phase 2 — + Dask.** A second compute fabric with a mature futures + dataflow model; also the strongest overlap with TaPS, for external triangulation.
- **Phase 3 — + Celery + Prefect.** The orchestrator category. This is where S5/S7 are expected to be non-idiomatic and where the cross-category contribution is demonstrated.

Version pinning is mandatory and recorded per run (see the Wool-vs-Ray note: results shift materially across Ray releases and across Wool's dispatch-optimization branch, so a bare framework name is not a reproducible baseline).

## 6. Task model and metrics

### 6.1 Task body and granularity

The task body is a busy-controlled no-op parameterized by a target duration `g` (a spin/sleep of `g` seconds; spin preferred to keep the GIL/scheduler honest for sub-millisecond `g`). For each shape we **sweep `g`** across a decade range (e.g., 0, 10µs, 100µs, 1ms, 10ms, 100ms) rather than reporting a single point.

This resolves the noop tension directly. Task Bench rejects fixed empty tasks because empty-task throughput is "only an upper bound." A granularity sweep does not report a single empty-task number; it reports the _function_ overhead(g), from which the meaningful summary is the granularity at which overhead stops dominating. TaPS's peer-reviewed synthetic app uses exactly this noop-sleep-with-duration-knob design, which is our precedent.

### 6.2 The overhead ratio and per-shape METG

For a shape run at granularity `g` on `W` workers, define the **ideal makespan** `T_ideal(g, W)` as the wall-clock a zero-overhead scheduler would achieve for that shape's dependency structure (e.g., for S2 with N tasks: `ceil(N/W) · g`; for S4 chain of length L: `L · g`; for S5 tree of depth d and branching b: `d · g` on the critical path given enough workers). Then:

- **Overhead ratio** `R_shape(g, W) = T_measured / T_ideal`. As `g → 0`, `R → ∞` (pure plumbing); as `g → ∞`, `R → 1` (overhead amortized). `R` is dimensionless and defined for _any_ framework, including orchestrators and streaming models, because it is wall-clock-based, not FLOP-based — this is the redefinition that lets METG cross out of HPC.
- **Per-shape METG** `METG_shape(W)` = the smallest `g` at which `R_shape(g, W) ≤ 2` (i.e. ≥50% efficiency, matching Task Bench's efficiency definition). One number per (framework, shape, worker-count); the primary comparable.
- **Granularity number** `G = g / T_overhead`, reported alongside for continuity with the 2026 characterization; `T_overhead` is read off the `g → 0` intercept.

Complementary raw metrics, reported but secondary to `R` and `METG`: single-dispatch latency p50/p99 (from S1), sustained throughput in tasks/s (from S2 at saturation), and absolute makespan per shape.

### 6.3 Per-task record schema (TaPS-derived)

Every task emits a line-delimited JSON record. Minimum fields:

```
task_id, shape, framework, function, parent_ids,
t_submit, t_start, t_complete, t_transform, t_resolve
```

From these we derive queue time (`t_start − t_submit`), execution time (`t_complete − t_start`), data-handling time (`t_transform + t_resolve`), and reconstruct the realized dependency graph to confirm the intended shape was actually executed (a guard against a framework silently collapsing or reordering the graph).

### 6.4 Data dimension (separate axis)

Independently of shape and granularity, each edge that carries a payload is run in two modes: **by-value** (the object travels through the framework's control/transport path) and **by-reference** (a small handle travels through the control path; the object lives in an external store — `multiprocessing.shared_memory`, Arrow, Redis, or the framework's own object store where it has one). Reporting these separately prevents conflating dispatch overhead with an optional bundled data plane, and it is where Wool's composable "bring-your-own object store" stance is measured on equal footing rather than penalized for not bundling one.

## 7. Statistical protocol (IISWC'20)

Fixed before any numbers are collected; no metric is reported without it.

- **Start-up vs steady-state are separated.** A warmup phase runs until the running mean is stable, and its samples are discarded; only steady-state samples are reported. Cold-start numbers, when relevant, are reported as a distinct "first-invocation" metric, never mixed into steady-state.
- **Stopping criteria.** Steady-state: continue sampling until the coefficient of variation of the last 4 iteration-batches is < 2%, or a hard cap is reached. Start-up: 95% confidence interval within 5% of the mean, or 30 invocations.
- **Reporting.** Report median and p99 (not mean) for latency; report mean ± standard deviation across ≥3 independent process-level runs for makespan/throughput. Every figure states `W`, `g`, `N`, framework versions, and run count.
- **Isolation.** One framework per process tree; workers pinned; no co-located frameworks during a measured run.

## 8. Execution environment and reproducibility

- **Phase 1–2: single node.** Matches TaPS's own scope and removes network variance as a confound while the shape ladder stabilizes. Multi-node is deferred until the single-node profiles are trusted.
- **Recorded per run:** hardware (CPU model, core count, pinning), OS/kernel, Python version, exact framework versions, and the full run config. Emitted alongside the per-task records so any figure is reconstructable from its artifact directory.
- **Wool baseline caveat.** v0.10.0 carries a known dispatch regression; the optimization work (per-worker lock #263, memoize #273, eager-first-next #274, worker-loop reuse #262) is not yet merged. Published Wool numbers must state the exact Wool build. Suite-structure work proceeds on v0.10.0; the latency re-evaluation waits for those to land.

## 9. Threats to validity

- **Metric transfer.** Applying an HPC-derived overhead metric (METG/G) to orchestrators and async models is an extension, not an established result. It is simultaneously the contribution and the exposure; the `R`-ratio redefinition (§6.2) is the specific claim that must survive review.
- **Idiom subjectivity.** "Idiomatic" per-framework implementations are a judgment call; each is documented, reviewed, and, where a framework offers several constructions of a shape, the fastest reasonable one is used and the alternatives noted.
- **Single-preprint dependency.** The topology-governs-overhead framing leans partly on one unreplicated 2026 preprint; it is used as framing, and our own measurements must stand on their own.
- **Single-node scope.** Early phases exclude network topology effects that dominate some real deployments; stated as a scope boundary, addressed in later phases.

## 10. Open questions (tracked, not yet resolved)

1. Can `T_ideal` be defined cleanly for a data-dependent/dynamic DAG whose structure is only known at runtime (moldesign-style weak dependencies)? Provisionally: measure against the realized critical path rather than a static one.
2. Is `R ≤ 2` (≥50% efficiency) the right METG threshold across categories, or do orchestrators need a category-specific threshold given their intended millisecond-plus task granularity?
3. Do we adopt WfCommons/WfFormat for the realistic-DAG anchor apps, or keep the suite entirely synthetic? (TaPS is moving toward WfFormat; alignment would aid triangulation.)
4. Streaming (S7) has no single-task granularity in the same sense — define its overhead as per-yield RTT versus an ideal streaming pipe.
