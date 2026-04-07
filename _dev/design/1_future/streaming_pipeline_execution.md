# Design: Streaming Pipeline Execution

**Date:** 2026-03-24
**Status:** Draft
**Analysis:** `_dev/analysis/streaming-pipeline-execution.md`

---

## Summary

Enable independent pipeline branches to stream through a chain of
operations, each with different resource requirements, with priority
scheduling that is configurable. The design is incremental — each
phase builds on the previous and delivers value independently.

**Motivating use case:** ~1000 independent protein design branches within a
single SLURM allocation on Perlmutter. Each branch flows through a chain of
operations (RFD3 → MPNN → Multimer → AF3 → post-processing) requiring
different resources (CPU vs GPU).

**Requirements:**

- **Multi-resource dispatch** — each operation declares its own resources
- **Independent branch streaming** — downstream starts before upstream
  finishes across all branches
- **Priority scheduling** — later pipeline stages dispatched first
- **Backend-agnostic** — works across LOCAL, SLURM, and future backends
  (SLURM_INTRA, Kubernetes, cloud). SLURM_INTRA (intra-allocation srun
  dispatch) is implemented — see `slurm_intra_backend.md`.

---

## Architecture

Three layers with distinct responsibilities:

**Orchestration layer (Artisan):** Owns the pipeline DAG, step dependencies,
priority scheduling, resource budget tracking, and dispatch ordering.
Decides what to dispatch and when. Contains the **step scheduler** —
a dedicated thread that manages the lifecycle of all tasks.

**Backend (dispatch layer):** Translates "run this unit with these resources"
into system-specific commands (srun, sbatch, subprocess, kubectl). Returns
a `DispatchHandle` for non-blocking completion tracking. Does not schedule,
prioritize, or track resources. Interchangeable.

**Underlying system (resource pool):** SLURM allocation, local machine, Ray
cluster, Kubernetes namespace. Owns the physical resources.

Priority, streaming, and resource-aware dispatch live in the orchestration
layer. The backend is a thin dispatch mechanism.

---

## Constraints

**Readiness-gated dispatch.** Tasks must not be dispatched until their
predecessors have completed. The step scheduler checks readiness
before dispatching — only tasks whose inputs are available enter the
dispatch queue. This prevents wasted resources and avoids priority
inversion (high-priority tasks that can't run yet blocking lower-priority
tasks that can).

**Thread count independent of task count.** The target scale is 1000
branches × 5 steps = 5000 tasks. Backend execution takes minutes to hours.
One OS thread per in-flight task is impractical for queue-managed backends
(thousands of threads) and wasteful for resource-managed backends (threads
blocked on I/O for 95%+ of their lifetime). The orchestration layer's
thread count must be bounded by a small constant, not by task count.

**Serialized Delta Lake writes.** Concurrent Delta Lake commits from many
threads create transaction log pressure and compaction conflicts. Write
concurrency must be controlled independently of dispatch concurrency.

---

## Design Decisions

### Streaming via parallel branches

Users split work into batches and create parallel branches of steps within
a single pipeline using existing `submit()` / `StepFuture` /
`OutputReference` wiring. Parallel step execution enables branches to
overlap. Streaming emerges from the dependency graph — branch 1's downstream
step starts as soon as its upstream finishes, independent of other branches.

No new execution model, no new inter-step communication, no changes to the
storage layer. Streaming is a consequence of parallel step execution plus
branched pipeline structure.

### Priority scheduling

Priority is configurable via a priority function that maps step metadata
(depth, step number, operation name, resource requirements) to a priority
value.

The default is **stream depth** — the number of steps from the branch's
root. Step number is submission order and conflates branch identity with
pipeline position. Stream depth is pure pipeline position: all AF3 tasks
are depth 3 regardless of branch.

Computed at `submit()` time from predecessor `OutputReference` objects:

```
depth = 0                                       (no predecessors)
depth = max(depth[pred] for pred in preds) + 1  (otherwise)
```

### Step scheduler

A single long-lived thread running a dispatch loop:

```
loop:
  drain completion queue → release resources, mark successors ready
  dispatch highest-priority ready tasks that fit → I/O pool
  poll in-flight DispatchHandles (is_done?)
  sleep until woken or timeout
```

The step scheduler never blocks on backend execution. It dispatches tasks via
an I/O pool (which handles the short-lived prep and commit phases) and
polls DispatchHandles for completion. The long backend wait (minutes to hours)
consumes no threads.

**I/O pool:** A `ThreadPoolExecutor` (~16–32 workers) handles blocking
operations: input resolution, cache lookup, backend dispatch calls, staging
verification, and Delta Lake commits. Each I/O pool thread is occupied for
seconds, not minutes — then returns to the pool.

**Thread count:** 1 step scheduler + ~16–32 I/O pool threads,
regardless of how many tasks are in flight.

**User API:** Unchanged. `submit()` registers the task with the step
scheduler and returns a `StepFuture`. `finalize()` signals the step
scheduler to drain remaining tasks, joins the scheduler thread, and
shuts down the I/O pool. The step scheduler is an internal implementation
detail.

### DispatchHandle interface

`DispatchHandle` replaces the current `create_flow()` callable interface on
`BackendBase`. Today, `create_flow()` returns a Prefect flow callable;
`DispatchHandle` subsumes that by wrapping backend-specific execution
(Prefect, srun, subprocess) behind a uniform non-blocking interface.
Backends implement `create_dispatch_handle()` which returns a
`DispatchHandle` instead of a callable. The existing `create_flow()` method
is removed. See `dispatch-handle.md` for the full interface specification.

The `DispatchHandle` supports both blocking execution (non-streaming
pipelines) and non-blocking dispatch with completion polling (step
scheduler):

```python
class DispatchHandle(ABC):
    def run(
        self,
        units: list[ExecutionUnit | ExecutionComposite],
        runtime_env: RuntimeEnvironment,
    ) -> list[UnitResult]:
        """Execute the step. Blocks until completion or cancellation."""

    def dispatch(
        self,
        units: list[ExecutionUnit | ExecutionComposite],
        runtime_env: RuntimeEnvironment,
    ) -> None:
        """Start execution, return immediately.

        The handle owns unit transport — it decides how to deliver
        units to workers (pickle to shared FS, Modal .map() args,
        Ray object store, etc.).
        """

    def is_done(self) -> bool:
        """Non-blocking completion check."""

    def collect(self) -> list[UnitResult]:
        """Get results. Valid only after is_done() returns True."""

    def cancel(self) -> None:
        """Cancel in-flight work. Thread-safe, idempotent."""
```

`UnitResult` is defined in `dispatch-handle.md`.

`run()` is equivalent to `dispatch()` + poll `is_done()` + `collect()`.
Non-streaming pipelines use `run()` via the step executor. The step
scheduler uses the non-blocking methods.

Backend implementations:

- **LocalDispatchHandle:** `dispatch()` submits to `ProcessPoolExecutor`.
  `is_done()` checks the future. `cancel()` sets cancel event.
- **SlurmDispatchHandle:** `dispatch()` calls `srun`/`sbatch`.
  `is_done()` checks job status. `cancel()` calls `scancel`.

### Task lifecycle

A task moves through phases, each handled by a different component:

```
submitted → ready → prepping → dispatched → in-flight → committing → done
              │        │           │            │           │
              │        I/O pool    I/O pool     step        I/O pool
              │        (~2s)       (<1s)        scheduler   (~5s)
              step                 (backend     polls       (commit +
              scheduler            .dispatch)   is_done()   compact)
              checks
              predecessors
```

The in-flight phase (minutes to hours) consumes no threads. The prep and
commit phases (~seconds each) use I/O pool threads that return to the pool
immediately.

### Resource-aware dispatch

The step scheduler tracks a resource budget: total resources,
per-task requirements, and what is currently dispatched. It dispatches the
highest-priority ready task that fits within available resources.

The total budget comes from the backend via a new `resource_budget()`
method on `BackendBase` — auto-detected from the environment (hardware
for LOCAL, SLURM env vars for SLURM_INTRA). Per-task requirements come
from the existing `ResourceConfig` on each step. The user can optionally
override the detected budget (e.g., to reserve CPUs for the orchestrator
process).

### Two backend resource models

**Resource-managed (LOCAL, SLURM_INTRA):** Exclusive, fixed resources. The
step scheduler tracks budget as bookkeeping (total minus dispatched). Dispatch
gates on resource availability.

| Backend | GPU source | CPU source |
|---------|-----------|-----------|
| LOCAL | Hardware detection | `os.cpu_count()` |
| SLURM_INTRA | `SLURM_GPUS`, `CUDA_VISIBLE_DEVICES` | `SLURM_CPUS_ON_NODE` |

**Queue-managed (SLURM sbatch, Cloud, Kubernetes):** External system owns
resources. The step scheduler submits in priority order and lets the external
system schedule. Optionally propagates priority hints (SLURM `--nice`,
Kubernetes `PriorityClass`). Optionally caps in-flight submissions.

The backend declares its model via `resource_budget()` on `BackendBase`.
Default implementation returns `None` (queue-managed). Resource-managed
backends override.

```python
@dataclass(frozen=True)
class ResourceBudget:
    """Fixed resource pool for resource-managed backends."""

    cpus: int
    gpus: int = 0

class BackendBase(ABC):
    def resource_budget(self) -> ResourceBudget | None:
        """Return the fixed resource pool, or None for queue-managed backends.

        Resource-managed backends (LOCAL, SLURM_INTRA) override this to
        report available resources. Queue-managed backends (SLURM sbatch,
        cloud) inherit the default None.
        """
        return None
```

Per-backend detection:

- `LocalBackend`: `ResourceBudget(cpus=os.cpu_count(), gpus=<hardware detection>)`
- `SlurmIntraBackend`: `ResourceBudget(cpus=SLURM_CPUS_ON_NODE, gpus=SLURM_GPUS)`
- `SlurmBackend`, cloud backends: inherit default `None`

The step scheduler always dispatches highest-priority first. The only variation
is whether it gates on resource availability or submits freely.

### Commit batching

The step scheduler batches Delta Lake commits: accumulate completed
tasks up to N completions or T seconds, then commit all staged data in one
pass per table. This reduces transaction log growth and compaction
frequency.

Compaction is currently scoped per step because tables are partitioned by
step number. With commit batching, a single batch may contain results from
multiple steps, but compaction can still target individual step partitions
within the batch. Compaction runs on a background thread or after pipeline
completion, decoupled from the commit path.

---

## Incremental Implementation

Each phase is independently shippable.

### Parallel step execution

Enable `max_parallel_steps > 1` on `PipelineConfig`. Move
`_wait_for_predecessors` into the `_run()` closure. Add `threading.Lock`
for `_step_results` and `StepTracker` writes. FIFO dispatch — no priority,
no resource awareness.

Sufficient for small branch counts (10–50) where thread waste is tolerable.

Fix: `_stopped` propagation is currently pipeline-global. Must be scoped
per-branch or replaced with input-resolution-based skip logic.

See: `parallel_step_execution.md`

### Step scheduler

Replace the current `ThreadPoolExecutor(max_workers=1)` in
`PipelineManager` with the step scheduler. Priority and resource awareness
ship together — priority ordering without readiness gating causes priority
inversion, so they are not separable.

- Step scheduler thread with dispatch loop
- DispatchHandle replacing `create_flow()` with `dispatch()` / `is_done()` / `collect()` / `cancel()`
- I/O pool for prep and commit phases
- Priority queue ordered by stream depth (configurable)
- Resource budget tracking for resource-managed backends
- Commit batching for Delta Lake writes

### Backpressure (future scope)

Limit in-flight work per stream depth to prevent fast upstream from
overwhelming slow downstream. The step scheduler enforces per-depth caps in
addition to resource constraints. Deferred until the step scheduler is
operational and we can observe real dispatch patterns.

---

## Prerequisites

Independent of each other and can be built in parallel.

- **Parallel step execution** (`parallel_step_execution.md`) — foundation
  for all phases. Configurable `max_parallel_steps`, thread safety,
  per-branch skip propagation.
- **DispatchHandle** (`dispatch_handle.md`) — the step scheduler's interface
  to the backend. Non-blocking dispatch with `dispatch()` / `is_done()` /
  `collect()`, cancellation with `cancel()`, and pipeline-level
  cancel support.

---

## Open Questions

- **Commit batching granularity:** Batch by time window, completion count,
  or stream depth? Per-depth batching is natural but adds latency.
  Time-window batching is simpler.
- **Step scheduler failure recovery:** If the step scheduler
  thread dies, in-flight tracking is lost. Crash-restart with re-scan for
  completed work, or persist state to Delta?
- **DispatchHandle polling overhead:** At 5000 in-flight handles, polling
  `is_done()` every 500ms is 10K checks/second. Acceptable for SLURM
  (sacct can be batched), but worth measuring. Callback-based notification
  is the escape hatch if polling becomes a bottleneck.
- **I/O pool sizing:** Too small starves prep/commit throughput. Too large
  creates Delta write contention. Needs empirical tuning per backend.
- **Backpressure mechanism:** Per-depth in-flight cap, total queue cap, or
  dynamic based on downstream consumption rate?
- **Step-level caching:** Streaming steps have incrementally-arriving inputs.
  Are they uncacheable at the step level (unit-level caching only)?
- **Step executor subsumption:** Non-streaming pipelines use
  `DispatchHandle.run()` via the step executor. The step scheduler uses
  the non-blocking methods. Does the step scheduler eventually subsume
  the step executor entirely, or do both paths coexist long-term?
