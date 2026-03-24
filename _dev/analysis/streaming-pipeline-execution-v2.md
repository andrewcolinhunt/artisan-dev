# Analysis: Streaming Pipeline Execution (v2)

**Date:** 2026-03-24
**Supersedes:** `streaming-pipeline-execution.md`
**Context:** Conversation with Brian (potential power user) about running
protein design pipelines on Perlmutter. Extended analysis of the solution
space, architectural layering, and resource-aware scheduling.

---

## The Use Case

Run ~1000 independent design branches within a single SLURM allocation.
Each branch flows through a chain of operations (RFD3 → MPNN → Multimer →
AF3 → post-processing). Each operation requires different resources (CPU vs
GPU). Branches must stream independently — branch 1 reaching AF3 should not
wait for branch 999 to finish MPNN. Later-stage work should be prioritized
over earlier-stage work to maximize throughput and avoid stragglers.

## Core Requirements

- **Multi-resource dispatch:** Each operation in the chain chooses its own
  resources (CPUs for MPNN, GPUs for AF3). No holding GPUs idle during
  CPU-only steps.
- **Independent branch streaming:** Branches progress through the chain
  independently, not blocked by other branches at step boundaries. Defined
  as: downstream operations begin consuming results before upstream
  operations finish producing all of them.
- **Priority scheduling:** Later pipeline stages get dispatched before earlier
  stages when competing for resources (Brian's "nice value" concept).
- **Backend-agnostic:** The streaming mechanism should live in the
  orchestration layer, not in any specific backend. The backend provides
  dispatch; the coordinator manages flow.

Brian's use case additionally requires single-allocation dispatch (srun, no
queue latency), but this is a backend choice, not a streaming constraint.

---

## Existing Enabling Components

### Dispatch infrastructure

**Intra-allocation backend** (`_dev/design/0_current/slurm_intra_backend.md`):
Dispatches work via `srun --exclusive` within an allocation. Zero queue
latency. This is the dispatch mechanism for Brian's use case.

**Backend abstraction** (`orchestration/backends/base.py`): `BackendBase` ABC
with `create_flow()` returning a callable, `WorkerTraits`/`OrchestratorTraits`
for worker-side and orchestrator-side behavior. The backend is a dispatch
mechanism — it translates "run this unit with these resources" into
system-specific commands. It does not schedule, prioritize, or track resource
availability.

### Composition and branching

**Collapsed composites:** Run a chain of operations on a single worker,
artifacts passing in-memory, one Delta Lake commit at the end.
`artifacts_per_unit=1` gives branch independence. But: single resource set
per worker — GPU idle during CPU steps.

**Expanded composites:** Each internal operation becomes a pipeline step with
its own resources. Multi-resource, but bulk — ALL branches must complete one
step before the next begins.

### Step execution and wiring

**Parallel step execution** (`_dev/design/2_future_future/parallel_step_execution.md`):
Enables independent pipeline steps to overlap by increasing
`ThreadPoolExecutor.max_workers` and moving `_wait_for_predecessors` into
the `_run()` closure. Necessary infrastructure but insufficient — steps with
data dependencies still block on full upstream completion.

**`StepFuture` and `OutputReference`:** Non-blocking step wiring. `submit()`
returns immediately; `OutputReference` is a lazy pointer resolved at dispatch
time. The dependency graph determines execution order.

**`threading.Event` cancel-aware polling:** Used throughout
(`_wait_for_predecessors`, `_run_curator_in_subprocess`, `finalize`). Proven
coordination primitive for inter-step communication.

### Storage primitives

**`DeltaCommitter.commit_batch(batch_id)`:** The storage layer already
supports batch-granular commits. Staging is organized by batch_id into
isolated directories. Multiple independent writes can be committed
separately — the physical infrastructure for incremental commits exists.

**`await_staging_files()` polling:** NFS-aware exponential backoff polling
(1s initial, 5s cap, 60s timeout) for staging file visibility. Directly
reusable for downstream-polls-for-upstream-commits patterns.

**Steps table as event log:** `table_schemas.py` explicitly documents this as
an "append-only event log of step state transitions." Could extend to
sub-step batch events for streaming coordination.

**Delta Lake optimistic concurrency:** Supports concurrent appends from
independent writers. Parallel streaming steps can commit independently.
Compaction is the conflict point.

### Caching and scalability

**Two-level batching and per-unit caching:** Execution units are the
scheduling/caching unit. Cache lookup is per-unit via deterministic
`execution_spec_id`. Resume naturally skips completed units.

**Bulk cache lookup** (`_dev/design/2_future_future/bulk_cache_lookup.md`):
Replaces per-unit Delta scans (20+ minutes at 11M artifacts) with a single
`is_in()` scan. Prerequisite for practical incremental dispatch — without it,
frequent small dispatches multiply the cache check cost.

### Related designs not yet implemented

**`InputRef` / `LazyBatchRef`** (`_dev/design/1_future/lazy_execution_units.md`):
Workers resolve their own partition from Delta Lake via ~200-byte query
descriptors instead of ~930 MB pickled artifact IDs. Makes incremental
dispatch nearly free. Query-derived spec IDs change caching semantics.

**`pipeline_run_id` scoping** (`_dev/design/2_future_future/pipeline_replay.md`):
Scoped output resolution prevents mixing artifacts from different runs or
streaming sessions. Without it, incremental output resolution needs manual
tracking of already-processed outputs.

**`FlowHandle` with `cancel()`** (`_dev/design/1_future/pipeline-cancellation-full.md`):
Control surface for cancelling in-flight work — essential for streaming where
upstream errors or priority changes require clean cancellation of dispatched
batches.

---

## Architectural Layering

Three distinct roles emerged from analyzing how Ray Data, Prefect, and
Artisan relate:

**Artisan (project manager):** Knows the plan — the pipeline DAG, step
dependencies, what's been done (caching), what came from where (provenance),
and how to save results (Delta Lake). Decides *what* work needs to happen and
*in what order*.

**Prefect / task runner (foreman):** Runs a batch of tasks in parallel and
reports results. Manages workers for one dispatch. Does not know about the
pipeline, steps, or what comes next. Interchangeable — the system is loosely
coupled to Prefect (5 source files, the `BackendBase.create_flow()` seam).

**The underlying system (resource pool):** SLURM allocation, Ray cluster,
Kubernetes namespace, local CPU/GPU set. Owns the physical resources.

The streaming/priority problem lives in Artisan's layer. The backend is too
thin — Prefect's `TaskRunner` interface has no hooks for priority,
backpressure, or inter-step flow control. This is true of every task runner:
`ProcessPoolTaskRunner`, `SlurmTaskRunner`, `RayTaskRunner`, and
`DaskTaskRunner`.

Ray Data solves streaming by being *both* project manager and foreman — its
`StreamingExecutor` manages the DAG, priority, backpressure, and dispatch in
one system. Adopting Ray Data would mean replacing Artisan's orchestration
layer, not just swapping task runners.

### Priority scheduling belongs above the backend

Priority requires pipeline DAG knowledge (which step is further along). This
knowledge lives in the orchestration layer, not the backend. The backend
translates "run this" into system-specific commands — it cannot know which
work is more important.

The backend can optionally propagate priority hints to native systems (SLURM
`--nice`, Kubernetes `PriorityClass`), but this is an optimization. The
orchestration layer's dispatch ordering is the primary control, and it works
even when the backend has no priority support (LOCAL, basic srun).

Ray Data proves this pattern: its downstream-first priority
(`select_operator_to_run`) is application-level scheduling built above
`ray.remote()`. Ray Core itself has no task-level priority.

### Priority is stream depth, not step number

Step number is submission order — an artifact of how the user's script runs,
not where a task sits in the pipeline. With many branches, branch 4's RFD3
(step 15) would outrank branch 1's AF3 (step 3) under step-number priority.
That's backwards.

The correct default metric is **stream depth**: how many steps from the
branch's root. All AF3 tasks are depth 3 regardless of which branch they
belong to. All RFD3 tasks are depth 0. Later pipeline position always wins,
independent of submission order.

Stream depth is trivial to compute at `submit()` time. Predecessor steps are
already known from the input `OutputReference` objects:

```
depth = 0                                       (no predecessors)
depth = max(depth[pred] for pred in preds) + 1  (otherwise)
```

One dict (`step_number → depth`) on `PipelineManager`. No graph traversal —
predecessors are always submitted first. Handles diamonds and merges
correctly (depth is the longest path from any root).

Priority should be configurable. Stream depth is the default — it gives the
"drain later stages first" behavior that maximizes throughput and minimizes
stragglers. But users may want alternative strategies: FIFO (submission
order), estimated runtime, or custom priority functions. The coordinator
should accept a priority function that maps a step's metadata (depth,
step number, operation name, resource requirements) to a priority value.

### Priority scheduling requires resource awareness

You cannot prioritize "dispatch AF3 before RFD3" without knowing whether
GPUs are available for AF3. Today Artisan has zero resource awareness — each
step creates an isolated Prefect flow and the backend manages parallelism
within it.

For cross-step priority, the orchestration layer needs:

- **Total resource budget** (32 GPUs, 200 CPUs — from allocation or config)
- **Per-operation requirements** (AF3 needs 1 GPU, MPNN needs 4 CPUs — from
  `ResourceConfig`)
- **Dispatched-but-not-returned tracking** (bookkeeping, not system queries)

This is sufficient to make resource-aware priority decisions: pick the
highest-priority ready task that fits within available resources. Skip tasks
whose resources are exhausted and find tasks that can run now. This is what
Ray Data's `ResourceManager` + `select_operator_to_run()` does.

### Backends have different resource models

Not all backends can be treated the same. They fall into two categories
based on who owns the resources:

**Resource-managed backends (LOCAL, SLURM_INTRA):** The coordinator owns
exclusive, fixed resources. It knows the total budget and tracks what it has
dispatched. Resource-aware scheduling works directly — dispatch when
resources are available, hold when they're not.

| Backend | GPU budget source | CPU budget source |
|---------|-------------------|-------------------|
| LOCAL | Hardware detection (`CUDA_VISIBLE_DEVICES`, device count) | `os.cpu_count()` |
| SLURM_INTRA | `SLURM_GPUS`, `CUDA_VISIBLE_DEVICES` | `SLURM_CPUS_ON_NODE` |

**Queue-managed backends (SLURM sbatch, Cloud, Kubernetes):** An external
system owns the resources. Other users compete for them. The coordinator
cannot know what's available — only the external scheduler knows. Holding
back submissions because "we think the cluster is full" is
counterproductive. The right approach: submit in priority order (propagating
priority hints where the system supports them — SLURM `--nice`, Kubernetes
`PriorityClass`) and let the external system schedule. Optionally cap
in-flight submissions to avoid overwhelming the scheduler.

The coordinator handles both modes. The difference is the dispatch policy:

- Resource-managed: "Is there a GPU slot free? If yes, dispatch the
  highest-priority GPU task. If no, try the highest-priority CPU task."
- Queue-managed: "Submit the next highest-priority task. If at the
  concurrency cap, wait for one to complete."

The backend declares which mode it uses:

```python
class BackendBase:
    def resource_budget(self) -> ResourceBudget | None:
        """Resource-managed backends return a budget.
        Queue-managed backends return None."""
```

This keeps the coordinator backend-agnostic. It always maintains a priority
queue. It always dispatches highest-priority first. The only variation is
whether it gates on resource availability or submits freely.

### This changes the dispatch model

The current model is step-level: "here are 50 units, run them all." Priority
+ resource awareness requires task-level dispatch: individual tasks submitted
with resource requirements and priority, drained from a shared queue as
resources free up. The backend becomes a task-level dispatcher rather than a
step-level dispatcher.

---

## Approaches

### Collapsed composite + `artifacts_per_unit=1`

Each branch runs the full chain on one worker. Branches are independent. But
each worker holds one resource set for the entire chain — GPU idle during CPU
steps.

### Orchestrated composite (new execution mode)

Head node manages per-branch chains, dispatching per-op tasks with different
resources. Right semantics but backend-coupled: per-op dispatch works with
srun (fast) but not sbatch (queue latency per internal op per branch).

### Multi-pipeline orchestrator

Run N independent pipelines (one per design or batch) within a shared
allocation. Each pipeline runs the full chain sequentially using existing
step machinery. Backend-agnostic. But: no cross-pipeline priority scheduling;
straggler blocking within each pipeline.

### Many parallel branches + parallel step execution

Split designs into batches. Each batch becomes a branch of steps within a
single pipeline. Enable parallel step execution. Streaming falls out of the
dependency graph — branch 1's MPNN starts as soon as branch 1's RFD3
finishes, even while branch 20's RFD3 is still running.

Uses 100% existing infrastructure (`submit()`, `StepFuture`,
`OutputReference`, `_wait_for_predecessors`). The only new code is the
parallel step execution design. Multi-resource per step, backend-agnostic,
natural branch independence.

But: no priority scheduling (FIFO dispatch), no resource awareness (branches
contend blindly), no backpressure (fast upstream piles up work). Batch size
controls streaming granularity vs overhead — 20 branches × 5 ops = 100
steps is manageable; 1000 × 5 = 5000 is not.

### Resource-aware priority coordinator

A shared dispatch queue in the orchestration layer. Tasks enter the queue
with a priority (stream depth) and resource requirements. The coordinator
tracks a resource budget and drains the queue: pick the highest-priority task
that fits within available resources, dispatch it, update the budget. When a
task completes, free the budget and dispatch the next eligible task.

This is the many-branches approach with a coordinator replacing the FIFO
`ThreadPoolExecutor`. It is also the streaming steps approach arrived at from
a different direction — the two converge once you add resource-aware priority
scheduling over a shared dispatch queue.

### Tradeoff summary

| Approach | Multi-resource | Streaming | Priority | Resource-aware | Backend-agnostic | Impl. scope |
|----------|:-:|:-:|:-:|:-:|:-:|---|
| Collapsed composite | No | No | No | No | Yes | Exists |
| Orchestrated composite | Yes | Yes | Possible | Possible | No | Medium |
| Multi-pipeline orchestrator | Yes | No | No | No | Yes | Small |
| Many parallel branches | Yes | Yes | No | No | Yes | Small |
| Resource-aware coordinator | Yes | Yes | Yes | Yes | Yes | Medium-Large |

---

## The Convergence

The many-branches approach and the streaming steps approach are not different
architectures — they are the same architecture at different levels of
sophistication. The progression:

- **Start:** Sequential steps, one at a time. No streaming.
- **Add parallel step execution:** Independent steps overlap. Streaming
  emerges from the dependency graph. But FIFO dispatch, no priority.
- **Add priority ordering:** Dispatch later-stage work first. But no resource
  awareness — may dispatch GPU work when only CPUs are free.
- **Add resource awareness:** Track budget, dispatch highest-priority task
  that fits. This is the coordinator.
- **Add backpressure:** Limit in-flight work per stage to prevent fast
  upstream from overwhelming slow downstream.

Each level is independently shippable. Each level reduces a different
bottleneck. The coordinator is the natural endpoint, but earlier levels
deliver value on their own.

---

## Implementation Path

### Near-term: many parallel branches (no coordinator)

Implement the parallel step execution design. Users split designs into
batches and create parallel branches in their pipeline script. Streaming
emerges from the dependency graph. FIFO dispatch — no priority, no resource
awareness.

Sufficient when: operation runtimes are roughly uniform (priority matters
less), or the batch count is small enough that contention is manageable.

Prerequisite: fix `_stopped` propagation (currently pipeline-global; needs
to be per-branch or replaced with input-resolution-based skip logic).

### Mid-term: priority-aware dispatch

Replace the FIFO `ThreadPoolExecutor` with a priority-ordered dispatch
mechanism. Steps with higher stream depth dispatch first when ready. No
resource awareness yet — just ordering.

Sufficient when: all operations use the same resource type (all GPU or all
CPU), so priority ordering alone prevents starvation.

### Long-term: resource-aware coordinator

Add resource budget tracking to the orchestration layer. The coordinator
maintains available resources, each operation declares requirements, dispatch
only happens when resources are available. The backend interface evolves from
step-level (`create_flow` returns a callable for an entire step) to
task-level (`dispatch_unit` runs a single unit with specified resources).

This is the full streaming architecture. It subsumes the parallel branches
approach — branches are just tasks in the coordinator's queue.

### Prerequisites across all levels

- **Parallel step execution** — the foundation for all of the above
- **Bulk cache lookup** — makes frequent small dispatches practical
- **`FlowHandle` cancellation** — clean cleanup of in-flight work

Note: `pipeline_run_id` scoping is NOT a streaming prerequisite. Parallel
branches within a single pipeline get unique step numbers from the
monotonically increasing `_current_step` counter. Output resolution by
`origin_step_number` is unambiguous within a single run.

---

## Open Questions

- **Backend interface evolution:** The current `create_flow()` returns a
  callable that runs an entire step. The coordinator needs task-level
  dispatch. Should the backend interface add `dispatch_unit()` alongside
  `create_flow()`, or replace it? Can both coexist during the transition?
- **Backpressure mechanism:** Limit in-flight tasks per stage? Cap total
  queued work? Dynamic based on downstream consumption rate (Ray Data's
  approach)?
- **Delta Lake write amplification:** Incremental commits (many small
  commits vs few large ones) grow the transaction log and degrade read
  performance. What compaction strategy keeps this manageable?
- **Failure semantics:** If a streaming step fails mid-execution, some
  batches are committed and some aren't. How does recovery handle partially
  completed steps? How do downstream steps that already consumed partial
  outputs handle upstream retries?
- **Spec ID computation for streaming:** Step-level caching assumes a fixed
  input set. With incremental dispatch, are streaming steps uncacheable at
  the step level (only unit-level caching), or is the spec ID computed over
  the complete input set after the step fully completes?
