# Design: Streaming Pipeline Execution

**Date:** 2026-03-24
**Status:** Draft
**Analysis:** `_dev/analysis/streaming-pipeline-execution-v2.md`

---

## Summary

Enable independent pipeline branches to stream through a chain of
operations, each with different resource requirements, with priority
scheduling that favors later-stage work. The design is incremental — each
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
- **Backend-agnostic** — works across LOCAL, SLURM, SLURM_INTRA, and
  future backends (Kubernetes, cloud)

---

## Architecture

Three layers with distinct responsibilities:

**Orchestration layer (Artisan):** Owns the pipeline DAG, step dependencies,
priority scheduling, and resource budget tracking. Decides what to dispatch
and when.

**Backend (dispatch layer):** Translates "run this unit with these resources"
into system-specific commands (srun, sbatch, subprocess, kubectl). Does not
schedule, prioritize, or track resources. Interchangeable.

**Underlying system (resource pool):** SLURM allocation, local machine, Ray
cluster, Kubernetes namespace. Owns the physical resources.

Priority and streaming live in the orchestration layer. The backend is a
thin dispatch mechanism.

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

### Priority by stream depth

Priority is determined by **stream depth** — the number of steps from the
branch's root — not step number. Step number is submission order and
conflates branch identity with pipeline position. Stream depth is pure
pipeline position: all AF3 tasks are depth 3 regardless of branch.

Computed at `submit()` time from predecessor `OutputReference` objects:

```
depth = 0                                       (no predecessors)
depth = max(depth[pred] for pred in preds) + 1  (otherwise)
```

Stream depth is the default. Priority is configurable via a priority
function that maps step metadata (depth, step number, operation name,
resource requirements) to a priority value.

### Resource-aware dispatch

The orchestration layer tracks a resource budget: total resources, per-task
requirements, and what is currently dispatched. It dispatches the
highest-priority ready task that fits within available resources.

### Two backend resource models

**Resource-managed (LOCAL, SLURM_INTRA):** Exclusive, fixed resources. The
coordinator tracks budget as bookkeeping (total minus dispatched). Dispatch
gates on resource availability.

| Backend | GPU source | CPU source |
|---------|-----------|-----------|
| LOCAL | Hardware detection | `os.cpu_count()` |
| SLURM_INTRA | `SLURM_GPUS`, `CUDA_VISIBLE_DEVICES` | `SLURM_CPUS_ON_NODE` |

**Queue-managed (SLURM sbatch, Cloud, Kubernetes):** External system owns
resources. Coordinator submits in priority order and lets the external
system schedule. Optionally propagates priority hints (SLURM `--nice`,
Kubernetes `PriorityClass`). Optionally caps in-flight submissions.

The backend declares its model:

```python
class BackendBase:
    def resource_budget(self) -> ResourceBudget | None:
        """Resource-managed backends return a budget.
        Queue-managed backends return None."""
```

The coordinator always dispatches highest-priority first. The only variation
is whether it gates on resource availability or submits freely.

### Task-level dispatch

The current backend interface is step-level: `create_flow()` returns a
callable that runs an entire step's units. The coordinator needs task-level
dispatch: submit individual units with resource requirements, receive
completion notifications.

The backend interface evolves to support both:

- `create_flow()` — existing step-level dispatch (retained for
  non-streaming pipelines)
- `dispatch_unit()` — new task-level dispatch for the coordinator

---

## Incremental Implementation

Each phase is independently shippable.

### Phase 1: Parallel step execution

Enable `max_parallel_steps > 1` on `PipelineManager`. Move
`_wait_for_predecessors` into the `_run()` closure. Add `threading.Lock`
for `_step_results` and `StepTracker` writes.

Streaming emerges from branched pipeline structure. FIFO dispatch — no
priority, no resource awareness.

Fix: `_stopped` propagation is currently pipeline-global. Must be scoped
per-branch or replaced with input-resolution-based skip logic.

See: `_dev/design/2_future_future/parallel_step_execution.md`

### Phase 2: Priority-aware dispatch

Replace FIFO `ThreadPoolExecutor` with a priority-ordered dispatch
mechanism. Compute stream depth at `submit()` time. Higher depth dispatches
first.

### Phase 3: Resource-aware coordinator

Add `resource_budget()` to `BackendBase`. Coordinator tracks available
resources and dispatches the highest-priority task that fits. Add
`dispatch_unit()` to the backend interface for task-level dispatch.

### Phase 4: Backpressure

Limit in-flight work per stream depth to prevent fast upstream from
overwhelming slow downstream.

---

## Prerequisites

- **Parallel step execution** — foundation for all phases
- **Bulk cache lookup** — frequent small dispatches multiply per-unit cache
  check cost without it
- **`pipeline_run_id` scoping** — prevents output resolution from mixing
  artifacts across concurrent branches
- **`FlowHandle` cancellation** — clean cleanup of in-flight work on error
  or priority change

---

## Open Questions

- **Backend interface coexistence:** Can `create_flow()` and
  `dispatch_unit()` coexist, or does the coordinator subsume `create_flow()`?
- **Backpressure mechanism:** Per-depth in-flight cap, total queue cap, or
  dynamic based on downstream consumption rate?
- **Delta Lake write amplification:** Many concurrent branch commits grow the
  transaction log. Compaction strategy?
- **Failure and partial completion:** How does recovery handle branches that
  partially completed? How do downstream branches handle upstream retries?
- **Step-level caching:** Streaming steps have incrementally-arriving inputs.
  Are they uncacheable at the step level (unit-level caching only)?
