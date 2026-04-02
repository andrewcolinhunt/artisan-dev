# Analysis: Artisan Streaming Design vs Ray Data

**Date:** 2026-03-25
**Scope:** Comparative feature analysis of Artisan's streaming pipeline design
(dispatch_handle.md, parallel_step_execution.md, streaming_pipeline_execution.md)
against Ray Data's streaming execution engine.

---

## Context: Why the comparison works (and where it doesn't)

**Ray Data** is a distributed data processing framework operating on in-memory
tabular data (Arrow/pandas blocks) flowing through operator chains on a Ray
cluster. Tasks are fine-grained (seconds), data lives in shared object store.

**Artisan** is a computational pipeline framework operating on persistent
artifacts (Delta Lake) with tasks dispatched to SLURM/local backends. Tasks are
coarse-grained (minutes to hours), data lives on disk.

Despite these differences, the core scheduling problems are identical:
streaming DAG execution with backpressure, resource-aware dispatch, priority
scheduling, cancellation, and fault tolerance. Ray Data has solved these
problems at scale. The comparison is about **scheduling architecture**, not
data format.

---

## Feature-by-Feature Comparison

### Streaming execution model

| Aspect | Ray Data | Artisan (planned) |
|--------|----------|-------------------|
| Scheduler | `StreamingExecutor` on background thread | Step scheduler on background thread |
| Loop | Process completions → dispatch eligible → update state → autoscale | Drain completions → dispatch ready tasks → poll handles → sleep |
| Granularity | Block-level (MiBs, seconds) | Task-level (units, minutes–hours) |
| Data flow | In-memory queues between operators | Disk-based (Delta Lake) between steps |

**Assessment: Well-aligned.** The architectures are structurally similar. The
granularity difference is appropriate — Artisan's tasks are orders of magnitude
longer than Ray Data's, so polling-based completion tracking (vs ray.wait()) is
the right choice. The step scheduler design mirrors StreamingExecutor's loop
structure.

---

### Backpressure and flow control

| Aspect | Ray Data | Artisan (planned) |
|--------|----------|-------------------|
| Mechanism | ConcurrencyCapPolicy + ResourceBudgetPolicy | Deferred ("future scope") |
| Memory-based | Yes (internal queue bytes, output queue bytes) | No |
| Per-operator caps | Yes (max_concurrency per operator) | No (mentioned as per-depth caps) |
| Dynamic adjustment | Output queue limits adjust based on downstream consumption | Not designed |

**Assessment: Gap.** Backpressure is deferred in the Artisan design. However,
the severity depends on the backend model:

- **Resource-managed backends (LOCAL, SLURM_INTRA):** Resource-aware dispatch
  provides *implicit* backpressure. If AF3 (downstream) needs GPUs and all GPUs
  are occupied by other AF3 tasks, RFD3 (upstream) can't produce more work that
  would consume GPUs. The ready queue may grow, but actual resource consumption
  is bounded. This is adequate for Phase 1.

- **Queue-managed backends (sbatch, K8s):** No natural bound. All 5000 tasks
  could be submitted immediately. The design mentions "optionally caps in-flight
  submissions" but doesn't specify the mechanism.

**Recommendation:** For Phase 1, resource-aware dispatch is sufficient
backpressure for the primary use case (SLURM_INTRA). Add a `max_in_flight`
cap for queue-managed backends as a simple safety valve. Defer dynamic
backpressure until real dispatch patterns are observed — the design already
plans this.

---

### Parallelism and resource management

| Aspect | Ray Data | Artisan (planned) |
|--------|----------|-------------------|
| Resources tracked | CPU, GPU, heap memory, object store | CPU, GPU |
| Allocation strategy | Reservation-based with shared pool | Total budget minus dispatched |
| Backend models | One (Ray cluster = resource-managed) | Two (resource-managed + queue-managed) |
| Priority | Downstream operators with smallest output queues | Configurable (default: stream depth = downstream first) |
| Auto-detection | From cluster state | From hardware / SLURM env vars |

**Assessment: Good coverage.** Artisan's two-model approach (resource-managed
vs queue-managed) is a cleaner abstraction than Ray Data's single model,
because Artisan genuinely supports heterogeneous backends. Priority scheduling
(downstream-first) matches Ray Data's philosophy.

**Minor gap: memory as a resource.** Ray Data tracks heap memory and object
store memory. Artisan doesn't track task memory requirements. For scientific
computing tasks with high memory footprints (AF3 can use 32+ GB), dispatching
too many memory-hungry tasks on the same node could cause OOM even if
CPU/GPU counts are within budget.

**Recommendation:** Consider adding optional memory tracking to ResourceBudget.
Not critical for Phase 1 (SLURM manages per-job memory via `--mem`), but
important for LOCAL backend at scale.

---

### Fault tolerance

| Aspect | Ray Data | Artisan (planned) |
|--------|----------|-------------------|
| Task retries | Default 3, configurable per-task | Not addressed |
| Error tolerance | max_errored_blocks (allow N failures) | Not addressed |
| Lineage reconstruction | Re-execute tasks that produced lost objects | Caching provides resume capability |
| Scheduler recovery | N/A (executor is the driver) | Open question |

**Assessment: Significant gap.** For tasks running minutes to hours on shared
HPC resources, failures are inevitable: node preemption, OOM kills, SLURM
timeouts, transient filesystem errors. The current design has no retry
mechanism within a streaming execution.

Artisan's caching provides coarse-grained recovery (re-run the pipeline, skip
completed steps), but within a streaming execution:
- A failed task leaves its branch incomplete
- The step scheduler has no policy for what to do next
- Other branches continue, but the failed branch is abandoned
- The user must re-run the entire pipeline to retry failed branches

**Recommendation:** Add task retry to the step scheduler design:
- Configurable `max_retries` per step (default: 0 for backwards compat)
- Failed tasks re-enter the ready queue up to max_retries
- After max_retries, mark the task as failed and continue other work
- Add `max_failed_tasks` (analogous to max_errored_blocks) — pipeline
  succeeds if failures stay within this threshold, fails otherwise
- Retry with exponential backoff for transient errors

This is architecturally straightforward — the step scheduler already tracks
task lifecycle, adding a "retry" transition from "failed" to "ready" is
minimal overhead.

---

### Error handling in streaming context

| Aspect | Ray Data | Artisan (planned) |
|--------|----------|-------------------|
| Error propagation | Wrapped in RayTaskError, propagated to consumer | Not specified for streaming |
| Partial failure | Continue with remaining blocks | Not specified |
| Error tolerance | Configurable (N blocks can fail) | Not specified |
| Error reporting | Per-operator error counts, error types logged | Not specified |

**Assessment: Gap.** The design docs focus on the happy path. Questions that
need answers:

- When a task fails, does the step scheduler continue dispatching?
- Are downstream tasks for the failed branch skipped or left waiting?
- Can the pipeline succeed if some branches fail entirely?
- How are errors surfaced to the user during a long-running streaming pipeline?

**Recommendation:** Define error handling as part of the step scheduler design:
- Failed tasks emit a failure event to the scheduler
- The scheduler marks downstream dependents as "skipped" (cascade)
- Pipeline reports N succeeded / M failed / K skipped at finalization
- `max_failed_branches` configuration for acceptable failure rate

---

### Memory management (orchestrator-side)

| Aspect | Ray Data | Artisan (planned) |
|--------|----------|-------------------|
| Queue bounds | Per-operator output queue limits, dynamic sizing | Not addressed |
| Spilling | Object store → disk when memory exceeded | N/A (data already on disk) |
| Block size management | 1–128 MiB blocks, automatic splitting | N/A |
| Orchestrator footprint | Bounded by topology size × queue limits | Unbounded (5000 task states?) |

**Assessment: Minor gap.** Artisan's data is already on disk (Delta Lake), so
data-level memory management isn't needed. But the orchestrator itself tracks
state for potentially thousands of tasks: metadata, dispatch handles, results.

At 5000 tasks, this is probably fine (kilobytes per task), but worth a sanity
check: what does the step scheduler hold in memory per task?

**Recommendation:** No action needed for Phase 1. Note as a monitoring concern
if scaling beyond 5000 tasks.

---

### Observability and progress tracking

| Aspect | Ray Data | Artisan (planned) |
|--------|----------|-------------------|
| Progress bars | Per-operator, real-time, with backpressure indicators | StepTracker (per-step, sequential) |
| Dashboard | Ray dashboard with per-operator metrics | None |
| Metrics | Prometheus with dataset/operator tags | None |
| Stats | ds.stats() for aggregated summaries | None |
| Logging | Structured log files updated every 5 seconds | Existing logging |

**Assessment: Gap for streaming execution.** StepTracker works well for
sequential pipelines but provides limited visibility into streaming execution
with thousands of concurrent tasks. During a multi-hour streaming run, the
user needs to know:

- How many branches have completed each stage?
- What's the current resource utilization?
- Are any stages bottlenecked? (backpressure indicators)
- What's the estimated time remaining?
- How many tasks have failed?

**Recommendation:** Extend StepTracker or add a streaming-aware progress
reporter:
- Per-depth aggregate counts (pending / in-flight / done / failed)
- Resource utilization snapshot (allocated / available)
- Periodic summary logging (every N seconds)
- This can be added incrementally after the step scheduler works

---

### Operator fusion

| Aspect | Ray Data | Artisan |
|--------|----------|---------|
| Mechanism | Merge consecutive map operators with same resources | N/A |
| Benefit | Eliminates serialization between operators | N/A |

**Assessment: Not applicable.** Ray Data fuses fine-grained operators to avoid
per-block serialization overhead. Artisan's steps are user-defined
computational units (minutes to hours each). The overhead of writing
intermediate results to Delta Lake is negligible relative to task runtime.

If Artisan ever adds sub-second micro-operations, fusion could become relevant.
Not a current concern.

---

### Data locality

| Aspect | Ray Data | Artisan |
|--------|----------|---------|
| Mechanism | Locality-aware scheduling, SPREAD strategy | Not addressed |
| Relevance | Critical (data in distributed object store) | Low (shared filesystem) |

**Assessment: Not a gap.** HPC clusters use shared filesystems (Lustre, GPFS)
where all nodes have equal access to data. Data locality scheduling would
provide minimal benefit. If Artisan adds cloud backends (S3 + ephemeral
compute), data locality becomes relevant.

---

### Dynamic repartitioning

**Assessment: Not applicable.** Ray Data repartitions tabular data across
blocks. Artisan's units within a step are independent — there's no concept
of repartitioning.

---

### Execution strategies

| Aspect | Ray Data | Artisan (planned) |
|--------|----------|-------------------|
| Sequential | Bulk synchronous (legacy) | Current (max_parallel_steps=1) |
| Parallel | N/A (always streaming) | Phase 1 (max_parallel_steps > 1) |
| Streaming | Default since 2.4 | Phase 2 (step scheduler) |
| Lazy | ds.lazy() defers until consumed | submit() + finalize() is already lazy |

**Assessment: Well-aligned.** The incremental progression
(sequential → parallel → streaming) is sound. Each phase is independently
shippable and delivers value. The blocking (run()) and non-blocking
(dispatch/poll/collect) paths coexist cleanly.

---

### Cancellation

| Aspect | Ray Data | Artisan (planned) |
|--------|----------|-------------------|
| Task-level | ray.cancel() sends KeyboardInterrupt | DispatchHandle.cancel() per backend |
| Pipeline-level | StreamingExecutor.shutdown() with lock/flag/join | PipelineManager.cancel() wired to handles |
| Cleanup | Hierarchical operator shutdown | Backend-specific (scancel, process pool exit) |
| Idempotent | Yes | Yes |

**Assessment: Well-covered.** Artisan's cancellation design is thorough. The
DispatchHandle abstraction with backend-specific cancel implementations
(scancel for SLURM, cancel event for local) is well-designed. Pipeline-level
cancellation wiring through PipelineManager to active handles is clean.

**Open question worth resolving:** Partial commit on cancellation. Should
completed units be committed? Ray Data discards partial results. For
scientific computing (hours of computation), committing completed work is
valuable. Recommend: commit completed units, mark the step as "partially
complete" in the tracker, and allow resume.

---

### Result consumption

| Aspect | Ray Data | Artisan |
|--------|----------|---------|
| Model | Lazy, streaming iterators | Persistent storage (Delta Lake) |
| Streaming consumption | iter_batches(), iter_rows() | Not applicable |
| Materialization | ds.materialize() caches in object store | All results materialized to Delta Lake |
| Split consumption | streaming_split(n) for parallel training | N/A |

**Assessment: Different models, both appropriate.** Ray Data results are
ephemeral (in-memory) and consumed via iterators. Artisan results are
persistent (Delta Lake) and consumed via the artifact storage layer.
Scientific computing results need to be persistent and reproducible, making
Delta Lake the right choice.

---

### Commit batching / write optimization

| Aspect | Ray Data | Artisan (planned) |
|--------|----------|-------------------|
| Write strategy | Blocks flow through to output without explicit commits | Batch N completions or T seconds before Delta Lake commit |
| Concurrency control | Object store handles concurrency | Serialized commits to avoid Delta Lake contention |

**Assessment: Good.** The commit batching design addresses a real concern.
Delta Lake transaction log growth with thousands of individual commits would be
problematic. Batching by count or time window is the right approach.

**Recommendation from open questions:** Time-window batching is simpler and
more predictable than count-based. Use a time window (e.g., 10 seconds) as
the default, with count as a secondary trigger for burst completions.

---

### Additional Ray Data features not in Artisan designs

| Feature | Ray Data | Artisan relevance |
|---------|----------|-------------------|
| Actor pools (long-lived stateful workers) | ActorPoolStrategy with autoscaling | Not needed — operations are stateless functions |
| Cluster autoscaling | Request cluster scaling based on pending work | Potentially useful for cloud backends (future) |
| Preserve ordering | ExecutionOptions.preserve_order | Not needed — branches are independent |
| Zero-copy data transfer | Arrow shared memory, zero_copy_batch | Not applicable — data on disk |
| Prefetching | Actor-based block prefetching | Not applicable — not streaming consumption |
| Push-based shuffle | For >1TB datasets | Not applicable |

---

## Summary

### Already well-covered in the designs

- **Streaming execution architecture** — step scheduler mirrors
  StreamingExecutor's loop structure, appropriate for coarse-grained tasks
- **Resource-aware dispatch** — two-model approach (resource-managed vs
  queue-managed) is cleaner than Ray Data's single model
- **Priority scheduling** — downstream-first matches Ray Data's philosophy
- **Cancellation** — thorough design with backend-specific implementations
- **DispatchHandle** — clean non-blocking interface for the scheduler
- **Incremental implementation** — sequential → parallel → streaming is sound
- **Commit batching** — addresses Delta Lake write pressure

### Gaps to address before or during implementation

| Gap | Severity | Recommendation |
|-----|----------|----------------|
| **Task retries** | High | Add `max_retries` per step with re-queue to ready state |
| **Error handling** | High | Define failure cascade (skip downstream dependents), `max_failed_tasks` threshold |
| **Queue-managed backpressure** | Medium | Add `max_in_flight` cap for sbatch/K8s backends |

### Gaps to address after initial implementation

| Gap | Severity | Recommendation |
|-----|----------|----------------|
| **Streaming observability** | Medium | Per-depth aggregate progress, resource utilization logging |
| **Memory as tracked resource** | Low | Optional memory in ResourceBudget for LOCAL backend |
| **Partial commit on cancel** | Low | Commit completed units, mark step as partially complete |

### Intentionally not applicable

- Operator fusion (tasks too coarse-grained)
- Data locality (shared filesystem)
- Dynamic repartitioning (units are independent)
- In-memory data management (data lives on disk)
- Streaming result consumption (results are persistent artifacts)
- Actor pools / zero-copy / prefetching (different execution model)
