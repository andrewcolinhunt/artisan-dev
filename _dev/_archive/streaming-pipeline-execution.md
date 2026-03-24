# Analysis: Streaming Pipeline Execution

**Date:** 2026-03-24
**Context:** Conversation with Brian (potential power user) about running
protein design pipelines on Perlmutter.

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
- **Backend-agnostic:** Streaming should work across all backends (LOCAL,
  SLURM, SLURM_INTRA), not just the intra-allocation case. LOCAL benefits
  from overlapping I/O-bound steps. SLURM benefits from starting downstream
  sbatch jobs before upstream fully completes. The streaming mechanism should
  live in the orchestration layer, not in any specific backend. The backend
  provides dispatch; the coordinator manages flow. Dispatch granularity
  differs by backend (LOCAL/SLURM_INTRA dispatch per-unit cheaply; SLURM
  batches into job arrays), so the interface should support batched dispatch
  with backend-tunable batch sizes.

Brian's use case additionally requires single-allocation dispatch (srun, no
queue latency), but this is a backend choice, not a streaming constraint.

## Existing Components That Enable This

**Intra-allocation backend** (`_dev/design/0_current/slurm_intra_backend.md`):
Dispatches work via `srun --exclusive` within an allocation using submitit's
pickle protocol. Handles multi-node GPU binding. This is the dispatch
mechanism for Brian's use case.

**Collapsed composites:** Run a chain of operations on a single worker,
artifacts passing in-memory, one Delta Lake commit at the end. Give branch
independence (`artifacts_per_unit=1` = one branch per worker). But: single
resource set per worker — the GPU waste problem.

**Expanded composites:** Each internal operation becomes a pipeline step with
its own resources. Multi-resource, but bulk — ALL branches must complete one
step before the next begins. No streaming.

**Parallel step execution** (`_dev/design/2_future_future/parallel_step_execution.md`):
Enables independent pipeline steps to overlap via `ThreadPoolExecutor` with
predecessor-wait-in-closure. Necessary infrastructure but doesn't solve
streaming on its own — steps with data dependencies still block on full
upstream completion.

**Two-level batching and per-unit caching:** Execution units are the
scheduling/caching unit. Cache lookup is per-unit via deterministic
`execution_spec_id`. Resume naturally skips completed units.

**Independent pipeline orchestration:** `PipelineManager` instances are
standalone — each manages its own step sequence, delta root, and execution
state. Multiple pipelines can run concurrently in separate threads/processes,
sharing a backend and SLURM allocation. No built-in cross-pipeline
coordination exists today.

## Approaches Explored

**Collapsed composite + `artifacts_per_unit=1`:** Each branch runs the full
chain on one worker. Branches are independent (don't block each other). But
each worker holds one resource set for the entire chain — GPU idle during CPU
steps. No streaming in the defined sense: all branches must complete before
the step commits and any downstream step can begin.

**Orchestrated composite (new execution mode):** Head node manages per-branch
chains, dispatching per-op tasks with different resources. Artifacts pass via
NFS between ops, Delta Lake commit only at end. Right semantics but heavy —
requires managing 1000 concurrent sequential chains on the head node.
Backend-coupled: the per-op dispatch model works well with srun (fast, no
queue) but poorly with sbatch (queue latency per internal op per branch).

**Per-artifact pipeline branches + parallel steps:** Decompose into 5000
pipeline steps (1000 branches × 5 ops). Parallel step execution overlaps
them. Correct semantics but impractical overhead (5000 Delta Lake commits,
needs per-artifact `OutputReference` slicing, needs priority-aware executor
instead of FIFO `ThreadPoolExecutor`).

**Multi-pipeline orchestrator:** Run N independent pipelines (one per design)
within a shared allocation, managed by a higher-level launcher. Each pipeline
runs the full chain sequentially using existing step machinery. A
`ThreadPoolExecutor` or similar limits how many pipelines run concurrently.
Each pipeline dispatches work via whatever backend is configured. Backend-
agnostic — works on LOCAL, SLURM, or SLURM_INTRA. Optionally, per-pipeline
delta roots avoid Delta Lake concurrency (shared delta root has an output-
resolution scoping gap per `pipeline_replay.md`). Key limitations: no
cross-pipeline priority scheduling (early-stage work from one pipeline
competes equally with late-stage work from another); straggler blocking
within each pipeline (49 completed results wait idle while the 50th holds up
the next step). Adding a shared dispatch queue across pipelines solves
priority, but at that point the coordinator resembles the streaming approach.

**Streaming step execution:** Decouple the commit boundary from the step
boundary. Steps commit results in configurable batches (`commit_batch_size`),
making partial outputs visible to downstream steps. Downstream polls Delta
Lake for newly committed upstream artifacts and dispatches incrementally.
Priority scheduling at the application level (dispatch queue ordered by step
number descending). Backend-agnostic — the coordinator manages inter-step
flow; backends provide dispatch.

## Tradeoff Summary

| Approach | Multi-resource | Streaming | Priority | Backend-agnostic | Delta Lake overhead | Impl. scope |
|----------|:-:|:-:|:-:|:-:|---|---|
| Collapsed composite | No | No | No | Yes | Low (1 commit) | Small (exists) |
| Orchestrated composite | Yes | Yes | Possible | No (sbatch adds queue latency per internal op) | Low (1 commit per branch) | Medium |
| Per-artifact branches | Yes | Yes | Needs priority executor | Yes | High (5000 commits) | Medium |
| Multi-pipeline orchestrator | Yes | No (bulk within each pipeline) | No (without shared queue) | Yes | Low per pipeline; N stores or scoping gap | Small |
| Streaming steps | Yes | Yes | Natural fit | Yes | Tunable (batch_size) | Large |

## Open Questions

- What is the right abstraction boundary? Composite mode vs. pipeline engine
  vs. standalone primitive.
- Is batch-commit-to-Delta-Lake the right inter-step communication, or should
  artifacts pass out-of-band (NFS scratch) to avoid write amplification?
- How does streaming interact with Prefect? Bypass it for srun dispatch, or
  adapt the flow model?
- What's the minimum viable slice that gets Brian running on Perlmutter?
  Collapsed composites (single resource set) may be sufficient for the
  near-term small molecule pipeline while streaming is developed.
- Backend dispatch granularity: LOCAL and SLURM_INTRA can dispatch per-unit
  cheaply. SLURM (sbatch) benefits from batched dispatch (job arrays). Should
  the backend interface expose `dispatch_units(batch) -> list[Future]` with
  backend-tunable batch sizes, or should the coordinator handle this?
