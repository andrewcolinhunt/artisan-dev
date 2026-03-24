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
  independently, not blocked by other branches at step boundaries.
- **Priority scheduling:** Later pipeline stages get dispatched before earlier
  stages when competing for resources (Brian's "nice value" concept).
- **Single allocation:** All work dispatched via `srun` within one `salloc`,
  no queue latency.

## Existing Components That Enable This

**Intra-allocation backend** (`_dev/design/0_current/slurm_intra_backend.md`):
Dispatches work via `srun --exclusive` within an allocation using submitit's
pickle protocol. Handles multi-node GPU binding. This is the dispatch
mechanism — well-designed for the use case.

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

## Approaches Explored

**Collapsed composite + `artifacts_per_unit=1`:** Each branch runs the full
chain on one worker. Branches are independent. But each worker holds one
resource set for the entire chain — GPU idle during CPU steps. Acceptable for
near-term single-resource chains; not acceptable when multi-resource matters.

**Orchestrated composite (new execution mode):** Head node manages per-branch
chains, dispatching per-op `srun` tasks with different resources. Artifacts
pass via NFS between ops, Delta Lake commit only at end. Right semantics but
heavy — requires managing 1000 concurrent sequential chains on the head node.

**Per-artifact pipeline branches + parallel steps:** Decompose into 5000
pipeline steps (1000 branches × 5 ops). Parallel step execution overlaps
them. Correct semantics but impractical overhead (5000 Delta Lake commits,
needs per-artifact `OutputReference` slicing, needs priority-aware executor
instead of FIFO `ThreadPoolExecutor`).

**Multi-pipeline orchestrator:** Run N independent pipelines (one per design)
within a shared allocation, managed by a higher-level launcher. Each pipeline
runs the full chain sequentially using existing step machinery. A
`ThreadPoolExecutor` or similar limits how many pipelines run concurrently.
Each pipeline dispatches its own srun tasks; SLURM handles resource
contention within the allocation. Optionally, per-pipeline delta roots avoid
all Delta Lake concurrency issues.

**Streaming step execution:** Decouple the commit
boundary from the step boundary. Steps commit results in configurable batches
(`commit_batch_size`), making partial outputs visible to downstream steps.
Downstream polls Delta Lake for newly committed upstream artifacts and
dispatches incrementally. Priority scheduling at the application level
(dispatch queue ordered by step number descending).

## Tradeoff Summary

| Approach | Multi-resource | Streaming | Priority | Delta Lake overhead | Implementation scope |
|----------|:-:|:-:|:-:|---|---|
| Collapsed composite | No | Branches independent, not resource-streaming | No | Low (1 commit) | Small (exists today) |
| Orchestrated composite | Yes | Yes | Possible | Low (1 commit per branch) | Medium (new composite mode + head-node scheduler) |
| Per-artifact branches | Yes | Yes | Needs priority executor | High (5000 commits) | Medium (needs OutputReference slicing) |
| Multi-pipeline orchestrator | Yes | Per-pipeline only (bulk within each) | No (without shared queue) | Low per pipeline; N separate stores or shared with scoping gap | Small (new launcher, existing pipeline machinery) |
| Streaming steps | Yes | Yes | Natural fit | Tunable (batch_size) | Large (new coordinator, batch commits, backend changes) |

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
