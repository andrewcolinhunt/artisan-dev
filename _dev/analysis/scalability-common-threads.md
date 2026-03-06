# Analysis: Common Threads in Scalability Design Docs

**Date:** 2026-03-06  **Status:** Active  **Author:** Claude + ach94

---

## Scope

This document distills the shared patterns, principles, and dependencies across
four design docs in `_dev/design/1_future/`:

- **Curator Input Scalability** — traces the 12-point materialization chain for
  curator operations and frames three levels of laziness
- **Data Plane Separation** — the design philosophy: stay columnar inside the
  framework, materialize to objects only at the user boundary
- **Filter Chunked Dispatch** — Level 1 fix: chunk filter inputs, dispatch each
  chunk to a short-lived subprocess
- **Lazy Execution Units** — carry query descriptors instead of materialized IDs
  across the dispatch pipeline

It also maps where these threads intersect with other in-flight and planned
work.

---

## Thread 1: Eager materialization is the root cost

All four docs trace the same structural problem: artifact IDs are materialized
as Python strings far earlier than needed, then carried through redundant
representations.

The curator scalability doc counts **12 materialization points** where the same
data gets re-represented. The lazy execution units doc shows the same pattern on
the creator dispatch path. Data plane separation names the core antipattern: the
inflate-deflate round-trip (`list[str]` -> `list[Artifact]` -> extract
`.artifact_id` -> `list[str]`).

**Concrete costs at 22.2M artifacts:**

| Representation | Size |
|----------------|------|
| Arrow Series (columnar) | ~710 MB |
| Python `list[str]` | ~1.4 GB |
| Pickle byte blob | ~930 MB |
| Unpickled Python strings (child) | ~1.4 GB |
| `list[dict]` execution edges | ~8.3 GB |

The gap between the Arrow representation (~710 MB) and the Python object
overhead (~1.4 GB) is pure waste. The gap between a query descriptor (~200
bytes) and the materialized result (~1.4 GB) is the full optimization target.

---

## Thread 2: Stay columnar inside the framework

Data plane separation states the principle most directly: **the Artifact layer
is a membrane at the boundary, not a universal internal currency.**

This principle shows up concretely in each doc:

- **Curator scalability:** `resolve_output_reference` should return Arrow Series,
  not Python lists. `build_execution_edges` should build column-lists, not
  `list[dict]`.
- **Filter chunked dispatch:** each subprocess works with bounded Arrow
  DataFrames, never inflating passthrough IDs to Artifact objects.
- **Lazy execution units:** `InputRef.collect_series()` returns `pl.Series`
  (~2.5x cheaper than `.collect_ids()` returning `list[str]`).

Data plane separation also identifies three distinct computational shapes for
curator operations — ID routing (Merge), columnar evaluation (Filter), and
object transformation (Ingest). Each shape has different data needs, but the
current executor forces all three through the same materialization funnel. The
principle: **operations declare what they need, the framework provides exactly
that.**

---

## Thread 3: Pass queries, not data

Three docs converge on the idea that the message between parent and child
processes should be a **query descriptor**, not materialized data:

- **Curator scalability:** introduces `InputRef` — a dataclass carrying
  `delta_root`, `source_step`, `role`, and `execution_run_ids`. Serializes to
  ~200 bytes instead of ~930 MB.
- **Lazy execution units:** elevates `InputRef` and `LazyBatchRef` to
  first-class schema types and defines three options for `ExecutionUnit`
  integration (union type, separate model, resolution at the boundary).
- **Filter chunked dispatch:** doesn't use lazy refs (it's Level 1), but its
  chunking naturally shrinks the per-subprocess payload from ~930 MB to ~32 MB —
  a partial step toward the same goal.

The enabling insight: **the same query always produces the same IDs**, so
hashing the query descriptor is semantically equivalent to hashing the IDs
themselves. This unlocks lazy spec-ID computation and eliminates the
materialization-for-hashing bottleneck.

---

## Thread 4: Bound memory via subprocess lifecycle

The curator scalability doc identifies jemalloc fragmentation as a 3x inflation
factor (15-20 GB logical -> ~60 GB RSS). The fix in filter chunked dispatch:
**short-lived subprocesses that exit after each chunk**, so the OS reclaims the
full address space.

This pattern also eliminates the serialization boundary cost: instead of one
~930 MB pickle, each chunk pickles ~32 MB. And it eliminates the recording
bottleneck: instead of one 22.2M-row execution edge DataFrame, each chunk
writes ~1M rows.

The lazy execution units doc takes this further: with `LazyBatchRef`, the
subprocess resolves its own partition from Delta Lake — **zero artifact ID data
crosses the process boundary**.

---

## Thread 5: Different operations need different execution paths

Data plane separation identifies three computational shapes. Filter chunked
dispatch is specific to one shape (columnar evaluation). Lazy execution units
distinguishes paired (group_by) vs. unpaired operations — only unpaired
operations can be fully lazy.

The docs collectively reject the "one executor path fits all" design. The
executor should inspect the operation type and choose the minimal execution
path, rather than running every operation through the lowest-common-denominator
materialization.

---

## The shared roadmap

All four docs describe layers of the same incremental improvement path:

| Level | Description | Parent memory | Child memory | Interface changes |
|-------|-------------|--------------|--------------|-------------------|
| **Current** | Eager everything | ~4-5 GB | ~60 GB RSS | — |
| **Level 1** | Chunk + subprocess exit | ~1.4 GB | ~500 MB | None |
| **Level 2** | Arrow in parent, chunk | ~400 MB | ~500 MB | Minor (return types) |
| **Level 3** | Lazy refs, worker-resolved | ~0 | bounded/partition | `InputRef`, split dispatch |

Filter chunked dispatch is Level 1. The Phase 1 items from lazy execution units
(Arrow Series returns, generator batches, bulk cache lookup) bridge to Level 2.
Full `InputRef`/`LazyBatchRef` adoption is Level 3.

Each level is independently shippable. Each level reduces a different bottleneck.
No level requires the next one to deliver value.

---

## Intersections with other work

### Composable operations (`0_current/composable-operations.md`)

Composable operations chain multiple operations on a single worker, streaming
artifacts in-memory between them. This intersects in two ways:

- **Data plane separation applies directly.** Intermediate artifacts between
  chained operations are the clearest case of "data that no user code touches" —
  they should stay columnar or in-memory, never round-tripping through the
  Artifact layer.
- **Lazy execution units change what gets dispatched to the worker.** If the
  execution unit carries a query descriptor instead of materialized IDs, the
  worker resolves its own inputs from Delta Lake. A composable operation chain
  would resolve inputs once for the first operation and stream outputs to the
  next — the lazy ref only needs to cover the chain's entry point.

These are complementary, not conflicting. Composable operations reduce
inter-step I/O (Delta Lake round-trips between tightly coupled steps). Lazy
execution units reduce intra-step I/O (materializing IDs in the parent just to
serialize them to the worker). Both attack unnecessary materialization, but at
different boundaries.

### Bulk cache lookup (`2_future_future/bulk_cache_lookup.md`)

Lazy execution units explicitly lists bulk cache lookup as a **Phase 1
prerequisite**. The current per-unit cache scan (N individual Delta scans) is
the dominant wall-clock bottleneck at scale (20+ minutes at 11M artifacts).
Batching all spec-ID lookups into a single `is_in()` scan eliminates this.

Without bulk cache lookup, lazy dispatch still pays the per-unit scan cost.
With it, the entire cache check phase drops from minutes to seconds.

### Filter criteria resolution (`2_future_future/filter_criteria_resolution.md`)

Filter chunked dispatch replaces the backward provenance walk
(`_match_explicit`) with single-hop `get_descendant_ids_df`. Filter criteria
resolution redesigns the criteria UX to eliminate the implicit/explicit split
entirely.

Both simplify the filter, but at different layers: chunked dispatch simplifies
the **execution path** (how metrics are resolved at runtime), while criteria
resolution simplifies the **user interface** (how criteria are specified). They
can be implemented independently, but both converge on the same architectural
direction: direct parent-to-metric lookups replace provenance graph walks.

### Intra-allocation orchestration (`analysis/intra-allocation-orchestration.md`)

Intra-allocation orchestration distributes work within a single large SLURM
allocation (head node pattern). The serialization boundary concerns from curator
scalability apply directly: if the head node dispatches work to co-allocated
workers, the cost of pickling 930 MB of IDs to each worker is multiplied by
the number of concurrent dispatches.

Lazy execution units would make intra-allocation dispatch nearly free — the
"message" to each worker is a ~200-byte query descriptor, and each worker reads
its partition from the shared filesystem.

### Parallel step execution (`2_future_future/parallel_step_execution.md`)

Parallel step execution enables independent pipeline steps to run concurrently.
This is orthogonal to the scalability work (which focuses on within-step memory
and I/O), but there's a resource interaction: if two large steps run in
parallel, their combined parent memory doubles. Lazy execution units (Level 3)
would keep parent memory near zero regardless of how many steps run
concurrently, making parallel step execution safer at scale.

### Pipeline replay (`2_future_future/pipeline_replay.md`)

Pipeline replay depends on spec-ID semantics for cache invalidation. The lazy
execution units doc proposes **query-derived spec IDs** (hashing the query
descriptor instead of artifact IDs). This changes spec-ID semantics — a
transition run would not find cache hits from previous eager runs. Pipeline
replay's cache invalidation design should account for this transition: if the
spec-ID scheme changes, "keep steps 0-1, redo from step 2" needs to work
across both old-style and new-style spec IDs.

---

## Summary: what to think about

The four scalability docs are facets of one idea: **the framework should route
queries, not data, and only materialize at the point of consumption.** They
differ in scope (curators vs. creators, parent vs. child, philosophy vs.
implementation) but share the same diagnosis and the same cure.

The key decisions that ripple across multiple docs:

- **When to introduce `InputRef` as a first-class type** — this is the single
  change that unlocks Level 3 across both creator and curator paths, but it
  touches `ExecutionUnit`, validation, spec-ID computation, and the recording
  path.
- **Whether spec IDs are derived from artifact IDs or query descriptors** — this
  affects caching semantics, pipeline replay, and the transition cost.
- **Whether the executor dispatches on operation type** — data plane separation
  argues yes; this would simplify filter chunked dispatch (filter-specific path)
  and composable operations (chain-aware path) while keeping the common case
  clean.

The incremental roadmap (Level 1 -> 2 -> 3) means these decisions don't all
need to be made at once. But understanding the full picture helps avoid
designing Level 1 solutions that make Level 3 harder.
