# Design Principles

Artisan's architecture is shaped by a small set of design principles. These
are not aspirational statements — they are constraints that guided concrete
decisions throughout the framework. Understanding them helps you predict how the
system behaves, why the APIs look the way they do, and where to look when
something surprises you.

---

## The problem space

Computational research pipelines share a pattern of failure modes:

- **Lost provenance.** Tracing a result back to its inputs means reconstructing
  a chain of scripts, log files, and ad hoc naming conventions. One missing link
  breaks the chain.
- **Scattered results.** Output files live on cluster nodes, local disks, and
  shared filesystems with no central index. Finding what ran and what it produced
  requires archaeology.
- **Fragile glue code.** Pipelines assembled from shell scripts break when
  moving between environments, when tool versions change, or when someone
  renames a directory.
- **Infrastructure as a tax.** Researchers spend more time managing job
  schedulers, file formats, and filesystem edge cases than doing the research
  the pipeline exists to support.
- **Silent data corruption.** Concurrent writes, missing fsync calls, and
  partial failures corrupt shared state in ways that are difficult to detect
  and impossible to recover from.

Artisan exists to make these problems structurally impossible, not merely
unlikely. Each principle below targets one or more of these failure modes.

---

## Content is identity

Every artifact is identified by the hash of its content
(`artifact_id = xxh3_128(content)`). The ID *is* the data. There is no
separate registry mapping names to values, no auto-incrementing counter, no
UUID that could accidentally refer to different content on two machines.

**What this buys you:**

- **Automatic deduplication.** Same content, same ID — stored once regardless
  of how many pipeline steps produce it.
- **Deterministic caching.** Cache keys are derived from content hashes of
  inputs plus operation parameters. No manual invalidation. Different inputs
  produce different keys automatically.
- **Immutability by construction.** Changing an artifact's content changes its
  ID, which means the original artifact still exists. You cannot silently
  overwrite prior results.

**The trade-off:** Content-addressed storage makes updates expensive. You don't
"edit" an artifact — you create a new one. This is intentional. In research
pipelines, the ability to trace exactly what was computed outweighs the cost of
storing a new copy.

**See:** [Artifacts and Content Addressing](artifacts-and-content-addressing.md)
for how hashing, draft/finalize, and artifact types work in practice.

---

## Provenance is always captured, never reconstructed

The framework records two complementary provenance graphs at execution time:
one for execution edges (which computation consumed and produced which
artifacts), one for artifact edges (which input artifact produced which output
artifact). Neither can be derived from the other, and neither is optional.

**Why capture at execution time?** Because the context needed to establish
derivation edges — filename stems, pairing order, explicit declarations — is
available only during execution. Attempting to reconstruct lineage after the
fact is brittle and unreliable.

**The trade-off:** Capturing provenance adds work to every operation. The
framework handles most of it automatically via filename stem matching, but
operations with non-standard naming need explicit declarations. The cost of
annotating lineage is far lower than the cost of not having it.

**See:** [Provenance System](provenance-system.md) for the dual system, stem
matching algorithm, and lineage declaration.

---

## Creator operations are pure computation

Creator operations — the primary operation type — know nothing about
orchestration, scheduling, storage, or infrastructure. They receive data in,
produce data out. All coordination — dispatching to workers, managing sandboxes,
staging results, committing to storage — is handled by the layers above.

**Why this matters:**

- **Testable in isolation.** You can unit test a creator by constructing inputs
  directly, without running a pipeline or connecting to storage.
- **Portable.** The same operation runs unchanged on a laptop with a process
  pool or on a cluster with SLURM.
- **Composable.** Operations can be combined freely because they have no hidden
  dependencies on each other or on global state.

**What this means concretely:** A creator operation cannot read from or write to
Delta Lake. It cannot check the cache. It cannot dispatch sub-tasks. It cannot
access the pipeline definition. It declares its inputs and outputs through
specs, and the framework provides everything it needs through its lifecycle
methods.

**The exception — curator operations:** Curators are a second operation type
that intentionally breaks this boundary. They receive an `ArtifactStore` and
can query or filter across the full artifact collection. This is a deliberate
trade-off: curators need storage access to perform collection-level operations
like filtering, merging, and ingestion. The purity guarantee applies to
creators, which make up the majority of pipeline operations.

**The trade-off:** Operations cannot make infrastructure decisions. An
operation that wants to "run this part on GPU and that part on CPU" cannot
express this — resource allocation is declared statically in the operation's
`resources` config. This keeps operations simple at the cost of dynamic
scheduling flexibility.

**See:** [Operations Model](operations-model.md) for the two operation types,
three-phase lifecycle, and spec system.

---

## Scale is transparent

The same code path runs for one artifact on a laptop or ten thousand artifacts
on an HPC cluster. Only the compute backend changes — from `LOCAL` (process
pool) to `SLURM` (cluster job submission). The backend interface is extensible,
so additional backends (cloud, Kubernetes) can be added without changing
operations, execution logic, provenance capture, or storage commits.

**Why this principle exists:** Research pipelines start as local prototypes
and grow to cluster-scale production. If different execution environments use
different code paths, bugs hide in the gap. "Works on my machine" becomes
"fails on the cluster" and vice versa. A pipeline that behaves differently
depending on where it runs is a pipeline you cannot trust.

**How the framework enforces it:** The orchestrator-worker split defines a
clean boundary. The orchestrator dispatches `ExecutionUnit` objects — sealed
packages containing everything a worker needs. Workers execute them identically
regardless of whether they are processes in a local pool or SLURM jobs on
remote nodes. The staging-commit pattern ensures that results are collected the
same way in all cases.

**The trade-off:** Uniformity means the local execution path carries some
overhead (sandbox directories, staged Parquet files) that a simple local-only
system would skip. This overhead is negligible in practice because operations
dominate runtime, but it means "run this one thing quickly" still goes through
the full lifecycle.

**See:** [Execution Flow](execution-flow.md) for the dispatch-execute-commit
phases and [Architecture Overview](architecture-overview.md) for the
orchestrator-worker split.

---

## Shared state is never mutated concurrently

Workers never write to Delta Lake directly. Instead, each worker writes
results to an isolated staging directory, and the orchestrator commits them
after all workers complete. Each Delta table is committed independently in a
fixed order (content tables first, then index, then provenance edges, then
executions) to minimize referential integrity issues on partial failure. No
optimistic concurrency, no write conflicts.

**Why this principle exists:** Pipelines run thousands of concurrent workers.
If each worker wrote directly to shared tables, write conflicts would be
frequent and data corruption would be a matter of time.

**The trade-off:** Serialized commits mean all results for a step must be
collected before any are committed. You cannot query intermediate results
while a step is still running. This is the cost of consistency: you always
see a complete, correct snapshot or nothing at all.

**See:** [Storage and Delta Lake](storage-and-delta-lake.md#the-staging-commit-pattern)
for the full staging-commit pattern.

---

## Layers depend downward only

The framework is organized into five layers — schemas, operations, storage,
execution, orchestration — with strict downward-only dependencies. Each layer
can be used without the ones above it.

```
orchestration  →  execution  →  operations  →  schemas
                      ↓
                   storage   →  schemas
```

**Why layering matters:**

- **Testing.** You can test operations without orchestration, execution without
  SLURM, and schemas without anything else.
- **Change isolation.** Modifying the orchestration layer cannot break
  operations. Adding a new artifact type (schemas) does not require changes
  to execution or orchestration.
- **Flexible composition.** Use the execution layer directly for one-off
  computation. Use operations with a different orchestrator. Each layer is a
  stable building block.

**The trade-off:** Strict layering occasionally means information cannot flow
where it would be convenient. An operation cannot check whether it is running
in a cached context, and the execution layer cannot influence dispatch
decisions. These restrictions keep the architecture clean at the cost of
some workarounds in specific scenarios.

**See:** [Architecture Overview](architecture-overview.md) for the full layer
diagram and dependency table.

---

## Fail fast, fail loud

The framework validates as much as possible before execution begins: input
types, spec compatibility, parameter schemas, step wiring. When an error does
occur at runtime, it propagates immediately with context — which operation,
which artifact, which phase — rather than being swallowed or deferred.

**Why this matters:** In a pipeline that takes hours to run, discovering an
error in step 8 that could have been caught at step 1 is a waste of compute
time and researcher attention. Early validation turns runtime surprises into
immediate, actionable errors.

**Examples of fail-fast behavior:**

- Input type mismatches caught at pipeline construction time
- Empty `infer_lineage_from = {}` rejected (ambiguous intent)
- Operations that implement neither `execute` nor `execute_curator` raise an
  error at class definition time
- Unrecognized resource configuration keys rejected before dispatch

**The trade-off:** Strict validation can be frustrating during exploration,
when you want to sketch a pipeline before all the pieces are ready. The
framework prioritizes correctness over flexibility in this regard.

**See:** [Error Handling](error-handling.md) for the error propagation model
and the fail-fast philosophy in detail.

---

## Principles in tension

These principles occasionally pull in different directions. When they do,
the framework resolves the tension by prioritizing in this order:

- **Correctness** (provenance, content addressing, serialized commits)
- **Reproducibility** (deterministic caching, immutable artifacts)
- **Simplicity** (pure operations, layered architecture)
- **Performance** (batching, caching, scale transparency)

An example: content-addressed storage means the framework hashes every artifact,
which has a cost. But this cost is paid to guarantee that cache keys are correct
(correctness) and that identical results are never recomputed (reproducibility).
Performance is optimized within the constraints of correctness, not at its
expense.

---

## Cross-references

- [Architecture Overview](architecture-overview.md) — System structure and the five layers
- [Operations Model](operations-model.md) — Two operation types and the three-phase lifecycle
- [Artifacts and Content Addressing](artifacts-and-content-addressing.md) — Immutable data and content hashing
- [Provenance System](provenance-system.md) — Dual provenance tracking and lineage declaration
- [Execution Flow](execution-flow.md) — Dispatch, execute, commit in detail
- [Storage and Delta Lake](storage-and-delta-lake.md) — Persistence and the staging-commit pattern
- [Error Handling](error-handling.md) — Fail-fast philosophy and error propagation
- [First Pipeline Tutorial](../tutorials/getting-started/01-first-pipeline.ipynb) — See these principles in action
