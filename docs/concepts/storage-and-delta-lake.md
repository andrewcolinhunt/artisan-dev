# Storage and Delta Lake

Artisan stores artifact content, provenance, and execution history in Delta
Lake tables. Workers stage isolated outputs; the orchestrator commits them and
records when the complete result is safe to read.

This page explains the table layout, that visibility boundary, and what remains
after an interrupted commit. These distinctions matter when inspecting results
or recovering a store.

---

## The store format is explicit

Every supported Delta root contains `_artisan/store.json` with the exact format
contract:

```json
{"store_format":3,"artifact_identity":1,"cache_identity":2}
```

Writers publish this manifest only after initializing an empty root. Readers
validate it before opening framework state. A missing, malformed, older, newer,
or partially matching manifest fails closed. This clean release boundary avoids
silently reading rows under the wrong identity or cache semantics.

---

## Why Delta Lake

Delta Lake stores columnar Parquet files with a transaction log, allowing local
pipelines to persist queryable results without running a database service.
Embedded artifact content lives in table rows; large external bytes stay at
verified locations recorded separately.

Each table write is atomic. A pipeline result spans several tables, so Artisan
adds a logical commit and completion checks to coordinate their visibility.
Delta Lake's per-table transactions alone do not make the whole step atomic.

Content and execution tables are partitioned by origin step number. That is a
physical layout choice, not a read-cost guarantee: current supported readers
load and verify the physical table before applying later filters. See
[Reading pipeline results](#reading-pipeline-results).

---

## Table architecture

The storage layer organizes data into three groups of tables, each serving a
distinct purpose:

```
delta_root/
├── _artisan/
│   └── store.json          Exact store and identity format contract
├── artifacts/              Content and metadata for every artifact
│   ├── index/              Type and origin lookup (artifact_id → type)
│   ├── locations/          Verified artifact identity → URI mappings
│   ├── metrics/            Metric values (built-in)
│   ├── configs/            Execution configuration snapshots (built-in)
│   ├── data/               Generic tabular data (built-in)
│   ├── file_refs/          External file references (built-in)
│   ├── appendables/        Appendable JSONL records (built-in)
│   ├── large_files/        Large external files (built-in)
│   └── [custom_type]/      Domain types registered at runtime
├── provenance/             Derivation and execution relationships
│   ├── artifact_edges/     Source → target derivation edges
│   └── execution_edges/    Input/output edges per execution
└── orchestration/          Execution history and step state
    ├── executions/         Operation execution log
    ├── cache_reuse/        Current step → reused execution links
    ├── steps/              Step-level state transitions
    └── logical_commits/    Planned, complete, or abandoned commit state
```

### Why three groups

**Artifact tables** store content and identity. Content tables are partitioned
by `origin_step_number`; the global index and location relations are not.

**Provenance tables** store directed derivation edges and execution inputs and
outputs. They are unpartitioned because traversal crosses step boundaries.

**Orchestration tables** record executions, run membership, cache reuse, step
state, and logical commit completion. Executions are partitioned by origin step
number. Steps and the other control relations are unpartitioned.

The orchestrator writes pending and running step snapshots directly. A terminal
snapshot that accepts persisted results is staged with those results and becomes
visible through their logical commit.

### The artifact index

The `index` table deserves special attention. It maps every `artifact_id` to
its type and origin step number. This allows the framework to locate an
artifact without scanning every content table — given an ID, the index reveals
which type-specific table contains the actual data.

The index is small (one row per artifact, no content bytes) and must support
fast lookups across all steps. It also powers bulk queries like loading type
maps and step maps for provenance graph rendering.

External locations form a separate global relation keyed by `(artifact_id,
uri)`. Content tables keep digest and size descriptors but do not own paths.
This separation lets verified external bytes move or gain a replica without
changing the artifact ID, and prevents first-writer location loss during
deduplication.

### The provenance store

Provenance queries (ancestor/descendant lookups, edge loading, type maps, step
maps) are handled by a dedicated `ProvenanceStore` class. `ArtifactStore`
exposes the `ProvenanceStore` through a `provenance` accessor rather than
mixing graph queries into artifact content queries, keeping the two cleanly
separated. This split means provenance
queries never need access to artifact content tables, and content queries never
need to load the edge graph.

---

## Registry-driven extensibility

The framework defines six built-in artifact types (metric, config, data,
file_ref, appendable, large_file). But the table architecture is not hardcoded
to these six. New
artifact types are added by defining two classes: an artifact model (the data
shape) and a type definition (the registry entry). Registration is automatic —
Python's `__init_subclass__` mechanism detects the new type definition and
registers it.

Once registered, a new type automatically gets:

- Its own Delta Lake table under `artifacts/`
- Content-addressed deduplication during commit
- Staging and commit integration
- Provenance tracking
- Cache-aware execution

No framework code is modified. No configuration files are edited. The domain
layer defines the type, and the storage infrastructure extends to cover it.

**Why this matters:** The framework has zero knowledge of domain-specific data.
A domain layer can register custom artifact types (e.g., `CustomArtifact`,
`UserDefinedArtifact`). Each gets the full storage infrastructure for free.

---

## The staging-commit pattern

This is the most important architectural pattern in the storage layer. It
solves the concurrent-write problem by separating worker output from shared
state.

### The core idea

Workers never write to Delta Lake tables. Instead, each worker writes its
results as Parquet files to an isolated staging directory. After all workers
complete, the orchestrator verifies the staged files and includes them in an
immutable logical commit plan. It also stages the terminal step snapshot.

```
                              staging_root/
                              ├── 1_tool_b/
Worker A ──writes──>          │   ├── ab/cd/{run_id_abcd...}/
Worker B ──writes──>          │   │   ├── data.parquet
Worker C ──writes──>          │   │   ├── metrics.parquet
                              │   │   ├── index.parquet
                              │   │   ├── artifact_edges.parquet
                              │   │   ├── execution_edges.parquet
                              │   │   └── executions.parquet  ← sentinel
                              │   ├── e1/f2/{run_id_e1f2...}/
                              │   │   └── ...
                              │   └── 7a/3b/{run_id_7a3b...}/
                              │       └── ...
                              │
Orchestrator ──verifies──>    └── seal plan → write tables → mark complete
```

The step directory name combines the step number and operation name
(e.g., `1_tool_b`). Beneath it, each execution's staging files are isolated in
a sharded subdirectory keyed by the execution run ID.

### Why staging directories are sharded

Worker staging paths use a two-level hash shard: the first two and next two
characters of the execution run ID become directory levels
(`{run_id[0:2]}/{run_id[2:4]}/`). This prevents creating a single directory
with thousands of subdirectories, which would degrade filesystem performance on
both local and networked filesystems.

### The sentinel file

Each staging directory contains multiple Parquet files (one per table type).
The `executions.parquet` file is always written last. Its presence signals that
all other files in the directory are complete and consistent. The orchestrator
uses this as the signal that a worker's results are ready for commit.

### Why not write directly to Delta Lake?

Three reasons:

**Concurrency.** Delta Lake uses optimistic concurrency control. Isolating
worker staging avoids workers competing to update the same shared tables;
the orchestrator coordinates those writes.

**Partial failure isolation.** Incomplete worker staging is not accepted as a
successful result. Finished sibling work and staged failure records can still
be persisted according to the step's failure policy.

**Consistency checks.** The orchestrator can validate staged data before
committing — checking for duplicates, verifying referential integrity, and
ensuring all expected results are present. Direct worker writes would bypass
these checks.

---

## NFS consistency

Shared filesystems can delay visibility between a worker and the orchestrator.
Shared-filesystem workers flush staged files and their directories. The
orchestrator refreshes directory listings and polls for readable
`executions.parquet` sentinels before committing.

These checks reduce the chance of treating a delayed file as missing; the
commit plan still verifies the actual staged evidence. Local runners skip the
shared-filesystem verification path.

---

## Commit ordering

A logical commit records an immutable plan of the staged evidence and expected
table effects. The orchestrator applies the present tables in this order:

```text
Artifact content tables
        ↓
Artifact index → Artifact locations
        ↓
Executions → Execution edges → Artifact edges
        ↓
Cache reuse links
        ↓
Terminal step snapshot
        ↓
Logical commit marked complete
```

The final completion marker is the visibility boundary. Supported Artisan
readers exclude rows owned by planned or abandoned commits and validate the
expected effects of completed commits. A crash can leave some physical table
writes behind without making them accepted pipeline results.

Write order does not replace a multi-table transaction. Each Delta write is
atomic independently; Artisan's completion checks coordinate visibility across
them. Raw Delta readers bypass those checks and may expose unfinished effects.
Pending and running step snapshots remain observable while work is in progress.
See [Crash recovery](#crash-recovery) for handling incomplete plans.

---

## Deduplication during commit

Content addressing enables automatic deduplication at commit time. The
committer compares rows with the same artifact ID and reuses matching content
from completed commits. Different content or semantic metadata under the same
ID is an integrity error.

This means:

- If two workers produce identical output, only one copy is stored
- Reproducing identical artifacts adds no new content rows; a new execution
  still records its own attempt and provenance
- Deduplication requires no configuration — it is a structural consequence of
  content-addressed identity

The content row and artifact index retain the first committed
`origin_step_number`, even when later steps produce or import the same artifact.
That origin is checked against its owning commit. Current-run membership comes
from execution and cache-reuse relations, so reusing an artifact does not move
its original row.

Content tables and the artifact index deduplicate by artifact ID. Locations
deduplicate by `(artifact_id, uri)`, preserving additional verified locations.
Provenance edges retain their execution-scoped keys so separate executions
remain distinguishable.

---

## Crash recovery

If the orchestrator crashes during persistence, the immutable commit plan,
control row, staged files, and any partial table effects remain as evidence.
Rows owned by that plan stay invisible until its completion marker is written.

Inspect the store explicitly with `artisan store repair --delta-root ...
--staging-root ...`. Report mode never mutates the roots. `--apply` replays only
validated plans through the normal idempotent commit path; explicit
`--abandon ID --reason ...` records a one-way operator decision without
deleting evidence.

---

## Compaction and maintenance

Repeated appends to Delta Lake tables create many small Parquet files — one per
commit. Over time this degrades read performance because each query must open
many files.

The framework provides two maintenance operations:

**Compaction** merges small files into larger ones. It can optionally apply
Z-ORDER clustering, which co-locates rows with similar values in key columns
(e.g., `artifact_id` for content tables, `execution_spec_id` for the
executions table). Z-ORDER improves predicate pushdown performance for queries
that filter on those columns.

**Vacuum** removes stale data files that are no longer referenced by the Delta
transaction log. The default retention period is 7 days, ensuring that
concurrent readers are not affected by cleanup.

Compaction can be scoped to a single partition (step number) or applied across
the entire table. Vacuum always operates on the whole table.

---

## Compression

All Parquet files — both staged files written by workers and Delta Lake commits
written by the orchestrator — use zstd compression. Zstd provides a good
balance of compression ratio and read/write speed, which matters when a
pipeline produces large volumes of artifact data that will be scanned
repeatedly during provenance queries and result analysis.

---

## Caching at the execution level

The `executions` table doubles as the cache store. Before dispatching work,
the orchestrator computes a deterministic cache key from content-addressed
artifact IDs and checks this table for a prior successful execution. A hit
skips worker dispatch. It does not create another execution or copy artifact
rows. Instead, the orchestrator commits one row to `cache_reuse` linking the
current step attempt to each existing execution whose result it accepted.

That two-column relation keeps pipeline runs isolated even when they share
step numbers or artifacts. A run-scoped reader starts from the run's own step
IDs, unions directly owned executions with the linked cached executions, and
then follows their execution edges to outputs. `origin_step_number` remains
the artifact's original production location; it is not used to reconstruct
which later runs reused the artifact. There is no cache service, TTL, or
manual invalidation.

For the full two-level caching mechanism (step-level and execution-level), see
[Execution Flow](execution-flow.md#two-level-caching).

---

## Reading pipeline results

Use Artisan's inspection helpers, `ArtifactStore`, and `ProvenanceStore` to read
accepted results. They apply logical-completion filtering and integrity checks.
For a store shared by several runs, select the `pipeline_run_id` when inspecting
run results. A cached artifact retains its original content row and origin step;
the current run's execution and cache-reuse relations identify where it was used.

These readers currently load and verify a physical table eagerly. Even when an
API returns a Polars `LazyFrame`, subsequent filters operate on that loaded
result and do not push predicates into the original Delta scan. Partitioning
therefore does not promise that a step-scoped read touches only that step's
files.

Delta-compatible tools remain useful for physical inspection. Direct
`pl.scan_delta(...)`, DuckDB, or other raw queries bypass Artisan's completion
filter and integrity validation. Their rows are not necessarily accepted
results, especially after an interrupted commit. Per-table version history also
does not reconstruct a complete historical run or its external files.

See [Inspect Pipeline Results and Provenance](../how-to-guides/inspecting-provenance.md)
for supported readers and run selection, and
[Exploring Results](../tutorials/01-getting-started/02-exploring-results.ipynb)
for an interactive example.

---

## Key design decisions

| Decision | Rationale |
|----------|-----------|
| Delta Lake over a database | No external services required on HPC clusters |
| Content in columns, not files | Prevents filesystem bloat from millions of small files |
| Staging before commit | Isolates worker output and preserves evidence for recovery |
| Sharded staging directories | Prevents single-directory performance degradation |
| Sentinel file pattern | Enables reliable completion detection over NFS |
| Registry-driven tables | Domain layers extend storage without framework changes |
| Exact store manifest | Prevents cross-version identity and cache misreads |
| Separate artifact locations | Keeps external availability independent of identity |
| Logical completion and verified reads | Keeps incomplete multi-table effects out of accepted results |
| Partition by origin step number | Organizes content and execution files; current readers still verify eagerly |
| Separate provenance store | Keeps graph queries independent of artifact content |
| Zstd compression everywhere | Good compression ratio with fast read/write performance |
| Conditional fsync | NFS flush only on shared filesystems, avoiding local overhead |

---

## Cross-references

- [Artifacts and Content Addressing](artifacts-and-content-addressing.md) --
  Content hashing, deduplication, and the draft/finalize lifecycle
- [Execution Flow](execution-flow.md) -- How staging fits into the
  dispatch-execute-commit lifecycle
- [Design Principles](design-principles.md) -- Foundational decisions that
  shaped the storage architecture
- [Exploring Results Tutorial](../tutorials/01-getting-started/02-exploring-results.ipynb) -- Query Delta Lake tables and inspect artifacts interactively
- [Creating Artifact Types](../how-to-guides/creating-artifact-types.md) --
  Step-by-step guide to registering new artifact types
