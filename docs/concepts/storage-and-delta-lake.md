# Storage and Delta Lake

A pipeline that loses results to a crashed worker, silently stores duplicate
data, or requires an external database to track what it produced is a pipeline
you cannot trust at scale. The storage layer exists to make these failures
structurally impossible — not through careful coding discipline, but through
architectural choices that eliminate entire categories of problems.

This page explains why the framework uses Delta Lake as its persistence
backbone, how the staging-commit pattern keeps shared state safe under
concurrent execution, and how the artifact type registry makes the storage
layer extensible without framework modifications.

---

## The problem storage solves

Computational pipelines on HPC clusters face a specific set of storage
challenges that general-purpose solutions handle poorly:

**Concurrent writes from many workers.** A pipeline step may dispatch thousands
of SLURM jobs. If each worker writes directly to shared state, write conflicts
and partial corruption are inevitable.

**No database services.** HPC clusters provide shared filesystems, not managed
database instances. A storage solution that requires PostgreSQL, Redis, or any
long-running service is impractical in this environment.

**Filesystem hygiene.** Creating millions of small files in a single directory
degrades filesystem performance and violates HPC best practices. The storage
system must control file proliferation at the architecture level.

**NFS consistency gaps.** Shared filesystems like NFS do not guarantee that a
file written by one node is immediately visible to another. The storage layer
must handle this explicitly, not hope for the best.

**Partial failure recovery.** When 3 of 1,000 workers fail, the 997 successful
results must be preserved. Rolling back everything because of a few failures
wastes hours of compute time.

---

## Why Delta Lake

Delta Lake provides transactional storage over Parquet files on a regular
filesystem. No external services, no connection strings, no processes to keep
alive. The Delta Lake library reads and writes Parquet files with a transaction
log that provides ACID guarantees.

Four properties make it the right choice for this problem:

**Atomic transactions.** Each commit to a Delta Lake table is all-or-nothing.
The orchestrator commits results from all workers in a single transaction per
table. If the commit succeeds, all results are visible. If it fails, none are.
There is no intermediate state where some results are visible and others are
not.

**Columnar storage.** Artifact content, metrics, and configuration data are
stored directly in Delta Lake columns rather than as separate files. A pipeline
that produces 50,000 metric artifacts stores them as rows in a single table,
not as 50,000 JSON files. This enforces filesystem hygiene at the architecture
level — the framework cannot accidentally create a directory with a million
loose files.

**Partition pruning.** Tables are partitioned by pipeline step number. When you
query results from step 3, only the Parquet files for step 3 are read. This
makes queries fast even when a pipeline has produced millions of artifacts
across dozens of steps.

**Ecosystem compatibility.** Delta Lake tables are Parquet files with a
transaction log. You can query them with Polars, DuckDB, pandas, or any
Delta-compatible tool. Pipeline results are not locked inside a proprietary
format.

---

## Table architecture

The storage layer organizes data into three groups of tables, each serving a
distinct purpose:

```
delta_root/
├── artifacts/              Content and metadata for every artifact
│   ├── index/              Type and origin lookup (artifact_id → type)
│   ├── metrics/            Metric values
│   ├── configs/            Computation specifications
│   ├── data/               Generic tabular data
│   ├── file_refs/          External file references
│   └── [domain types]/     Registered by domain layers (e.g., custom_type)
├── provenance/             Derivation and execution relationships
│   ├── artifact_edges/     Source → target derivation edges
│   └── execution_edges/    Input/output edges per execution
└── orchestration/          Execution history and step state
    ├── executions/         Operation execution log
    └── steps/              Step-level state transitions
```

### Why three groups

The grouping reflects how data is written and read:

**Artifact tables** are written during commit after every pipeline step. They
grow with each step. Queries against them are almost always filtered by step
number, so content tables are partitioned by `origin_step_number` for fast
predicate pushdown.

**Provenance tables** are also written during commit, but are queried
differently — typically by artifact ID rather than step number. They are not
partitioned, because provenance queries need to traverse across steps.

**Orchestration tables** track execution metadata. The `executions` table is
partitioned by step number (queries are step-scoped). The `steps` table is
written directly by the orchestrator (not through staging) and is not
partitioned.

### The artifact index

The `index` table deserves special attention. It maps every `artifact_id` to
its type and origin step number. This allows the framework to locate an
artifact without scanning every content table — given an ID, the index reveals
which type-specific table contains the actual data.

The index is not partitioned. It is small (one row per artifact, no content
bytes) and must support fast lookups across all steps.

---

## Registry-driven extensibility

The framework defines four built-in artifact types (metric, config, data,
file_ref). But the table architecture is not hardcoded to these four. New
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
complete, the orchestrator reads the staged files and commits them atomically
to Delta Lake.

```
                              staging_root/
                              ├── 1_ToolB/
Worker A ──writes──>          │   ├── a1/b2/{run_abc}/
Worker B ──writes──>          │   │   ├── custom_type.parquet
Worker C ──writes──>          │   │   ├── metrics.parquet
                              │   │   ├── index.parquet
                              │   │   ├── artifact_edges.parquet
                              │   │   ├── execution_edges.parquet
                              │   │   └── executions.parquet  ← sentinel
                              │   ├── c3/d4/{run_def}/
                              │   │   └── ...
                              │   └── e5/f6/{run_ghi}/
                              │       └── ...
                              │
Orchestrator ──reads all──>   └── commit atomically ──> Delta Lake
```

### Why staging directories are sharded

Worker staging paths use a two-level hash shard: the first two and next two
characters of the execution run ID become directory levels
(`{hash[0:2]}/{hash[2:4]}/`). This prevents creating a single directory with
thousands of subdirectories, which would degrade filesystem performance on both
local and networked filesystems.

### The sentinel file

Each staging directory contains multiple Parquet files (one per table type).
The `executions.parquet` file is always written last. Its presence signals that
all other files in the directory are complete and consistent. The orchestrator
uses this as the signal that a worker's results are ready for commit.

### Why not write directly to Delta Lake?

Three reasons:

**Concurrency.** Delta Lake uses optimistic concurrency control. If 1,000
workers attempt concurrent writes to the same table, most would encounter
transaction conflicts and need to retry. With staging, there are zero write
conflicts — each worker writes to its own directory.

**Partial failure isolation.** If a worker crashes, its staging directory is
simply ignored during commit. The orchestrator has a complete view of which
workers succeeded and which failed before committing anything. There is no need
to roll back partially-written data.

**Consistency checks.** The orchestrator can validate staged data before
committing — checking for duplicates, verifying referential integrity, and
ensuring all expected results are present. Direct worker writes would bypass
these checks.

---

## NFS consistency

When workers run on SLURM cluster nodes and the staging directory lives on a
shared NFS filesystem, a write-then-read race condition exists: a worker writes
a file, but the orchestrator (running on a different node) may not see it
immediately due to NFS caching.

The framework handles this with a three-part strategy:

**Writer-side fsync.** After writing all staging files, SLURM workers call
`fsync()` on each file and its containing directory. This forces the NFS client
to flush data to the server.

**Reader-side directory cache invalidation.** Before checking for a staging
file, the orchestrator walks the ancestor directories of the expected path and
calls `listdir()` on each one. This forces READDIR RPCs that flush stale NFS
directory entry caches, ensuring newly created directories become visible.

**Reader-side file verification.** The orchestrator deterministically computes
where each worker's staging files should be and polls for the sentinel file
(`executions.parquet`) with exponential backoff. The polling uses `open()` +
`read(1)` rather than `os.path.exists()`, because the open-read pattern
triggers NFS close-to-open consistency guarantees that stat-based checks do not.

This is not an optimization — it is a correctness requirement. Without these
measures, the orchestrator could miss worker results that were successfully
written but not yet visible through NFS caching.

---

## Commit ordering

The orchestrator commits tables in a specific order designed to maintain
referential integrity even if a crash occurs mid-commit:

1. **Artifact content tables** (data, metrics, configs, etc.)
2. **Artifact index** (maps IDs to types)
3. **Provenance tables** (artifact edges, execution edges)
4. **Execution records** (execution log)

**Why this order:** Content exists before its index entry. Index entries exist
before provenance edges reference them. If a crash interrupts the commit
sequence, the database is in a consistent (if incomplete) state — there are
never dangling references pointing to content that does not exist.

Delta Lake does not support multi-table transactions. Each table commit is
atomic individually, but the sequence across tables is not. The commit ordering
is the mitigation: it ensures that partial commits degrade gracefully rather
than creating inconsistencies.

---

## Deduplication during commit

Content addressing enables automatic deduplication at commit time. Before
writing staged artifacts to a Delta Lake table, the committer checks which
artifact IDs already exist in the table and drops duplicates via an anti-join.

This means:

- If two workers produce identical output, only one copy is stored
- Re-running a pipeline step that produces the same results adds zero new rows
- Deduplication requires no configuration — it is a structural consequence of
  content-addressed identity

The deduplication check is a single Polars scan of the existing table's
`artifact_id` column, joined against the incoming staged data. The cost is
proportional to the number of existing artifacts, not the total data volume,
because only the ID column is read.

---

## Caching at the execution level

The `executions` table doubles as the cache store. Before dispatching work,
the orchestrator computes a deterministic cache key from content-addressed
artifact IDs and checks this table for a prior successful execution. A hit
skips dispatch, staging, and commit entirely. There is no separate cache
service, no TTL management, and no manual invalidation.

For the full two-level caching mechanism (step-level and execution-level), see
[Execution Flow](execution-flow.md#two-level-caching).

---

## Reading pipeline results

Because Delta Lake tables are Parquet files with a transaction log, you can
query pipeline results with any Delta-compatible tool. The most common approach
is Polars with lazy scanning, which uses partition pruning to read only the
data you need.

This interoperability is intentional. Pipeline results are not locked inside
the framework — they are accessible to any data tool that reads Parquet. You
can build dashboards, run ad hoc analyses, or feed results into other systems
without going through the framework's API.

For hands-on examples of querying pipeline results, see the
[Exploring Results Tutorial](../tutorials/getting-started/02-exploring-results.ipynb).

---

## Key design decisions

| Decision | Rationale |
|----------|-----------|
| Delta Lake over a database | No external services required on HPC clusters |
| Content in columns, not files | Prevents filesystem bloat from millions of small files |
| Staging before commit | Eliminates concurrent write conflicts and enables partial failure recovery |
| Sharded staging directories | Prevents single-directory performance degradation |
| Sentinel file pattern | Enables reliable completion detection over NFS |
| Registry-driven tables | Domain layers extend storage without framework changes |
| Ordered table commits | Maintains referential integrity without multi-table transactions |
| Partition by step number | Enables fast predicate pushdown for step-scoped queries |

---

## Cross-References

- [Artifacts and Content Addressing](artifacts-and-content-addressing.md) --
  Content hashing, deduplication, and the draft/finalize lifecycle
- [Execution Flow](execution-flow.md) -- How staging fits into the
  dispatch-execute-commit lifecycle
- [Design Principles](design-principles.md) -- Foundational decisions that
  shaped the storage architecture
- [Exploring Results Tutorial](../tutorials/getting-started/02-exploring-results.ipynb) -- Query Delta Lake tables and inspect artifacts interactively
- [Creating Artifact Types](../how-to-guides/creating-artifact-types.md) --
  Step-by-step guide to registering new artifact types
