# Storage and Delta Lake

Artisan stores artifact content, provenance, and execution history in Delta
Lake tables. Workers stage isolated outputs; the orchestrator commits them and
records when the complete result is safe to read.

---

## The store format is explicit

Every supported Delta root contains `_artisan/store.json` with the exact format
contract:

```json
{"store_format":7,"artifact_identity":1,"cache_identity":2}
```

Format 7 uses `file_ref` for complete external files and requires
operation-declared lineage. Earlier stores, including format 6, and their sealed
staging are rejected before cache reuse or recovery. Deploy updated workers
and tool endpoints with the release. Cache reuse and replay preserve explicit
edges; authors must still bump operation versions when changing lineage
semantics.

Writers publish this manifest only after initializing an empty root. Readers
validate it before opening framework state. A missing, malformed, older, newer,
or partially matching manifest fails closed. Start a new root for this release;
there is no migration, fallback reader, or manifest-edit upgrade. Keep an older
root with its matching Artisan version. Opening it does not migrate or clean it.

---

## Why Delta Lake

Delta Lake stores columnar Parquet files with a transaction log, allowing local
pipelines to persist queryable results without running a database service.
Embedded artifact content lives in table rows; large external bytes stay at
verified locations recorded separately.

Each table write is atomic. A pipeline result spans several tables, so Artisan
adds a logical commit and completion checks to coordinate their visibility.
Delta Lake's per-table transactions alone do not make the whole step atomic.

Content and execution tables are partitioned by origin step number. The
completion-aware scan remains lazy, so query filters can reach the Delta scan.
Higher-level readers may collect data to assemble results, and run-scoped reads
may span several origin steps. See [Reading pipeline results](#reading-pipeline-results).

---

## Table architecture

The store separates artifact content, provenance, and orchestration history:

```
delta_root/
├── _artisan/
│   ├── store.json          Exact store and identity format contract
│   ├── commit_plans/       Immutable evidence for logical commits
│   └── worker_logs/        Provider logs keyed by execution run ID
├── artifacts/              Content and metadata for every artifact
│   ├── index/              Type and origin lookup (artifact_id → type)
│   ├── locations/          Verified artifact identity → URI mappings
│   ├── metrics/            Metric values (built-in)
│   ├── configs/            Execution configuration snapshots (built-in)
│   ├── data/               Generic tabular data (built-in)
│   ├── file_refs/          Complete external files (built-in)
│   ├── appendables/        Appendable JSONL records (built-in)
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

The `index` table maps each `artifact_id` to its type and origin step number.
This locates an artifact's content table without scanning every artifact type.

The index has one row per artifact without content bytes. It also supplies type
and step maps for provenance graphs.

External locations form a separate global relation keyed by `(artifact_id,
uri)`. Content tables keep digest and size descriptors but do not own paths.
This separation lets verified external bytes move or gain a replica without
changing the artifact ID, and prevents first-writer location loss during
deduplication.

### The provenance store

Provenance queries (ancestor/descendant lookups, edge loading, type maps, step
maps) are handled by a dedicated `ProvenanceStore` class. `ArtifactStore`
exposes the `ProvenanceStore` through a `provenance` accessor rather than
mixing graph traversal with artifact content access. Provenance queries can
traverse the edge tables without loading artifact payloads.

---

## Registry-driven extensibility

An application registers a type by defining its artifact model and type
definition. The type participates in staging, content-addressed commitment,
provenance, and cache reuse without modifying framework storage code.

Registration happens when the defining module is imported. Install the
application and select its trusted modules in every process that opens its
store. Startup and repair check required registrations before changing selected
evidence. See [Load application definitions](../how-to-guides/configuring-execution.md#load-application-definitions-in-every-process)
and [Create Artifact Types](../how-to-guides/creating-artifact-types.md).

## The staging-commit pattern

Staging separates concurrent worker output from shared table writes.

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

Each staging directory contains multiple Parquet files. The worker closes and
flushes its payloads, then atomically publishes `executions.parquet` last. This
file seals the execution: it contains the outcome and an inventory of the other
files, including their sizes and content digests. The orchestrator verifies that
inventory before accepting the result. A missing output file cannot be mistaken
for a successful execution with no outputs.

Once sealed, the shard is immutable. Provider logs that arrive later are stored
separately under `_artisan/worker_logs/` in the Delta root; use
[`inspect_worker_log()`](../how-to-guides/debugging-executions.md#inspect-provider-logs)
to read them. An unsealed shard remains incomplete evidence.

### Why not write directly to Delta Lake?

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
Workers flush payload files and directory metadata before publishing the seal
on all local filesystems. On shared filesystems, the orchestrator also refreshes
directory listings and polls for readable `executions.parquet` seals.

These checks reduce the chance of treating a delayed file as missing. Inventory
and commit-plan verification still check the actual staged evidence.

---

## Commit ordering

A commit plan records the exact staged files and expected rows for a write
spanning several Delta tables. After a crash, it identifies completed and
remaining writes. Artisan creates the plan automatically.

A plan alone does not prove that data was committed. The orchestrator applies
the present tables in this order:

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

The writer verifies the planned effects before recording completion. That
completion marker is the visibility boundary: supported Artisan readers expose
rows from completed commits and exclude planned or abandoned effects. They do
not repeat the historical commit audit on every query. A crash can leave some
physical table writes behind without making them accepted pipeline results.

Write order does not replace a multi-table transaction. Each Delta write is
atomic independently; Artisan's completion checks coordinate visibility across
them. Raw Delta readers bypass those checks and may expose unfinished effects.
Pending and running step snapshots remain observable while work is in progress.
See [Crash recovery](storage-and-delta-lake.md#storage-crash-recovery) for handling incomplete plans.

---

## Deduplication during commit

Content addressing enables automatic deduplication at commit time. The
committer compares rows with the same artifact ID and reuses matching content
from completed commits. Different content or semantic metadata under the same
ID is an integrity error.

Reproducing an identical artifact adds no content row; the new execution still
records its own attempt and provenance. Deduplication requires no configuration.

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

## Staging preservation

Uncommitted staging is retained after cancellation, failure, or interruption.
With the default `preserve_staging=False`, Artisan removes only files that match
a completed commit plan after verifying all of its Delta effects. It leaves
unlisted or changed files in place. Cancelling a step is never proof that its
staged work was committed.

Set `preserve_staging=True` to keep those files even after successful commitment,
for example to investigate a worker result. This controls staging retention;
`preserve_working` independently controls the worker's temporary working files.
Preservation applies to the current action. A later recovery or repair with
preservation off verifies the corresponding committed batch before cleaning its
retained files. Immediate cleanup uses the verification from the commit that
just finished.

(storage-crash-recovery)=
## Crash recovery

By default, `PipelineManager.create()` and `resume()` recover staging before any
cache lookup or new dispatch. Recovery first retries eligible recorded commit
plans. It then validates sealed successful executions that never reached plan
creation, groups them by their original step attempt, and commits each group
through the ordinary batch writer. Retrying recovery does not duplicate
executions or artifacts.

For example, ninety execution units finish before a hundred-unit step is
cancelled. A new run can recover and reuse those ninety units, then execute the
ten missing units. Those ninety units form one recovery batch. If recovery
itself stops between table writes, the next attempt finishes that same batch.
The original step remains cancelled and exposes no accepted outputs. The new
step records its reuse of the original executions and exposes outputs when its
own result commits. Recovery does not turn a failed or
cancelled step into a whole-step cache hit.

The two controls answer separate questions:

| Setting | Effect |
| --- | --- |
| `recover_staging=True` (default) | Recover eligible finished work before cache lookup. |
| `recover_staging=False` | Leave earlier staging untouched during startup. |
| `preserve_staging=True` | Retain staging after verified commitment, including recovery. |
| `preserve_staging=False` (default) | Permit cleanup of verified committed files. |

All combinations retain uncommitted evidence. Cache policy still governs
whole-step hits. `skip_cache=True` can force new execution even when startup has
recovered earlier results, and non-cacheable operations remain non-cacheable.

Incomplete, failed, diagnostic, and unknown-owner shards stay in place and are
reported. Corrupt or conflicting sealed evidence stops startup with an integrity
error. Recovery does not import partial output files or guess ownership. Existing
plans retain ownership of their files, including abandoned plans. Completed
batches need no further payload verification during startup unless retained
staging needs cleanup. New worker results discovered later form a new batch.

Only one orchestrator or repair process may write a store at a time. Confirm
that the previous driver has stopped before starting recovery; a persisted
`running` status cannot establish this. Workers may still finish their isolated
shards, but seals published after the startup scan wait until a later recovery
pass. Recovery does not reconnect to active jobs or resume an unfinished
execution.

To reuse completed units after interruption, rerun the pipeline script with
`create()` and the same roots. `resume()` restores a previous run's accepted
state; standalone recovery does not resolve an old `pending` or `running` step,
so normal resume checks still apply.

Use [explicit store repair](../how-to-guides/configuring-execution.md#recovering-from-crashes)
to audit historical effects and inspect retained evidence, or apply recovery
separately. Read-only inspection checks the recorded plans against the stored
tables. Applied recovery validates its affected work and reports the actions it
performed; it does not audit unrelated completed data. Abandonment records an
operator decision without deleting evidence.

---

## Compaction and maintenance

Repeated appends create small Parquet files. Queries slow down as they need to
open more files.

**Compaction** merges small files into larger ones. It can optionally apply
Z-ORDER clustering, which co-locates rows with similar values in key columns
(e.g., `artifact_id` for content tables, `execution_spec_id` for the
executions table). Z-ORDER improves predicate pushdown performance for queries
that filter on those columns.

**Vacuum** removes stale data files that are no longer referenced by the Delta
transaction log. The default retention period is 7 days. Choose retention long enough for your
readers and recovery needs; older table versions can lose their data files after
vacuum.

Compaction can be scoped to a single partition (step number) or applied across
the entire table. Vacuum always operates on the whole table.

---

## Compression

Worker staging and orchestrator Delta writes use zstd compression.

---

## Caching at the execution level

The `executions` table stores execution cache entries. On a hit, the orchestrator
records a `cache_reuse` link from the current step attempt to each accepted
execution. It skips that work without copying execution or artifact rows.

Run-scoped readers combine directly owned executions with these reuse links
and follow their output edges. They select the newest attempt at each logical
step, so a failed newer attempt does not expose an older success. Select an
explicit `step_run_id` for historical evidence. Artifact origin step numbers
remain unchanged. There is no cache service, TTL, or manual invalidation.

For the full two-level caching mechanism (step-level and execution-level), see
[Execution Flow](execution-flow.md#two-level-caching).

---

(reading-pipeline-results)=
## Reading pipeline results

Use Artisan's inspection helpers, `ArtifactStore`, and `ProvenanceStore` to read
accepted results. They apply logical-completion filtering and validate artifact
content when it is loaded. For a store shared by several runs, select the
`pipeline_run_id` when inspecting run results. A cached artifact retains its
original content row and origin step; the current run's execution and cache-reuse
relations identify where it was used.

The committed scan stays lazy, so filters can be applied to the Delta scan
before collecting rows. Some higher-level readers collect data to assemble
results or traverse provenance. Partition pruning depends on the query; a
run-scoped read may need artifacts originating in several steps.

Ordinary queries trust recorded completion. They do not verify every historical
plan or detect every later alteration to stored rows. Use read-only
[store repair inspection](../how-to-guides/configuring-execution.md#recovering-from-crashes)
when you need a historical integrity audit.

Delta-compatible tools remain useful for physical inspection. Direct
`pl.scan_delta(...)`, DuckDB, or other raw queries bypass Artisan's completion
filter and artifact validation. Their rows are not necessarily accepted
results, especially after an interrupted commit. Per-table version history also
does not reconstruct a complete historical run or its external files.

See [Inspect Pipeline Results and Provenance](../how-to-guides/inspecting-provenance.md)
for supported readers and run selection, and
[Exploring Results](../tutorials/01-getting-started/02-exploring-results.ipynb)
for an interactive example.

---

## Cross-references

- [Artifacts and Content Addressing](artifacts-and-content-addressing.md) --
  Content hashing, deduplication, and the draft/finalize lifecycle
- [Execution Flow](execution-flow.md) -- Dispatch, execution, and step tracking
