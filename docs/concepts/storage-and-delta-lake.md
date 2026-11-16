# Storage and Delta Lake

Artisan stores artifact content, provenance, and execution history in Delta
Lake tables. Workers stage isolated outputs; the orchestrator commits them and
records when the complete result is safe to read.

---

## The store format is explicit

Every supported Delta root contains `_artisan/store.json` with the exact domain
contract:

```json
{"domain":"artisan.retained-store","schema_version":1}
```

This domain records retained output occurrences, their lineage and associations,
explicit input snapshots, and acceptance decisions. Content-addressed artifacts
can deduplicate while distinct output occurrences keep their original identities.
The built-in CSV type uses `artifacts/csvs`; complete external files use `file_ref`.

Initialization claims an empty root and creates every required table. Opening a
store validates the manifest and all registered physical schemas without creating
missing tables. A manifest by itself is not evidence of a complete store.
Historical numeric formats, unlabeled nonempty roots, and unknown versions reject
without modification. There is no automatic migration or manifest-edit upgrade.
Use fresh Delta and staging roots; inspect older stores with their matching Artisan
version before exporting data for a deliberate import.

---

## Why Delta Lake

Delta Lake stores columnar Parquet files with a transaction log, allowing local
pipelines to persist queryable results without running a database service.
Embedded artifact content lives in table rows; large external bytes stay at
verified locations recorded separately.

Each table write is atomic. A pipeline result spans several tables, so Artisan
adds a logical commit and completion checks to coordinate their visibility.
Delta Lake's per-table transactions alone do not make the whole step atomic.

Artifact origin is recorded independently of current-run membership. A read may
need content produced by several earlier steps, including accepted cache
sources. See [Reading pipeline results](#reading-pipeline-results).

---

## Table architecture

The store separates artifact content, provenance, and orchestration history:

```
delta_root/
├── _artisan/
│   ├── store.json          Exact store and identity format contract
│   ├── ...                Immutable admissions, seals, and commit plans
│   └── worker_logs/        Provider logs keyed by execution run ID
├── artifacts/              Content and metadata for every artifact
│   ├── index/              Type and origin lookup (artifact_id → type)
│   ├── locations/          Verified artifact identity → URI mappings
│   ├── metrics/            Metric values (built-in)
│   ├── configs/            Execution configuration snapshots (built-in)
│   ├── csvs/               CSV content (built-in)
│   ├── file_refs/          Complete external files (built-in)
│   ├── appendables/        Appendable JSONL records (built-in)
│   └── [custom_type]/      Domain types registered at runtime
├── provenance/             Derivation and execution relationships
│   ├── assignment_decisions/ Admission, winner, cancellation, and reuse decisions
│   ├── retained_*/         Ordered occurrences, roles, relations, and frozen context
│   ├── artifact_edges/     Source → target derivation edges
│   └── execution_edges/    Input/output edges per execution
└── orchestration/          Execution history and step state
    ├── executions/         Operation execution log
    ├── cache_reuse/        Current step → reused execution links
    ├── steps/              Step-level state transitions
    └── logical_commits/    Exact plans and verified completion
```

### Why three groups

**Artifact tables** store content and identity. A content row's
`origin_step_number` records its original owner, not every later use.

**Provenance tables** store ordered occurrences, exact producer attribution,
direct relations, collapsed ancestry, and frozen dependency context. Content
deduplication does not collapse these occurrences. The curator path also records
its artifact and execution edges.

**Orchestration tables** record executions, run membership, cache reuse, step
state, and logical commit completion. Shared content and result membership have
different keys because the same bytes can appear in several accepted results.

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

Workers never write to shared Delta tables. Each stages a complete result in its
own delivery directory. The controller verifies the selected delivery against
its admission and records a winner before publishing its table effects. A
terminal step plan selects accepted unit results; this is separate from worker
packing and from each unit's completion.

```
Creator worker
    └─ delivery directory
        ├─ retained content and occurrence tables
        ├─ required external files
        └─ retained-result.json (published last)
                     │
Controller: verify admission and seal → claim winner → apply exact plan
                     │
              verify effects → complete → select in step result
```

Distinct deliveries have distinct identities and directories. A retry cannot
overwrite an earlier delivery, and a complete directory cannot grant itself
permission to become a pipeline result.

### Why staging directories are sharded

Curator staging and operation scratch paths use a two-level hash shard: the first
two and next two characters of the execution run ID become directory levels
(`{run_id[0:2]}/{run_id[2:4]}/`). This prevents creating a single directory
with thousands of subdirectories, which would degrade filesystem performance on
both local and networked filesystems.

### The sentinel file

The creator's `retained-result.json` seal binds its delivery identity, output
projection, Parquet inventories, and external file digests. Curators publish
their sealed `executions.parquet` record last. The controller checks the
appropriate complete inventory; it never treats a missing output file as a
successful execution with no outputs.

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
Publishing a seal must follow completed payload writes. Providers must make the
configured staging backend available to both worker and controller. A visible
directory or successful scheduler exit does not establish complete transfer:
inventory and commit-plan verification check the actual bytes before acceptance.

---

## Memory and local scratch space

Persistence processes payloads in bounded batches. Workers write successive
Parquet row groups; the orchestrator captures verified staging on local disk,
compares planned keys, and reads only the relevant historical content. It writes
destination files sequentially and adds all files for one table effect in one
Delta transaction. Recovery and explicit repair use the same bounded readers.
A small deposit therefore does not load the whole historical content table.

The orchestrator needs writable local temporary storage even when the Delta and
staging roots use S3. Allow space for captured staging, canonical comparison
data, and a destination file being written. Binary content expands when written
to the canonical comparison spool, so scratch can exceed the compressed staging
size substantially. Exhausting scratch fails the operation and retains durable
staging for inspection or retry. Artisan does not fall back to collecting the
payload in memory.

Memory still includes planned keys, file metadata, lineage, the largest
individual artifact or metadata row, and the Parquet decoder's page and
dictionary buffers. A legacy file with a very large page or dictionary can
therefore require more memory than the normal batch allowance. Generic
operations may also retain their complete input or output collections before
recording begins.

`IngestCSV` converts and stages one complete CSV at a time, releasing prior
content before converting the next file. Each CSV still needs to fit in worker
memory, including parsing and serialization overhead. Importing many CSVs no
longer requires keeping all converted CSVs in memory together. See
[ingesting external files](../how-to-guides/building-a-pipeline.md#ingest-external-files)
for the execution and failure behavior.

---

## Commit ordering

A commit plan records the exact staged files and expected rows for a write
spanning several Delta tables. After a crash, it identifies completed and
remaining writes. Artisan creates the plan automatically.

A plan alone does not grant acceptance or prove that data was committed. The
creator result path follows this sequence:

```text
Validate exact admission, output contract, seal, and files
        ↓
Record winning delivery (before any recovery can accept it)
        ↓
Seal exact effects; publish verified artifact content and external files
        ↓
Write occurrence membership, relations, and dependency context
        ↓
Verify effects and mark this result complete
        ↓
Publish terminal step selection, including any accepted cache sources
```

The writer verifies the planned effects before recording completion. That
completion is part of the visibility proof: supported readers also require the
selected admission, owner, and result membership to agree. A crash can leave
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
Occurrence and relation keys retain producer attribution. Exporting `[B, A, B]`
therefore preserves three ordered positions even when A and B have identical
content IDs.

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

Recovery finishes results that the controller already selected for acceptance.
A worker seal proves that a staged result is complete; it does not authorize
acceptance on its own. The controller first records the winning delivery for an
exact execution unit, then publishes its table effects and completion record.

If the controller stops after that claim, resume or offline repair can finish
the same result. A second delivery cannot replace it or supply different table
rows. Repeated recovery converges on the same accepted membership. If cancellation
wins before a claim, a later worker result remains unaccepted, even if sealed.

`PipelineManager.resume()` can recover the selected run's recorded winners and
restore its accepted state. Creating a new pipeline does not scan orphan staging
and claim arbitrary prior output. Already accepted results may still be reused
under ordinary cache rules. Recovery does not restart scientific computation,
reconnect to a scheduler job, or infer that a recorded running worker has stopped.

Unclaimed, incomplete, or uncertain evidence remains available for diagnosis.
`preserve_staging=True` keeps staged files after successful publication; it does
not make them acceptance authority. Diagnostic replay attempts cannot become
ordinary cache entries or supply ordinary recovery results.

Only one controller or offline repair process may mutate a store at a time.
Confirm that the previous writer has stopped before recovery. Use
[explicit store repair](../how-to-guides/configuring-execution.md#recovering-from-crashes)
to inspect evidence read-only, then apply eligible recovery deliberately. The
supported repair path does not relabel old stores or fabricate missing decisions.

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

Maintenance must preserve the evidence and physical schemas used by accepted
results and recovery. Stop writers and retain the versions needed by active
readers before performing Delta-level maintenance.

---

## Compression

Worker staging and orchestrator Delta writes use zstd compression.

---

## Caching at the execution level

Cache lookup binds the admitted recipe, resolved software, exact input semantics,
and original accepted producer. On a hit, the controller records the selected
source result for the current attempt. It skips that work without creating a
new scientific producer or copying artifact rows.

Run-scoped readers resolve directly accepted and reused result membership.
They select the newest attempt at each logical
step, so a failed newer attempt does not expose an older success. Select an
explicit `step_run_id` for historical evidence. Artifact origin step numbers
remain unchanged. There is no cache service, TTL, or manual invalidation.

For the full two-level caching mechanism (step-level and execution-level), see
[Execution Flow](execution-flow.md#two-level-caching).

---

(reading-pipeline-results)=
## Reading pipeline results

Use Artisan's inspection helpers, `ArtifactStore`, and `ProvenanceStore` to read
accepted results. They validate selected result ownership, completion, membership,
and artifact content. For a store shared by several runs, select the
`pipeline_run_id` when inspecting run results. A cached artifact retains its
original content row and origin step; the current run's execution and cache-reuse
relations identify where it was used.

Readers capture the evidence needed for the selected result and may collect
metadata to assemble ordered outputs or traverse provenance. They do not audit
every unrelated historical result. Use read-only
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
