# Glossary

Key terms used throughout the Artisan documentation.

---

(glossary-acid)=
## ACID

Atomicity, Consistency, Isolation, Durability -- the four properties that
guarantee reliable database transactions. Delta Lake provides ACID transactions
over Parquet files, ensuring that concurrent worker writes and partial failures
never corrupt the shared artifact store.

---

(glossary-artifact)=
## Artifact

An immutable, content-addressed data node identified by the `xxh3_128` hash of
its content. Artifacts are the fundamental data units flowing through pipelines.
Once finalized, an artifact's ID is a permanent commitment to its exact content.
Artifacts follow a [draft/finalize](#glossary-draft-finalize) lifecycle: they
are created as mutable drafts and become immutable when finalized. The four
built-in artifact types are `data`, `metric`, `file_ref`, and `config`.

---

(glossary-artifact-store)=
## ArtifactStore

The central interface for reading artifacts and provenance from Delta Lake
tables. Provides methods such as `get_artifact()` for retrieving artifacts by
content-addressed ID, with optional [hydration](#glossary-hydration) control.
Lazily initializes a `ProvenanceStore` for graph queries.

---

(glossary-artifact-type-def)=
## ArtifactTypeDef

A registry entry describing an artifact type. Each concrete subclass declares a
`key` (e.g. `"data"`), a `table_path` for its Delta Lake table, and a `model`
class with serialization methods (`to_row`, `from_row`, `POLARS_SCHEMA`).
Registration is automatic at class definition time via `__init_subclass__`.

---

(glossary-backend)=
## Backend

An execution backend that controls where and how workers run.
Artisan ships with three backends: `local` (ProcessPool on the orchestrator
machine), `slurm` (job array submission via submitit on HPC clusters), and
`slurm_intra` (srun dispatch within an existing SLURM allocation). Each
backend defines `WorkerTraits` (worker-side I/O behavior) and
`OrchestratorTraits` (post-dispatch behavior on the orchestrator).

---

(glossary-batching)=
## Batching

The two-level strategy for dividing work across workers. Level 1
(`artifacts_per_unit`) controls how many artifacts go into each
[execution unit](#glossary-execution-unit). Level 2 (`units_per_worker`)
controls how many execution units are sent to each worker process. Both are
configured via [ExecutionConfig](#glossary-execution-config) on the operation.

---

(glossary-cache-policy)=
## CachePolicy

Controls when a completed step qualifies as a cache hit on subsequent pipeline
runs. `ALL_SUCCEEDED` (default) requires zero execution failures.
`STEP_COMPLETED` accepts any completed step, regardless of individual failures.
Infrastructure errors (dispatch or commit failures) always block caching under
both policies.

---

(glossary-composite)=
## Composite

A reusable composition of operations with declared inputs and outputs. Defined
by subclassing `CompositeDefinition` and implementing `compose()`. Can run
**collapsed** (`pipeline.run()` — single step, in-memory artifact passing) or
**expanded** (`pipeline.expand()` — each internal operation becomes its own
pipeline step). The `intermediates` setting controls whether intermediate
artifacts are discarded, persisted, or exposed.

---

(glossary-content-addressing)=
## Content addressing

A storage strategy where data is identified by the hash of its content rather
than by location or name. In Artisan, `artifact_id = xxh3_128(content)`,
meaning identical content always produces the same ID. This enables automatic
deduplication and deterministic caching.

---

(glossary-creator-operation)=
## Creator operation

An [operation](#glossary-operation) subclass that runs heavy computation
(external tools, GPU work) through a three-phase lifecycle: `preprocess`,
`execute`, `postprocess`. Creators produce new artifacts from inputs, run in
worker processes via a [backend](#glossary-backend), and execute inside an
isolated [sandbox](#glossary-sandbox) directory. See
[Operations Model](../concepts/operations-model.md).

---

(glossary-curator-operation)=
## Curator operation

A lightweight [operation](#glossary-operation) subclass that routes, filters,
or merges artifacts without heavy computation. Curators implement a single
`execute_curator()` method and run in-process on the orchestrator, with no
worker dispatch or sandboxing. See
[Operations Model](../concepts/operations-model.md).

---

(glossary-delta-lake)=
## Delta Lake

An open-source storage layer that brings ACID transactions to Parquet files.
Artisan uses Delta Lake as its backing store for all artifacts, provenance, and
execution records. It provides transactional writes, time travel, and partition
pruning without requiring an external database server.

---

(glossary-draft-finalize)=
## Draft/finalize

The artifact lifecycle pattern. A **draft** artifact has `artifact_id=None` and
is mutable -- created via `Subclass.draft()`. Calling `artifact.finalize()`
computes the content hash, sets the `artifact_id`, and makes the artifact
semantically immutable. Operations create drafts in `postprocess` and the
framework finalizes them before committing to storage.

---

(glossary-execution-config)=
## ExecutionConfig

Per-operation configuration controlling how work is divided and distributed.
Fields include `artifacts_per_unit`, `units_per_worker`, `max_workers`, and
`estimated_seconds` (used for scheduler hints such as SLURM time limits). Set
as the `execution` attribute on an [OperationDefinition](#glossary-operation-definition).

---

(glossary-execution-context)=
## ExecutionContext

An immutable (frozen) dataclass carrying all runtime state for a single
execution: run ID, spec ID, step number, worker ID, artifact store reference,
staging root, and sandbox path. Created once at execution start and threaded
through the entire execution flow.

---

(glossary-execution-record)=
## ExecutionRecord

A row in the executions Delta Lake table logging a single execution attempt.
Carries dual identity: `execution_spec_id` (deterministic cache key, computed
from operation name, input artifact IDs, and merged parameters) and
`execution_run_id` (unique per attempt, used for provenance edges).

---

(glossary-execution-unit)=
## Execution unit

The work package dispatched to a worker. An `ExecutionUnit` carries a fully
configured operation instance, a batch of input artifact IDs keyed by
[role](#glossary-role), the cache key, and the step number. The number of
artifacts per unit is controlled by `execution.artifacts_per_unit`.

---

(glossary-failure-policy)=
## FailurePolicy

Controls how the framework handles individual execution failures within a step.
`CONTINUE` (default) logs failures and commits successful items, reporting
failure counts in [StepResult](#glossary-step-result). `FAIL_FAST` stops on the
first failure and raises an exception with no commit.

---

(glossary-group-by-strategy)=
## GroupByStrategy

Strategy for pairing artifacts from multiple input
[roles](#glossary-role) before dispatch. `ZIP` matches by position (first with
first). `LINEAGE` matches artifacts sharing a common ancestor. `CROSS_PRODUCT`
generates all combinations. Set via `OperationDefinition.group_by`.

---

(glossary-hydration)=
## Hydration

The process of loading an artifact's full content from storage. `hydrate=True`
(default) loads all fields including content bytes. `hydrate=False` loads only
the artifact ID and type, which is sufficient for passthrough operations like
Filter and Merge that route artifacts without reading their content.

---

(glossary-input-spec)=
## InputSpec

A declarative specification on an operation class that describes one named
input: its artifact type, whether it is required, and how it should be
delivered. Key options include `materialize` (write to disk vs. pass in memory),
`hydrate` (full content vs. ID-only), and `with_associated` (auto-resolve
related artifacts via provenance). Defined in the operation's `inputs` class
variable keyed by [role](#glossary-role) name.

---

(glossary-lineage-mapping)=
## LineageMapping

An explicit declaration of a parent-child relationship between an input artifact
and an output draft. Used in `ArtifactResult.lineage` when the default
[stem matching](#glossary-stem-matching) inference is not appropriate. Each
mapping specifies the draft's `draft_original_name`, the source
`source_artifact_id`, and the `source_role`.

---

(glossary-nfs)=
## NFS

Network File System, a distributed filesystem protocol common on HPC clusters.
When workers run on different cluster nodes, Artisan calls `fsync()` on staged
files to ensure NFS close-to-open consistency before the orchestrator reads them.

---

(glossary-operation)=
## Operation

A Python class that consumes artifacts and produces artifacts. Operations
declare typed input and output specifications and are either
[creators](#glossary-creator-operation) (heavy computation with a three-phase
lifecycle) or [curators](#glossary-curator-operation) (lightweight routing).
See [Operations Model](../concepts/operations-model.md).

---

(glossary-operation-definition)=
## OperationDefinition

The Pydantic base class for all pipeline operations. Subclasses declare
`inputs`, `outputs`, `name`, and lifecycle methods. The framework validates
subclass declarations at class definition time (role enums, lineage
configuration, method implementations) and registers the operation by name.

---

(glossary-output-reference)=
## OutputReference

A lightweight reference to a previous step's output, created by
`step_result.output("role")` or `step_future.output("role")`. Used to wire
steps together without moving data. The framework resolves references to
concrete artifact IDs when the downstream step executes.

---

(glossary-output-spec)=
## OutputSpec

A declarative specification on an operation class that describes one named
output: its artifact type, and lineage inference strategy
(`infer_lineage_from`). Lineage can point to input roles
(`{"inputs": ["data"]}`), output roles (`{"outputs": ["data"]}`), or declare
no parents (`{"inputs": []}`). Defined in the operation's `outputs` class
variable keyed by [role](#glossary-role) name.

---

(glossary-parquet)=
## Parquet

A columnar file format optimized for analytical queries. Delta Lake tables are
composed of Parquet files. Artisan stages worker results as Parquet files before
committing them atomically to Delta Lake.

---

(glossary-pipeline)=
## Pipeline

A directed acyclic graph (DAG) of [steps](#glossary-step) managed by
[PipelineManager](#glossary-pipeline-manager). Steps are added by calling
`pipeline.run()` (blocking) or `pipeline.submit()` (non-blocking) and wired
together via [output references](#glossary-output-reference).

---

(glossary-pipeline-manager)=
## PipelineManager

The main user-facing interface for defining and executing pipelines. Provides
`run()` (blocking step execution), `submit()` (non-blocking, returns a
[StepFuture](#glossary-step-future)), `expand()` (expand a
[composite](#glossary-composite) into separate steps), and `output()` (reference
a step's outputs). Configured with a `PipelineConfig`
specifying the Delta Lake root, staging root, failure policy, and cache policy.

---

(glossary-provenance)=
## Provenance

The record of where data came from and how it was produced. Artisan maintains
dual provenance: *execution provenance* (what computation happened, stored as
`ExecutionEdge` records) and *artifact provenance* (which specific input
produced which specific output, stored as `ArtifactProvenanceEdge` records).
See [Provenance System](../concepts/provenance-system.md).

---

(glossary-resource-config)=
## ResourceConfig

Portable hardware resource requirements declared on an operation. Specifies
`cpus`, `memory_gb`, `gpus`, `time_limit`, and an `extra` dict for
backend-specific settings (e.g. `{"partition": "gpu"}`). Each
[backend](#glossary-backend) translates these to its native format.

---

(glossary-role)=
## Role

A named port on an operation's input or output interface. Each role maps to an
[InputSpec](#glossary-input-spec) or [OutputSpec](#glossary-output-spec) and
carries artifacts of a declared type. Roles are the unit of wiring between
steps: `step_result.output("data")` creates an
[OutputReference](#glossary-output-reference) for the `"data"` role. Operations
must define matching `InputRole` and `OutputRole` `StrEnum` classes.

---

(glossary-sandbox)=
## Sandbox

An isolated directory tree created for each
[creator operation](#glossary-creator-operation) execution. Contains three
subdirectories:
`preprocess/` (input materialization), `execute/` (operation writes output
files here), and `postprocess/` (draft artifact construction). The sandbox is
cleaned up after execution unless `preserve_working=True` is set on the
pipeline config.

---

(glossary-staging)=
## Staging

The intermediate write area where workers write Parquet files instead of
committing directly to Delta Lake. This avoids transaction conflicts on shared
filesystems. After all workers in a step complete, the orchestrator commits
staged files atomically via `DeltaCommitter`. On [NFS](#glossary-nfs), staging
verification polling ensures file visibility before commit.

---

(glossary-stem-matching)=
## Stem matching

The default algorithm for inferring artifact [provenance](#glossary-provenance)
(parent-child relationships). It strips file extensions from input and output
filenames, then matches output stems to input stems using longest-prefix lookup.
Digit boundary protection prevents `design_1` from matching `design_10`.
Exactly one match is required per output; zero or multiple matches at every
prefix level leave the output without a lineage mapping. A subsequent
validation pass then raises a `LineageCompletenessError` for any unmapped
output. For custom lineage, use explicit
[LineageMapping](#glossary-lineage-mapping) declarations.

---

(glossary-step)=
## Step

A single operation invocation within a [pipeline](#glossary-pipeline). Each step
has a sequential step number, an operation, resolved inputs, and produces a
[StepResult](#glossary-step-result) on completion. Steps can be blocking
(`pipeline.run()`) or non-blocking (`pipeline.submit()`).

---

(glossary-step-future)=
## StepFuture

A non-blocking handle returned by `pipeline.submit()`. Wraps a concurrent
future and provides `output()` for wiring to downstream steps without waiting
for completion, plus `result()` for blocking retrieval and a `status` property
(`"running"`, `"completed"`, or `"failed"`).

---

(glossary-step-result)=
## StepResult

The object returned by `pipeline.run()`. Contains metadata about the step
execution: success status, artifact counts, duration, and output
[roles](#glossary-role). Call `step_result.output("role")` to create an
[OutputReference](#glossary-output-reference) for wiring into downstream steps.

---

(glossary-tool-spec)=
## ToolSpec

Declares the external binary or script that a
[creator operation](#glossary-creator-operation) invokes. Specifies `executable`
(path or name resolved via PATH), optional `interpreter` prefix (e.g.
`"python"`), and optional `subcommand`. Set as the `tool` attribute on an
operation. `None` for pure-Python operations.

---

## See also

- [Architecture Overview](../concepts/architecture-overview.md) -- design
  rationale for the framework's key abstractions
- [Operations Model](../concepts/operations-model.md) -- how creators and
  curators work
- [Provenance System](../concepts/provenance-system.md) -- how lineage and
  execution provenance are tracked
