# Glossary

Terms used in Artisan's public interfaces and documentation.

---

(glossary-acid)=
## ACID

Atomicity, Consistency, Isolation, Durability -- the four properties that
describe reliable database transactions. Delta Lake provides transactions for
individual tables. Artisan coordinates multi-table result visibility through
[logical commits](#glossary-logical-commit).

---

(glossary-artifact)=
## Artifact

Typed pipeline data identified by a hash of its type, canonical content, and
semantic metadata. Artifacts start as mutable drafts;
[finalization](#glossary-draft-finalize) protects their durable state.
External storage locations do not affect
identity. See [artifact types](../concepts/artifacts-and-content-addressing.md#artifact-types).

---

(glossary-artisan-error)=
## ArtisanError

The base class for structured framework exceptions. It carries a stable code
and an `ArtisanErrorEnvelope` with a readable message, available context, and
recovery guidance. Failed executions can persist that envelope alongside the
error text; ordinary exceptions may have no envelope. See
[Structured failure identity](../concepts/error-handling.md#structured-failure-identity)
and [Python API](python-api.md).

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

A registry entry binding a type key, Delta table path, and `Artifact` model.
Importing the defining module validates and registers the type. Every worker
and reader that uses it must load the definition; see
[Create Artifact Types](../how-to-guides/creating-artifact-types.md).

---

(glossary-backend)=
## Step runner

An execution component that places scientific or curator workers and owns their
transport and process resources. Core supplies `LocalRunner`; external packages
supply runners such as `SlurmRunner` and `SlurmIntraRunner` from `artisan-submitit`.
Providers implement software resolution and worker launch using the contracts
exported by `artisan.orchestration.runner_api`. Unit result handles and the shared
worker's ownership state distinguish result delivery from confirmed closure.

---

(glossary-batching)=
## Batching

The two-level strategy for dividing work across workers. Level 1
(`artifacts_per_unit`) controls how many artifacts go into each
[execution unit](#glossary-execution-unit). Level 2 (`units_per_worker`)
controls how many execution units are packed into one worker task. A worker
process may handle several tasks. Both are configured via [BatchStrategy](#glossary-execution-config) on the operation.

---

(glossary-cache-policy)=
## CachePolicy

Controls which usable terminal step qualifies as a cache hit on subsequent
pipeline runs. `ALL_SUCCEEDED` (default) accepts only `succeeded` attempts.
`STEP_COMPLETED` accepts `succeeded` and `partial` attempts. Failed, cancelled,
and skipped attempts never qualify.

---

(glossary-cancellation-status)=
## CancellationStatus

The state of a cancellation request: `requested`, then `confirmed`, `rejected`,
or `unknown`. Confirmation requires evidence that work never started or has
stopped. After a rejected request, work may finish naturally. Unknown cleanup
fails the step.
See [cancellation](../concepts/error-handling.md#cancellation-and-uncertain-cleanup).

---

(glossary-composite)=
## Composite

A reusable composition of operations with declared inputs and outputs. Defined
by subclassing `CompositeDefinition` and implementing `compose()`. Running a
composite with `pipeline.run()` executes its children in one worker per unit.
Each `ctx.call()` declares internal wiring; `ctx.output()` chooses the outputs
retained at the outer persistence and cache boundary.

---

(glossary-compute-provider)=
## Compute provider

Infrastructure that places an Artisan worker, selected through a configured
[step runner](#glossary-backend). Local, Modal, and Slurm workers use the same
operation/composite contract. A command's declared environment selects its
software independently of placement. See
[Run Remote Workers](../how-to-guides/running-remote-workers.md).

---

(glossary-content-addressing)=
## Content addressing

Identifying data by a hash of its content rather than its storage location.
Artisan includes the artifact type and semantic metadata in that hash, so equal
bytes with different types or semantic names remain distinct. See
[Content addressing](../concepts/artifacts-and-content-addressing.md#content-addressing).

---

(glossary-creator-operation)=
## Creator operation

An [operation](#glossary-operation) subclass that runs heavy computation
(external tools, GPU work) through a three-phase lifecycle: `preprocess`,
`execute_function` or `execute_command`, and `postprocess`. Creators produce
new artifacts from inputs, run in worker processes via a
[step runner](#glossary-backend), and execute inside an isolated
[sandbox](#glossary-sandbox) directory. See
[Operations Model](../concepts/operations-model.md).

---

(glossary-curator-operation)=
## Curator operation

A lightweight [operation](#glossary-operation) subclass that routes, filters,
or merges artifacts without heavy computation. Curators implement a single
`execute_curator()` method and run in an isolated spawned subprocess on the
orchestrator, with no external step-runner dispatch or working sandbox. See
[Operations Model](../concepts/operations-model.md).

---

(glossary-definition)=
## Definition

A reusable operation or composite class, derived from `OperationDefinition` or
`CompositeDefinition`. It declares inputs, outputs, parameters, and behavior.
Submitting a definition to a pipeline creates a [step](#glossary-step).

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

The artifact lifecycle. `Subclass.draft()` creates a mutable artifact with
`artifact_id=None`. `finalize()` validates it, assigns its ID, protects durable
fields, and snapshots them to detect later nested changes. Artisan finalizes
returned drafts before commitment.

---

(glossary-environment)=
## Environment

A software declaration backed by a locked Pixi project. An operation declares
its command software; a runner declares its worker software. The runner prepares
the required installation or image before execution. See
[Prepare Execute Environments](../how-to-guides/execute-environments.md).

---

(glossary-execution-config)=
## BatchStrategy

Per-operation configuration controlling how work is divided and distributed.
Fields include `artifacts_per_unit`, `units_per_worker`, `max_workers`, and
`estimated_seconds` (available to runners as a scheduling hint). Set
as the `batch_strategy` attribute on an [OperationDefinition](#glossary-operation-definition).

---

(glossary-execution-context)=
## ExecutionContext

A frozen dataclass carrying one execution's identities, worker and step details,
store reference, and working paths. Artisan creates it at execution start.

---

(glossary-execution-record)=
## Execution record

A stored record of one execution attempt, with a deterministic cache key
(`execution_spec_id`) and a unique attempt ID (`execution_run_id`). See
[Dual identity for executions](../concepts/provenance-system.md#dual-identity-for-executions).

---

(glossary-execution-unit)=
## Execution unit

One independently admitted invocation of an operation or composite. It binds
the compiled recipe, exact inputs, selected software, and attempt identity.
`batch_strategy.artifacts_per_unit` controls scientific partitioning;
`units_per_worker` packs compatible units sequentially without combining their
persistence or cache boundaries.

---

(glossary-executor)=
## Executor

A context manager for sequential local creator and composite calls. It keeps
temporary files available between calls and removes them after confirmed closure
unless preservation was requested. It neither accesses a store nor prepares
software. See [local operation verification](../how-to-guides/writing-creator-operations.md#verify).

---

(glossary-failure-policy)=
## FailurePolicy

Controls how the framework handles individual execution failures within a step.
`CONTINUE` (default) logs failures and commits successful items, reporting
failure counts in [StepResult](#glossary-step-result). `FAIL_FAST` makes an
observed failure return a failed step result with no downstream outputs. The
failed execution and finished sibling work remain persisted for audit.

---

(glossary-group-by-strategy)=
## GroupByStrategy

The strategy for pairing input [roles](#glossary-role) before dispatch, set via
`OperationDefinition.group_by`: `ZIP`, `LINEAGE`, `CROSS_PRODUCT`, or `NAME`.
See [Pairing strategies](../concepts/operations-model.md#pairing-strategies) for
matching rules, unmatched inputs, and ambiguity handling.

---

(glossary-hydration)=
## Hydration

The process of loading an artifact's content or external content descriptors
from storage. `hydrate=True` (default) loads embedded bytes for content-bearing
artifacts. For file references, it loads metadata and a verified `path`, streaming
the file for integrity checks without caching all its bytes. `hydrate=False`
delivers only the artifact ID and type, which is sufficient for passthrough
operations like Filter and Merge that route artifacts without consuming content.
Orchestration still reads and verifies inputs before cache lookup or dispatch;
ID-only delivery is not a guarantee that pipeline preparation avoids content I/O.
See [Hydration](../concepts/artifacts-and-content-addressing.md#hydration-controlling-what-gets-loaded).

---

(glossary-input-spec)=
## InputSpec

An input role's artifact type, required status, and delivery settings, declared
in an operation's `inputs` mapping. See
[Input specs](../concepts/operations-model.md#input-specs) for hydration,
materialization, and typed config dependencies.

---

(glossary-lineage-mapping)=
## LineageMapping

An exact parent relationship declared in `ArtifactResult.lineage`. It identifies
an output by role and index, and its parent by input ID or sibling-output index.
See [Explicit lineage](../concepts/provenance-system.md#explicit-lineage).

---

(glossary-logical-commit)=
## Logical commit

An immutable plan that coordinates a batch of effects across Delta tables. A
normal step commit includes its terminal result snapshot; a recovery batch
retains the source step's existing outcome. Each physical table write is atomic.
The writer verifies the effects before recording completion, and supported
Artisan readers use that record for visibility. Raw Delta reads can include
incomplete writes. See [Commit ordering](../concepts/storage-and-delta-lake.md#commit-ordering).

---

(glossary-nfs)=
## NFS

Network File System, a distributed filesystem protocol common on HPC clusters.
Artisan flushes local staging payloads before publishing an execution seal.
Before accepting a result, the controller checks the seal and its complete
inventory. See
[NFS consistency](../concepts/storage-and-delta-lake.md#nfs-consistency).

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
concrete artifact IDs during downstream input preparation.

---

(glossary-output-spec)=
## OutputSpec

An output role's artifact type and parent-role contract (`derives_from`),
declared in an operation's `outputs` mapping. The operation supplies exact
parents at runtime. See [Output specs](../concepts/operations-model.md#output-specs).

---

(glossary-parquet)=
## Parquet

A columnar file format optimized for analytical queries. Delta Lake tables are
composed of Parquet files. Artisan stages worker results as Parquet files before
including them in a [logical commit](#glossary-logical-commit).

---

(glossary-pipeline)=
## Pipeline

A directed acyclic graph (DAG) of [steps](#glossary-step) managed by
[PipelineManager](#glossary-pipeline-manager). Steps are added by calling
`pipeline.run()` (waits for completion) or `pipeline.submit()` (returns a
completion handle after capturing parameters and input references) and wired
through [output references](#glossary-output-reference). The current manager
executes steps serially and can parallelize worker batches within a creator step.
Prepare required software explicitly through the runner before submission; see
[Prepare Execute Environments](../how-to-guides/execute-environments.md).

---

(glossary-pipeline-manager)=
## PipelineManager

The interface for submitting operations and composites, connecting their
outputs, and managing a run. `PipelineConfig` supplies its storage roots and
execution defaults. See [Build a Pipeline](../how-to-guides/building-a-pipeline.md).

---

(glossary-provenance)=
## Provenance

The record of where data came from and how it was produced. Artisan maintains
dual provenance: *execution provenance* (execution records and their input/output
`ExecutionEdge` relationships) and *artifact provenance* (which specific input
produced which specific output, stored as `ArtifactProvenanceEdge` records).
See [Provenance System](../concepts/provenance-system.md).

---

(glossary-resource-config)=
## RunnerResources

Hardware requirements for a step runner, including CPUs, memory, GPUs, and time
limit. A scheduler runner translates them into an allocation. The local runner
uses GPU requests to assign devices and constrain concurrency; it does not
reserve CPU or memory.
See [resource placement](../how-to-guides/configuring-execution.md#allocate-resources-at-the-right-layer).

---

(glossary-role)=
## Role

A named port on an operation's input or output interface. Each role maps to an
[InputSpec](#glossary-input-spec) or [OutputSpec](#glossary-output-spec) and
carries artifacts of a declared type. Roles are the unit of wiring between
steps: `step_result.output("data")` creates an
[OutputReference](#glossary-output-reference) for the `"data"` role. Operations
define a matching `InputRole` or `OutputRole` `StrEnum` for each nonempty
static role mapping.

---

(glossary-sandbox)=
## Sandbox

A separate directory tree created for each
[creator operation](#glossary-creator-operation) execution. Contains three
phase subdirectories: `preprocess/` (input adaptation), `execute/` (computation
outputs), and `postprocess/` (draft artifact construction). Materialized inputs
live separately in `materialized_inputs/`. This organizes files; it is not a
security boundary. `preserve_working=True` retains working directories for
inspection. Interrupted work may also leave directories behind; see
[debugging executions](../how-to-guides/debugging-executions.md).

---

(glossary-staging)=
## Staging

The intermediate write area where workers write Parquet files instead of
committing directly to Delta Lake. This avoids transaction conflicts on shared
filesystems. After all workers in a step complete, the orchestrator commits
staged files and the terminal step snapshot through a
[logical commit](#glossary-logical-commit). The controller verifies the
worker's sealed staging inventory before accepting the result.

---

(glossary-stem-matching)=
## Stem matching

Matching output filenames to candidate names and artifact IDs with
`artisan.operations.lineage.match_outputs_to_inputs_by_stem`. An operation may
call it to choose parents; executors never do. See
[Optional filename matching](../concepts/provenance-system.md#optional-filename-matching).

---

(glossary-step)=
## Step

A submitted operation or composite within a [pipeline](#glossary-pipeline).
Each step has a sequential step number, a definition, and inputs, and produces a
[StepResult](#glossary-step-result) on completion. `pipeline.run()` waits for
completion; `pipeline.submit()` returns a completion handle without waiting for
the step to finish.

---

(glossary-step-disposition)=
## StepDisposition

How a usable `StepResult` was obtained: `executed` or `cache_hit`. Only
`succeeded` and `partial` results carry a disposition.

---

(glossary-step-future)=
## StepFuture

A completion handle returned by `pipeline.submit()` after capturing parameters
and input references. Pending dependencies from the same manager are waited on
during execution. References from other runs in the same store must already be
terminal with resolved worker ownership. Provides `output()` to create a
downstream reference without waiting, `result()` for blocking retrieval, and a
`status` property containing the latest durably persisted
[StepStatus](#glossary-step-status).

---

(glossary-step-result)=
## StepResult

The terminal object returned by `pipeline.run()`. Its `status` is authoritative;
it also contains a disposition for usable output, item counts, duration, one
diagnostic `error`, optional cancellation acknowledgement, and output
[roles](#glossary-role). Output roles are exposed only for `succeeded` and
`partial` results. Call `step_result.output("role")` to create an
[OutputReference](#glossary-output-reference) for downstream wiring.

---

(glossary-step-status)=
## StepStatus

The authoritative state of one step attempt: `pending`, `running`, `succeeded`,
`partial`, `failed`, `cancelled`, or `skipped`. An attempt starts `pending`, may
become `running`, and ends in exactly one terminal state. Terminal states never
transition again.

---

(glossary-tool-spec)=
## ToolSpec

Declares the external binary or script that a
[creator operation](#glossary-creator-operation) invokes. Specifies `executable`
(path or name resolved via PATH), optional `interpreter` prefix (e.g.
`"python"`), and optional `subcommand`. Set as the `tool` attribute on an
operation. `None` for pure-Python operations.

---

(glossary-worker-config)=
## WorkerConfig

An internal configuration model for storage access, scratch paths, cleanup
preferences, and worker metadata. Software dependencies belong to
[Environment](#glossary-environment) declarations.

---

## See also

- [Python API](python-api.md) -- public entry points, source definitions, and docstrings
- [Architecture Overview](../concepts/architecture-overview.md) -- design
  rationale for the framework's key abstractions
- [Operations Model](../concepts/operations-model.md) -- how creators and
  curators work
- [Provenance System](../concepts/provenance-system.md) -- how lineage and
  execution provenance are tracked
