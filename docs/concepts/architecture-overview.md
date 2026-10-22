# Architecture Overview

Artisan runs operations on typed artifacts and records their results and
declared lineage. Operations define the work, workers execute it, and the
orchestrator sequences steps and commits accepted results.

---

## The core idea

A pipeline is a sequence of steps. Each step runs an operation on a batch of
artifacts and creates or routes artifacts. Artisan resolves inputs, dispatches
work, tracks provenance, checks caches, and commits results to durable storage.
The operation supplies its computation, output names, and exact parent declarations.

```
You write:   "Run this operation on these datasets"
                      │
Framework handles:    resolve + verify + group inputs → check cache → dispatch
                      → materialize inputs → run operation → validate declared lineage
                      → stage results → complete a logical Delta Lake commit
```

## Five layers

The main packages separate data models, operations, persistence, worker
execution, and orchestration:

```
┌──────────────────────────────────────────────────────────┐
│  Orchestration                                           │
│  Coordinates step sequencing, caching, dispatch, commit  │
└──────────────────────┬───────────────────────────────────┘
                       │
┌──────────────────────▼───────────────────────────────────┐
│  Execution                                               │
│  Runs operations on workers: sandbox, materialize,       │
│  lifecycle phases, lineage capture, result staging       │
└──────────┬───────────────────────────────────┬───────────┘
           │                                   │
┌──────────▼───────────┐  ┌────────────────────▼──────────┐
│  Operations          │  │  Storage                      │
│  Computation         │  │  Delta Lake persistence,      │
│  with declared I/O   │  │  content-addressed files,     │
│                      │  │  cache lookup                 │
└──────────┬───────────┘  └────────────────────┬──────────┘
           │                                   │
┌──────────▼───────────────────────────────────▼──────────┐
│  Schemas                                                │
│  All data models: artifacts, provenance, specs, config   │
└─────────────────────────────────────────────────────────┘
```

Creators receive prepared inputs without needing store access. Curators receive
an `ArtifactStore` for collection queries. Both can be tested directly, while
pipeline tests also exercise dispatch, validation, and persistence.

---

## The orchestrator-worker split

The **orchestrator** coordinates one pipeline run. **Workers** execute its
operations, potentially on different machines. Workers write isolated staging
files; the orchestrator owns shared store writes.

```
 Orchestrator                            Workers
┌─────────────────┐                     ┌──────────────────────┐
│ Resolve inputs  │   ExecutionUnit     │ Materialize inputs   │
│ Check cache     │ ──────────────────► │ Run operation phases │
│ Batch + dispatch│   (what to run)     │ Capture lineage      │
│                 │                     │ Stage results        │
│ Collect results │ ◄────────────────── │                      │
│ Commit to Delta │   staged Parquet    └──────────────────────┘
└─────────────────┘   files
```

`ExecutionUnit` carries **what** to execute: the operation instance, input
artifact IDs, parameters, and cache key. `RuntimeEnvironment` specifies
**where**: the Delta root, working root, staging root, and step-runner traits.

Incomplete worker output is not accepted as a successful result. Verified,
sealed output can be recovered after interruption. The orchestrator writes the
tables and marks each logical commit complete; supported Artisan readers expose
its rows only after completion. See
[crash recovery](storage-and-delta-lake.md#storage-crash-recovery).

### Step runners

A step runner defines dispatch, worker behavior (filesystem sharing and worker
IDs), and post-dispatch verification (NFS attribute caching and staging timeouts).

Core ships one step runner. External packages can implement the same public
`RunnerBase` and `LifecycleRouter` contract:

| Step runner | Dispatch mechanism | Filesystem | Use case |
|-------------|-------------------|------------|----------|
| Local | ProcessPool on the orchestrator machine | Local (no sharing) | Development, small jobs |
| `artisan-submitit` (optional) | SLURM arrays or srun via Submitit | Shared NFS | HPC clusters and existing allocations |

Runners receive `ExecutionUnit` objects and return ordered `UnitResult` records.
The native lifecycle router handles dispatch, polling, collection, and cancellation;
the local runner uses Python's `ProcessPoolExecutor`.

Confirmed cancellation requires evidence that work never started, or that
workers and managed commands stopped.
An accepted remote cancellation request alone does not prove termination. See
[Error Handling](error-handling.md#cancellation-and-uncertain-cleanup).

---

## How a step executes

Each pipeline step follows three phases: **dispatch**, **execute**, and **commit**.

```
         Orchestrator                 Worker                    Orchestrator
  ┌──────────────────────┐  ┌───────────────────────┐  ┌────────────────────────┐
  │  DISPATCH            │  │  EXECUTE              │  │  COMMIT                │
  │                      │  │                       │  │                        │
  │  1. Resolve refs     │  │  1. Create sandbox    │  │  1. Collect staging    │
  │  2. Compute cache key│──│  2. Materialize inputs│──│  2. Write table effects│
  │  3. Check cache      │  │  3. Preprocess        │  │  3. Complete commit    │
  │  4. Batch + dispatch │  │  4. Execute operation │  │  4. Return StepResult  │
  │                      │  │  5. Postprocess       │  │                        │
  │                      │  │  6. Capture lineage   │  │                        │
  │                      │  │  7. Stage to Parquet  │  │                        │
  └──────────────────────┘  └───────────────────────┘  └────────────────────────┘
```

Dispatch validates stored inputs before cache lookup or worker execution.
Commit verifies staged results and marks the logical commit complete last.
Each Delta table write is atomic; Artisan's completion filter coordinates
visibility across tables. Raw Delta reads bypass that filter.
See [Execution Flow](execution-flow.md) for each phase.

The execution layer also routes where an operation's computation runs. A plain
`execute_function` runs in the lifecycle worker. A local command operation or
`execute_as_tool` operation runs as a managed subprocess in its selected
environment. Remote compute routing invokes a deployed tool endpoint. The
**operation-as-tool endpoint** exposes a single operation as a standalone
service or container CLI, so an external harness can run it without the
surrounding pipeline. See
[Configure Execution](../how-to-guides/configuring-execution.md) and
[Op Container Images](../how-to-guides/op-container-images.md).

---

## The building blocks

### Artifacts: immutable, content-addressed data

An **artifact** is typed data identified by a hash of its type, canonical content,
and semantic metadata. Identical artifacts share a stored content row; changing
their content creates a new identity. Runtime locations and producing step
numbers stay outside that identity.

For built-in and custom types, external locations, and finalization, see
[Artifacts and Content Addressing](artifacts-and-content-addressing.md).

### Operations: computation with declared I/O

An operation declares its inputs, outputs, parameters, and the exact parents of
its new artifacts. Pipeline code selects its runner and scheduling configuration.
The framework provides two types:

- **Creators** run computation through `preprocess`, an execute phase, and
  `postprocess`, with a working directory for each phase.
- **Curators** filter, merge, or ingest artifacts through `execute_curator` in
  an isolated local subprocess.

See [Operations Model](operations-model.md) for their contracts and lifecycle.

### Provenance: dual tracking system

**Execution provenance** records an execution's configuration, inputs, and
outputs. **Artifact provenance** records each output's exact parents. A batch's
input and output lists do not reveal those relationships, so the operation
must declare them.

For declarations, matching helpers, parent groups, and traversal, see
[Provenance System](provenance-system.md).

### Storage: Delta Lake and artifact locations

Delta Lake tables store artifact content, metadata, provenance, and execution
history as Parquet files on local or object storage. Local pipelines need no
database service. External artifact bytes remain at separately recorded,
verified locations.

`ArtifactStore` and `ProvenanceStore` expose accepted results. For table layout,
commit visibility, and recovery, see
[Storage and Delta Lake](storage-and-delta-lake.md).

### Composites: reusable compositions of operations

A **composite** declares reusable wiring, such as transform-then-score, in a
`CompositeDefinition.compose()` method. It expands into ordinary pipeline
steps, each with its own caching, persistence, and provenance.
See [Composites and Composition](composites-and-composition.md) for overrides and
[Writing Composite Operations](../how-to-guides/writing-composite-operations.md)
for a complete example.

---

## Synchronous and asynchronous execution

`pipeline.run()` waits for a terminal `StepResult`. `pipeline.submit()` returns
a `StepFuture` after synchronous preparation, which can include waiting for
predecessors, verifying inputs, and checking caches. It is not an immediate
queueing operation. A future's output reference wires dependent work, and
`result()` waits for completion.

The current manager uses a single-worker step executor: separate steps execute
serially, while a creator step can dispatch its batches to parallel workers.
An asynchronous completion handle does not imply concurrent independent steps.
Composite submission follows the same preparation and waiting rules.

`pipeline.finalize()` waits for outstanding futures and shuts down the executor.
See [Build a Pipeline](../how-to-guides/building-a-pipeline.md) for usage and
[Python API](../reference/python-api.md) for method definitions.

---

## Support packages

Supporting packages provide:

- **`utils`** — hashing, managed commands, paths, and logging.
- **`visualization`** — provenance graphs, result inspection, outcome assessment,
  run comparison, and timing analysis.
- **`inspection`** — size-limited queries shared by Python, the CLI, and the
  optional MCP server, plus operating guidance and report exports.
- **`provenance`** — graph traversal for ancestry queries, input pairing, and
  visualization.

Inspection reads recorded evidence; it does not schedule work. Python and CLI
callers need no MCP server. The MCP adapter exposes the same queries to clients
that use that protocol. See
[local inspection](../how-to-guides/inspecting-evidence.md) for discovery and
paging, and [complete evidence exports](../how-to-guides/exporting-evidence.md)
to save an assessment or comparison beyond the response-size limit.

---

## Putting it together

For a pipeline that generates and transforms three datasets:

```text
DataGenerator → three data artifacts → DataTransformer → three transformed artifacts
```

1. The orchestrator dispatches `DataGenerator` as step 0. Its worker produces
   datasets A, B, and C, declares them as roots, and stages the validated artifacts.
2. The orchestrator commits step 0 and returns output references.
3. Step 1 resolves those references to three artifact IDs, verifies the inputs,
   checks the cache, and dispatches `DataTransformer` on a miss.
4. The transformer produces D, E, and F and declares their exact parents:
   A→D, B→E, and C→F. Its worker stages the artifacts and edges together.
5. The orchestrator commits step 1. The transformed artifacts become available.

On a rerun, matching accepted work may be reused according to the cache policy.
See [Build a Pipeline](../how-to-guides/building-a-pipeline.md) for the runnable
version.

---

## Where to go next

| If you want to... | Read |
|--------------------|------|
| Understand the operation lifecycle | [Operations Model](operations-model.md) |
| Understand artifact types and hashing | [Artifacts and Content Addressing](artifacts-and-content-addressing.md) |
| Understand lineage tracking | [Provenance System](provenance-system.md) |
| Understand the execution phases in detail | [Execution Flow](execution-flow.md) |
| Understand the storage layer | [Storage and Delta Lake](storage-and-delta-lake.md) |
| Understand design rationale | [Design Principles](design-principles.md) |
| Build your first pipeline | [First Pipeline Tutorial](../tutorials/01-getting-started/01-first-pipeline.ipynb) |
| Look up terminology | [Glossary](../reference/glossary.md) |
