# Architecture Overview

Artisan is a framework for building reproducible computational pipelines with
automatic provenance tracking. Before diving into individual subsystems, it
helps to understand the shape of the system as a whole — how the pieces fit
together, why they are separated the way they are, and what mental model to
carry when reading the rest of the documentation.

The main boundaries are the operation's computation, worker execution, and the
orchestrator's sequencing and persistence.

---

## The core idea

A pipeline is a sequence of steps. Each step runs an operation on a batch of
artifacts and produces new artifacts. The framework handles everything around
the operation: resolving inputs, dispatching work, tracking provenance,
caching, and committing results to durable storage. The operation itself is
a pure computation — it receives data in, produces data out, and knows nothing
about the infrastructure running it.

```
You write:   "Run this operation on these datasets"
                      │
Framework handles:    resolve + verify + group inputs → check cache → dispatch
                      → materialize inputs → run operation → capture lineage
                      → stage results → complete a logical Delta Lake commit
```

This separation is the organizing principle behind the architecture. Everything
flows from it.

---

## Five layers

The framework is organized into five layers with strict downward-only
dependencies. Each layer has a single responsibility, and you can use any
layer without the ones above it.

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
│  Pure computation    │  │  Delta Lake persistence,      │
│  with declared I/O   │  │  content-addressed files,     │
│                      │  │  cache lookup                 │
└──────────┬───────────┘  └────────────────────┬──────────┘
           │                                   │
┌──────────▼───────────────────────────────────▼──────────┐
│  Schemas                                                │
│  All data models: artifacts, provenance, specs, config   │
└─────────────────────────────────────────────────────────┘
```

**Why five layers instead of three or ten?** Each layer represents a distinct
concern with different change frequencies. Schemas change when data models
change. Operations change when you add new computations. Execution changes
when runtime behavior changes (sandboxing, staging). Storage changes when
persistence changes. Orchestration changes when coordination logic changes.
Most of the time, you touch only one layer.

**Why strict downward dependencies?** So you can test operations without
orchestration, run execution without SLURM, and use schemas without anything
else. Each layer is independently testable and replaceable.

| Layer | Depends on | Responsibility |
|-------|-----------|----------------|
| Schemas | Nothing | Data models (artifacts, provenance edges, specs, config) |
| Operations | Schemas | Pure computation with declared inputs and outputs |
| Storage | Schemas | Delta Lake tables, content-addressed persistence, cache |
| Execution | Operations, Schemas, Storage | Worker-side lifecycle, sandboxing, lineage, staging |
| Orchestration | Everything below | Step sequencing, caching, dispatch, logical commit |

---

## The orchestrator-worker split

The system has two distinct runtime roles: the **orchestrator** (one per
pipeline run) and **workers** (many, potentially on different machines). They
communicate through two data structures and never share mutable state.

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

**Why this split matters:**

- **Fault isolation.** Workers write to isolated staging directories. If a
  worker crashes, its partial results are ignored. Shared state is never
  corrupted.
- **Scale transparency.** The same code runs locally (process pool) or through
  an optional cluster provider. The step-runner abstraction swaps the dispatch
  mechanism while keeping operations, execution logic, and storage identical.
- **No shared mutable state.** Workers never write to Delta Lake directly.
  Thousands of concurrent workers would cause write conflicts. Instead, they
  stage Parquet files, and the orchestrator writes the tables and marks the
  logical commit complete. Artisan readers expose those rows only after
  completion.

### Step runners

The dispatch mechanism is pluggable through the step-runner abstraction.
A step runner bundles three concerns: how to dispatch work, how workers behave
(filesystem sharing, worker IDs), and how the orchestrator handles post-
dispatch verification (NFS attribute caching, staging timeouts).

Core ships one step runner. External packages can implement the same public
`RunnerBase` and `LifecycleRouter` contract:

| Step runner | Dispatch mechanism | Filesystem | Use case |
|-------------|-------------------|------------|----------|
| Local | ProcessPool on the orchestrator machine | Local (no sharing) | Development, small jobs |
| `artisan-submitit` (optional) | SLURM arrays or srun via Submitit | Shared NFS | HPC clusters and existing allocations |

The native lifecycle router owns dispatch, polling, ordered result collection,
and cancellation. Local execution uses Python's `ProcessPoolExecutor` directly.
Provider runners receive the same `ExecutionUnit` objects and return the same
ordered `UnitResult` records, so everything above and below that boundary stays
the same.

---

## How a step executes

Each pipeline step follows three phases: **dispatch**, **execute**, **commit**.
The separation ensures that coordination, computation, and persistence are
cleanly isolated.

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

**Dispatch** (orchestrator) resolves input references into concrete artifact
IDs, validates their stored type and content, applies grouping, computes cache
keys from the same prepared snapshot, and dispatches work to workers.
**Execute** (workers) creates an isolated sandbox, materializes inputs to disk,
runs the operation lifecycle, captures lineage, and stages results as Parquet
files. **Commit** (orchestrator) verifies staged files, writes table effects in
dependency order, and marks the logical commit complete last. Each Delta table
write is atomic; Artisan's completion filter coordinates visibility across the
tables. Reading raw Delta tables bypasses that filter.

For the full phase-by-phase breakdown, see [Execution Flow](execution-flow.md).

The execution layer also routes *where* an operation's computation runs.
**Compute-provider routing** either runs the operation in-process on the worker
(local passthrough) or invokes it on a remote tool endpoint. The
**operation-as-tool endpoint** exposes a single operation as a standalone
service or container CLI, so an external harness can run it without the
surrounding pipeline. See
[Configure Execution](../how-to-guides/configuring-execution.md) and
[Op Container Images](../how-to-guides/op-container-images.md).

---

## The building blocks

### Artifacts: immutable, content-addressed data

Every piece of data in the system — metrics, configurations, datasets, file
references — is an **artifact** identified by a versioned hash of its registered
type, canonical content, and semantic metadata. Runtime locations and producing
step numbers stay outside that identity.

This gives you three things for free:

- **Deduplication** — identical typed semantic artifacts are stored once
- **Deterministic caching** — same inputs + same parameters = same cache key
- **Immutable provenance** — edges between artifacts are permanent because
  artifacts never change

The framework ships six built-in artifact types (metric, file ref, config,
data, large file, and appendable) and supports registering custom types through
the artifact type registry. External artifact locations live in a separate
relation so verified bytes can move or be replicated without changing identity.

For artifact types and the draft/finalize lifecycle, see
[Artifacts and Content Addressing](artifacts-and-content-addressing.md).

### Operations: pure computation with declared I/O

An operation is a self-contained computation that declares its inputs, outputs,
and parameters. It has no knowledge of orchestration, scheduling, or storage.
The framework provides two types:

**Creators** wrap heavy computation (external tools, ML inference, file
transforms). They follow a three-phase lifecycle — `preprocess` adapts inputs,
the execute phase (`execute_function` or `execute_command`) runs the
computation, `postprocess` constructs output artifacts. Each phase runs in its
own sandbox directory.

**Curators** perform lightweight metadata manipulation (filtering, merging,
ingesting). They run a single `execute_curator` method in an isolated local
subprocess. They skip creator sandbox phases and configurable creator-runner
dispatch.

The framework detects the type automatically: if a class overrides
`execute_curator()`, it is a curator. Otherwise, it is a creator.

For the full model including specs, lifecycle phases, and pairing strategies,
see [Operations Model](operations-model.md).

### Provenance: dual tracking system

The framework maintains two complementary provenance systems:

**Execution provenance** records what happened — which operation ran, with
what parameters, consuming which artifacts and producing which others. This
is the activity log.

**Artifact provenance** records derivation chains — which specific input
artifact produced which specific output artifact. This cannot be derived from
execution provenance because operations process batches, and the individual
correspondence requires context available only at execution time (filename
matching, positional grouping, or explicit declaration).

The framework also includes a `provenance` package with graph traversal
utilities — forward and backward BFS walks through provenance edges using
DataFrame joins. These are used for metric discovery, lineage matching, and
multi-input pairing.

For lineage declaration, filename matching, and co-input edges, see
[Provenance System](provenance-system.md).

### Storage: Delta Lake and artifact locations

Artifact-type tables hold typed content and metadata. Framework tables track
artifact identities and locations, executions and provenance, run membership,
cache reuse, and logical commit completion. External artifact bytes remain at
their recorded locations rather than being embedded in table rows.

Tables use Parquet files on a local filesystem or configured object storage.
Local pipelines need no database service. Delta Lake provides atomic
transactions and version history for each individual table. A logical Artisan
commit spans several table writes; supported Artisan readers expose its rows
only after completion. Raw per-table time travel does not by itself reconstruct
a complete historical pipeline or its external files.

The storage layer is split into three concerns:

- **Core** — `ArtifactStore` and `ProvenanceStore` for reading artifacts and
  provenance edges, with shared completion filtering and table schemas.
- **Cache** — deterministic cache lookup using step specification hashes.
- **I/O** — staging, verification, and coordinated logical commits to Delta Lake.

For table layout, partitioning, and the staging-commit pattern, see
[Storage and Delta Lake](storage-and-delta-lake.md).

### Composites: reusable compositions of operations

When multiple operations are tightly coupled — for example, transform then
score where you always score immediately after transforming — copying that
wiring into every pipeline duplicates it, and the copies drift. A
**composite** names the wiring once as a reusable unit.

A `CompositeDefinition` declares inputs, outputs, and a `compose()` method
that wires operations together using a `CompositeContext`. Running a
composite with `pipeline.run_composite()` expands it into real pipeline
steps — each internal `ctx.run()` becomes its own step.

Each internal operation has its own persistence boundary, caching, and
provenance. See [Composites and Composition](composites-and-composition.md) for
how grouping and overrides work, and
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

Beyond the five architectural layers, the framework includes three support
packages:

- **`utils`** — shared utilities: content hashing (xxh3_128), subprocess
  wrappers (`run_command`), path helpers, DataFrame utilities, error
  formatting, and logging configuration.
- **`visualization`** — provenance graph rendering (macro and micro views via
  Graphviz), pipeline and step inspection helpers, and execution timing
  analysis.
- **`provenance`** — graph traversal algorithms (forward and backward BFS
  walks through provenance edges) used by both the execution layer for lineage
  matching and the visualization layer for graph rendering.

These packages do not participate in the layered dependency hierarchy. They
are consumed by whichever layer needs them.

---

## Putting it together

Here is a concrete example of how the layers cooperate. You write a
two-step pipeline:

```
pipeline = PipelineManager.create(name="example", delta_root=delta_root, staging_root=staging_root)
output = pipeline.output
pipeline.run(operation=DataGenerator, name="generate", params={"count": 3})
pipeline.run(operation=DataTransformer, name="transform", inputs={"dataset": output("generate", "datasets")})
```

What happens:

1. **Orchestration** creates step 0, sees no inputs to resolve, dispatches
   `DataGenerator` to workers.
2. **Execution** creates an isolated sandbox. `DataGenerator.execute_function()`
   produces three files. `postprocess()` wraps them as draft artifacts.
   The worker finalizes artifact IDs, captures lineage edges, and stages
   results as Parquet.
3. **Orchestration** completes step 0's logical commit, including its terminal
   snapshot, then returns a `StepResult` with output references.
4. **Orchestration** creates step 1. Resolves `output("generate", "datasets")`
   into three concrete artifact IDs. Computes cache key. No cache hit.
   Dispatches `DataTransformer` to workers.
5. **Execution** materializes the three input artifacts to disk. Runs
   `preprocess` → `execute_function` → `postprocess`. Captures lineage edges
   A→D, B→E, C→F via filename stem matching. Stages results.
6. **Orchestration** completes step 1's logical commit. Pipeline complete.

Every artifact has a content-addressed ID. Every derivation is tracked. Every
execution is recorded. The pipeline can be re-run and cached steps will be
skipped automatically.

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

---

## Cross-references

- [Operations Model](operations-model.md) — Two operation types and the three-phase lifecycle
- [Artifacts and Content Addressing](artifacts-and-content-addressing.md) — Immutable data and content hashing
- [Provenance System](provenance-system.md) — Dual provenance tracking
- [Execution Flow](execution-flow.md) — Dispatch, execute, commit in detail
- [Storage and Delta Lake](storage-and-delta-lake.md) — Persistence and the staging pattern
- [Composites and Composition](composites-and-composition.md) — Reusable operation composition
- [Design Principles](design-principles.md) — Rationale for key decisions
- [First Pipeline Tutorial](../tutorials/01-getting-started/01-first-pipeline.ipynb) — Build and run your first pipeline
- [Coding Conventions](../contributing/coding-conventions.md) — Package boundaries and standards
