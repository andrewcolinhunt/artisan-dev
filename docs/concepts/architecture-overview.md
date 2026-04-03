# Architecture Overview

Artisan is a framework for building reproducible computational pipelines with
automatic provenance tracking. Before diving into individual subsystems, it
helps to understand the shape of the system as a whole — how the pieces fit
together, why they are separated the way they are, and what mental model to
carry when reading the rest of the documentation.

This page gives you that mental model.

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
Framework handles:    resolve inputs → check cache → dispatch to workers
                      → materialize inputs → run operation → capture lineage
                      → stage results → commit atomically to Delta Lake
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
| Orchestration | Everything below | Step sequencing, caching, dispatch, atomic commit |

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
**where**: the Delta root, working root, staging root, and backend traits.

**Why this split matters:**

- **Fault isolation.** Workers write to isolated staging directories. If a
  worker crashes, its partial results are ignored. Shared state is never
  corrupted.
- **Scale transparency.** The same code runs locally (process pool) or on a
  cluster (SLURM job arrays). The backend abstraction swaps the dispatch
  mechanism while keeping operations, execution logic, and storage identical.
- **No shared mutable state.** Workers never write to Delta Lake directly.
  Thousands of concurrent workers would cause write conflicts. Instead, they
  stage Parquet files, and the orchestrator commits them atomically.

### Backends

The dispatch mechanism is pluggable through the backend abstraction.
A backend bundles three concerns: how to dispatch work (Prefect flow
configuration), how workers behave (filesystem sharing, worker IDs), and
how the orchestrator handles post-dispatch verification (NFS attribute
caching, staging timeouts).

The framework ships three backends:

| Backend | Dispatch mechanism | Filesystem | Use case |
|---------|-------------------|------------|----------|
| Local | ProcessPool on the orchestrator machine | Local (no sharing) | Development, small jobs |
| SLURM | Job arrays via submitit | Shared NFS | HPC clusters, large-scale runs |
| SLURM Intra | srun within existing allocation | Shared NFS | Interactive salloc sessions, zero queue wait |

All backends use Prefect as the underlying task execution layer. The backend
controls which `TaskRunner` Prefect uses — `ProcessPoolTaskRunner` for local,
`SlurmTaskRunner` for SLURM and SLURM Intra — but everything above and below
that boundary stays the same.

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
  │  2. Compute cache key│──│  2. Materialize inputs│──│  2. Atomic Delta write │
  │  3. Check cache      │  │  3. Preprocess        │  │  3. Deduplicate        │
  │  4. Batch + dispatch │  │  4. Execute operation │  │  4. Return StepResult  │
  │                      │  │  5. Postprocess       │  │                        │
  │                      │  │  6. Capture lineage   │  │                        │
  │                      │  │  7. Stage to Parquet  │  │                        │
  └──────────────────────┘  └───────────────────────┘  └────────────────────────┘
```

**Dispatch** (orchestrator) resolves input references into concrete artifact
IDs, computes a cache key, checks the cache, and dispatches work to workers.
**Execute** (workers) creates an isolated sandbox, materializes inputs to disk,
runs the operation lifecycle, captures lineage, and stages results as Parquet
files. **Commit** (orchestrator) collects staged files and writes them
atomically to Delta Lake.

For the full phase-by-phase breakdown, see [Execution Flow](execution-flow.md).

---

## The building blocks

### Artifacts: immutable, content-addressed data

Every piece of data in the system — metrics, configurations, datasets, file
references — is an **artifact** identified by the hash of its content
(`artifact_id = xxh3_128(content)`). Same content always produces the same
ID.

This gives you three things for free:

- **Deduplication** — identical results are stored once
- **Deterministic caching** — same inputs + same parameters = same cache key
- **Immutable provenance** — edges between artifacts are permanent because
  artifacts never change

The framework ships four built-in artifact types (metric, file_ref, config,
data) and supports registering custom types through the artifact type registry.

For artifact types and the draft/finalize lifecycle, see
[Artifacts and Content Addressing](artifacts-and-content-addressing.md).

### Operations: pure computation with declared I/O

An operation is a self-contained computation that declares its inputs, outputs,
and parameters. It has no knowledge of orchestration, scheduling, or storage.
The framework provides two types:

**Creators** wrap heavy computation (external tools, ML inference, file
transforms). They follow a three-phase lifecycle — `preprocess` adapts inputs,
`execute` runs the computation, `postprocess` constructs output
artifacts. Each phase runs in its own sandbox directory.

**Curators** perform lightweight metadata manipulation (filtering, merging,
ingesting). They run a single `execute_curator` method in-memory, with no
sandboxing or worker dispatch.

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

### Storage: Delta Lake for everything

All persistent state — artifacts, provenance edges, execution records — lives
in Delta Lake tables backed by Parquet files on the local filesystem. No
external database, no connection strings, no services to keep alive.

Delta Lake provides ACID transactions (atomic commits, no partial corruption),
time travel (reproduce any historical state), and queryability (Polars, DuckDB,
any Delta-compatible tool).

The storage layer is split into three concerns:

- **Core** — `ArtifactStore` and `ProvenanceStore` for reading artifacts and
  provenance edges. Table schemas define the Polars column layouts for the
  five framework tables (executions, execution_edges, artifact_edges,
  artifact_index, steps).
- **Cache** — deterministic cache lookup using step specification hashes.
- **I/O** — staging, staging verification, and atomic commit to Delta Lake.

For table layout, partitioning, and the staging-commit pattern, see
[Storage and Delta Lake](storage-and-delta-lake.md).

### Composites: reusable compositions of operations

When multiple operations are tightly coupled — for example, transform then
score where you always score immediately after transforming — running them
as separate steps wastes I/O on intermediate artifacts that are immediately
consumed. A **composite** solves this.

A `CompositeDefinition` declares inputs, outputs, and a `compose()` method
that wires operations together using a `CompositeContext`. The same composite
can run **collapsed** (single worker, in-memory artifact passing via
`pipeline.run()`) or **expanded** (each internal operation becomes its own
pipeline step via `pipeline.expand()`).

```python
class TransformAndScore(CompositeDefinition):
    name = "transform_and_score"
    # ... inputs, outputs, Params ...

    def compose(self, ctx: CompositeContext) -> None:
        transformed = ctx.run(DataTransformer, inputs={"dataset": ctx.input("dataset")})
        scored = ctx.run(MetricCalculator, inputs={"dataset": transformed.output("dataset")})
        ctx.output("metrics", scored.output("metrics"))

# Collapsed: single step
pipeline.run(TransformAndScore, inputs={"dataset": output("gen", "datasets")})

# Expanded: each internal op becomes its own step
pipeline.expand(TransformAndScore, inputs={"dataset": output("gen", "datasets")})
```

Composites share the same dispatch-execute-commit lifecycle as regular steps.
Intermediate artifacts can be discarded (default), persisted, or fully exposed
depending on the `intermediates` setting. For the full conceptual model, see
[Composites and Composition](composites-and-composition.md).

---

## Synchronous and asynchronous execution

`pipeline.run()` blocks until the step completes. For pipelines with
independent steps that can execute concurrently, `pipeline.submit()` returns
a `StepFuture` immediately. You wire subsequent steps using
`future.output(role)`, which produces a lazy `OutputReference` that resolves
at dispatch time.

When all steps have been submitted, `pipeline.finalize()` waits for any
in-flight futures and shuts down the executor.

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
2. **Execution** creates an isolated sandbox. `DataGenerator.execute()`
   produces three files. `postprocess()` wraps them as draft artifacts.
   Lineage edges are captured. Results are staged as Parquet.
3. **Orchestration** commits step 0 to Delta Lake. Artifacts get finalized
   IDs. Returns `StepResult` with `OutputReference`.
4. **Orchestration** creates step 1. Resolves `output("generate", "datasets")`
   into three concrete artifact IDs. Computes cache key. No cache hit.
   Dispatches `DataTransformer` to workers.
5. **Execution** materializes the three input artifacts to disk. Runs
   `preprocess` → `execute` → `postprocess`. Captures lineage edges
   A→D, B→E, C→F via filename stem matching. Stages results.
6. **Orchestration** commits step 1 to Delta Lake. Pipeline complete.

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
| Build your first pipeline | [First Pipeline Tutorial](../tutorials/getting-started/01-first-pipeline.ipynb) |
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
- [First Pipeline Tutorial](../tutorials/getting-started/01-first-pipeline.ipynb) — Build and run your first pipeline
- [Coding Conventions](../contributing/coding-conventions.md) — Package boundaries and standards
