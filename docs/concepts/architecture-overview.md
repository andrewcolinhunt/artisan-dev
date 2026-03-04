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
You write:   "Run ToolA on these datasets"
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
│  with declared I/O   │  │  content-addressed files,      │
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
Most of the time, you only touch one layer.

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
│ Check cache     │ ──────────────────► │ Run operation phase  │
│ Batch + dispatch│   (what to run)     │ Capture lineage      │
│                 │                     │ Stage results        │
│ Collect results │ ◄────────────────── │                      │
│ Commit to Delta │   staged Parquet    └──────────────────────┘
└─────────────────┘   files
```

`ExecutionUnit` carries **what** to execute: the operation instance, input
artifact IDs, parameters, and cache key. `RuntimeEnvironment` specifies
**where**: the Delta root, working root, and staging root.

**Why this split matters:**

- **Fault isolation.** Workers write to isolated staging directories. If a
  worker crashes, its partial results are ignored. Shared state is never
  corrupted.
- **Scale transparency.** The same code runs locally (ThreadPool) or on a
  cluster (SLURM). Only the dispatch backend changes — operations, execution
  logic, and storage remain identical.
- **No shared mutable state.** Workers never write to Delta Lake directly.
  Thousands of concurrent workers would cause write conflicts. Instead, they
  stage Parquet files, and the orchestrator commits them atomically.

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
  │  1. Resolve refs     │  │  1. Materialize       │  │  1. Collect staging    │
  │  2. Compute cache key│──│  2. Preprocess        │──│  2. Atomic Delta write │
  │  3. Check cache      │  │  3. Execute operation │  │  3. Deduplicate        │
  │  4. Batch + dispatch │  │  4. Postprocess       │  │  4. Return StepResult  │
  │                      │  │  5. Capture lineage   │  │                        │
  │                      │  │  6. Stage to Parquet  │  │                        │
  └──────────────────────┘  └───────────────────────┘  └────────────────────────┘
```

**Dispatch** (orchestrator) resolves inputs, checks the cache, and dispatches
work. **Execute** (workers) materializes inputs, runs the operation lifecycle,
captures lineage, and stages results. **Commit** (orchestrator) collects staged
files and writes them atomically to Delta Lake.

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

For artifact types and the draft/finalize lifecycle, see
[Artifacts and Content Addressing](artifacts-and-content-addressing.md).

### Operations: pure computation with declared I/O

An operation is a self-contained computation that declares its inputs, outputs,
and parameters. It has no knowledge of orchestration, scheduling, or storage.
The framework provides two types:

**Creators** wrap heavy computation (external tools, ML inference, file
transforms). They follow a three-phase lifecycle — `preprocess` adapts inputs,
`execute` runs the computation as a black box, `postprocess` constructs output
artifacts. Each phase runs in its own sandbox directory.

**Curators** perform lightweight metadata manipulation (filtering, merging,
ingesting). They run a single `execute_curator` method in-memory, with no
sandboxing or worker dispatch.

For the full model including specs, lifecycle phases, and pairing strategies,
see [Operations Model](operations-model.md).

### Provenance: dual tracking system

The framework maintains two complementary provenance systems:

**Execution provenance** records what happened — which operation ran, with
what parameters, consuming which artifacts and producing which others. This
is the activity log.

**Artifact provenance** records derivation chains — which specific input
produced which specific output (A produced D, B produced E, C produced F).
This cannot be derived from execution provenance because operations process
batches, and the individual correspondence requires context available only
at execution time.

For lineage declaration, filename matching, and co-input edges, see
[Provenance System](provenance-system.md).

### Storage: Delta Lake for everything

All persistent state — artifacts, provenance edges, execution records — lives
in Delta Lake tables backed by Parquet files on the local filesystem. No
external database, no connection strings, no services to keep alive.

Delta Lake provides ACID transactions (atomic commits, no partial corruption),
time travel (reproduce any historical state), and queryability (Polars, DuckDB,
any Delta-compatible tool).

For table layout, partitioning, and the staging-commit pattern, see
[Storage and Delta Lake](storage-and-delta-lake.md).

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
2. **Execution** runs the three-phase lifecycle. `DataGenerator.execute()`
   produces three files. `postprocess()` wraps them as draft artifacts.
   Lineage edges are captured. Results are staged as Parquet.
3. **Orchestration** commits step 0 to Delta Lake. Artifacts get finalized
   IDs. Returns `StepResult` with `OutputReference`.
4. **Orchestration** creates step 1. Resolves `step0.output("datasets")`
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
| Look up API details | [Glossary](../reference/glossary.md) |

---

## Cross-References

- [Operations Model](operations-model.md) — Two operation types and the three-phase lifecycle
- [Artifacts and Content Addressing](artifacts-and-content-addressing.md) — Immutable data and content hashing
- [Provenance System](provenance-system.md) — Dual provenance tracking
- [Execution Flow](execution-flow.md) — Dispatch, execute, commit in detail
- [Storage and Delta Lake](storage-and-delta-lake.md) — Persistence and the staging pattern
- [Design Principles](design-principles.md) — Rationale for key decisions
- [First Pipeline Tutorial](../tutorials/getting-started/01-first-pipeline.ipynb) — Build and run your first pipeline
- [Coding Conventions](../../contributing/coding-conventions.md) — Package boundaries and standards
