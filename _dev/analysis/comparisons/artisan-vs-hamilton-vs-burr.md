# Artisan vs Hamilton vs Burr: Comparative Analysis

*Analysis date: 2026-04-04*

---

## Overview

Three frameworks, three paradigms:

| Framework | Paradigm | Core unit | Primary domain |
|-----------|----------|-----------|----------------|
| **Artisan** | Artifact-centric pipeline | OperationDefinition (class) | Computational pipelines with provenance (HPC, cloud, local) |
| **Hamilton** | Function-centric DAG | Python function (decorated) | Feature engineering, ML pipelines, data transforms |
| **Burr** | State machine | Action + State + Transitions | Agent workflows, chatbots, interactive apps |

Hamilton and Burr are sister projects from the same team (DAGWorks, now
Apache incubating). They're complementary: Hamilton handles stateless data
transformations; Burr handles stateful, potentially cyclic workflows.

Artisan is an artifact lifecycle framework with provenance-first design.
Its architecture is backend-agnostic by design: operations are isolated from
infrastructure concerns, the backend ABC abstracts dispatch, and storage is
decoupled from compute. Today it runs on local machines and SLURM clusters.
Cloud backends (Kubernetes, Modal, AWS Batch) and streaming pipeline execution
are designed and in implementation -- see "Evolving Landscape" below.

---

## Core Abstractions

### Artisan

- **OperationDefinition** -- Pydantic model subclass with ClassVar declarations
  for `inputs`, `outputs`, `name`. Two families: Creators (produce artifacts via
  `execute()`) and Curators (route/filter artifacts via `execute_curator()`).
- **Artifact** -- Content-addressed data unit (xxh3_128 hash). Draft/finalize
  lifecycle. Four built-in types: DATA, METRIC, FILE_REF, CONFIG.
- **InputSpec / OutputSpec** -- Declares what an operation accepts/produces,
  including declarative lineage (`infer_lineage_from`).
- **PipelineManager** -- User-facing orchestrator. Steps are wired via explicit
  `pipeline.run()` calls with `OutputReference` objects as lazy pointers.
- **CompositeDefinition** -- Reusable operation compositions with two execution
  modes (collapsed in-process vs expanded as independent steps).

### Hamilton

- **Functions as nodes** -- A plain Python function becomes a DAG node. The
  function name is the node identity. Parameter names declare dependencies by
  matching other function names. Type annotations are mandatory.
- **Driver / Builder** -- Inspects modules, resolves dependencies, constructs
  the DAG automatically. No manual wiring.
- **Decorators** -- ~30+ decorators for parameterization (`@parameterize`),
  config branching (`@config.when`), composition (`@subdag`), I/O
  (`@dataloader`/`@datasaver`), column extraction (`@extract_columns`), piping
  (`@pipe`), caching (`@cache`), and more.
- **Adapters** -- Lifecycle hooks for observability, execution backends, result
  building. Composable via `with_adapters()`.

### Burr

- **Action** -- A function or class that reads state, computes, and returns
  `(result, state_update)`. Declares `reads`/`writes` for validation.
- **State** -- Immutable dict-like object. Every mutation returns a new instance.
  Enables time-travel debugging.
- **Transitions** -- Conditional edges between actions. Supports cycles, making
  it a true state machine (not just a DAG).
- **Application** -- Assembled via `ApplicationBuilder` with actions, transitions,
  entry point, persistence, and tracker configuration.

### Comparison

| Dimension | Artisan | Hamilton | Burr |
|-----------|---------|----------|------|
| Definition style | Subclass a base class | Write a function | Write a function or class |
| DAG construction | Explicit (`pipeline.run()`) | Implicit (function signatures) | Explicit (transitions) |
| Supports cycles | No (DAG only) | No (DAG only) | Yes (state machine) |
| Boilerplate | Higher (class, specs, lifecycle) | Lower (function + type hints) | Moderate (action + builder) |
| Framework coupling | Medium (import base class, schemas) | Minimal (zero Hamilton imports in transforms) | Light (decorator or base class) |

---

## Execution Model

### Artisan

- **Backend-agnostic dispatch** -- A `DispatchHandle` ABC decouples dispatch
  from any specific runtime. Current backends: local (ProcessPool) and SLURM
  (job arrays). Designed backends: Kubernetes, Modal, AWS Batch. Each handle
  owns unit transport (pickle to shared FS, `.map()` arguments, object store).
- **Two-level batching** -- Level 1: `artifacts_per_unit` splits inputs into
  ExecutionUnits. Level 2: `units_per_worker` groups units per worker/job.
- **Three-phase creator lifecycle** -- `preprocess()` (hydrate/reshape) ->
  `execute()` (core computation) -> `postprocess()` (build draft artifacts).
- **Curator operations** run in-process against Polars DataFrames.
- **Composites** can run collapsed (all ops in one worker) or expanded (each op
  dispatched independently).
- **Streaming execution** (designed, in implementation) -- parallel branches
  stream through operation chains with priority scheduling, resource-aware
  dispatch, and a step scheduler thread. Streaming emerges from the dependency
  graph: branch N's downstream step starts as soon as its upstream finishes,
  independent of other branches.

### Hamilton

- **Default: synchronous DFS** in-process. No parallelism out of the box.
- **Task-based execution** via `enable_dynamic_execution()` with local/remote
  executors (MultiProcessing, ThreadPool).
- **Adapter-based parallelism** -- Ray, Dask, PySpark adapters farm out node
  execution to external clusters with zero code changes.
- **Parallelizable/Collect** -- Explicit map-reduce pattern via type annotations.
  Single-level nesting only.
- **AsyncDriver** for async workloads.

### Burr

- **Single-threaded, in-process** by default. No built-in distributed execution.
- **Four execution APIs**: `step()` (one action), `iterate()` (generator),
  `run()` (to halt condition), `stream_result()` (streaming).
- **Sync and async variants** for all APIs.
- **Parallelism** via `MapStates`/`MapActions` (map-reduce within the state
  machine).
- **Distributed execution** is delegated to the serving layer (FastAPI, Ray).

### Comparison

| Dimension | Artisan | Hamilton | Burr |
|-----------|---------|----------|------|
| Default execution | Multi-process (DispatchHandle) | Single-process DFS | Single-threaded |
| Built-in parallelism | Yes (batching + backends) | Via executors/adapters | Map-reduce pattern |
| HPC / SLURM | Native backend | No | No |
| Cloud (K8s, Modal, Batch) | Designed, in implementation | Via Ray/Dask adapters | External |
| Distributed | DispatchHandle + backends | Ray, Dask, Spark adapters | External (FastAPI, Ray) |
| Batching | Two-level, configurable | Not built-in | Not applicable |
| Streaming | Branch-level (designed) | No | First-class (`@streaming_action`) |
| Async | No | Yes (AsyncDriver) | Yes (async variants) |

**Key insight:** These three frameworks have fundamentally different streaming
models. Artisan's streaming is *branch-level*: independent pipeline branches
overlap, with downstream steps starting as soon as upstream finishes per-branch.
This is pipeline throughput streaming -- getting results sooner by overlapping
work, not token-by-token output streaming. Burr's streaming is *output-level*:
yielding partial results from a single action (e.g., LLM tokens). Hamilton has
no streaming model.

Similarly, their parallelism models differ in kind:
- **Artisan**: dispatch many execution units across workers/jobs/containers
- **Hamilton**: distribute node execution across Ray/Dask clusters
- **Burr**: map-reduce within a state machine, or delegate to external systems

---

## Data Model

### Artisan

- **Content-addressed artifacts** with typed content (DATA, METRIC, FILE_REF,
  CONFIG). Each artifact gets a deterministic 32-char hex ID from xxh3_128 hash.
- **Draft/finalize lifecycle** -- mutable draft, then immutable after finalization.
- **Polars schemas** -- each artifact type defines its columnar schema for
  Delta Lake storage.
- **Delta Lake persistence** -- ACID commits, time travel, partitioned by step
  number, ZSTD compression.

### Hamilton

- **Type-agnostic** -- nodes can return anything (DataFrames, scalars, dicts,
  custom objects). No imposed data model.
- **Structural type checking** at build time via annotations.
- **No built-in persistence** -- materialization handled by adapters/plugins.
- **Column-level granularity** via `@extract_columns`.

### Burr

- **Immutable State object** -- dict-like with `.update()` returning new
  instances. Optional Pydantic typed state.
- **Persistence via pluggable persisters** (SQLite, PostgreSQL, Redis, MongoDB,
  S3). State keyed by `(app_id, partition_key, sequence_id)`.
- **Fork/resume** from any historical state point.

### Comparison

| Dimension | Artisan | Hamilton | Burr |
|-----------|---------|----------|------|
| Data identity | Content-addressed hash | None (ephemeral) | Sequence ID |
| Type system | Custom artifact types + Polars schemas | Python type hints | Python types / Pydantic |
| Persistence | Built-in (Delta Lake, ACID) | External (adapters/plugins) | Built-in (pluggable persisters) |
| Immutability | Finalized artifacts are immutable | N/A (transient) | State is immutable |
| Deduplication | Automatic (content hash) | Via cache keys | N/A |
| Time travel | Via Delta Lake | Via cache | Via sequence IDs |

---

## Provenance & Lineage

### Artisan

- **Declarative lineage** -- operations declare derivation patterns at definition
  time via `OutputSpec.infer_lineage_from`. The framework infers concrete edges
  automatically.
- **Two provenance graphs** -- artifact-level (source -> target derivation) and
  execution-level (which execution consumed/produced what).
- **Stored in Delta Lake** as first-class tables (`artifact_edges`,
  `execution_edges`, `executions`).
- **BFS traversal** on Polars DataFrames for ancestry/descendant queries.
- **Visualization** via Graphviz at both macro (step-level) and micro
  (artifact-level) granularity.

### Hamilton

- **Structural lineage** -- the DAG IS the lineage. Because dependencies are
  declared via function parameter names, Hamilton knows the complete dependency
  graph at build time.
- **Column-level lineage** via `@extract_columns`.
- **Hamilton UI** provides interactive lineage exploration and data catalog.
- **OpenLineage integration** emits standardized lineage events for external
  systems (Marquez, Amundsen).
- **Built-in DAG visualization** via Graphviz.

### Burr

- **State transition history** tracked by the persistence layer.
- **Tracking UI** on port 7241 shows step-by-step timeline, state at any
  point, and graph visualization.
- **OpenTelemetry-compatible** spans for integration with external observability.
- **No artifact-level lineage** -- Burr tracks state transitions, not data
  derivation.

### Comparison

| Dimension | Artisan | Hamilton | Burr |
|-----------|---------|----------|------|
| Lineage approach | Declarative + stored | Structural (DAG = lineage) | State transition history |
| Granularity | Artifact-level | Function/column-level | Action/state-level |
| Persistence | Built-in (Delta tables) | External (UI, OpenLineage) | Built-in (tracker) |
| Cross-run lineage | Yes (content-addressed) | Via cache metadata | Via app_id + sequence_id |
| Visualization | Graphviz (macro/micro) | Graphviz + Hamilton UI | Tracking UI |
| External integration | None built-in | OpenLineage, Datadog | OpenTelemetry |

**Key insight:** Artisan's provenance is the most sophisticated -- it's a
first-class concern stored alongside data with artifact-level granularity and
cross-run deduplication. Hamilton gets lineage "for free" from the DAG structure
but doesn't persist it as a separate concern. Burr tracks state history, not
data derivation.

---

## Caching

### Artisan

- **Two-level caching** -- step-level (keyed by operation + position + upstream
  refs + params) and execution-level (keyed by operation + resolved artifact
  IDs + params).
- **Content-addressed** -- same content always produces same ID, enabling
  deterministic cache lookups.
- **Cross-run reuse** -- execution-level cache works across different pipeline
  runs.
- **Cache policy** configurable: `ALL_SUCCEEDED` or `STEP_COMPLETED`.

### Hamilton

- **SQLite-based** metadata store with file-based result store.
- **Cache key** = `{node_name, code_version_SHA256, dependency_data_versions}`.
- **Four behaviors** per node: DEFAULT, RECOMPUTE, DISABLE, IGNORE.
- **Known limitation** -- code versioning only hashes the immediate function
  body. Changes to called utilities or upgraded dependencies do NOT invalidate.
- **Cache keys may be unstable** across Python/Hamilton version upgrades.

### Burr

- **State-level persistence** -- not caching per se, but resume from any
  persisted state point.
- **Fork/resume** enables branching from historical points.

### Comparison

| Dimension | Artisan | Hamilton | Burr |
|-----------|---------|----------|------|
| Caching strategy | Content-addressed, two-level | Code+data version hashing | State persistence |
| Cross-run reuse | Yes (automatic) | Yes (if cache exists) | Via fork/resume |
| Determinism | Hash-based (reliable) | Code hash (fragile for transitive deps) | Sequence-based |
| Granularity | Step + execution batch | Per-node | Per-action |

---

## Composition & Reuse

### Artisan

- **CompositeDefinition** -- class-based, declares inputs/outputs, implements
  `compose(ctx)` which orchestrates internal operations.
- **Two execution modes** -- collapsed (in-process, lower overhead) and expanded
  (full dispatch per internal op).
- **Same `compose()` code** works in both modes.

### Hamilton

- **Multiple modules** loaded into one Driver.
- **`@subdag`** embeds a reusable DAG fragment with namespacing.
- **`@parameterized_subdag`** for N parameterized instances.
- **`@parameterize` family** generates N nodes from one function.
- **`@pipe`** chains transformations as explicit DAG nodes.

### Burr

- **Sub-applications** via manual wiring (build child Application inside a
  parent action).
- **`MapStates`/`MapActions`** for map-reduce composition.
- **Recursive composition is planned** but not yet shipped (as of research date).

### Comparison

| Dimension | Artisan | Hamilton | Burr |
|-----------|---------|----------|------|
| Composition model | CompositeDefinition (class) | @subdag + modules | Sub-applications (manual) |
| Parameterization | Via Params (Pydantic model) | @parameterize (generates N nodes) | Per-action config |
| Reuse mechanism | Named composites with I/O specs | Module import + @subdag | Rebuild Application |
| Maturity | Strong (two execution modes) | Strong (~30 decorators) | Developing |

---

## Configuration

### Artisan

- **Hierarchical overrides** -- class defaults -> `pipeline.run()` overrides ->
  deep merge via `model_copy(update=...)`.
- **Structured config** -- `Params` (algorithm), `ResourceConfig` (CPUs, memory,
  GPUs), `ExecutionConfig` (batching), `Environments` (local/Docker/Apptainer/Pixi),
  `ToolSpec` (external binaries).
- **Per-step overrides** at pipeline construction time.

### Hamilton

- **Build-time config** via `@config.when` -- controls which function
  implementations are loaded (affects DAG topology, not just values).
- **Runtime inputs** via `execute(inputs={...})` and `overrides={...}`.
- **Critical distinction** -- config changes graph structure; inputs/overrides
  change values in a fixed graph.

### Burr

- **Builder-level config** for persistence, tracking, lifecycle hooks.
- **Per-action config** via constructor arguments.
- **No declarative config branching** (handle conditionally in transitions).

---

## When to Use Each

### Use Artisan when:

- You need **artifact-level provenance** tracked across pipeline runs
- You need **content-addressed deduplication** and deterministic caching
- Your pipeline produces **files, datasets, and metrics** as first-class outputs
- You want **ACID persistence** without an external database
- Your operations involve **external tools** (binaries, scripts, containers)
- You need **two-level batching** over large artifact collections
- You're running on **HPC (SLURM)**, **cloud (K8s, Modal, AWS Batch)**, or local
- You need **many independent branches** streaming through multi-step chains
  with different resource requirements per step

### Use Hamilton when:

- You're building **data transformation pipelines** (feature engineering, ETL)
- You want **minimal boilerplate** (functions, not classes)
- You need **column-level lineage** in DataFrame workflows
- You want **zero-framework-coupling** in your transform code (plain pytest-testable functions)
- You need to **plug into existing orchestrators** (Airflow, Dagster, Prefect)
- You want **distributed execution** via Ray/Dask/Spark with no code changes
- You need a **lightweight library** (not an opinionated framework)

### Use Burr when:

- You're building **stateful interactive applications** (chatbots, agents)
- You need **cyclic workflows** (state machines, not just DAGs)
- You want **human-in-the-loop** patterns with pause/resume
- You need **streaming** output (LLM token streaming, progressive results)
- You want **state time-travel** and fork-based debugging
- Your application has **conditional branching** based on runtime state

---

## Strengths and Weaknesses

### Artisan

**Strengths:**
- Provenance is a first-class architectural concern, not an afterthought
- Content-addressed identity enables automatic, reliable caching and dedup
- Backend-agnostic: same operations run on local, SLURM, K8s, Modal, AWS Batch
- Delta Lake provides ACID, time travel, and columnar efficiency without external DB
- Strong type safety via Pydantic throughout
- Creator/Curator split avoids unnecessary overhead for metadata operations
- Composite two-mode execution (collapsed/expanded) is elegant
- Cloud storage via fsspec (S3/GCS) with zero operation code changes
- Branch-level streaming with priority scheduling and resource-aware dispatch

**Weaknesses:**
- Higher ceremony than Hamilton (class-based, specs, three-phase lifecycle)
- Smaller ecosystem -- no external orchestrator integrations
- Creator/Curator bifurcation adds conceptual overhead
- Learning curve is steeper than "write a function"
- No column-level lineage (artifact-level granularity)
- No output-level streaming (token-by-token); streaming is at the pipeline branch level

### Hamilton

**Strengths:**
- Lowest ceremony of the three (plain functions, zero framework imports)
- Transforms are trivially unit-testable with pytest
- Portable across scripts, notebooks, APIs, orchestrators, distributed engines
- Column-level lineage via `@extract_columns`
- Extensive decorator system (~30+) for progressive complexity
- Battle-tested in production (Stitch Fix since 2019, IBM, Adobe, others)
- Minimal dependencies, lightweight

**Weaknesses:**
- Not an orchestrator (no scheduling, retries, sensors, compute provisioning)
- No built-in persistence or artifact management
- Cache key instability (transitive dependency changes not tracked)
- Naming-as-wiring can be fragile (renaming breaks downstream)
- Dynamic DAGs are limited (single-level Parallelizable/Collect)
- pandas/numpy as hard dependencies even when unused
- No HPC/SLURM support

### Burr

**Strengths:**
- Explicit state machine gives visibility, testability, debuggability
- Zero required dependencies (minimal core)
- Built-in tracking UI with time-travel debugging
- Streaming and human-in-the-loop are first-class
- Immutable state enables fork/resume from any historical point
- Framework-agnostic (use any LLM library, any serving layer)
- Persistence is trivial to configure

**Weaknesses:**
- Requires state machine thinking upfront (paradigm shift)
- No distributed execution built-in
- No data lineage (tracks state transitions, not data derivation)
- Recursive composition is verbose/manual
- No batching or parallel processing for data-heavy workloads
- Smaller ecosystem than LangGraph for agent workflows
- Not designed for batch data pipelines

---

## Architectural Comparison

```
                     Artisan                Hamilton               Burr
                  ┌──────────┐          ┌──────────┐          ┌──────────┐
User code         │ Class    │          │ Function │          │ Function │
                  │ subclass │          │ (plain)  │          │ or class │
                  └────┬─────┘          └────┬─────┘          └────┬─────┘
                       │                     │                     │
Graph construction     │ Explicit            │ Implicit            │ Explicit
                  ┌────▼─────┐          ┌────▼─────┐          ┌────▼─────┐
                  │ Pipeline │          │ Driver/  │          │ App      │
                  │ Manager  │          │ Builder  │          │ Builder  │
                  └────┬─────┘          └────┬─────┘          └────┬─────┘
                       │                     │                     │
Dispatch          ┌────▼─────┐          ┌────▼─────┐          ┌────▼─────┐
                  │ Dispatch │          │ In-proc  │          │ In-proc  │
                  │ Handle   │          │ DFS /    │          │ step()   │
                  │ (any     │          │ adapters │          │ iterate()│
                  │ backend) │          │          │          │          │
                  └────┬─────┘          └────┬─────┘          └────┬─────┘
                       │                     │                     │
Storage           ┌────▼─────┐          ┌────▼─────┐          ┌────▼─────┐
                  │ Delta    │          │ External │          │ Pluggable│
                  │ Lake via │          │ (SQLite  │          │ persisters│
                  │ fsspec   │          │  cache)  │          │ (SQL,S3) │
                  │(local/S3)│          │          │          │          │
                  └────┬─────┘          └────┬─────┘          └────┬─────┘
                       │                     │                     │
Lineage           ┌────▼─────┐          ┌────▼─────┐          ┌────▼─────┐
                  │ Artifact │          │ DAG =    │          │ State    │
                  │ edges    │          │ lineage  │          │ history  │
                  │ (stored) │          │ (struct) │          │ (tracked)│
                  └──────────┘          └──────────┘          └──────────┘
```

---

## Key Differentiators Summary

| What makes it unique | Artisan | Hamilton | Burr |
|---------------------|---------|----------|------|
| **Killer feature** | Content-addressed artifacts with provenance-first design | Functions-as-nodes with zero framework coupling | State machine with time-travel and streaming |
| **Paradigm bet** | Data is more valuable than code; infrastructure should be invisible | Transforms should be plain, testable functions | Interactive apps need explicit state management |
| **Where it shines** | Computational pipelines (HPC or cloud) with rich provenance | Data transformation DAGs in ML/analytics | Stateful agent loops with human-in-the-loop |
| **Where it struggles** | Interactive/output-level streaming workflows | Orchestration, artifact management | Batch data processing |

---

## Evolving Landscape: Artisan's Cloud and Streaming Trajectory

The comparison above reflects current shipped capabilities. Artisan has three
major design efforts in implementation that shift the competitive picture.

### Cloud storage (fsspec abstraction)

**Design:** `cloud-storage-design.md`

All storage operations move to fsspec. Local runs use `LocalFileSystem`,
cloud runs use `S3FileSystem` or `GCSFileSystem`. The staging code, commit
code, and store access code are identical regardless of backend. Delta Lake
and Polars already support S3/GCS URIs natively -- the framework passes URI
strings and `storage_options` dicts.

**Impact on comparison:** This eliminates the "POSIX-coupled" limitation.
Artisan's storage becomes as portable as Hamilton's (which delegates to
external systems) or Burr's pluggable persisters -- but with ACID guarantees
via Delta Lake that neither Hamilton nor Burr provides natively on cloud
storage. The worker sandbox stays local (`/tmp`), so operations never see
the storage abstraction.

### Backend-agnostic dispatch (DispatchHandle)

**Design:** `dispatch-handle.md`, `cloud-deployment.md`

`DispatchHandle` replaces Prefect coupling with a lifecycle interface:
`dispatch()` / `is_done()` / `collect()` / `cancel()`. Each backend
implements its own handle. Unit transport is handle-owned -- the backend
decides how to deliver ExecutionUnits to workers.

Planned backends:
- **Kubernetes** -- K8s Jobs with NFS PVC or S3 staging
- **Modal** -- serverless, `Function.map()` dispatch, workers run inside
  the operation's Docker image
- **AWS Batch** -- `SubmitJob` dispatch, S3 staging

**Impact on comparison:** This makes Artisan comparable to Hamilton's
backend portability (Ray, Dask, Spark) but with a key difference. Hamilton
distributes *node execution* -- individual function calls farmed out to
clusters. Artisan distributes *execution units* -- batches of artifacts
dispatched to workers running full operation lifecycles. The granularity
is different: Hamilton parallelizes at the function level, Artisan at the
batch level. Both achieve "same code, different backend."

Burr has no comparable backend abstraction -- distributed execution is
delegated entirely to the serving layer.

### Streaming pipeline execution

**Design:** `streaming_pipeline_execution.md`

Streaming in Artisan means something different from streaming in Burr.

**Burr streaming** = yielding partial output from a single action
incrementally (e.g., LLM tokens via `@streaming_action`). This is
*output-level* streaming for interactive UX.

**Artisan streaming** = independent pipeline branches flowing through
multi-step chains concurrently, with priority scheduling and resource-aware
dispatch. Downstream steps start as soon as their upstream finishes
per-branch, independent of other branches. This is *pipeline throughput*
streaming for computational workloads.

The architecture:
- **Step scheduler** -- a single long-lived thread managing all tasks.
  Dispatch loop: drain completions -> release resources -> dispatch
  highest-priority ready tasks -> poll in-flight handles -> sleep.
- **Priority scheduling** -- configurable priority function (default: stream
  depth). Later pipeline stages dispatch first, maximizing time-to-first-result.
- **Resource-aware dispatch** -- the scheduler tracks a resource budget
  (CPUs, GPUs) for resource-managed backends (local, SLURM intra-allocation)
  and dispatches in priority order within available resources. Queue-managed
  backends (SLURM sbatch, K8s, cloud) submit freely with optional priority hints.
- **Thread efficiency** -- 1 scheduler thread + ~16-32 I/O pool threads,
  regardless of task count (targeting 5000+ concurrent tasks).

**Impact on comparison:** Neither Hamilton nor Burr addresses this use case.
Hamilton's parallelism is node-level within a single DAG execution -- it
doesn't handle many independent branches streaming through different
operations with different resource requirements. Burr's streaming is about
incremental output, not pipeline throughput. Artisan's streaming design
targets the specific case of hundreds to thousands of independent work
items flowing through multi-step computational chains -- common in
scientific computing (protein design, molecular simulation, genomics) but
also relevant to large-scale ML (hyperparameter sweeps, model evaluation
matrices, batch inference pipelines).

### What changes in the comparison

| Dimension | Before | After |
|-----------|--------|-------|
| Artisan's backend story | POSIX-coupled, Prefect-dependent | Backend-agnostic (local, SLURM, K8s, Modal, AWS Batch) |
| Artisan's storage story | Local filesystem only | fsspec (local, S3, GCS) with ACID Delta Lake |
| Artisan's streaming story | None | Branch-level streaming with priority + resource awareness |
| "Where it struggles" | Cloud, streaming | Output-level streaming (not pipeline throughput streaming) |
| Comparison to Hamilton backends | Hamilton portable, Artisan locked to SLURM | Both portable, different parallelism granularity |

The net effect: Artisan's domain expands from "HPC batch pipelines" to
"computational pipelines on any infrastructure." The ceremony tradeoff
remains -- Artisan asks more of the developer upfront (classes, specs,
lifecycle) in exchange for provenance, caching, and backend portability
that the developer doesn't have to build themselves.

---

## Could They Work Together?

**Hamilton inside Artisan operations:** An Artisan Creator could use Hamilton
internally to define its data transformation logic as functions, benefiting from
Hamilton's testability while Artisan handles artifact lifecycle, provenance, and
dispatch (local, SLURM, or cloud). The Creator's `execute()` method would build
a Hamilton Driver and call `dr.execute()`. Hamilton handles the in-process data
transforms; Artisan handles everything around them (batching, storage,
provenance, infrastructure).

**Burr orchestrating Artisan pipelines:** A Burr state machine could orchestrate
multi-stage Artisan pipelines where the decision of *which* pipeline to run next
depends on results from previous runs. Burr handles the stateful decision logic
(including human-in-the-loop checkpoints); Artisan handles the heavy
computation. With Artisan's streaming design, Burr could monitor per-branch
completion events and make adaptive decisions about what to run next.

**Hamilton inside Burr actions:** This is the documented integration pattern.
Burr actions call Hamilton for stateless transforms, while Burr manages the
stateful control flow between them.

**The three-layer stack:** For complex computational workflows with interactive
decision-making, all three could compose naturally:
- **Burr** = outer loop (stateful decisions, human checkpoints, adaptive control)
- **Artisan** = pipeline execution (provenance, caching, dispatch to any backend)
- **Hamilton** = in-operation transforms (testable functions, column-level lineage)

All three occupy distinct enough niches that composition is more natural than
competition.
