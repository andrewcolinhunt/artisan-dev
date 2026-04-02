# Hamilton's Cloud Distributed Execution: Deep Analysis

An analysis of how Apache Hamilton adapts its function-level DAG framework to
distributed cloud backends (Ray, Dask, Spark), with lessons for Artisan's
planned cloud compute support.

---

## Hamilton's Execution Architecture

Hamilton has two generations of execution abstraction. Understanding both is
important because they represent different answers to the same problem Artisan
faces: how do you decouple computation logic from execution backends?

### V1: The GraphAdapter Pattern

The `GraphAdapter` is Hamilton's original extension point. It intercepts how the
Driver walks and executes its DAG via lifecycle methods:

```
do_node_execute(run_id, node_, kwargs, task_id) -> Any
```

- **`node_`** — contains the callable, name, type, tags, input_types
- **`kwargs`** — resolved keyword arguments (outputs from upstream nodes)
- **Returns** — a concrete value OR a future/delayed object

The Driver walks the DAG in DFS order. For each node, it calls
`do_node_execute`. The adapter decides *how* to execute: locally, on Ray, on
Dask, etc. When an adapter returns a future (Ray `ObjectRef`, Dask `Delayed`),
the framework passes that future as-is to downstream nodes. The backend resolves
dependencies internally.

Additional adapter methods handle type bridging:
- `check_node_type_equivalence` — relaxes static type checks (e.g., treats
  `pd.DataFrame` and `dask.DataFrame` as equivalent)
- `check_input_type` — accepts `ObjectRef` where the declared type is `pd.Series`

### V2: TaskBasedGraphExecutor

The newer architecture (requires `enable_dynamic_execution(allow_experimental_mode=True)`)
introduces **task grouping** — nodes are grouped into discrete execution units
between serialization boundaries.

```python
dr = (
    driver.Builder()
    .with_modules(my_module)
    .enable_dynamic_execution(allow_experimental_mode=True)
    .with_local_executor(executors.SynchronousLocalTaskExecutor())
    .with_remote_executor(executors.MultiProcessingExecutor(max_tasks=5))
    .build()
)
```

Key changes from V1:

| Aspect | V1 (GraphAdapter) | V2 (TaskBasedGraphExecutor) |
|--------|--------------------|-----------------------------|
| Execution unit | Individual node | Group of nodes (task) |
| Parallelism | Implicit via futures | Explicit via `Parallelizable[T]` / `Collect[T]` |
| Serialization boundary | Every node | Only at task boundaries |
| Dynamic DAGs | Not supported | Supported (runtime node generation) |
| Executor config | Single adapter | Separate local + remote executors |

The `TaskExecutor` interface:

| Method | Purpose |
|--------|---------|
| `submit_task(TaskImplementation) -> TaskFuture` | Submit a group of nodes |
| `can_submit_task() -> bool` | Backpressure check |

V2's task grouping directly addresses V1's biggest problem: per-node
serialization overhead. Nodes within a task group execute in-process (no
serialization between them); only inputs/outputs at task boundaries cross
serialization boundaries.

### Lifecycle Hooks

Hamilton provides fine-grained hooks at every execution phase. Multiple hooks
can be stacked (e.g., logging + telemetry simultaneously), but only one
"method" per phase (e.g., one way to actually execute a node):

- **Pre/post graph execution** — setup/teardown
- **Pre/post node execution** — per-node observability
- **Pre/post task submission** (V2 only) — per-task-group tracking
- **Task grouping hook** — customize how nodes are grouped into tasks

---

## Backend-Specific Integrations

### Ray Integration

**Mechanism**: The `RayGraphAdapter` wraps every Hamilton function with
`ray.remote`, returning `ObjectRef` futures instead of values.

**DAG partitioning**: Every Hamilton function = one Ray task. No grouping, no
fusion, no subgraph partitioning. A DAG with 1000 functions produces 1000 Ray
tasks.

**Data flow**:
- Hamilton walks the DAG sequentially on the driver, submitting Ray tasks eagerly
- Downstream nodes receive `ObjectRef` as kwargs; Ray auto-resolves when the
  task actually executes
- Results live in Ray's distributed object store (Plasma)
- Final `ray.get()` happens only when collecting results at the end

**Serialization**: Entirely Ray's cloudpickle. Functions, arguments, and results
are all serialized through Ray's protocol. Non-serializable objects (DB
connections, API clients) break the adapter entirely.

**Failure handling**: None at the Hamilton level. Ray's built-in retries
(`max_retries`) and object reconstruction are the only mechanisms. A single node
failure propagates to all downstream nodes. No partial result recovery.

**Caching**: Broken in distributed mode. Hamilton's cache stores metadata and
results on local disk (`.hamilton_cache/`). Worker A's cache is invisible to
Worker B. The Hamilton docs explicitly acknowledge this: "For distributed
execution, Hamilton will need ResultStore and MetadataStore that can be remote
and are safe for concurrent access."

**Key limitation**: The per-node granularity means small functions pay
disproportionate serialization overhead. Hamilton's docs warn: "Serialization
costs can outweigh the benefits of parallelism."

### Dask Integration

**Mechanism**: The `DaskGraphAdapter` has two modes:

- **`use_delayed=True` (default)**: Every function is wrapped with `dask.delayed()`.
  The DAG walk builds a tree of `Delayed` objects. No computation happens until
  `.compute()` is called at the end, submitting the entire Dask task graph in one
  shot.

- **`use_delayed=False`**: Functions execute eagerly on the driver, but operate on
  Dask DataFrames (which are themselves lazy). Parallelism comes from Dask's
  DataFrame layer, not from wrapping functions.

**Two simultaneous DAGs**: When `use_delayed=True`, Hamilton's walk produces
both Hamilton's function-level DAG and Dask's isomorphic task graph. The Dask
scheduler optimizes and executes the latter.

**Data locality**: Entirely delegated to Dask's scheduler (work-stealing +
locality heuristics). Hamilton provides zero control over task placement, worker
affinity, or data partitioning.

**Serialization**: Dask's cloudpickle. Same limitations as Ray — non-serializable
objects break the pipeline.

**Failure handling**: Same as Ray — entirely delegated to Dask. No partial
results, no Hamilton-level retries.

**Caching**: Same fundamental incompatibility. `CachingGraphAdapter` and
`DaskGraphAdapter` cannot be combined because one expects concrete values and
the other returns `Delayed` objects.

### Spark Integration

Spark is the most architecturally interesting because it required Hamilton to
solve a fundamental mismatch: function-per-node vs. DataFrame-centric
execution. Hamilton provides three approaches, each representing a different
compilation strategy:

#### Approach A: SparkKoalasGraphAdapter (API compatibility)

Exploits Spark 3.2+'s Pandas API on Spark. Hamilton functions written for pandas
receive pandas-on-Spark objects instead. Data stays distributed; the Pandas API
is implemented on top of Spark's execution engine.

- **Pro**: Zero code changes to existing Hamilton functions
- **Con**: Only ~70% of pandas API is implemented; failures are runtime, not static

#### Approach B: PySparkUDFGraphAdapter (per-node wrapping)

Maps Hamilton functions to Spark UDFs. Function parameter names match DataFrame
column names. Each function becomes a UDF appended as a new column.

- **Pro**: Direct mapping from Hamilton functions to Spark operations
- **Con**: Only map operations (no joins/aggregations/filters); experimental

#### Approach C: `@with_columns` Decorator (sub-DAG compilation)

The most sophisticated approach. Takes a sub-DAG of column-level Hamilton
functions and **compiles it into a chain of `.withColumn()` calls** on a single
Spark DataFrame.

This solves the core antipattern: in Pandas, extracting columns, transforming
them independently, and joining back is cheap. In Spark, it's catastrophically
expensive (requires data shuffling). `@with_columns` avoids this by linearizing
the sub-DAG into sequential `.withColumn()` operations on a single DataFrame.

- **Pro**: Preserves Hamilton's per-column lineage while generating efficient
  Spark execution plans. Column expressions (`pyspark.sql.Column`) stay entirely
  in the JVM — zero serialization.
- **Con**: Only works for cardinality-preserving operations (row-wise maps).
  Joins, aggregations, and filters must be separate Hamilton functions operating
  on PySpark DataFrames directly.

**Key architectural insight**: The `@with_columns` approach demonstrates that
the most efficient backend adaptation changes the granularity of execution.
Individual DAG nodes are too fine-grained for Spark, so Hamilton compiles
multiple nodes into a single backend operation while preserving the fine-grained
abstraction for the user.

---

## Cross-Cutting Concerns

### Serialization: The Universal Bottleneck

Every Hamilton distributed backend has the same fundamental problem: serialization
at node boundaries. The patterns:

| Backend | Serialization protocol | Per-node overhead |
|---------|----------------------|-------------------|
| Ray | cloudpickle + Arrow for numpy/pandas | High for small functions |
| Dask | cloudpickle + MsgPack metadata | High for small functions |
| Spark UDF | Arrow (Pandas UDF) or pickle (regular UDF) | Moderate to high |
| Spark `@with_columns` | Arrow or zero (Column expressions) | Low to zero |

Hamilton's V2 task grouping mitigates this by reducing the number of
serialization boundaries. But the fundamental tension remains: fine-grained DAGs
produce high-quality lineage but expensive distributed execution.

### Caching: Not Solved for Distributed Execution

Hamilton's caching is local-only:
- **Cache key**: `(node_name, code_version, data_version_of_dependencies)`
- **Storage**: Local `.hamilton_cache/` directory
- **Problem**: Workers on different machines have independent caches

The docs explicitly acknowledge this as an open problem. There are no
remote-capable, concurrency-safe cache stores in the current architecture.

### Failure Handling: Fully Delegated

Hamilton provides no cross-backend failure handling:
- No partial result recovery
- No Hamilton-level retries
- No circuit-breaking or fallback values
- A single node failure cascades to all downstream nodes
- The entire `execute()` call fails

All resilience is delegated to the underlying backend (Ray retries, Dask worker
recovery, Spark task retry). This keeps Hamilton simple but means users get
different failure semantics depending on their backend.

### Lineage: Structural, Not Behavioral

Hamilton's lineage is derived from the code structure (function parameter names
= dependencies), not from execution. This means:
- Lineage is identical whether running locally or on a 100-node Ray cluster
- No per-execution provenance recording (which worker, when, with what resources)
- No artifact-level tracking within batches
- The Hamilton UI adds execution observability but it's a separate product

---

## Patterns and Anti-Patterns for Artisan

### Patterns Worth Learning From

**The adapter/executor interface is clean and minimal.** Hamilton's
`GraphAdapter` has a very small surface area (`do_node_execute` + type
bridging). This maps well to Artisan's `BackendBase` (which is similarly
minimal: `create_flow` + `capture_logs` + traits).

**V2 task grouping reduces serialization overhead.** Instead of one task per
node, group nodes into execution units with serialization only at group
boundaries. Artisan already does this with two-level batching
(artifacts-per-unit x units-per-worker). The pattern is validated.

**The `@with_columns` compilation strategy is architecturally profound.** It
demonstrates that the right abstraction level for the user (per-column
functions) and the right execution granularity for the backend (one DataFrame
operation) can differ — and a compilation step can bridge them. This is exactly
what Artisan's composites do: the user defines per-operation logic, but
collapsed execution compiles them into a single worker invocation.

**Type bridging at the adapter level.** Hamilton's adapters override type
checking to bridge between declared types and runtime types (e.g., `pd.DataFrame`
↔ `dask.DataFrame`). Any Artisan cloud backend will need similar bridging for
path types (`pathlib.Path` ↔ S3 URIs).

### Anti-Patterns to Avoid

**Per-node granularity without fusion.** Hamilton's Ray/Dask adapters submit
every function as a separate distributed task. For lightweight functions, the
serialization overhead dominates. Artisan's `ExecutionUnit` batching avoids this
by grouping multiple artifacts into a single unit.

**Local-only caching that breaks in distributed mode.** Hamilton's cache stores
are local filesystem only. Artisan's content-addressed caching via Delta Lake
already solves this — Delta Lake tables are readable from any node with access
to the storage backend (local, NFS, S3, GCS).

**No framework-level failure handling.** Delegating all resilience to the
underlying backend leaves Hamilton unable to handle partial failures gracefully.
Artisan's existing failure policy (`FailurePolicy` on operations) and
per-execution-unit result tracking already provide finer-grained failure
handling.

**No execution provenance in distributed mode.** Hamilton's lineage is
structural only — there's no record of which worker ran what. Artisan's
staging-commit pattern preserves full execution records (including worker IDs,
timing, and artifact provenance edges) regardless of which backend dispatches
the work.

**Futures passed through without framework awareness.** When Ray/Dask adapters
return futures, Hamilton's framework has no visibility into what's happening.
The framework can't implement timeouts, progress tracking, or cancellation.
Artisan's planned `DispatchHandle` refactor (from the existing design doc)
addresses this with explicit `dispatch()`, `is_done()`, `collect()`, `cancel()`
methods.

---

## Implications for Artisan's Cloud Backend Design

Cross-referencing with the existing `cloud_compute_backends.md` design analysis,
Hamilton's experience validates several of Artisan's planned decisions:

### Artisan is already ahead on several fronts

| Concern | Hamilton | Artisan |
|---------|----------|---------|
| Distributed caching | Broken (local-only) | Works (Delta Lake is storage-agnostic) |
| Execution provenance | None (structural lineage only) | Per-artifact edges preserved across backends |
| Failure handling | None (delegated to backend) | Per-unit result tracking + failure policies |
| Serialization overhead | Per-node (V1) or per-task-group (V2) | Per-ExecutionUnit batching |
| Unit serialization | In-memory Python objects | Pickle files on filesystem (transportable) |

### Validated decisions from the cloud design doc

**Model A (shared filesystem) is the path of least resistance.** Hamilton
doesn't offer this because it's not designed for HPC, but Kubernetes + NFS PVC,
Modal + Volumes, and EFS-mounted AWS Batch all provide shared filesystems. The
existing staging-commit pattern works unmodified in these environments.

**Model B (object storage + local sandbox) is the right next step.** Hamilton's
approach (pass futures, let the backend handle data transfer) is elegant but
loses framework-level control. Artisan's proposed `TransferStrategy` abstraction
(upload/download units and staged files via S3/GCS) is a better fit because it
preserves the staging-commit pattern's safety guarantees.

**The `DispatchHandle` refactor is critical.** Hamilton's callable-return
pattern (`create_flow()` returns a callable) is essentially what Artisan has
now. Hamilton's experience shows this is too opaque for cloud backends that
need polling, cancellation, and progress tracking. The proposed handle with
`dispatch()`, `is_done()`, `collect()`, `cancel()` is the right evolution.

### New lessons from Hamilton's experience

**Serialization is THE bottleneck for distributed execution.** Artisan's pickle
files for `ExecutionUnit` transport are already on the right side of this —
they serialize once per unit, not per artifact. But cloud backends add a new
serialization boundary: uploading/downloading the pickle file itself. The design
should ensure this is a single transfer per unit, not per artifact.

**The Spark `@with_columns` pattern validates composites.** Hamilton had to
invent sub-DAG compilation for Spark efficiency. Artisan already has this via
collapsed composites (multiple operations compiled into a single worker
invocation). This is a genuine architectural advantage for cloud backends where
minimizing cross-machine communication is critical.

**Type bridging will be needed for cloud paths.** Hamilton relaxes `pd.DataFrame`
↔ `dask.DataFrame`. Artisan will need to relax `pathlib.Path` ↔ `str` (URI)
throughout `RuntimeEnvironment`, `PipelineConfig`, `StagingArea`, and
`DeltaCommitter`. This is the "High severity" gap identified in the design doc.

**Consider two executor modes, not just swappable backends.** Hamilton's V2
distinction between local executor (main thread, cheap operations) and remote
executor (distributed, expensive operations) maps to Artisan's creator/curator
split. Curators already run locally on the orchestrator; creators dispatch to
backends. This existing separation is a strength that cloud backends should
preserve, not flatten.

---

## Summary

Hamilton's distributed execution is best understood as a **thin adapter layer**
that delegates nearly everything to the underlying backend. This keeps Hamilton
simple but leaves significant gaps (caching, failure handling, provenance) that
users must solve themselves.

Artisan's architecture is fundamentally better positioned for cloud execution
because it already solves the hard problems Hamilton punts on:
- Content-addressed artifacts decouple identity from storage location
- Delta Lake provides a distributed, queryable persistence layer
- The staging-commit pattern separates worker writes from storage transactions
- Per-artifact provenance edges are captured during execution, not inferred from code
- Two-level batching reduces serialization boundaries
- Failure policies provide per-unit error handling

The primary work for Artisan's cloud backends is in the **storage and data
transfer layer** (paths → URIs, filesystem ops → object store ops), not in the
execution abstraction itself. Hamilton's experience confirms this — their
execution adapter is clean and minimal, but they never solved the storage
problem because they don't have one (Hamilton has no built-in persistence).
