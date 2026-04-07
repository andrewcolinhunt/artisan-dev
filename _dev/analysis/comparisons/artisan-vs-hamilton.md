# Artisan vs Apache Hamilton: Detailed Comparison

## Executive Summary

Artisan and Hamilton are both Python DAG frameworks, but they occupy fundamentally
different niches. Hamilton is a **micro-orchestrator** — a lightweight library for
defining fine-grained, function-level dataflows that run in-memory. Artisan is a
**batch pipeline framework** — a heavier system for orchestrating multi-step
scientific computations on HPC clusters with per-artifact provenance and
persistent storage.

They are **more complementary than competing**. Hamilton could run *inside* an
Artisan operation's `execute()` method to structure the internal computation.
Artisan provides the outer pipeline structure, provenance tracking, artifact
persistence, and HPC dispatch that Hamilton explicitly does not handle.

---

## Programming Model

| Dimension | Artisan | Hamilton |
|-----------|---------|----------|
| **Unit of computation** | Class (subclass `OperationDefinition`) | Function (plain Python function) |
| **Dependency declaration** | Explicit wiring via `output(step, role)` references | Implicit via function parameter names matching function names |
| **DAG construction** | Imperative: `pipeline.run()` / `pipeline.submit()` calls | Declarative: DAG inferred from function signatures at build time |
| **Pipeline definition** | Script calling `PipelineManager` methods | Module(s) of functions passed to `Driver.Builder()` |
| **Configuration** | `Params(BaseModel)` nested class on operations | `@config.when` decorators + config dict on Builder |
| **Type system** | `InputSpec` / `OutputSpec` with artifact types, materialization modes, hydration modes | Python type annotations on function signatures |
| **Code reuse** | `CompositeDefinition` (reusable op compositions) | `@subdag`, `@parameterize`, module swapping |
| **Framework coupling** | Operations import framework types (`PreprocessInput`, `ArtifactResult`, etc.) | Transform functions are pure Python — zero Hamilton imports |

**Key insight**: Hamilton's "function name = node name = output name" convention
is elegant for in-memory dataflows where every value has a single name. Artisan's
class-based model is heavier but necessary for operations that produce *batches*
of typed artifacts (e.g., 10,000 DataArtifacts from one step), each with their
own identity and provenance.

---

## Data Model

| Dimension | Artisan | Hamilton |
|-----------|---------|----------|
| **Core data abstraction** | Content-addressed `Artifact` objects with types (Data, Metric, FileRef, Config) | Any Python object — no framework abstraction |
| **Identity** | `artifact_id` = xxh3_128 hash of content bytes | Node name (function name) |
| **Batch support** | First-class: one step produces/consumes lists of artifacts | Not native: one function produces one value (fan-out via `Parallelizable` is separate) |
| **Schema** | `InputSpec` / `OutputSpec` declare types, materialization, hydration | Type annotations only; `@schema` decorator for DataFrame metadata |
| **Validation** | Spec-level (required roles, type matching, batch size consistency) | `@check_output` decorator (datatype, range, null checks, Pandera/Pydantic) |
| **Extensibility** | Register new `ArtifactTypeDef` subclasses | N/A — any Python type is valid |

**Key insight**: Artisan's artifact model adds overhead but enables content-addressed
deduplication, per-artifact provenance edges, and persistent queryable storage.
Hamilton's "any Python object" model is zero-overhead but provides no identity or
persistence guarantees — those are the user's responsibility.

---

## Execution Model

| Dimension | Artisan | Hamilton |
|-----------|---------|----------|
| **Default execution** | ProcessPool via Prefect (multi-process) | Single-threaded Python |
| **HPC support** | Native SLURM backend via submitit | None — not in scope |
| **Cloud/distributed** | Extensible `BackendBase` (SLURM, local; cloud planned) | Ray, Dask, Spark via GraphAdapters |
| **Parallelism model** | Two-level batching (artifacts-per-unit × units-per-worker) | `Parallelizable[T]` / `Collect[T]` fan-out/fan-in |
| **Isolation** | Full sandbox: each execution gets its own directory tree (preprocess/, execute/, postprocess/) | None — functions share process state |
| **Async support** | `pipeline.submit()` returns `StepFuture` | `AsyncDriver` for async/await |
| **Worker safety** | Staging-commit pattern prevents concurrent write corruption on shared NFS | N/A — designed for single-machine or managed distributed systems |

**Key insight**: They target opposite ends of the execution spectrum. Hamilton
scales *out* to cloud-native distributed systems (Ray/Dask/Spark) where nodes
communicate via object stores and serialization. Artisan scales *out* to HPC
clusters where workers share a parallel filesystem and coordination happens via
file staging and Delta Lake commits.

---

## Provenance and Lineage

| Dimension | Artisan | Hamilton |
|-----------|---------|----------|
| **Lineage granularity** | Per-artifact within batches | Per-node (function-level) |
| **Lineage mechanism** | Declared via `OutputSpec.infer_lineage_from` + filename stem matching or explicit `LineageMapping` | Implicit from DAG structure (parameter names → function names) |
| **Storage** | Persistent: `ArtifactProvenanceEdge` and `ExecutionEdge` tables in Delta Lake | Transient: exists in-memory as the DAG; persisted only via Hamilton UI |
| **Query model** | Forward/backward BFS walks via Polars DataFrames; `walk_forward()`, `walk_backward()` | Visual DAG inspection; `visualize_path_between()` |
| **Multi-input tracking** | `group_id` on provenance edges links co-input artifacts from multi-input operations | Not applicable — each node has a single output |
| **W3C PROV alignment** | Yes — execution provenance (activity) + artifact provenance (derivation) | No formal standard alignment |
| **Visualization** | Graphviz graphs at both macro (step-level) and micro (artifact-level) | Graphviz DAG visualization (always in sync with code) |

**Key insight**: This is Artisan's strongest differentiator. Hamilton's lineage
is the DAG itself — you can trace from any function back through its parameter
chain. But Artisan tracks which *specific input artifact* produced which
*specific output artifact* within a batch, with persistent edges queryable after
execution. This is critical for scientific reproducibility ("which of my 50,000
samples led to this anomalous metric?").

---

## Caching

| Dimension | Artisan | Hamilton |
|-----------|---------|----------|
| **Cache key** | `execution_spec_id` = hash(operation_name + sorted input artifact_ids + merged params) | `(node_name, code_version, data_version_of_dependencies)` |
| **Content addressing** | `artifact_id` = xxh3_128(content bytes) — same content always produces same ID | Code hash (SHA-256 of function source) + recursive data fingerprinting |
| **Code versioning** | Not tracked separately — params and input IDs fully determine the cache key | Yes — function source is hashed; code change invalidates cache |
| **Granularity** | Per-step (entire operation execution) | Per-node (individual function) |
| **Storage** | Delta Lake `executions` table | Local `.hamilton_cache/` directory (metadata + result stores) |
| **Limitation** | Does not detect internal code changes (same inputs → cache hit even if operation logic changed) | Does not detect changes in nested/helper function calls |

**Key insight**: Both have a meaningful caching blind spot. Artisan doesn't hash
operation source code, so changing an operation's logic without changing its
inputs/params hits a stale cache. Hamilton hashes the top-level function source
but not called utility functions. Hamilton's per-node granularity is finer — it
can skip individual unchanged nodes within a dataflow, while Artisan caches at
the whole-step level.

---

## Storage and Persistence

| Dimension | Artisan | Hamilton |
|-----------|---------|----------|
| **Built-in persistence** | Delta Lake tables for all artifacts, provenance, executions, metrics | None — cache directory only; I/O via `@load_from` / `@save_to` decorators |
| **Query model** | Polars / DuckDB scans over Delta tables; `inspect_pipeline()`, `inspect_metrics()` | Return dict from `driver.execute()` |
| **Deduplication** | Automatic via content-addressed artifact IDs | None |
| **Crash recovery** | `PipelineManager.resume()` + `DeltaCommitter.recover_staged()` | None built-in |
| **Result format** | Structured: typed artifact tables with schemas | Unstructured: dict of `{node_name: value}` or DataFrame assembly via ResultBuilder |

**Key insight**: Artisan treats storage as a first-class concern — Delta Lake is
the single source of truth, and all results are structured, queryable, and
persistent by default. Hamilton treats storage as the user's problem — it's a
computation framework, not a data management framework.

---

## Composability and Reuse

| Dimension | Artisan | Hamilton |
|-----------|---------|----------|
| **Composition unit** | `CompositeDefinition` class with `compose(ctx)` method | Python modules passed to `Builder.with_modules()` |
| **Execution modes** | Collapsed (single worker, in-memory) vs Expanded (multi-step, independent caching) | Single mode — all nodes execute in the same graph |
| **Module swapping** | Not native — operations are classes | Native — pass different modules to Builder for different contexts (train vs serve) |
| **Parameterization** | Operation `Params(BaseModel)` | `@parameterize` family of decorators |
| **Code reuse pattern** | Subclass operations, compose via composites | `@subdag` for sub-DAG embedding, `@parameterize` for DRY |
| **Nesting** | Composites can nest (composite containing composites) | Subdags can nest |

---

## Where They Compete

These frameworks compete in a narrow overlap zone:

- **Single-machine Python pipelines with caching**: Both can run multi-step
  pipelines locally with cached results. Hamilton is lighter and faster to set
  up. Artisan provides more structure and persistence.

- **Provenance-tracked transformations**: Both track lineage, but at different
  granularities. For simple "which function produced this?" questions, Hamilton's
  implicit DAG lineage is simpler. For "which specific input sample produced
  this specific metric?", Artisan is necessary.

- **DAG visualization**: Both generate Graphviz graphs. Hamilton's are derived
  directly from code (always in sync). Artisan's exist at two levels (macro/micro).

---

## Where They Are Complementary

**Hamilton inside Artisan operations**: An Artisan operation's `execute()` method
could use Hamilton to structure its internal computation. The operation handles
I/O (preprocess reads artifacts, postprocess writes them), and Hamilton handles
the in-memory transformation DAG.

```python
class FeatureEngineering(OperationDefinition):
    def execute(self, inputs: ExecuteInput) -> Any:
        import hamilton
        dr = hamilton.driver.Builder().with_modules(my_features).build()
        return dr.execute(["final_features"], inputs={"raw": inputs.data})
```

**Artisan provides what Hamilton lacks**:
- Persistent, queryable artifact storage (Delta Lake)
- Per-artifact provenance within batches
- HPC cluster execution (SLURM)
- Crash recovery and resume
- Content-addressed deduplication
- Staging-commit pattern for filesystem safety

**Hamilton provides what Artisan lacks**:
- Lightweight, zero-boilerplate function definitions
- Implicit DAG construction from function signatures
- Cloud-native distributed execution (Ray, Dask, Spark)
- Rich decorator system for DRY code reuse
- Module-swapping for context-dependent behavior (train vs serve)
- Built-in data quality checks (`@check_output`)
- LLM/ML feature engineering patterns

**Hamilton runs inside macro-orchestrators**: Hamilton explicitly positions
itself as running *inside* Airflow, Dagster, Prefect. Similarly, it could run
inside Artisan. Artisan already uses Prefect as a dispatch layer — Hamilton could
be the computation layer within individual Prefect tasks.

---

## Philosophical Differences

| Principle | Artisan | Hamilton |
|-----------|---------|----------|
| **"What is a pipeline?"** | A sequence of heavyweight operations producing persistent, typed, content-addressed artifacts | A graph of lightweight functions producing in-memory Python values |
| **"Where does lineage live?"** | In persistent Delta Lake tables, queryable after execution | In the code itself — the DAG IS the lineage |
| **"Who manages state?"** | The framework (Delta Lake, staging, commit) | The user (or downstream tools) |
| **"What's the deployment unit?"** | A pipeline script + SLURM cluster + shared filesystem | A Python module + any execution environment |
| **"What problem are we solving?"** | "I can't reproduce my HPC results or trace which input caused this output" | "My transformation code is tangled, untestable, and I can't see the dataflow" |

---

## Summary Table

| Capability | Artisan | Hamilton |
|------------|---------|----------|
| Function-level DAG | No (step-level) | **Yes** |
| Per-artifact provenance | **Yes** | No |
| Content-addressed storage | **Yes** | No |
| HPC/SLURM execution | **Yes** | No |
| Cloud distributed (Ray/Dask/Spark) | No | **Yes** |
| Crash recovery / resume | **Yes** | No |
| Zero-boilerplate definitions | No (class-based) | **Yes** (functions) |
| Built-in persistence | **Yes** (Delta Lake) | No |
| Decorator system | No | **Yes** (rich) |
| Data quality checks | No | **Yes** (`@check_output`) |
| Module swapping | No | **Yes** |
| Batch artifact processing | **Yes** | No (single values) |
| Queryable results | **Yes** (Polars/DuckDB) | No (in-memory dict) |
| LLM/ML patterns | No | **Yes** |
| Composite operations | **Yes** (collapsed/expanded) | **Yes** (`@subdag`) |
| Caching | **Yes** (step-level) | **Yes** (node-level) |

---

## Recommendations

**If considering Hamilton integration with Artisan:**

- **Short-term**: Use Hamilton inside operation `execute()` methods for
  complex in-memory transformations. This gets Hamilton's testability and
  visualization benefits within Artisan's provenance and storage framework.

- **Medium-term**: Consider whether Hamilton's decorator patterns (especially
  `@parameterize`, `@config.when`, `@extract_columns`) suggest improvements
  to Artisan's operation definition model — could operations be lighter?

- **Long-term**: Hamilton's cloud-native execution adapters (Ray, Dask) could
  inform Artisan's planned cloud compute backends, though the execution models
  are different enough that direct adaptation is unlikely.

**What NOT to adopt from Hamilton:**

- The "function name = dependency" convention doesn't scale to batch artifact
  processing where a single step produces thousands of individually-tracked
  outputs.

- Hamilton's lack of built-in persistence is a feature for its use case
  (lightweight dataflows) but would be a regression for Artisan's
  (reproducible science).
