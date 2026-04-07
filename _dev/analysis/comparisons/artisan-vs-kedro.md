# Artisan vs Kedro: Provenance & Artifact Tracking Comparison

## Executive Summary

Artisan and Kedro are both Python pipeline frameworks, but they represent fundamentally different philosophies on data management and provenance. Kedro (QuantumBlack / McKinsey, now Linux Foundation) is a **project-templating and data-catalog framework** that standardizes how data science code is structured, how datasets are declared and loaded, and how pipelines are composed from pure functions. Artisan is a **batch computation framework** built around content-addressed artifacts, dual-level provenance tracking, and HPC-native execution. Kedro's strength is developer experience, project conventions, and deployment breadth; artisan's is provenance depth, deterministic caching, and batch-native execution on shared filesystems.

---

## Provenance & Lineage

| Dimension | Artisan | Kedro |
|-----------|---------|-------|
| **Structural lineage** | Dual: macro (step DAG) + micro (per-artifact derivation chains) | Node DAG only (dataset-name edges between nodes) |
| **Runtime provenance** | ExecutionRecord per execution: timing, parameters, worker, errors, inputs/outputs with artifact IDs | Session store: timestamp, git user/branch, run metadata. No per-artifact runtime record. |
| **Per-artifact derivation** | ArtifactProvenanceEdge links individual input artifact to individual output artifact (A->D, B->E, C->F within a batch) | Not tracked. Lineage is structural: "node X produced dataset Y". No per-row or per-item tracking. |
| **Provenance standard** | W3C PROV-aligned: ExecutionRecord=Activity, Artifact=Entity, edge=wasDerivedFrom | No formal provenance standard. Metadata captured via hooks and session store. |
| **Capture mechanism** | Automatic and structural. Provenance edges are produced as a side effect of execution -- not opt-in. | Structural DAG is automatic. Runtime metadata requires session store configuration (SQLite) and Kedro-Viz. |
| **Visualization** | Graphviz-based micro/macro provenance graphs. Can trace any artifact back to its full derivation chain. | Kedro-Viz: interactive web UI showing pipeline DAG, dataset metadata, node status, and execution timing. |

**Analysis**: Kedro's lineage is the pipeline DAG itself -- if node A outputs `cleaned_data` and node B consumes `cleaned_data`, that structural edge is the lineage. This is sufficient for understanding data flow at the pipeline level, but there is no mechanism to ask "which specific input rows produced which specific output rows" or to trace individual artifacts through a batch operation. Artisan's dual provenance system captures both the macro DAG and the micro derivation chains, at the cost of additional storage and complexity.

Kedro's experiment tracking (via Kedro-Viz + session store) records run-level metadata -- timestamps, git branch, metrics over time -- but this is closer to MLflow-style experiment tracking than W3C PROV-style provenance. There are no immutable, content-addressed edges linking specific data versions to specific outputs.

---

## Data Catalog vs Typed Artifacts

| Dimension | Artisan | Kedro |
|-----------|---------|-------|
| **Core abstraction** | Content-addressed `Artifact` with typed subtypes (Data, Metric, FileRef, Config, ExecutionConfig) | Named dataset entry in `catalog.yml` with configurable backend type |
| **Identity** | `artifact_id` = xxh3_128(content). Identity IS the content. | Dataset name (string). Identity is the catalog key. |
| **Declaration** | `InputSpec` / `OutputSpec` on operations define typed roles | YAML in `catalog.yml`: name, type class, filepath, load/save args |
| **Backend flexibility** | Delta Lake tables (single storage backend, queryable) | 100+ dataset types: Pandas CSV/Parquet, Spark, SQL, Pickle, JSON, GBQ, Plotly, custom |
| **Versioning** | Implicit via content addressing. Same content = same artifact ID, always. | Timestamp-based: `versioned: true` creates `path/<YYYY-MM-DDThh.mm.ss.sssZ>/file` subdirectories |
| **Deduplication** | Automatic. Same content produces same hash, stored once. | None. Each version is a full copy in a timestamped directory. |
| **Intermediate data** | All artifacts persisted to Delta Lake with provenance | Unregistered outputs stored as `MemoryDataset` (in-memory only, GC'd after use). Must explicitly register in catalog to persist. |
| **Typing** | Structural: artifact type determines serialization, hydration, and storage behavior | Structural: dataset type class determines load/save behavior |
| **Extensibility** | Register new `ArtifactTypeDef` subclasses | Implement `AbstractDataset` (load/save/describe) or use existing 100+ types |
| **Transcoding** | N/A (single storage format) | Supported: `my_data@spark` and `my_data@pandas` refer to same physical file, loaded differently |

**Analysis**: These represent genuinely different design philosophies. Kedro's Data Catalog is a powerful I/O abstraction layer -- it decouples pipeline logic from storage format, so switching from CSV to Parquet to S3 is a YAML change. The 100+ dataset types and transcoding support give it broad flexibility. However, it provides no content-based identity, no automatic deduplication, and no immutable provenance guarantees. Two runs producing identical results create two separate versioned copies.

Artisan's artifact model is narrower (Delta Lake only) but deeper. Content addressing provides identity, deduplication, and cache-key computation in one mechanism. The trade-off is reduced backend flexibility -- you cannot point an artifact at an existing Spark table or S3 parquet file the way Kedro can.

The intermediate data handling also differs significantly. Kedro's `MemoryDataset` pattern means intermediate results vanish unless explicitly cataloged, which is memory-efficient but means pipeline debugging requires re-running. Artisan persists everything, which costs storage but enables post-hoc querying and full provenance reconstruction.

---

## Pipeline Model

| Dimension | Artisan | Kedro |
|-----------|---------|-------|
| **Computation unit** | Class: `OperationDefinition` subclass with `preprocess`/`execute`/`postprocess` lifecycle | Function: any Python callable wrapped in `node()` |
| **Dependency declaration** | Explicit: `output(step_name, role_name)` references in pipeline script | Implicit: string-name matching. Node output names matching other node input names creates edges. |
| **Batch semantics** | First-class: operations produce/consume lists of artifacts. One step processes N items. | Not native. Functions process single datasets. Batching is user-implemented within node functions. |
| **Composition** | `CompositeDefinition`: reusable multi-operation units with collapsed or expanded execution | Modular pipelines + namespaces: same pipeline instantiated multiple times with different parameters |
| **Pipeline slicing** | Not directly -- pipelines are built programmatically | Rich: `--from-nodes`, `--to-nodes`, `--from-inputs`, `--to-outputs`, `--tags` |
| **Parameters** | `Params(BaseModel)` nested class on each operation | Global `parameters.yml` in `conf/`, injected via `params:` prefix in node inputs |
| **Configuration** | Python code (no YAML configuration layer) | YAML-heavy: `catalog.yml`, `parameters.yml`, per-environment `conf/base` and `conf/local` |

**Analysis**: Kedro's function-based node model is simpler to learn. You write a plain Python function, wrap it in `node()` with string names for inputs and outputs, and the framework infers the DAG. This is elegant and keeps computation logic decoupled from the framework. The canonical example:

```python
node(func=split_data, inputs=["model_input_table", "params:split_ratio"],
     outputs=["X_train", "X_test", "y_train", "y_test"])
```

Artisan's class-based model is heavier but provides a three-phase lifecycle (`preprocess`/`execute`/`postprocess`) designed for wrapping external tools and managing sandbox isolation. The batch-native semantics -- where one operation inherently processes lists of artifacts -- are necessary for scientific workflows producing thousands of results per step.

Kedro's string-name dependency resolution is both a strength and a weakness. It is simple and readable, but typos in names are caught only at pipeline compilation time (not at the language level), and the indirection can make large pipelines harder to trace.

Kedro's modular pipeline system with namespaces provides clean reuse -- the same pipeline definition can be instantiated multiple times with different parameters and dataset prefixes, and Kedro-Viz renders namespaced pipelines as collapsible "super nodes." Artisan's `CompositeDefinition` solves a different reuse problem: composing multiple operations into a single logical step that executes as a unit.

---

## Storage & Persistence

| Dimension | Artisan | Kedro |
|-----------|---------|-------|
| **Storage backend** | Delta Lake tables on filesystem (ACID transactions) | Pluggable: any `AbstractDataset` implementation (filesystem, S3, GCS, HDFS, SQL, etc.) |
| **Persistence model** | All artifacts persisted with content hashes | Explicit: only datasets registered in `catalog.yml` are persisted. Unregistered intermediates are in-memory only. |
| **Write safety** | Staging-commit pattern prevents concurrent write corruption on shared NFS/HPC filesystems | No built-in concurrent write protection. Dataset save is a direct write. |
| **Queryability** | Direct: Polars/DuckDB scans over Delta Lake tables | Indirect: load datasets via catalog API, then query in-memory |
| **Versioning mechanism** | Content hash IS the version. Immutable by construction. | Timestamp directories. Mutable: nothing prevents overwriting a versioned file. |
| **Dataset factories** | N/A | Supported: pattern-based catalog entries that auto-resolve dataset configurations |
| **Environment support** | N/A (single store per pipeline) | Built-in: `conf/base`, `conf/local`, custom environments. Catalog entries merge/override across environments. |

**Analysis**: Kedro's storage layer is fundamentally an I/O abstraction -- it tells each dataset how to load and save itself, then gets out of the way. This gives enormous flexibility (swap CSV for Parquet for Spark for BigQuery via YAML) but provides no transactional guarantees, no deduplication, and no built-in concurrent write safety.

Artisan's Delta Lake backend is opinionated but provides ACID transactions, the staging-commit pattern for HPC safety, and direct queryability. The trade-off is clear: you get one storage backend with strong guarantees vs. many backends with weaker guarantees.

Kedro's environment system (`conf/base`, `conf/local`, custom) is a genuine strength for managing dev/staging/production configurations in a single project.

---

## Caching & Reproducibility

| Dimension | Artisan | Kedro |
|-----------|---------|-------|
| **Cache mechanism** | Content-addressed: cache key = hash(input artifact IDs + params + operation version). Automatic. | No built-in cache. "Run only missing" skips nodes whose output datasets already exist on disk. |
| **Cache granularity** | Per-artifact. If 999/1000 artifacts in a batch are cached, only the 1 new one is computed. | Per-node. If the output dataset file exists, the entire node is skipped. No within-batch granularity. |
| **Determinism** | Structural: same inputs + same params + same code = same content hash = cache hit | Not guaranteed. "Run only missing" checks file existence, not content. Stale data is possible. |
| **Invalidation** | Automatic: any input change produces a different hash, invalidating the cache | Manual: delete the output file to force re-run. No content-based invalidation. |
| **Third-party solutions** | N/A (built-in) | `kedro-cache` plugin adds input/code hashing for change detection. Not part of core Kedro. |
| **Reproducibility strategy** | Content addressing + immutable artifacts + provenance edges | Project conventions + `kedro run --load-versions` + Git for code + external tools (DVC, MLflow) for data |

**Analysis**: This is one of the sharpest differences. Artisan's content-addressed caching is automatic, deterministic, and fine-grained to individual artifacts. It requires no configuration and cannot produce stale results because cache keys are derived from content, not timestamps or file existence.

Kedro's approach to reproducibility relies on conventions and external tools. The "run only missing" feature checks whether an output dataset file exists and skips the node if so, but this is file-existence checking, not content hashing. If upstream data changes but the output file already exists, the stale output persists. True content-based caching was explicitly noted as a gap in Kedro's GitHub issues, and the third-party `kedro-cache` plugin partially addresses it.

For experiment reproducibility, Kedro leans on its project structure (Git for code, versioned datasets for data snapshots, `parameters.yml` for configuration) plus external tools like DVC for data versioning and MLflow for experiment tracking. This is a workable but multi-tool solution.

---

## Execution Model

| Dimension | Artisan | Kedro |
|-----------|---------|-------|
| **Default runner** | ProcessPool via Prefect (multi-process) | `SequentialRunner` (single-threaded) |
| **Built-in parallelism** | Two-level batching: artifacts-per-unit x units-per-worker | `ParallelRunner` (multiprocessing), `ThreadRunner` (threading) |
| **HPC/SLURM** | Native: `SlurmBackend` via submitit, SLURM job arrays, `SlurmIntraBackend` for srun | Not supported. HPC is not a target use case. |
| **Cloud/distributed** | Extensible `BackendBase` (currently LOCAL, SLURM, SLURM_INTRA) | Not built-in. Deploy pipelines TO external systems via plugins. |
| **Deployment targets** | Direct execution on HPC/local | Airflow, Kubeflow, Vertex AI, AWS Batch, SageMaker, Databricks, Argo, Prefect, Dask |
| **Isolation** | Full sandbox per execution: dedicated directory tree | None. Nodes share process state. Dataset isolation via catalog. |
| **Async / streaming** | `pipeline.submit()` returns `StepFuture` | `--async` flag for async dataset load/save |
| **Error handling** | Per-item containment with configurable policy (CONTINUE or FAIL_FAST) | Node-level: failed node stops the pipeline (or retry via hooks) |

**Analysis**: These frameworks target fundamentally different deployment patterns. Kedro is designed to define pipelines that are then deployed TO external orchestrators -- you write the pipeline in Kedro, then export it as an Airflow DAG, a Kubeflow pipeline, or a Databricks job. Kedro itself is not the execution engine in production; it is the development framework.

Artisan IS the execution engine. It dispatches work to SLURM job arrays, manages sandbox isolation per worker, handles two-level batching, and coordinates staging/commit cycles. This makes it stronger for HPC computational workloads but means it lacks the breadth of deployment targets that Kedro's ecosystem provides.

Kedro's `ParallelRunner` uses Python multiprocessing, which has known limitations: datasets must be serializable, SparkDataSets are incompatible (requiring `ThreadRunner` instead), and the `PartitionedDataset` caching behavior has documented issues with parallel execution. Artisan's Prefect-based dispatch avoids these issues by using proper process isolation with filesystem-based coordination.

---

## Project Structure & Conventions

| Dimension | Artisan | Kedro |
|-----------|---------|-------|
| **Project scaffolding** | None. Pipelines are Python scripts. No CLI for project creation. | `kedro new`: Cookiecutter-based project templates with full directory structure |
| **Configuration** | Python code (Params classes, programmatic pipeline construction) | YAML-heavy: `catalog.yml`, `parameters.yml`, `logging.yml`, environment directories |
| **Environment management** | External (user manages) | Built-in: `conf/base`, `conf/local`, custom environments with merge semantics |
| **Notebook integration** | Standard Jupyter (no special integration) | `kedro jupyter notebook` / `kedro jupyter lab` with auto-loaded catalog and context |
| **CLI** | None | Rich: `kedro run`, `kedro test`, `kedro lint`, `kedro package`, `kedro viz`, etc. |
| **Packaging** | Standard Python package | `kedro package` creates distributable `.whl` with pipeline and catalog |
| **IDE support** | Standard Python | VS Code extension with pipeline visualization |
| **Starters** | N/A | Pre-built project templates: spaceflights, pandas-iris, PySpark, etc. |

**Analysis**: Kedro's opinionated project structure is one of its strongest features. The standardized directory layout, configuration management, Cookiecutter starters, and CLI tooling provide a complete development experience that reduces friction for teams. A new team member joining a Kedro project knows where to find configurations, where pipelines are defined, and how to run things.

Artisan provides none of this scaffolding. Pipelines are built programmatically in Python scripts, and project structure is left to the user. This is less prescriptive but means each project must establish its own conventions.

---

## Where Kedro is Stronger

**Project conventions and developer experience.** Kedro's opinionated project template, CLI tooling, and configuration management provide a standardized development experience. Teams adopting Kedro get immediate structure.

**Data Catalog flexibility.** The 100+ dataset types, YAML-based configuration, transcoding, and environment-specific overrides make it easy to swap storage backends, manage dev/prod differences, and integrate with existing data infrastructure.

**Deployment breadth.** Kedro pipelines can be exported to Airflow, Kubeflow, Vertex AI, Databricks, SageMaker, and more. This is the primary production deployment story: Kedro for development, external orchestrators for production.

**Visualization.** Kedro-Viz is a polished interactive web UI with experiment tracking, metric comparison, pipeline visualization, dataset metadata, and collaborative features. This is significantly more accessible than Graphviz-generated static graphs.

**Enterprise backing and ecosystem.** QuantumBlack (McKinsey), Linux Foundation hosting, active community, VS Code extension, extensive documentation. Kedro 1.0 (released 2025) signals long-term stability.

**Notebook integration.** `kedro jupyter` provides a workflow bridge between exploratory notebook development and production pipeline code.

---

## Where Artisan is Stronger

**Provenance depth.** Dual provenance (execution + per-artifact derivation) with W3C PROV alignment provides lineage that Kedro cannot match. The ability to trace any output artifact back through its individual derivation chain -- not just "which node produced this dataset" but "which specific input artifact produced this specific output artifact within a batch" -- is a categorical difference.

**Content addressing.** Artifact identity as content hash provides automatic deduplication, deterministic cache keys, and immutable provenance edges in one mechanism. Kedro has no equivalent.

**Deterministic caching.** Automatic, per-artifact, content-based caching that cannot produce stale results. Kedro's file-existence checking and timestamp versioning do not provide the same guarantees.

**Batch-native processing.** Operations inherently work on lists of artifacts with per-item provenance tracking. A single step can process 10,000 items with individual derivation chains. Kedro's node model processes single datasets.

**HPC execution.** Native SLURM backend with job arrays, two-level batching, sandbox isolation, and the staging-commit pattern for shared filesystem safety. Kedro does not target HPC.

**Typed artifact system.** Domain-extensible type registry where new artifact types get full infrastructure (serialization, storage, provenance) for free. Kedro's dataset types are I/O adapters, not typed domain objects.

**Storage guarantees.** ACID transactions via Delta Lake and the staging-commit pattern prevent concurrent write corruption. Kedro's dataset save is a direct write with no transactional protection.

---

## Key Differentiators Summary

| Dimension | Kedro | Artisan |
|-----------|-------|---------|
| **Philosophy** | Standardize data science project structure and I/O | Track every artifact and its full derivation chain |
| **Lineage depth** | Pipeline DAG (node-to-dataset edges) | Dual: macro DAG + micro per-artifact derivation |
| **Data identity** | Named datasets (string keys) | Content-addressed hashes (xxh3_128) |
| **Caching** | File-existence / "run only missing" | Content-hash based, automatic, per-artifact |
| **Deduplication** | None | Automatic (same content = same ID = stored once) |
| **Storage** | Pluggable (100+ backends) | Delta Lake (ACID, queryable, single backend) |
| **Versioning** | Timestamp directories | Content hash IS the version |
| **Batch processing** | Single-dataset node functions | Batch-native (lists of artifacts, per-item provenance) |
| **Execution targets** | Export to Airflow, Kubeflow, Databricks, etc. | Native SLURM, local process pool |
| **Project structure** | Opinionated template + CLI | Programmatic (no scaffolding) |
| **Configuration** | YAML (catalog, params, environments) | Python (Params classes) |
| **Visualization** | Kedro-Viz (interactive web UI) | Graphviz provenance graphs |
| **Provenance standard** | None | W3C PROV-aligned |
| **Concurrent write safety** | None | Staging-commit pattern |
| **Backing** | QuantumBlack/McKinsey, Linux Foundation | Independent |

---

## When to Choose Which

**Choose Kedro when:**
- Your team needs standardized project structure and development conventions
- You are building ML/data science pipelines that will deploy to cloud orchestrators (Airflow, Kubeflow, Databricks)
- You need to integrate with diverse data sources and want the Data Catalog's I/O flexibility
- Per-artifact provenance and content-addressed caching are not requirements
- You want a mature ecosystem with enterprise backing, VS Code integration, and interactive visualization

**Choose Artisan when:**
- You need per-artifact provenance and derivation tracking within batch operations
- You are running computational workloads on HPC/SLURM clusters
- Deterministic, content-addressed caching is important for your workflow
- You produce large batches of typed results that need to be queried after the fact
- Concurrent write safety on shared filesystems is a requirement
- W3C PROV-aligned lineage tracking matters for your domain
