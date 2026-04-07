# Artisan vs MLflow: Provenance & Artifact Tracking Comparison

## Executive Summary

MLflow and Artisan occupy overlapping but fundamentally different positions in the ML/scientific computing landscape. MLflow is a mature, widely-adopted platform for experiment tracking, model management, and ML lifecycle governance -- it excels at recording what happened during runs and managing the path from experiment to production deployment. Artisan is a batch pipeline framework for scientific/computational workflows that treats provenance and artifact identity as structural guarantees rather than opt-in metadata logging. The core architectural difference is that MLflow identifies artifacts by location (run ID + path), while Artisan identifies artifacts by content (hash of the data itself), which cascades into fundamentally different properties around deduplication, caching, and provenance integrity.

---

## Provenance & Lineage

### MLflow

MLflow's provenance model is **run-centric**. The primary unit of tracking is the *run* -- a single execution of some code. Each run belongs to an *experiment* (a logical grouping), and records:

- **Parameters**: key-value pairs (string:string) for hyperparameters and configuration
- **Metrics**: key-value pairs (string:float) with timestamps, supporting step-based logging
- **Tags**: mutable key-value metadata
- **Artifacts**: opaque files (model weights, images, data files) stored under the run's artifact URI
- **Source**: code version (git commit), entry point, environment

The **Model Registry** adds a second layer of lineage: a registered model version links back to the specific experiment and run that produced it, with lifecycle stages (Staging, Production, Archived) and annotations. MLflow 3.0 introduced **LoggedModel** objects that carry their own metrics and parameters, tightening the connection between a model artifact and its evaluation data.

**Dataset tracking** (via `mlflow.data` / `mlflow.log_input()`) records which datasets were used as inputs to a run, including a computed digest/fingerprint. However, this is metadata-level tracking -- it records "this run used dataset X with digest Y" rather than tracking individual data items through transformations.

**What MLflow does NOT provide:**

- **Per-artifact derivation chains.** There is no built-in mechanism to express "artifact B was derived from artifact A." Lineage connects runs to their inputs and outputs, but the granularity stops at the run level. If a run consumes 1000 data items and produces 1000 transformed items, MLflow sees one run with logged inputs and outputs -- not 1000 individual A->B edges.
- **Automatic provenance capture.** Provenance in MLflow is opt-in. Developers must explicitly call `log_param()`, `log_metric()`, `log_artifact()`, `log_input()`, etc. Autologging for popular frameworks (sklearn, PyTorch, etc.) helps, but is limited to the frameworks MLflow knows about.
- **W3C PROV alignment.** MLflow's data model is its own; it does not map to PROV-DM concepts like Activity, Entity, and wasDerivedFrom.

Research literature confirms this gap. The MLflow2PROV project was specifically created to extract W3C PROV-compliant provenance from MLflow by combining MLflow metadata with Git history, indicating that MLflow's native provenance model was insufficient for standards-aligned provenance queries.

### Artisan

Artisan provides **two complementary provenance systems**:

- **Execution provenance** (macro): the step-level DAG -- which operations ran, in what order, with what configurations. ExecutionRecords map to W3C PROV Activities.
- **Artifact provenance** (micro): individual derivation chains at the artifact level. If an operation transforms artifacts [A, B, C] into [D, E, F], Artisan captures edges A->D, B->E, C->F -- not just "this step produced these outputs." ArtifactProvenanceEdges map to W3C PROV wasDerivedFrom.

This provenance is **structural, not opt-in** -- edges are captured automatically during execution based on the operation's InputSpec/OutputSpec declarations and the batch processing model. There is no `log_provenance()` call to forget.

### Verdict

MLflow provides run-level lineage suitable for "which experiment produced this model" queries. Artisan provides item-level derivation chains suitable for "which specific input artifacts contributed to this specific output, and through what chain of transformations." These serve fundamentally different use cases. MLflow's model is adequate for ML experiment comparison; Artisan's model is necessary for scientific workflows where individual data provenance matters for reproducibility or auditing.

---

## Artifact Model

### MLflow

MLflow artifacts are **opaque files identified by location**:

- **Identity**: `runs:/<run_id>/<artifact_path>` -- a run UUID (v4, randomly generated) combined with a relative file path. The same file content logged in two different runs gets two different identities and is stored twice.
- **No content addressing**: MLflow does not hash artifact contents for identification. Run IDs are random UUIDs, not content-derived.
- **No deduplication**: If the same model checkpoint is logged across 50 hyperparameter search runs, it is stored 50 times. There is no mechanism to detect or eliminate this redundancy.
- **Type system**: MLflow has a loose category system rather than a typed artifact registry. The primary categories are:
  - *Parameters* (string:string key-value pairs)
  - *Metrics* (string:float key-value pairs with timestamps)
  - *Tags* (mutable string:string metadata)
  - *Artifacts* (opaque files -- model weights, images, data files)
  - *Models* (artifacts with a specific MLflow model format, including flavor metadata)
  - *Datasets* (metadata objects with digest, source, schema, and profile)
  
  These categories are hard-coded rather than user-extensible. You cannot define a new artifact type with custom schema validation.
- **Batch support**: `log_artifacts()` logs all files in a directory, and `log_batch()` logs multiple metrics/params/tags in one call. However, these are convenience APIs for bulk logging, not a batch-native processing model. MLflow does not natively model "this operation consumes a list of N artifacts and produces a list of M artifacts."

**Dataset fingerprinting** is a notable addition. `mlflow.data` computes digests for logged datasets (using content-based hashing for Pandas DataFrames, metadata-based hashing for MetaDatasets). However, this applies only to datasets, not to arbitrary artifacts, and the digest is used for identification/tracking purposes rather than as the primary key for storage or deduplication.

### Artisan

- **Identity**: `artifact_id = xxh3_128(content)`. The artifact's identity IS its content hash. Same content always produces the same ID, regardless of when, where, or how many times it was produced.
- **Content-addressed deduplication**: Same content = same ID = stored once. This is a storage-layer guarantee, not an opt-in feature.
- **Type system**: Extensible via `ArtifactTypeDef` subclasses. Built-in types include Data, Metric, FileRef, Config, ExecutionConfig -- each with schema validation. Users can define domain-specific artifact types.
- **Roles**: Artifacts have named input/output roles on operations (e.g., "training_data", "model_checkpoint"), providing semantic context beyond just file paths.
- **Batch-native**: Operations declare that they consume and produce *lists* of artifacts. The framework manages the many-to-many relationships, including individual provenance edges within the batch.

### Verdict

MLflow treats artifacts as files-attached-to-runs. Artisan treats artifacts as first-class content-addressed entities with typed schemas and batch semantics. MLflow's model is simpler and more familiar (it looks like a file system), but it pays for that simplicity with storage duplication, no automatic deduplication, and coarser identity semantics.

---

## Storage & Persistence

### MLflow

MLflow separates storage into two components:

- **Backend store** (metadata): SQLAlchemy-backed databases -- SQLite (default), PostgreSQL, MySQL, MSSQL. Stores experiment metadata, run parameters, metrics, tags, and references to artifacts.
- **Artifact store** (binary data): Pluggable backends -- local filesystem (default), Amazon S3, Azure Blob Storage, Google Cloud Storage, HDFS, SFTP, NFS, Backblaze B2.

**Consistency**: MLflow inherits the consistency guarantees of its chosen backends. With a proper database backend (PostgreSQL), metadata operations are ACID. However, artifact stores have no transactional guarantees -- there is no atomic "commit" that ensures metadata and artifacts are consistent. If a run fails mid-write, artifacts may be partially uploaded while metadata references them.

**Concurrent access**: MLflow's file-based backend (the default) has documented race conditions -- concurrent writes to `meta.yaml` can produce empty files, causing downstream failures. The official guidance is to use a database-backed tracking server for concurrent access. Even then, artifact stores on shared filesystems (NFS) exhibit NFS-specific consistency issues (close-to-open semantics only). Users report `search_runs()` taking 30+ seconds for 120 runs on shared filesystems.

**Querying**: MLflow provides a SQL-like `search_runs()` API supporting filters on parameters, metrics, tags, and attributes with operators (`=`, `!=`, `>`, `<`, `LIKE`, `ILIKE`, `IN`). Conditions can be combined with AND (but not OR). Results can be ordered by any column. This is adequate for experiment comparison but cannot express provenance-graph queries like "find all ancestors of artifact X."

### Artisan

- **Delta Lake-backed**: ACID transactions via Delta Lake, which provides serializable isolation even on shared/NFS filesystems.
- **Staging-commit pattern**: Writes go through a staging directory before atomic commit, preventing concurrent write corruption -- a critical property for HPC environments where multiple SLURM jobs may write simultaneously to the same NFS-mounted storage.
- **Content-addressed deduplication at storage layer**: Because artifact identity is content-based, the storage layer automatically deduplicates.
- **Querying**: Provenance graph queries (ancestors, descendants, derivation chains) are first-class operations, not bolted-on search filters.

### Verdict

MLflow has broader cloud storage integration (S3, Azure Blob, GCS out of the box). Artisan has stronger consistency guarantees for shared-filesystem scenarios common in HPC. MLflow's query model is oriented toward experiment comparison; Artisan's is oriented toward provenance traversal.

---

## Caching & Reproducibility

### MLflow

MLflow does **not** provide deterministic caching. There is no mechanism to say "this operation with these inputs and parameters has already been computed; skip it and return the cached result." Each `mlflow run` executes from scratch.

For **reproducibility**, MLflow provides:

- **MLflow Projects**: Package code with an `MLproject` file specifying entry points, parameters, and environment (Conda, virtualenv, Docker, or system). This captures *how* to reproduce a run, but does not automate checking whether the run needs to be re-executed.
- **Environment capture**: Conda environment YAML or Docker image references are recorded, enabling environment reconstruction.
- **Source versioning**: Git commit hash is logged automatically.
- **Dataset digests**: Fingerprints for input datasets help detect when data has changed.

The approach is **record-and-replay**: MLflow gives you enough metadata to manually reproduce a run, but does not offer push-button deterministic re-execution with automatic cache hit detection. If you run the same code with the same parameters on the same data, MLflow will create a new run with a new UUID and store all outputs again.

### Artisan

- **Deterministic caching**: Because artifact IDs are content hashes and operation inputs are content-addressed, the cache key for any operation is deterministic: `f(content_hash(inputs), params)`. If the result already exists, execution is skipped entirely.
- **Automatic**: Caching is structural. No explicit cache keys need to be specified by the user.
- **Correct**: Because identity is content-based, cache invalidation is automatic when inputs change -- a property that location-based systems cannot guarantee without additional bookkeeping.

### Verdict

This is one of the sharpest differences. MLflow enables manual reproducibility through metadata recording. Artisan enables automatic reproducibility through content-addressed caching. For iterative scientific workflows where re-running unchanged pipeline stages is expensive (hours of HPC compute), Artisan's caching model provides concrete time and resource savings that MLflow cannot match without external tooling.

---

## Execution Model

### MLflow

MLflow is primarily a **tracking and governance platform**, not an execution orchestrator:

- **MLflow Projects** can execute a single entry point (a Python script, shell command, etc.) with specified parameters and environment. This is sufficient for "run this training script," but Projects do not manage DAGs, retries, conditional branching, or step dependencies.
- **MLflow Pipelines/Recipes** (introduced in MLflow 2.0, removed in MLflow 3.0) provided templated pipelines for common ML workflows but were limited and ultimately deprecated.
- **No native orchestration**: For multi-step workflows, MLflow is designed to be paired with an external orchestrator (Airflow, Prefect, Kubeflow, Dagster, etc.). MLflow tracks what happened; the orchestrator decides what to run.
- **SLURM support**: A community-maintained `mlflow-slurm` plugin exists, but it is not first-party, can lag behind MLflow releases, and has limited flexibility for complex HPC job specifications.
- **No execution isolation**: MLflow does not provide sandbox isolation between steps. If one step writes to a shared directory, another step can see and modify those files.

### Artisan

- **Full pipeline orchestration**: DAG-based pipeline engine with branching, merging, filtering, and composite operations.
- **HPC-native execution**: First-party SLURM support via submitit, local process pool, with a unified interface.
- **Two-level batching**: artifacts-per-unit and units-per-worker, allowing fine-grained control over parallelism.
- **Sandbox isolation**: Each execution gets isolated preprocess/execute/postprocess directories, preventing interference between concurrent steps.
- **Execution as a first-class concern**: Operations are classes with declared InputSpec/OutputSpec, not arbitrary scripts. The framework manages the lifecycle.

### Verdict

MLflow tracks execution; Artisan performs it. This is not a weakness of MLflow per se -- it is a deliberate design choice that enables MLflow to integrate with any execution system. But it means that for pipeline-centric workflows, MLflow always requires a second tool to handle orchestration, while Artisan is self-contained.

---

## Where MLflow is Stronger

These are genuine advantages, not caveats:

- **Ecosystem breadth**: MLflow integrates with 100+ tools and frameworks. Autologging support for scikit-learn, PyTorch, TensorFlow, XGBoost, LightGBM, Hugging Face, and many more means basic experiment tracking often requires zero additional code. Artisan, as a newer and more specialized framework, has a much smaller integration surface.

- **Model lifecycle management**: The Model Registry with versioning, stage transitions (Staging/Production/Archived), annotations, aliases, and webhooks provides a complete governance workflow from experiment to production deployment. Artisan focuses on pipeline execution and provenance, not deployment lifecycle.

- **UI and visualization**: MLflow ships with a polished web UI for comparing runs, visualizing metrics over time, inspecting artifacts, and managing registered models. This is a significant productivity tool for iterative ML development.

- **Cloud-native deployment**: First-class integration with Databricks (managed MLflow), Azure ML, AWS SageMaker, and Google Cloud. Production deployments can scale with minimal infrastructure work.

- **Model serving**: Built-in model serving via REST API endpoints, with support for batch inference (including Spark UDF deployment) and real-time serving via Docker containers or cloud endpoints. Artisan does not address model serving.

- **GenAI/LLM support (MLflow 3.x)**: Prompt registry, prompt optimization, agent tracing, LLM evaluation, and trace-based observability for generative AI applications. This is a rapidly growing area where MLflow has made significant investment.

- **Community and adoption**: MLflow is backed by the Linux Foundation, has thousands of organizational users, extensive documentation, tutorials, and community support. This matters for long-term viability, hiring, and knowledge sharing.

- **Language support**: Python, Java, R, and TypeScript/JavaScript APIs. Artisan is Python-only.

- **Query and comparison**: The `search_runs()` API and UI make it easy to filter, sort, and compare hundreds of experiment runs on metrics and parameters. This is MLflow's core workflow and it does it well.

---

## Where Artisan is Stronger

- **Provenance depth and correctness**: Per-artifact derivation chains (A->B->C) vs. per-run associations. W3C PROV alignment. Structural (automatic) provenance vs. opt-in logging. For workflows where "which specific inputs produced this specific output" matters, Artisan's provenance is categorically more informative.

- **Content-addressed identity**: Artifact identity derived from content eliminates an entire class of problems -- stale references, duplicate storage, inconsistent caches, phantom provenance edges pointing to overwritten data. MLflow's location-based identity (run UUID + path) provides none of these guarantees.

- **Deterministic caching**: Automatic skip of already-computed results based on content hashes. For expensive scientific computations where pipeline stages take hours on HPC clusters, this is a material resource savings that MLflow cannot provide without external tooling.

- **Storage efficiency**: Content-addressed deduplication means identical artifacts are stored exactly once, regardless of how many pipeline runs produce them. MLflow stores every artifact independently, which can lead to significant storage bloat in hyperparameter search or iterative refinement workflows.

- **HPC/shared-filesystem robustness**: Delta Lake ACID transactions and staging-commit pattern provide correct concurrent writes on NFS -- the exact environment where MLflow's file-based backend has documented race conditions and performance problems.

- **Batch-native artifact model**: Operations that consume and produce lists of artifacts with individual provenance edges per item. MLflow's model is fundamentally single-run-centric; batch processing is a convenience API layer, not a core abstraction.

- **Pipeline orchestration**: Self-contained DAG execution with branching, merging, filtering, composites, and two-level batching. MLflow requires pairing with an external orchestrator for equivalent capability.

- **Typed, extensible artifacts**: Domain-specific artifact types with schema validation and named roles, vs. MLflow's fixed categories (param/metric/tag/artifact/model/dataset) with no extensibility path.

---

## Key Differentiators Summary Table

| Dimension | Artisan | MLflow |
|---|---|---|
| **Primary purpose** | Batch pipeline framework for scientific/computational workflows | ML lifecycle platform for experiment tracking, model management, and deployment |
| **Artifact identity** | Content-addressed (xxh3_128 hash of content) | Location-based (random UUID run ID + file path) |
| **Deduplication** | Automatic at storage layer (same content = stored once) | None (same content in N runs = stored N times) |
| **Provenance granularity** | Per-artifact derivation chains (A->B->C) | Per-run associations (run used dataset X, produced model Y) |
| **Provenance capture** | Structural/automatic (from InputSpec/OutputSpec) | Opt-in (explicit log calls, autologging for supported frameworks) |
| **W3C PROV alignment** | Yes (Activity, Entity, wasDerivedFrom) | No (requires external tools like MLflow2PROV) |
| **Deterministic caching** | Yes (content-hash-based, automatic) | No (every run executes from scratch) |
| **Artifact type system** | Extensible typed system (ArtifactTypeDef subclasses with validation) | Fixed categories (param, metric, tag, artifact, model, dataset) |
| **Batch processing** | First-class (operations consume/produce artifact lists with per-item provenance) | Convenience APIs (log_batch, log_artifacts) on top of single-run model |
| **Pipeline orchestration** | Built-in DAG engine with branching, merging, filtering, composites | Not an orchestrator; requires Airflow/Prefect/Kubeflow/etc. |
| **Execution** | HPC-native (SLURM via submitit), local process pool, sandbox isolation | MLflow Projects (single entry point execution); SLURM via community plugin |
| **Storage backend** | Delta Lake with ACID transactions, staging-commit pattern | SQL database (metadata) + pluggable blob store (S3, Azure, GCS, NFS, local) |
| **Concurrent write safety** | ACID on shared NFS via Delta Lake | Race conditions on file backend; requires DB-backed server for concurrent access |
| **Model lifecycle** | Not addressed | Full registry with versioning, stages, aliases, webhooks, serving |
| **Cloud/SaaS integration** | Not addressed | Databricks managed MLflow, Azure ML, SageMaker, GCP integrations |
| **UI** | Provenance graph visualization | Full web UI for experiment comparison, metric visualization, model management |
| **Ecosystem breadth** | Python-focused, scientific computing | 100+ integrations, autologging, Python/Java/R/TypeScript |
| **GenAI/LLM support** | Not addressed | Prompt registry, agent tracing, LLM evaluation (MLflow 3.x) |
| **Community** | Emerging | Large established community (Linux Foundation, thousands of organizations) |
| **Reproducibility approach** | Deterministic (content-addressed caching prevents redundant computation) | Record-and-replay (metadata sufficient to manually reproduce, no automatic re-execution skip) |
| **Query model** | Provenance graph traversal (ancestors, descendants, derivation chains) | SQL-like run search (filter/sort on metrics, parameters, tags) |
