# Artisan vs Neptune: Provenance & Artifact Tracking Comparison

## Executive Summary

Artisan and Neptune (neptune.ai) occupy fundamentally different positions in the ML/scientific computing tooling landscape. Neptune is (was) a hosted experiment tracking platform optimized for logging, comparing, and visualizing ML training runs -- particularly foundation model training at scale. Artisan is a self-hosted batch pipeline framework for scientific and computational workflows, built around content-addressed artifacts and structural provenance. Neptune excels at real-time metric visualization, team collaboration, and ML framework integrations; Artisan excels at deterministic reproducibility, fine-grained data lineage, and HPC-native batch execution. With Neptune's acquisition by OpenAI and public service shutdown on March 5, 2026, the platform is no longer a viable long-term option for new adopters.

## Provenance & Lineage

**Neptune's approach:** Neptune tracks experiment history through its Run model. Each training run is a container of structured metadata -- parameters, metrics, artifacts, tags, and system info -- organized under namespaces (hierarchical, dict-like paths). The `sys` namespace captures environment and lifecycle metadata automatically. Neptune provides run-to-run comparison and its "forking" feature lets teams branch from checkpoints, creating a tree of experiment variations with lineage back to the fork point.

However, Neptune's lineage model is fundamentally **run-centric, not data-centric**. It tracks "which experiment produced which results" but does not capture individual artifact derivation chains (A produced B, B produced C). There is no equivalent to W3C PROV's `wasDerivedFrom` edges between individual data items. If you train a model, evaluate it, then use those evaluation metrics to select hyperparameters for a second run, Neptune can show you both runs existed and their metadata -- but the causal chain between specific artifacts across runs is not structurally captured. You would need to manually log references (e.g., storing the source run ID as a parameter in the downstream run) and reconstruct lineage yourself.

**Artisan's approach:** Artisan provides two complementary provenance systems that operate at different granularities:

- **Macro provenance**: The step-level DAG showing which operations ran and in what order (pipeline structure).
- **Micro provenance**: Individual artifact-to-artifact derivation chains. If operation F takes artifacts A, B, C and produces D, E, F, the system captures exactly which inputs produced which outputs, with W3C PROV-aligned semantics (ExecutionRecord = Activity, Artifact = Entity, ArtifactProvenanceEdge = wasDerivedFrom).

Critically, this provenance is **structural and automatic** -- it is captured during execution without opt-in logging calls. Operations declare typed input/output roles, and the framework records the edges.

**Verdict:** Neptune tracks experiment metadata comprehensively but treats lineage as run-level ancestry (parent run, forked-from checkpoint). Artisan tracks fine-grained, individual artifact derivation chains structurally. If your question is "what hyperparameters produced the best accuracy?", Neptune answers it well. If your question is "which specific input files contributed to this specific output artifact, through which intermediate transformations?", Artisan answers it and Neptune does not.

## Artifact Model

**Neptune's approach:** Neptune's artifact system uses `track_files()` to log metadata (hash, path, size, modification time, folder structure) about files that live elsewhere -- on local disk, NFS, or S3. The hash is computed from content *plus* metadata (path, size, mtime), meaning the same file content at two different paths or with different modification times produces different hashes. Artifacts can be downloaded later via `download()`, and metadata can be inspected via `fetch_files_list()`. Neptune also supports direct file upload for smaller artifacts.

Key characteristics:
- **Reference-based**: Neptune primarily stores metadata *about* artifacts, not the artifacts themselves. The actual files remain wherever they were when logged.
- **Hash includes metadata**: Not purely content-addressed. Same bytes at different paths = different hash.
- **No typed artifact system**: Artifacts are files or file collections. There is no type hierarchy (Data vs Metric vs Config, etc.) -- the type semantics live in how you organize your namespaces.
- **No batch semantics**: Neptune's artifact tracking is per-file or per-folder, not batch-aware. There is no concept of operations producing lists of typed artifacts with named roles.
- **Versioning via hash comparison**: You can compare artifact hashes across runs to detect changes, but there is no built-in version chain linking artifact V1 to artifact V2.

**Artisan's approach:**
- **Content-addressed**: `artifact_id = xxh3_128(content)`. Identity *is* the content hash. Same content always gets the same ID, regardless of path, timestamp, or any other metadata.
- **Automatic deduplication**: Same content stored once. Two pipeline runs that produce identical intermediate results share storage.
- **Typed artifact system**: Data, Metric, FileRef, Config, ExecutionConfig -- extensible via `ArtifactTypeDef` subclasses. The type is part of the artifact's contract, not just a convention.
- **Role-based I/O**: Operations declare named input/output slots (roles). Artifacts are bound to roles, making the relationship between data and computation explicit and queryable.
- **Batch-native**: Operations produce and consume lists of artifacts. The framework handles batching, partitioning, and fan-out natively.
- **Immutable provenance**: Because artifact IDs are content hashes, provenance edges (artifact A derived from artifact B) cannot go stale. The IDs are deterministic.

**Verdict:** Neptune's artifact model is a lightweight reference tracker -- useful for knowing which files were associated with which runs, but it does not provide content-addressed identity, type safety, or batch semantics. Artisan's model makes artifacts first-class, content-addressed, typed entities with deterministic identity.

## Storage & Persistence

**Neptune:**
- **SaaS (primary)**: Hosted by Neptune. Metadata stored in their backend (ClickHouse + MySQL + Redis on Kubernetes). File artifacts reference external storage (S3, local, etc.) or are uploaded to Neptune's storage.
- **Self-hosted**: Deployable on Kubernetes with your own ClickHouse, MySQL, Redis instances. Supports S3, Azure Blob Storage, or PVC as storage backends.
- **Querying**: Neptune Query Language (NQL) enables filtering runs by any logged field, with typed attribute references. The `neptune-query` package provides a read-only API for fetching metadata as DataFrames.
- **Offline mode**: Metadata stored locally in `.neptune/` directory as SQLite files. Must be manually synced to the Neptune server later via `neptune sync`.
- **Shutdown caveat**: SaaS platform shuts down March 5, 2026. Self-hosted Helm charts and container images deleted March 8, 2026. Data export to Parquet is available during transition.

**Artisan:**
- **Local-first**: Delta Lake-backed storage with ACID transactions. No external service dependency.
- **HPC-friendly**: Staging-commit pattern prevents concurrent write corruption on shared NFS/HPC filesystems -- a real-world concern Neptune's architecture does not address.
- **Content-addressed deduplication**: Storage-layer deduplication based on content hashes. Multiple pipeline runs referencing the same intermediate data share physical storage.
- **Querying**: Programmatic access through the artifact store API. Delta Lake supports time travel and schema evolution.
- **No hosted option**: Fully self-hosted. No web dashboard for querying (notebook/CLI-based exploration).

**Verdict:** Neptune offers a more polished querying and exploration experience via NQL and its web UI, plus both hosted and self-hosted deployment. Artisan offers stronger storage-layer guarantees (ACID transactions, content deduplication, NFS-safe concurrent writes) and zero external dependencies. Neptune's storage is designed for metadata about experiments; Artisan's storage is designed for the artifacts themselves.

## Metadata & Tracking

This is Neptune's core strength. Neptune supports:

- **Metric logging**: Float series, string series, file series with up to 1,000,000+ data points per run, rendered with no lag. Per-layer gradient, loss, and activation tracking for foundation models.
- **Parameter tracking**: Automatic capture of hyperparameters, code versions, environment details. Dictionary-based logging that auto-creates nested namespaces.
- **System metrics**: GPU utilization, memory, CPU -- logged automatically.
- **Visualization**: Real-time training curves, side-by-side run comparison with synchronized charts, parameter diff tables, statistical significance testing.
- **Experiment comparison**: Unlimited runs compared simultaneously with dynamic filtering/grouping. Diff highlighting across parameter spaces.
- **Custom dashboards**: Drag-and-drop widgets combining metrics from multiple experiments. Saved views for common queries.

Artisan does not compete directly here. Artisan tracks execution metadata (what ran, with what config, what it produced) and artifact provenance, but it does not provide real-time metric streaming, training curve visualization, or interactive dashboards. Artisan's "metrics" are Metric-typed artifacts -- discrete values produced by operations, not streaming time series.

**Verdict:** For ML training observability -- watching loss curves, comparing hyperparameter sweeps, monitoring GPU utilization -- Neptune is categorically stronger. Artisan does not attempt to solve this problem; its metadata model is oriented toward batch pipeline execution records, not real-time training telemetry.

## Execution Model

**Neptune:** Neptune is a **passive observer**. It does not orchestrate, schedule, or execute computation. You integrate the Neptune client into your training scripts, and it logs metadata as your code runs. Neptune has no opinions about how you run your code -- it works with any execution environment (local, cloud, SLURM, Kubernetes) because it is just a logging library. This is both a strength (universal compatibility) and a limitation (no orchestration, no caching, no execution management).

Neptune's integrations with frameworks (PyTorch, TensorFlow, Keras, XGBoost, scikit-learn, LightGBM, etc.) are callback-based: you pass a `NeptuneCallback` to your training loop, and it logs metrics automatically. There are 25+ framework integrations.

**Artisan:** Artisan is an **active orchestrator**. It:
- Defines and executes pipelines as DAGs of operations
- Manages two-level batching (artifacts-per-unit x units-per-worker)
- Provides full sandbox isolation per execution
- Supports HPC-native execution (SLURM via submitit, local process pool)
- Implements deterministic caching: same inputs + same parameters + same operation = cache hit, skipping re-execution

Artisan does not just observe your computation -- it *is* the computation framework. Operations are defined as subclasses with typed inputs/outputs, and the pipeline engine handles dispatch, batching, and execution.

**Verdict:** These tools solve different problems. Neptune watches your existing training code run. Artisan *is* the framework your code runs inside. Neptune integrates with anything; Artisan provides guarantees (caching, isolation, provenance) that are only possible when the framework controls execution.

## Collaboration & UI

**Neptune:**
- **Web UI**: Full-featured dashboard with experiment tables, comparison views, custom visualizations, and saved views. Real-time updates during training.
- **Team features**: Project-based organization, RBAC (admin/owner/contributor/view-only), workspace-level and project-level access control, SSO integration.
- **Sharing**: Shareable dashboard views, automated reports, one-click colleague invitations.
- **Notebook integration**: `neptune-notebooks` extension for versioning and sharing Jupyter checkpoints. Programmatic access via `fetch_runs_table()` to pull results into notebooks.
- **Forking**: Branch experiments from checkpoints, inheriting metadata while exploring new directions.

**Artisan:**
- **No web UI**: All interaction via Python API, CLI, and Jupyter notebooks.
- **Visualization**: Provenance graph rendering via Graphviz (micro and macro graphs), timing analysis. Notebook-based exploration.
- **Team features**: None built-in. Collaboration is through shared filesystem access (NFS on HPC clusters) and standard code collaboration tools (git, etc.).
- **Sharing**: Share pipeline definitions and results via code. Provenance graphs are exportable.

**Verdict:** Neptune is built for team collaboration with a polished web UI, access controls, and sharing features. Artisan is built for individual researchers or small teams working on shared HPC infrastructure, with collaboration handled at the code/filesystem level. For organizations that need role-based access, visual dashboards, and cross-team experiment comparison, Neptune is unambiguously stronger.

## Where Neptune is Stronger

- **Real-time training observability**: Streaming metrics, loss curves, gradient monitoring at scale (1M+ data points per run). Purpose-built for foundation model training telemetry.
- **Web UI and dashboards**: Interactive, customizable experiment tables and comparison views. No equivalent in Artisan.
- **ML framework integrations**: 25+ callback-based integrations (PyTorch, TensorFlow, Keras, XGBoost, etc.). Drop-in with minimal code changes.
- **Team collaboration**: RBAC, SSO, project-based organization, shareable views, experiment forking.
- **Hosted infrastructure**: Zero-ops SaaS option (when it was available). No storage management, no database administration.
- **NQL querying**: Purpose-built query language for filtering experiments by any metadata field, usable in both UI and API.
- **Ecosystem compatibility**: Works with any execution environment because it is a passive logger, not an orchestrator. Compatible with Kubernetes, SLURM, cloud, local -- anything.
- **Onboarding speed**: Add `import neptune` and a few logging calls to existing code. No architectural commitment required.

## Where Artisan is Stronger

- **Provenance depth**: Structural, automatic, artifact-level derivation chains (A->B->C). W3C PROV-aligned. Neptune only tracks run-level metadata, not individual artifact lineage.
- **Content addressing**: `artifact_id = hash(content)`. Deterministic identity enables automatic deduplication, deterministic caching, and immutable provenance edges. Neptune's artifact hashes include mutable metadata (path, mtime).
- **Deterministic caching**: Same inputs + same operation = automatic cache hit. Neptune has no execution-level caching because it does not control execution.
- **Batch processing**: Operations natively produce/consume lists of typed artifacts. Two-level batching (artifacts-per-unit x units-per-worker). Neptune has no batch computation model.
- **HPC-native execution**: SLURM integration via submitit, NFS-safe concurrent writes via staging-commit pattern, process pool execution. Neptune's offline mode can sync SLURM jobs but does not manage execution.
- **Pipeline orchestration**: DAG-based pipeline engine with dispatch, step execution, and fan-out/fan-in. Neptune is observation-only.
- **Typed artifact system**: Data, Metric, FileRef, Config, ExecutionConfig with extensible type definitions and role-based I/O contracts. Neptune treats artifacts as opaque files.
- **Storage guarantees**: ACID transactions via Delta Lake, content-addressed deduplication at storage layer, NFS-safe concurrent writes. No external service dependency.
- **Self-hosted with zero dependencies**: Runs on local filesystem or shared NFS. No Kubernetes cluster, no database servers, no cloud accounts required.
- **Long-term viability**: Open-source, self-hosted, no vendor dependency. Neptune's public platform shut down March 2026.

## Key Differentiators Summary Table

| Dimension | Neptune | Artisan |
|---|---|---|
| **Primary purpose** | Experiment tracking & visualization | Batch pipeline orchestration & provenance |
| **Execution model** | Passive observer (logging library) | Active orchestrator (pipeline framework) |
| **Provenance granularity** | Run-level (which experiment, what params) | Artifact-level (A derived from B derived from C) |
| **Provenance capture** | Opt-in (explicit logging calls) | Structural (automatic during execution) |
| **Artifact identity** | Hash of content + metadata (path, mtime) | Content hash only (xxh3_128) |
| **Artifact deduplication** | None at storage layer | Automatic (same content = same ID = stored once) |
| **Artifact type system** | Untyped (files/folders) | Typed (Data, Metric, FileRef, Config, etc.) |
| **Batch support** | Per-run logging | Native batch operations with two-level batching |
| **Caching** | None (does not control execution) | Deterministic (content-hash-based) |
| **Storage** | SaaS hosted or self-hosted Kubernetes (ClickHouse/MySQL/Redis) | Local Delta Lake with ACID transactions |
| **Concurrent write safety** | Managed by backend service | Staging-commit pattern for NFS/shared filesystems |
| **Web UI** | Full-featured dashboards, comparison tables, NQL | None (notebook/CLI/Graphviz) |
| **Collaboration** | RBAC, SSO, shareable views, forking | Shared filesystem, git-based code sharing |
| **ML framework integrations** | 25+ (PyTorch, TensorFlow, Keras, etc.) | Framework-agnostic (operations are Python classes) |
| **HPC support** | Offline mode with manual sync | Native SLURM via submitit, process pools |
| **Real-time metrics** | Yes (streaming, 1M+ points, GPU monitoring) | No (metrics are discrete artifact values) |
| **Deployment** | SaaS or Kubernetes | Local filesystem / NFS |
| **Vendor risk** | Acquired by OpenAI, shutting down March 2026 | Self-hosted, no vendor dependency |
| **Ideal user** | ML teams training & comparing models | Researchers running reproducible batch pipelines |
