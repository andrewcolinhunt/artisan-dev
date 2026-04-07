# Artisan vs Weights & Biases: Provenance & Artifact Tracking Comparison

## Executive Summary

Weights & Biases (W&B) and Artisan are complementary rather than competing tools that overlap primarily in artifact tracking and lineage. W&B is an **experiment tracking and MLOps platform** -- its core value is logging metrics, visualizing training runs, comparing hyperparameters, and managing model/dataset versioning via a polished web UI. Artisan is a **batch pipeline framework** -- its core value is orchestrating multi-step scientific computations on HPC clusters with content-addressed artifacts, per-artifact provenance chains, and queryable Delta Lake storage. W&B tracks *what happened during training*; Artisan tracks *what produced what, at the individual artifact level, and orchestrates the pipeline that does it*.

---

## Provenance & Lineage

This is the most important comparison point, since W&B's artifact lineage system is the closest analogue to Artisan's provenance model.

### W&B's Lineage Model

W&B constructs a **bipartite DAG** of Runs and Artifacts. The two node types alternate -- runs produce artifacts, artifacts are consumed by runs. The edges are:

- `run.log_artifact(artifact)` -- "this run produced this artifact"
- `run.use_artifact(artifact)` -- "this run consumed this artifact"

The lineage graph can be traversed programmatically:

- `artifact.logged_by()` -- which run produced this artifact?
- `artifact.used_by()` -- which runs consumed this artifact?
- `run.logged_artifacts()` -- what did this run produce?
- `run.used_artifacts()` -- what did this run consume?

This creates a chain like:
`Raw Data (artifact) --> Preprocessing Run --> Clean Data (artifact) --> Training Run --> Model (artifact)`

### Artisan's Dual Provenance Model

Artisan maintains two complementary provenance systems:

- **Execution provenance** (macro): `ExecutionRecord` = W3C PROV Activity. Records what ran, when, with what parameters. Analogous to W&B's Run concept.
- **Artifact provenance** (micro): `ArtifactProvenanceEdge` = W3C PROV wasDerivedFrom. Records individual artifact-to-artifact derivation chains (A derived from B, C derived from D). No run intermediary needed.

### Key Differences

| Dimension | W&B | Artisan |
|---|---|---|
| **Graph structure** | Bipartite: Run <-> Artifact (edges always pass through a Run node) | Direct: Artifact -> Artifact edges (plus separate execution records) |
| **Granularity** | Run-level: "Training Run produced Model v3" | Artifact-level: "Output artifact X was derived from input artifacts Y and Z" |
| **Batch provenance** | A run that processes 1,000 items logs one "dataset" artifact -- internal item-to-item derivation is not captured | Each of the 1,000 output artifacts has its own provenance edge to its specific input(s) |
| **Provenance capture** | Opt-in: user must call `log_artifact()` and `use_artifact()` | Structural: provenance edges are captured automatically during execution based on operation input/output specs |
| **Standards alignment** | Proprietary graph model | W3C PROV-aligned (Activity, Entity, wasDerivedFrom) |
| **Queryability** | Web UI graph viewer + Python API traversal | Polars/DuckDB queries over Delta Lake provenance tables |

**The critical distinction**: W&B's lineage is run-mediated. To trace "which training data produced this model," you walk: Model -> Training Run -> Dataset. This is effective for ML workflows where the relationship is typically one-dataset-in, one-model-out. Artisan's lineage is artifact-direct. In a batch pipeline where one step processes 10,000 input items into 10,000 output items, Artisan captures 10,000 individual derivation edges -- the provenance of output item #7,342 traces back to its specific input, not to "the run that processed the batch."

W&B's lineage is best understood as **pipeline-level lineage** (which steps connected to which). Artisan provides both pipeline-level lineage (via execution records) AND **item-level lineage** (via artifact provenance edges).

---

## Artifact Model

### W&B Artifacts

W&B Artifacts are versioned, named collections of files and metadata:

- **Naming**: `{type}/{name}:{version_or_alias}` (e.g., `dataset/mnist:v3` or `model/resnet:latest`)
- **Types**: User-defined string labels (e.g., "dataset", "model", "predictions"). No enforced schema per type.
- **Versioning**: Sequential version numbers (`v0`, `v1`, `v2`...) within a named artifact sequence. The `latest` alias is assigned automatically.
- **Aliases**: Human-readable pointers to specific versions (e.g., `best`, `production`, `staging`). Each alias within a collection is unique.
- **Collections**: Ordered groups of artifact versions within a Registry, representing a distinct task or use case.
- **Content**: An artifact is a directory of files. Files are added via `add_file()`, `add_dir()`, or `add_reference()`.
- **Manifest**: Each artifact version has a manifest listing all files with their checksums (MD5) and sizes. The artifact's overall **digest** is a checksum of the manifest.
- **Deduplication**: File-level deduplication across versions. If you log a new version of an 80GB dataset that differs by one file, W&B only uploads the changed file. If the artifact has the same digest as the current latest version, `log_artifact()` is a no-op.
- **Reference artifacts**: Metadata-only artifacts that point to files in external storage (S3, GCS, Azure, NFS) without copying them. W&B tracks checksums, sizes, and version IDs for the referenced objects.
- **Metadata**: Arbitrary JSON dictionary attached to each artifact version.

### Artisan Artifacts

Artisan artifacts are content-addressed, typed, immutable data objects:

- **Identity**: `artifact_id = xxh3_128(content_bytes)`. The hash IS the identity. There is no separate name/version scheme.
- **Types**: Typed via `ArtifactTypeDef` subclasses: `Data`, `Metric`, `FileRef`, `Config`, `ExecutionConfig`. Each type defines serialization, hashing, and storage behavior. Extensible by domain layers.
- **Versioning**: None in the traditional sense. Since identity = content hash, "changing" an artifact produces a new artifact with a new ID. The provenance graph captures the relationship.
- **Roles**: Artifacts have named roles within operations (e.g., "input_data", "model_weights") defined by `InputSpec`/`OutputSpec`.
- **Content**: Each artifact holds a single typed value (a DataFrame, a metric value, a file reference, etc.), not a directory of files.
- **Deduplication**: Automatic and total. Same content = same hash = same artifact ID = stored once. This is an inherent property of content addressing, not an optimization layer.
- **Immutability**: Artifacts cannot be modified after creation. Content addressing makes this a mathematical guarantee.

### Key Differences

| Dimension | W&B | Artisan |
|---|---|---|
| **Identity model** | Name + version number (`mnist:v3`) | Content hash (`xxh3_128(bytes)`) |
| **What an artifact contains** | Directory of files + metadata | Single typed value (data, metric, config, file ref) |
| **Deduplication** | File-level within artifact versions (optimization) | Total deduplication by identity (structural) |
| **Mutability** | Metadata can be updated; aliases can be reassigned | Fully immutable (hash = identity) |
| **Type system** | User-defined string label, no schema enforcement | Typed class hierarchy with defined serialization and hashing |
| **Versioning** | Sequential versions with mutable aliases | No versioning; new content = new artifact with provenance edge |
| **Granularity** | Coarse: one artifact = a dataset, a model, a table | Fine: one artifact = one data item, one metric, one config |
| **Batch support** | One artifact per `log_artifact()` call (though a run can log many) | Native: operations produce/consume lists of artifacts |

**The critical distinction**: W&B artifacts are **named, versioned containers** (like Git tags on a directory). Artisan artifacts are **content-addressed values** (like Git blob objects). W&B's model is familiar and user-friendly -- you name things and track versions. Artisan's model is more rigorous -- identity is derived from content, making deduplication and caching deterministic properties rather than features.

---

## Storage & Persistence

| Dimension | W&B | Artisan |
|---|---|---|
| **Default storage** | W&B cloud (managed by W&B/CoreWeave) | Local filesystem (Delta Lake tables) |
| **Self-hosted** | Yes: Dedicated Cloud (single-tenant managed) or Self-Managed (customer infra) | Default -- runs wherever there is a filesystem |
| **Storage format** | Proprietary cloud object store + manifest metadata | Delta Lake (Parquet + transaction log) with ACID commits |
| **External references** | Reference artifacts: metadata-only pointers to S3/GCS/Azure/NFS objects | FileRef artifacts: references to files on the filesystem |
| **Querying artifacts** | Python API (`api.artifacts()`) with filter/tag support; GraphQL internally; Web UI filters | Direct SQL-like queries via Polars `scan_delta()` or DuckDB |
| **Concurrent writes** | Managed by W&B server (cloud handles coordination) | Staging-commit pattern prevents corruption on shared NFS/HPC filesystems |
| **Data residency** | W&B cloud, Dedicated Cloud (your region), or self-managed | Always local -- no data leaves your infrastructure |
| **Cost model** | Free tier (100GB storage); Teams plan per-user pricing; Enterprise custom | No storage costs beyond your own filesystem |

W&B's cloud-first model provides convenience, managed infrastructure, and automatic scaling. Artisan's filesystem-first model provides data sovereignty, zero operational overhead, and natural compatibility with HPC shared filesystems where cloud access may be restricted or prohibited.

---

## Experiment Tracking

This is W&B's **core strength** and the area where the gap is largest.

### W&B Experiment Tracking

W&B provides a comprehensive experiment tracking system:

- **Metrics logging**: `wandb.log({"loss": 0.5, "accuracy": 0.92})` streams metrics from training loops in real-time. Time-series visualizations update live.
- **Hyperparameter tracking**: `wandb.config` captures all configuration. Parallel coordinates plots, parameter importance analysis.
- **System metrics**: Automatic GPU/CPU/memory utilization monitoring.
- **Comparison**: Side-by-side run comparison, diff views, custom dashboards with drag-and-drop panels.
- **Tables**: Interactive data tables for predictions, evaluation results, images, audio, video, point clouds, molecules.
- **Reports**: Collaborative documents combining narrative text, live visualizations, and embedded run data.
- **Sweeps**: Automated hyperparameter search (grid, random, Bayesian) with early termination (Hyperband). Integrated into the tracking UI.
- **Reproducibility**: Automatic capture of git commit, code, environment, and configuration for each run.

### Artisan's Approach

Artisan does not provide real-time experiment tracking, dashboards, or visualization. Its relationship to metrics is structural:

- **MetricArtifact**: A typed artifact that holds a single metric value. Metrics are first-class artifacts with content-addressed identity and provenance.
- **Queryable results**: All metrics from all pipeline steps are stored in Delta Lake tables and queryable via Polars/DuckDB.
- **Per-artifact provenance**: Each metric traces back through the full derivation chain to the inputs and parameters that produced it.

### Key Differences

| Dimension | W&B | Artisan |
|---|---|---|
| **Real-time streaming** | Yes -- live metric updates during training | No -- metrics are artifacts captured at step completion |
| **Visualization** | Rich web dashboard with customizable panels | None built-in; query results with standard Python tools |
| **Run comparison** | Built-in side-by-side comparison, parallel coordinates | Query Delta Lake tables; use Polars/matplotlib/etc. |
| **Hyperparameter sweeps** | Integrated (grid, random, Bayesian + early stopping) | Not a feature -- use external tools or script loops |
| **System monitoring** | Automatic GPU/CPU/memory tracking | Not a feature |
| **Metric identity** | Metric name string attached to a run | Content-addressed artifact with provenance |
| **Metric querying** | Web UI filters + Python API | SQL-like queries over Delta Lake |

**The trade-off**: W&B gives you a polished, interactive, real-time experience for understanding what happened during training. Artisan gives you structured, queryable, provenance-tracked metric data that you analyze with standard tools. For iterative ML experimentation, W&B is clearly superior. For batch scientific pipelines where you need to query metrics across thousands of runs with full provenance, Artisan's approach is more powerful.

---

## Execution Model

### W&B Launch

W&B Launch provides job execution infrastructure:

- **Jobs**: Packaged, reproducible computations (Docker images or git references)
- **Queues**: FIFO queues that hold jobs waiting to execute
- **Agents**: Lightweight programs that poll queues and execute jobs on target resources
- **Target resources**: Docker (local), Kubernetes, Amazon SageMaker, Vertex AI
- **Sweeps on Launch**: Hyperparameter sweep schedulers that push sweep runs onto Launch queues

W&B Launch is primarily a **job dispatch system** -- it gets your training code running on the right infrastructure. It does not define pipeline DAGs, manage step dependencies, or orchestrate multi-step workflows.

### Artisan's Pipeline Engine

Artisan provides full pipeline orchestration:

- **PipelineManager**: Step sequencer with automatic dependency resolution, caching, and provenance
- **BackendBase**: Pluggable compute backends (LocalBackend, SlurmBackend, SlurmIntraBackend)
- **Two-level batching**: Artifacts-per-unit x units-per-worker for fine-grained parallelism control
- **Sandbox isolation**: Each execution gets its own directory tree
- **Deterministic caching**: Content-addressed hashes mean re-running with the same inputs skips computation automatically
- **Staging-commit**: Atomic writes prevent corruption from concurrent workers

### Key Differences

| Dimension | W&B Launch | Artisan |
|---|---|---|
| **Primary function** | Job dispatch to compute infrastructure | Pipeline orchestration with dependency management |
| **Pipeline DAG** | Not supported -- individual jobs only | Full DAG: steps, dependencies, fan-out, fan-in |
| **Caching** | None -- jobs always run | Automatic content-addressed caching |
| **HPC/SLURM** | Community workarounds (wandb agent + sbatch) | Native SlurmBackend via submitit job arrays |
| **Batching** | Not applicable | Two-level batching (artifacts-per-unit x units-per-worker) |
| **Isolation** | Docker containers | Filesystem sandbox per execution unit |
| **Target resources** | Docker, Kubernetes, SageMaker, Vertex AI | Local processes, SLURM; extensible via BackendBase |

**The critical distinction**: W&B Launch answers "where should this job run?" Artisan answers "what should run, in what order, with what inputs, and how should the results be stored and tracked?" They operate at different levels of abstraction. A team could use Artisan for pipeline orchestration and W&B for experiment tracking within individual steps.

---

## Collaboration & UI

### W&B

W&B's web platform is a major differentiator:

- **Dashboard**: Customizable panels (line charts, scatter plots, histograms, parallel coordinates, confusion matrices, images, tables)
- **Reports**: Collaborative documents with embedded live visualizations. Shareable with view/edit permissions. Conflict detection for concurrent edits.
- **Teams**: Shared workspaces with role-based access control. Team profile pages with showcased reports and projects.
- **Registry**: Organization-wide artifact governance with collections, aliases, role-based access, and audit trails for stage transitions (development -> staging -> production).
- **Artifact lineage viewer**: Visual DAG explorer in the web UI showing run and artifact relationships.
- **Sharing**: Links, email invitations, public/private projects.

### Artisan

Artisan is a library, not a platform:

- **Jupyter notebooks**: Primary interface for exploration and analysis.
- **Provenance graphs**: Graphviz-rendered DAGs (both micro and macro) viewable in notebooks or exported.
- **CLI**: Pipeline execution via Python scripts.
- **Querying**: Polars/DuckDB over Delta Lake for programmatic exploration.
- **No web UI**: No dashboard, no real-time monitoring, no collaborative editing.
- **No access control**: Filesystem permissions only.

**The trade-off**: W&B provides a turn-key collaborative experience. Artisan provides raw data access and expects users to build their own analysis workflows with standard tools. For teams that need to share results with stakeholders, W&B is far more accessible. For individual researchers or small teams on HPC clusters, Artisan's notebook-based workflow may be sufficient.

---

## Where W&B is Stronger

- **Experiment tracking UI**: Real-time metric streaming, interactive dashboards, run comparison, and hyperparameter visualization. This is W&B's raison d'etre and nothing in Artisan competes.
- **Collaboration**: Reports, team workspaces, sharing, role-based access. W&B is built for teams; Artisan is built for computation.
- **Ecosystem integration**: First-class integrations with PyTorch, TensorFlow, Keras, Hugging Face, Lightning, scikit-learn, XGBoost, and dozens more. Artisan is framework-agnostic but has no pre-built integrations.
- **Hyperparameter sweeps**: Built-in grid/random/Bayesian search with early termination and Launch integration. Artisan has no sweep functionality.
- **Managed infrastructure**: Cloud hosting, managed storage, automatic backups, 24/7 SRE team. Artisan requires you to manage your own infrastructure.
- **Artifact UI**: Web-based artifact browser, version comparison, lineage visualization. Artisan's lineage is viewable via Graphviz graphs in notebooks but has no interactive web explorer.
- **LLM tooling**: W&B Weave provides LLM-specific observability, tracing, and evaluation. Artisan has no LLM-specific features.
- **Model/dataset registry**: Organization-wide governance with collections, aliases, stage transitions, audit trails, and RBAC. Artisan has no registry concept.
- **Community and scale**: Large user base, extensive documentation, active community, corporate backing (acquired by CoreWeave in 2025 for ~$1.7B).

---

## Where Artisan is Stronger

- **Provenance depth**: Per-artifact derivation chains, not just run-level lineage. In a batch of 10,000 items, Artisan tracks which specific input produced which specific output. W&B tracks "this run consumed this dataset and produced this dataset."
- **Pipeline orchestration**: Full DAG-based pipeline engine with step sequencing, dependency resolution, fan-out, and fan-in. W&B Launch dispatches individual jobs but does not manage multi-step pipelines.
- **Deterministic caching**: Content-addressed hashing means identical inputs + parameters = automatic cache hit. No configuration needed, no false positives, no stale cache. W&B has no computation caching.
- **Batch artifact processing**: Operations natively produce and consume lists of artifacts. W&B's model is oriented toward single artifacts per `log_artifact()` call.
- **HPC/SLURM**: Native SlurmBackend with job arrays, two-level batching, and staging-commit for safe concurrent writes on shared filesystems. W&B's SLURM support requires manual agent setup and has known limitations with offline sweeps.
- **Self-hosted by default**: No cloud dependency. Runs on any filesystem. Critical for air-gapped HPC environments, data sovereignty requirements, and avoiding per-user SaaS costs.
- **Storage queryability**: Artifacts live in Delta Lake tables, directly queryable via Polars or DuckDB. "Give me all metrics from step 3 where parameter X > 0.5" is a one-liner. W&B queries go through their API or web UI.
- **Artifact identity model**: Content-addressed identity (hash = ID) provides mathematical guarantees: automatic deduplication, immutable provenance edges, deterministic caching. W&B uses file-level deduplication as an optimization but artifact identity is name + version, not content.
- **Typed artifact system**: Extensible type hierarchy (`ArtifactTypeDef` subclasses) with per-type serialization, hashing, and storage behavior. W&B artifact types are string labels with no enforced schema.
- **Cost model**: No per-user pricing, no storage fees, no cloud vendor. Infrastructure cost is whatever your HPC cluster or local machine costs.
- **Concurrent write safety**: Staging-commit pattern with atomic Delta Lake commits prevents corruption when thousands of SLURM workers write simultaneously to a shared filesystem.

---

## Key Differentiators Summary

| Dimension | W&B | Artisan |
|---|---|---|
| **Primary purpose** | Experiment tracking & MLOps platform | Batch pipeline framework for scientific computation |
| **Artifact identity** | Name + sequential version | Content hash (xxh3_128) |
| **Lineage model** | Bipartite DAG: Run <-> Artifact | Direct: Artifact -> Artifact (+ execution records) |
| **Lineage granularity** | Run-level | Individual artifact-level |
| **Provenance capture** | Opt-in (`log_artifact`/`use_artifact`) | Structural (automatic during execution) |
| **Pipeline orchestration** | No (Launch dispatches jobs, not pipelines) | Yes (PipelineManager with DAG, caching, provenance) |
| **Computation caching** | No | Yes (content-addressed, automatic) |
| **Batch processing** | Not native | First-class (lists of artifacts) |
| **Experiment visualization** | Rich web dashboard (core strength) | None built-in |
| **Hyperparameter sweeps** | Built-in (grid, random, Bayesian) | None |
| **Collaboration** | Teams, reports, sharing, RBAC | Notebook-based, filesystem permissions |
| **HPC/SLURM** | Community workarounds | Native backend |
| **Storage** | Cloud-managed (or self-hosted) | Local Delta Lake (ACID, queryable) |
| **Deduplication** | File-level optimization | Total (inherent in content addressing) |
| **Type system** | String labels | Typed class hierarchy with schema |
| **Infrastructure** | SaaS / Dedicated Cloud / Self-Managed | None required (filesystem only) |
| **Cost** | Free tier; per-user pricing for teams | Free (open source); self-hosted |
| **Ecosystem** | 50+ framework integrations | Framework-agnostic, extensible |
| **LLM tooling** | Weave (tracing, evaluation) | None |

---

## Relationship: Complementary, Not Competing

The most natural relationship between these tools is **complementary**:

- Use **Artisan** for pipeline orchestration, artifact management, provenance tracking, caching, and HPC execution.
- Use **W&B** for experiment tracking, metric visualization, hyperparameter sweeps, and team collaboration.

A concrete integration pattern: Artisan operations could call `wandb.log()` during execution to stream real-time metrics to W&B dashboards, while Artisan handles the pipeline orchestration, artifact provenance, and persistent storage. W&B provides the UI layer; Artisan provides the computational substrate.

This mirrors how many teams combine workflow engines (Nextflow, Snakemake) with experiment trackers (W&B, MLflow) -- the tools solve different problems and layer naturally.
