# Artisan vs DVC: Provenance & Artifact Tracking Comparison

## Executive Summary

DVC (Data Version Control) is a Git-centric tool for versioning large files and defining reproducible ML pipelines, built around the metaphor of "Git for data." Artisan is a batch pipeline framework for scientific/computational workflows with content-addressed artifacts, dual provenance systems, and native HPC execution. They operate at fundamentally different abstraction levels: DVC versions **files** and tracks **stage-level** lineage via lock files committed to Git, while artisan versions **typed artifacts** and tracks both **per-artifact derivation chains** and **execution provenance** in a persistent Delta Lake store. A team could conceivably use both -- DVC for raw input data versioning, artisan for the computational pipeline that consumes it -- though in practice their scopes overlap enough that most projects would choose one.

---

## Provenance & Lineage

| Dimension | DVC | Artisan |
|---|---|---|
| **Provenance model** | Stage-level: dvc.lock records input/output hashes per stage | Dual: execution provenance (what ran) + artifact provenance (per-artifact derivation chains) |
| **Granularity** | Stage inputs/outputs as files or directories | Individual artifacts within a batch (A->D, B->E, C->F) |
| **Lineage representation** | DAG of stages in dvc.yaml, viewable via `dvc dag` | Micro provenance (artifact-to-artifact edges) and macro provenance (step-level DAG) |
| **Standards alignment** | None (proprietary lock file format) | W3C PROV-aligned: ExecutionRecord=Activity, Artifact=Entity, ArtifactProvenanceEdge=wasDerivedFrom |
| **Capture mechanism** | Declared explicitly in dvc.yaml (deps/outs) | Structural -- captured automatically during execution |
| **Derivation chains** | Stage-to-stage only; no within-stage item tracking | Per-artifact derivation edges, even within batch operations |

### How DVC tracks lineage

DVC's lineage model is anchored in two files: `dvc.yaml` defines the pipeline DAG as a series of stages with declared dependencies (`deps`), parameters (`params`), and outputs (`outs`). When `dvc repro` executes, `dvc.lock` is generated/updated with the MD5 hash of every dependency and output for each stage. The `dvc dag` command renders the stage-level DAG. This gives you reproducibility (re-run from any Git commit) and coarse lineage (which stages produced which files).

What DVC does **not** capture is fine-grained provenance within a stage. If a stage processes a directory of 10,000 images and produces 10,000 feature vectors, DVC tracks the directory as a single artifact. There is no record of which input image produced which output feature vector. The entire output directory is a single hash entry; if one input changes, the entire stage re-runs and the entire output is replaced.

### How artisan tracks lineage

Artisan captures provenance at two complementary levels. Execution provenance records what ran: which operation, what parameters, when, how long, on which worker. Artifact provenance records derivation edges between individual artifacts -- if a batch operation transforms artifacts [A, B, C] into [D, E, F], artisan records the edges A->D, B->E, C->F. Because artifact identity is the content hash, these edges are immutable: they cannot go stale or refer to modified data.

### Assessment

DVC's stage-level lineage is sufficient for typical ML workflows (preprocess -> train -> evaluate) where you care about which *pipeline version* produced a model. It falls short for scientific workflows where you need to trace an individual result back through a chain of transformations -- the kind of question artisan's micro provenance is designed to answer. DVC's lineage is also opt-in (you must correctly declare deps/outs); artisan's is structural.

---

## Artifact / Data Model

| Dimension | DVC | Artisan |
|---|---|---|
| **Fundamental unit** | File or directory | Typed artifact (Data, Metric, FileRef, Config, ExecutionConfig) |
| **Identity** | MD5 hash of file contents (32-character hex) | xxh3_128 hash of content bytes |
| **Identity file** | `.dvc` metadata file (YAML with md5, size, path) | No sidecar files; identity is an intrinsic property |
| **Type system** | Implicit categories: outs, metrics, plots, params | Explicit typed hierarchy via ArtifactTypeDef subclasses |
| **Metadata** | Artifact declarations in dvc.yaml (type, description, labels, custom metadata) | Typed artifact fields, roles (named input/output slots), specs |
| **Batch semantics** | Directory = single artifact; no per-item identity | First-class: operations produce/consume lists of artifacts, each individually addressable |
| **Deduplication** | Content-addressable cache; same file content stored once | Content-addressable storage; same content = same artifact_id, stored once |
| **Schema / validation** | None for data content; metrics must be JSON/YAML key-value | InputSpec/OutputSpec declarations with type matching and role validation |

### DVC's data model

DVC operates at the file/directory granularity. When you run `dvc add data.csv`, DVC computes the MD5 hash, moves the file into `.dvc/cache/files/md5/<first-2-chars>/<remaining-chars>`, and creates a `data.csv.dvc` sidecar file containing the hash, size, and path. This `.dvc` file is committed to Git in place of the actual data.

For directories, DVC creates a `.dir` manifest -- a JSON array mapping each file's relative path to its MD5 hash. The manifest itself is hashed and cached. This means DVC does track individual file hashes within directories, but this tracking is for cache integrity and deduplication, not for provenance edges.

DVC distinguishes output categories at the stage level: `outs` (regular outputs), `metrics` (JSON/YAML key-value files for experiment comparison), `plots` (CSV/JSON/TSV/YAML for visualization), and `params` (configuration consumed by stages). These are more like output *annotations* than a type system -- a metric file is still just a file; DVC does not enforce a schema or validate content structure.

Since the lakeFS acquisition, DVC also supports artifact declarations in dvc.yaml with metadata fields (type, description, labels, custom metadata), primarily for model registry integration. This is a metadata layer on top of files, not a typed artifact system.

### Artisan's data model

Artisan's artifacts are content-addressed Python objects with explicit types. `artifact_id = xxh3_128(content)` means identity is literally the content hash -- not a sidecar reference to one. The type system (Data, Metric, FileRef, Config, ExecutionConfig) is extensible via subclassing, and operations declare what they consume and produce via InputSpec/OutputSpec with named roles. Artifacts carry their type, role, and provenance edges as intrinsic properties.

The batch-native design is a key differentiator. Where DVC treats a directory of 10,000 files as one artifact, artisan treats 10,000 Data artifacts as 10,000 individually addressable, individually provenanced entities. Operations consume and produce lists, and the framework tracks which input artifact(s) contributed to each output artifact.

### Assessment

DVC's file-level model is simpler and more universal -- anything on disk can be a DVC artifact with zero code changes. Artisan's typed artifact model requires operations to work within the framework's abstractions but delivers per-item identity, provenance, deduplication, and queryability that file-level tracking cannot provide. The tradeoff is adoption cost vs tracking depth.

---

## Storage & Persistence

| Dimension | DVC | Artisan |
|---|---|---|
| **Local storage** | `.dvc/cache/` content-addressable file store | Delta Lake tables on local/shared filesystem |
| **Remote storage** | S3, GCS, Azure Blob, SSH/SFTP, HDFS, HTTP, local/NAS | Shared filesystem (NFS, Lustre, GPFS -- typical HPC) |
| **Cache link types** | reflink, hardlink, symlink, copy (OS-dependent) | N/A -- artifacts are rows in Delta Lake tables, not linked files |
| **Transaction model** | File-level; `rw.lock` prevents concurrent DVC operations | ACID via Delta Lake; staging-commit pattern for concurrent write safety |
| **Queryability** | None built-in; parse files or use DVC Studio | SQL-like queries via Polars/DuckDB on Delta Lake tables |
| **Concurrency safety** | Single writer per repo (`rw.lock`); problematic on HPC | Staging-commit pattern designed for shared NFS/HPC filesystems |
| **Cloud versioning** | Native support for versioned S3/GCS buckets | Not applicable (filesystem-oriented) |

### DVC's storage model

DVC's cache is a straightforward content-addressable file store. Files are stored under `.dvc/cache/files/md5/` using their MD5 hash as the path. Remote storage mirrors this structure on any supported backend (S3, GCS, Azure, SSH, local). `dvc push` and `dvc pull` synchronize between local cache and remote.

DVC uses filesystem-level optimization where available: reflinks (copy-on-write) on supported filesystems (APFS, XFS, Btrfs), falling back to hardlinks, symlinks, or copies. This minimizes disk usage when checking out different versions.

A significant limitation for HPC use is the `rw.lock` file. Only one DVC process can modify the repository at a time. Multiple concurrent `dvc repro` invocations -- the natural pattern on a SLURM cluster where many jobs run in parallel -- are blocked by this lock. This is a known pain point documented in DVC's issue tracker.

### Artisan's storage model

Artisan stores artifacts as rows in Delta Lake tables, providing ACID transactions and schema enforcement. The staging-commit pattern is specifically designed for the shared-filesystem reality of HPC: each worker stages results in an isolated directory, then a coordinator commits them atomically. This prevents the concurrent-write corruption that plagues file-based systems on NFS.

Artisan does not support cloud object stores as a primary storage backend (it targets shared HPC filesystems), but the Delta Lake format is inherently portable. The queryability advantage is substantial for analysis workflows: you can query artifact metadata, provenance edges, and even content using standard data tools without custom parsing.

### Assessment

DVC wins on storage backend breadth -- it works with essentially any cloud or local storage. Artisan wins on transactional safety and queryability. For HPC environments with shared filesystems, artisan's staging-commit pattern is a direct answer to a real DVC limitation (the rw.lock bottleneck). For teams whose data lives in cloud object stores, DVC's native remote support is a clear advantage.

---

## Caching & Reproducibility

| Dimension | DVC | Artisan |
|---|---|---|
| **Cache key** | MD5 of dependencies + parameters + command text | xxh3_128 of input content hashes + operation parameters |
| **Cache granularity** | Per-stage | Per-operation invocation (which may process many artifacts) |
| **Run cache** | Automatic; stored in `.dvc/cache/runs/`; keyed by stage signature | Automatic; content-addressed -- same inputs + params = same cache key |
| **Incremental runs** | `dvc repro` skips unchanged stages | Pipeline engine skips operations where all inputs match cached results |
| **Lock file** | `dvc.lock` -- committed to Git, records exact hashes | No lock file; cache is intrinsic to the content-addressed store |
| **Forced re-run** | `dvc repro --force` | Configurable via CachePolicy |
| **Reproducibility model** | Git commit + dvc.lock + remote data = exact reproduction | Content hashes guarantee deterministic reproduction without external version control |

### DVC's caching

DVC's run cache is elegant and automatic. Every stage execution is logged with a signature derived from the MD5 hashes of all dependencies, parameter values, and the literal command string. On subsequent `dvc repro`, DVC compares current signatures against the run cache. If a match is found, outputs are restored from cache without re-execution.

The `dvc.lock` file serves as a portable reproducibility manifest: given the same Git commit (which pins code and dvc.lock), `dvc pull` retrieves the exact data versions, and `dvc repro` either confirms everything is cached or re-runs only changed stages.

One subtlety: DVC's cache is file-granularity. If a stage produces a directory, the entire directory is cached as one unit. Changing one file in an input directory invalidates the entire stage, even if the stage could theoretically process only the changed file.

### Artisan's caching

Artisan's caching follows directly from content addressing. Since `artifact_id = hash(content)`, two runs with identical inputs and parameters produce identical artifact IDs. The cache lookup is a simple existence check: does this artifact_id already exist in storage? If so, skip the computation.

This is both simpler and more granular than DVC's approach. There is no separate "run cache" mechanism -- caching is an emergent property of content addressing. And because artisan tracks individual artifacts rather than directories, caching operates at the per-artifact level within a batch.

### Assessment

Both systems achieve the same high-level goal (skip redundant computation), but through different mechanisms. DVC's approach is file-centric and requires the dvc.lock/Git coordination loop. Artisan's approach is self-contained -- the content-addressed store *is* the cache. The practical difference shows up in batch processing: artisan can cache individual items within a batch, while DVC must re-run an entire stage if any input changes.

---

## Execution Model

| Dimension | DVC | Artisan |
|---|---|---|
| **Pipeline runner** | `dvc repro` (sequential stage execution) | Pipeline engine with configurable backends |
| **Parallelism** | Independent branches can be manually parallelized; `dvc exp run --jobs` for experiments | Two-level batching: artifacts-per-unit x units-per-worker |
| **HPC / SLURM** | No native support; community workarounds via sbatch wrapping | Native SlurmBackend via submitit with job arrays |
| **Stage execution** | Shell command (`cmd` field) | Python class method (operation.execute()) |
| **Worker isolation** | Working directory per stage | Full sandbox: preprocess/execute/postprocess directories per execution |
| **Error handling** | `--keep-going` to skip failed branches | Per-item containment with CONTINUE or FAIL_FAST policy |
| **Experiment parallelism** | `dvc exp run --jobs N` for queued experiments | Inherent via batching and worker pools |

### DVC's execution model

DVC executes pipeline stages sequentially by default via `dvc repro`, respecting the dependency order defined in the DAG. Each stage runs a shell command in the repository's working directory. There is no built-in parallel execution of stages within a single `dvc repro` invocation -- a long-standing feature request (issue #755, opened 2018) that remains unresolved.

For experiment parallelism, DVC provides `dvc exp run --jobs N`, which can run multiple experiment variations in parallel. However, this parallelizes across experiments, not within a pipeline.

HPC/SLURM integration is a notable gap. DVC's `rw.lock` prevents concurrent DVC operations on the same repository, making it fundamentally incompatible with the multi-job-on-shared-filesystem pattern common in HPC. Community workarounds exist (wrapping `dvc repro` in sbatch scripts, using `dvc commit` after external job completion), but these are fragile and lose DVC's caching and lineage benefits for the externally-executed steps.

### Artisan's execution model

Artisan's pipeline engine natively manages parallel execution across multiple backends. The SlurmBackend submits operations as SLURM jobs via submitit, with two-level batching controlling how artifacts are distributed across workers and how many workers are launched. Each execution runs in a fully isolated sandbox (separate preprocess, execute, and postprocess directories), preventing interference between concurrent workers on a shared filesystem.

The staging-commit pattern coordinates results from parallel workers: each worker writes to its own staging area, and a coordinator atomically commits all results to the Delta Lake store. This is specifically engineered for HPC shared filesystems.

### Assessment

DVC assumes a single-machine, sequential execution model with cloud remotes for data. Its execution is minimal by design -- it runs shell commands and checks hashes. Artisan assumes a multi-worker, potentially multi-node execution model on shared filesystems. For ML experimentation workflows on a developer laptop, DVC's simplicity is an advantage. For batch scientific computation on HPC clusters, artisan's execution model addresses real problems that DVC cannot.

---

## Version Control Integration

| Dimension | DVC | Artisan |
|---|---|---|
| **Git coupling** | Tight: `.dvc` files and `dvc.lock` committed to Git; data versions tied to Git commits | Loose: pipeline scripts are code (Git-versioned); artifacts stored independently in Delta Lake |
| **Version semantics** | Git commit = snapshot of code + data pointers | Content hash = artifact identity; versions are implicit |
| **Branching** | Git branches create data branches (via `.dvc` file versions) | No data branching mechanism |
| **Checkout** | `git checkout <commit> && dvc checkout` restores exact code + data state | Query Delta Lake store for artifacts by hash, pipeline run, or time |
| **Collaboration** | Standard Git workflow (PR, merge, etc.) with `dvc push`/`dvc pull` for data | Shared filesystem; multiple users query same Delta Lake store |
| **Experiment management** | `dvc exp` commands: run, show, diff, apply (Git-backed) | Pipeline re-runs with different parameters; compare via provenance queries |

### DVC's Git integration

Git integration is DVC's defining feature and its strongest differentiator. The `.dvc` sidecar files and `dvc.lock` are small enough to commit to Git, creating a tight coupling between code versions and data versions. `git log` shows you the history of both. `git checkout v1.0 && dvc checkout` gives you the exact code and data from that release. Git branches create parallel data versions. Git tags bookmark reproducible states.

This model is immediately intuitive to anyone familiar with Git. It requires no new infrastructure, no database, no server -- just Git and a storage remote. The experiment tracking system (`dvc exp`) extends this further, storing experimental variations as lightweight Git refs.

### Artisan's approach

Artisan does not couple to Git at the storage level. Pipeline scripts are normal Python files versioned in Git, but artifacts and their provenance live in Delta Lake tables on the filesystem. Artifact identity is the content hash, not a Git commit reference. You retrieve artifacts by querying the store, not by checking out a Git revision.

This means artisan has no native "data branching" or "data tagging" mechanism analogous to DVC's Git-backed approach. On the other hand, it means artisan's storage is self-contained -- you do not need to coordinate Git commits with data state.

### Assessment

DVC's Git integration is genuinely powerful and provides a mental model that teams already understand. It is the primary reason to choose DVC. Artisan's independence from Git means less ceremony for HPC batch workflows where the "commit code, push data" loop adds friction, but it sacrifices the branch/tag/checkout semantics that make DVC's version control story compelling.

---

## Where DVC is Stronger

**Git integration and version semantics.** DVC's tight coupling with Git provides branch-based data versioning, tag-based snapshots, and a familiar workflow that integrates with existing team practices (PRs, code review, CI/CD). No other tool in this space does this as naturally.

**Storage backend breadth.** S3, GCS, Azure, SSH, HDFS, HTTP, local, Google Drive -- DVC works with whatever storage you already have. Artisan is filesystem-only.

**Ecosystem and community.** DVC has years of community adoption, extensive documentation, VS Code extension, DVC Studio (web UI for experiment comparison), DVCLive (automatic logging from ML frameworks), and a model registry. As of 2025, it was acquired by lakeFS, giving it backing from a well-funded data infrastructure company.

**Simplicity and adoption cost.** DVC requires zero code changes to existing projects. Run `dvc init`, `dvc add` your data, define stages in `dvc.yaml`, and you have versioning and reproducibility. Artisan requires writing operations as Python classes within its framework.

**Experiment tracking.** `dvc exp run`, `dvc exp show`, `dvc exp diff`, `dvc plots` provide a complete experiment comparison workflow for ML. Artisan does not have a dedicated experiment management subsystem.

**ML-specific features.** Metrics tracking, plots generation, parameter management, and model registry are purpose-built for the ML lifecycle. Artisan's typed artifact system is more general but lacks these ML-specific conveniences.

---

## Where Artisan is Stronger

**Provenance depth.** Artisan's dual provenance system (execution + per-artifact derivation) is categorically richer than DVC's stage-level lineage. The ability to trace an individual result back through a chain of transformations -- not just "which stage produced this directory" but "which specific input artifact produced this specific output artifact" -- is essential for scientific reproducibility and audit.

**Batch artifact processing.** Artisan natively handles operations that produce and consume lists of individually-identified artifacts. DVC treats directories atomically: one changed file invalidates the entire stage. For workflows processing thousands of items (simulations, image pipelines, genomic samples), artisan's per-artifact caching and provenance avoids massive redundant recomputation.

**Typed artifact system.** Artisan's extensible type hierarchy (Data, Metric, FileRef, Config, ExecutionConfig) with InputSpec/OutputSpec declarations provides compile-time-like validation of pipeline wiring. DVC's output annotations (outs, metrics, plots) are hints, not enforced types.

**HPC execution.** Native SLURM support with two-level batching, job arrays, sandbox isolation, and the staging-commit pattern for shared-filesystem safety. DVC's rw.lock makes it fundamentally incompatible with concurrent multi-node execution on shared filesystems.

**Concurrent write safety.** The staging-commit pattern with Delta Lake ACID transactions prevents the corruption that file-based systems suffer on shared NFS. DVC's rw.lock serializes all operations to a single writer, which is a bottleneck in any parallel environment.

**Queryable storage.** Artifacts, metadata, and provenance edges stored in Delta Lake tables are directly queryable via Polars/DuckDB. With DVC, analyzing results requires parsing output files and manually correlating with `dvc.lock` hashes.

**Content-addressed identity.** Artisan's `artifact_id = hash(content)` makes identity intrinsic. DVC's identity is a sidecar file referencing an MD5 hash. The practical difference: artisan's caching and deduplication are emergent properties of the data model, not a separate mechanism layered on top.

---

## Key Differentiators Summary

| Dimension | DVC | Artisan |
|---|---|---|
| **Primary metaphor** | "Git for data" | "Content-addressed artifact pipeline" |
| **Target user** | ML engineers, data scientists | Computational scientists, HPC researchers |
| **Provenance granularity** | Stage-level (file/directory) | Individual artifact (within batches) |
| **Provenance capture** | Declarative (deps/outs in dvc.yaml) | Structural (automatic during execution) |
| **Data identity** | MD5 hash in sidecar `.dvc` file | xxh3_128 hash as intrinsic artifact_id |
| **Type system** | Output annotations (outs/metrics/plots) | Extensible ArtifactTypeDef hierarchy |
| **Batch handling** | Directory = single unit | List of individually-addressed artifacts |
| **Git coupling** | Tight (core design principle) | Loose (scripts in Git; artifacts in Delta Lake) |
| **Storage backends** | Cloud + local + NAS + SSH | Shared filesystem (Delta Lake) |
| **HPC / SLURM** | No native support | Native with two-level batching |
| **Concurrent writes** | rw.lock (single writer) | Staging-commit with ACID transactions |
| **Caching mechanism** | Run cache (stage signatures) | Content-addressed store (emergent) |
| **Result queryability** | Parse files / DVC Studio | SQL-like via Polars/DuckDB |
| **Adoption cost** | Low (no code changes) | Higher (operations as framework classes) |
| **Ecosystem** | Mature (Studio, DVCLive, VS Code, model registry) | Focused (framework-only) |
| **Community** | Large (acquired by lakeFS, 2025) | Project-scale |
| **Standards alignment** | None | W3C PROV (Activity, Entity, wasDerivedFrom) |
