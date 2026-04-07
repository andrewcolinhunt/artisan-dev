# Artisan vs Sacred: Provenance & Artifact Tracking Comparison

## Executive Summary

Sacred is a pioneering open-source Python framework for experiment management developed at IDSIA, first presented at SciPy 2017 by Klaus Greff et al. It focuses on configuration management, experiment logging, and reproducibility for individual experiment runs. Artisan is a batch pipeline framework for scientific/computational workflows that treats provenance and content-addressed artifacts as structural primitives, not opt-in logging. Sacred occupies the "experiment tracker" niche -- capturing what happened during a single run -- while artisan occupies the "pipeline orchestration with deep provenance" niche, tracking how individual artifacts derive from one another across multi-step workflows. Sacred was historically significant as one of the first serious Python experiment tracking tools, but its development has slowed considerably (latest release 0.8.7, ~late 2024, after a long gap from 0.8.4 in January 2023), and the broader ecosystem has largely moved to MLflow, Weights & Biases, and similar platforms.

## Provenance & Lineage

### Sacred

Sacred's provenance model is **experiment-run-centric**. Each run captures:

- **Configuration**: The full resolved config dict, including what was changed from defaults and whether changes are "suspicious" (undeclared variables).
- **Source code**: Auto-discovered Python source files for the experiment, stored alongside the run.
- **Git info**: Repository URL, current commit hash, and dirty status (if the experiment lives in a git repo).
- **Dependencies**: Auto-discovered package imports and their versions.
- **Host info**: Machine metadata (hostname, OS, CPU, GPU, Python version, etc.).
- **Metrics**: Time-series scalar values logged via `_run.log_scalar(metric_name, value, step)`, with automatic step counters.
- **Artifacts**: Files produced during the run, added via `ex.add_artifact()`.
- **Resources**: Files read during the run, opened via `ex.open_resource()`, with MD5 hashes logged.
- **Stdout/stderr**: Captured output, reported to observers via periodic heartbeat events (default 10-second interval).
- **Result**: The return value of the main function.
- **Timing**: Start time, stop time, heartbeat timestamps.
- **Status**: QUEUED, RUNNING, COMPLETED, FAILED, INTERRUPTED.
- **Custom info dict**: A mutable dictionary for arbitrary user data (e.g., training curves), reported with each heartbeat.

What Sacred does **not** track is **data lineage between experiments**. There is no concept of "experiment B consumed the output of experiment A." Each run is an isolated record. If experiment 3 used a model file produced by experiment 1, that relationship exists only in the researcher's head (or in convention-based file naming). There is no `wasDerivedFrom` edge, no upstream/downstream linking, no artifact-level derivation chain.

### Artisan

Artisan's provenance model is **artifact-derivation-centric** with two complementary layers:

- **Micro provenance**: Individual artifact-level derivation chains (A derived from B derived from C), captured automatically as `ArtifactProvenanceEdge` records (W3C PROV `wasDerivedFrom` semantics). These are structural -- they emerge from the pipeline execution, not from manual logging calls.
- **Macro provenance**: Step-level DAG showing which pipeline steps produced which outputs from which inputs.
- **Execution provenance**: `ExecutionRecord` objects (W3C PROV `Activity`) recording what operation ran, with what parameters, consuming which inputs, producing which outputs.

The fundamental difference: Sacred answers "what happened in run #47?" Artisan answers "where did this specific artifact come from, through what chain of transformations, and can I trace any output back to its ultimate sources?"

## Artifact Model

### Sacred

Sacred distinguishes two file categories:

- **Artifacts**: Files created during a run, added explicitly via `_run.add_artifact(filename, name=None)`. This fires an `artifact_event` to observers. With MongoObserver, the file is stored in GridFS (MongoDB's chunked file storage). The artifact is associated with the run by name.
- **Resources**: Files read during a run, opened via `ex.open_resource(filename)`. The MongoObserver checks if the file already exists in the database (by MD5 hash) before storing. The filename and MD5 hash are logged regardless.

Key characteristics:
- Artifacts are **opaque files** -- Sacred does not understand their content, type, or schema.
- Identity is by **name** (the filename or explicit name string), not by content.
- No content addressing for artifacts (resources do get MD5 hashes for deduplication in MongoDB, and the TinyDbObserver uses HashFS for content-addressed file storage).
- No typed artifact system -- a model checkpoint, a CSV of metrics, and a PNG plot are all just "artifact files."
- No role system -- artifacts are not assigned semantic roles (input slot, output slot).
- No batch semantics -- artifacts are individual files, not lists of typed records.
- Deduplication is partial: MongoObserver deduplicates resources by MD5, and TinyDbObserver deduplicates all files via HashFS. But artifact identity is not content-based at the API level.

### Artisan

- Artifacts are **content-addressed**: `artifact_id = xxh3_128(content)`. The identity IS the content hash (32-character hex string). Same content always produces the same ID, stored once.
- **Typed**: Data, Metric, FileRef, Config, ExecutionConfig -- each with its own schema, extensible via `ArtifactTypeDef` subclasses.
- **Roles**: Artifacts have named input/output slots on operations (e.g., `"data"`, `"metric"`, `"file"`).
- **Batch-native**: Operations produce and consume lists of artifacts, not single values.
- **Immutable**: Once created, an artifact's content and ID never change. Provenance edges cannot go stale because they reference content hashes.
- **Automatic deduplication**: Same content = same ID = stored once, globally, across all pipeline runs.

The gap is significant. Sacred's artifact model is "here's a file I made during this run." Artisan's is "here's a typed, content-addressed record with a deterministic identity, semantic role, and full derivation history."

## Storage & Persistence

### Sacred

Sacred uses an **observer pattern** with pluggable backends:

| Observer | Storage | Querying | Setup |
|---|---|---|---|
| **MongoObserver** | MongoDB + GridFS | Powerful (MongoDB queries on any config/result field) | Requires MongoDB server |
| **FileStorageObserver** | Local filesystem (JSON + files) | Manual file inspection only | Zero setup |
| **TinyDbObserver** | TinyDB (JSON) + HashFS | Basic Python queries via TinyDbReader | Zero setup, content-addressed files |
| **SqlObserver** | SQL database (PostgreSQL, SQLite, etc.) | SQL queries | Requires database |
| **S3Observer** | Amazon S3 | None built-in | Requires S3 credentials |

The MongoObserver is the "recommended" backend and the most capable, but it requires running a MongoDB instance. The FileStorageObserver is the most commonly used for quick experiments (zero infrastructure), but has no query capability -- you just get directories of JSON files. Multiple observers can be attached simultaneously.

A notable limitation: `_run.log_scalar()` metrics are only fully supported by MongoObserver. Other observers have limited or no metrics support.

### Artisan

- **Delta Lake** backed storage with ACID transactions.
- **Staging-commit pattern**: prevents concurrent write corruption on shared NFS/HPC filesystems (a real concern Sacred does not address).
- Querying via Polars DataFrames on Delta tables -- full analytical query capability without external infrastructure.
- Single storage model, not a pluggable observer pattern.

The trade-off: Sacred offers more storage flexibility (MongoDB, filesystem, SQL, S3) but with inconsistent feature support across backends. Artisan has one storage model with full feature parity and ACID guarantees.

## Configuration Management

### Sacred -- a genuine strength

Sacred's configuration system is arguably its most innovative feature:

- **Config Scopes**: Functions decorated with `@ex.config` where local variables become config entries. Full Python available inside the scope (conditionals, loops, dict comprehensions).
- **Config Injection**: Captured functions (decorated with `@ex.capture`) automatically receive config values matched by parameter name. No explicit plumbing needed.
- **Named Configs**: Bundles of non-default config values (`@ex.named_config`) that can be activated from the command line (`python my_experiment.py with my_named_config`).
- **Command-line overrides**: `python my_experiment.py with learning_rate=0.01 optimizer.type='adam'` -- dot-notation for nested config.
- **Config composition**: Multiple named configs can be layered (order matters -- later configs override earlier ones).
- **Ingredients**: Reusable config+function bundles that can be composed hierarchically. An Ingredient has its own config scope, captured functions, and can be used across experiments.
- **Change tracking**: Sacred records what config values were changed from defaults and flags "suspicious" changes (setting values that don't exist in the default config).

Known limitations:
- Dictionary keys must be valid Python identifiers (problematic for libraries like scikit-learn).
- The configuration implementation has been described by maintainers as "rather messy and hard to maintain" (GitHub issue #610).
- Config values are read-only in captured functions (Sacred raises an exception on mutation), which catches bugs but can be surprising.
- The Ingredient system has limitations in practice -- ingredients must be self-contained and cannot be easily configured from the parent experiment context.
- Type annotations in config scopes were only added in recent versions (0.8.4+).

### Artisan

- Params are Pydantic `BaseModel` subclasses defined as `Params` inner classes on operations.
- Type validation, default values, field descriptions via Pydantic.
- No magic injection -- params are explicitly passed and accessed.
- No command-line override system (pipeline parameters are set programmatically).
- No equivalent to Sacred's named configs or config composition.
- Params are included in the content hash for deterministic caching (`compute_execution_spec_id` includes canonicalized params).

Sacred's config system is more expressive and researcher-friendly for interactive experimentation. Artisan's is more structured and type-safe, but less flexible for ad-hoc exploration.

## Execution Model

### Sacred

Sacred's execution model is **single-experiment, decorator-driven**:

```python
from sacred import Experiment
ex = Experiment('my_experiment')

@ex.config
def cfg():
    learning_rate = 0.01
    epochs = 100

@ex.automain
def main(learning_rate, epochs):
    # Config values injected by name
    model = train(learning_rate, epochs)
    return final_metric
```

- One experiment = one decorated main function.
- Execution is triggered by running the script (`python experiment.py`) or calling `ex.run()`.
- Observer lifecycle: `started_event` -> periodic `heartbeat_event` (10s) -> `completed_event` / `failed_event` / `interrupted_event`.
- **Queue flag** (`-q`/`--queue`): Creates a database entry with all config needed to run, without actually executing. Designed for distributed worker pools that pull from the database. However, Sacred provides no worker implementation -- users must build their own.
- No pipeline concept. No step chaining. No DAG execution. Each experiment is independent.
- No batch processing -- the main function runs once per invocation.
- No built-in parallelism or HPC support (SLURM, etc.).
- No sandbox isolation between runs.
- No deterministic caching (no way to say "skip this run, it was already computed with the same inputs").

### Artisan

- Pipeline engine with DAG-based step execution.
- Operations are classes (not decorated functions) with typed inputs, outputs, and params.
- Batch-native: operations process lists of artifacts.
- Built-in executors: local process pool and SLURM (via submitit) for HPC.
- Full sandbox isolation per execution.
- Deterministic caching: if the same operation with the same inputs and params has already been run, the cached result is returned (enabled by content-addressed artifact IDs).
- Orchestration handles batching, dispatch, input resolution, and output staging.

Sacred is designed for "run this function and log what happens." Artisan is designed for "orchestrate this multi-step workflow, track every artifact transformation, and cache intermediate results."

## Maintenance & Ecosystem

### Sacred

- **Origin**: Developed at IDSIA (Swiss AI Lab), published at SciPy 2017 by Klaus Greff, Aaron Klein, Martin Chovanec, Frank Hutter, and Jurgen Schmidhuber.
- **GitHub**: ~4,200 stars. Repository at `IDSIA/sacred`.
- **Release history**: 0.8.4 (January 2023) was the last release for a long period. 0.8.5-0.8.7 followed later, with 0.8.7 appearing in ~late 2024. Releases are infrequent -- the project is in maintenance mode, not active development.
- **Open issues**: Significant backlog. Issues continue to be filed (including Python compatibility problems), but response times are slow. A GitHub issue titled "Is sacred dead?" (#761) from 2020 reflects community sentiment.
- **Python support**: Officially 3.8-3.11, with compatibility issues reported for 3.10+ (deprecated `collections` attributes). Newer Python versions may require workarounds.
- **Ecosystem tools**:
  - **Omniboard**: React/Node.js web dashboard for Sacred+MongoDB. Maintained separately.
  - **Sacredboard**: Older Python-based web dashboard. Less actively maintained.
  - **Incense**: Python library for querying Sacred runs in Jupyter notebooks.
  - **Neptune integration**: `neptune-sacred` package for logging Sacred experiments to Neptune's platform.

### Historical significance

Sacred was one of the earliest serious Python tools for ML experiment tracking (predating MLflow by several years). Its config scope system was genuinely innovative. It influenced the design of many later tools.

### What overtook it

- **MLflow** (~23K GitHub stars): Open-source, broader scope (tracking, models, projects, registry), backed by Databricks.
- **Weights & Biases** (~10K GitHub stars): Cloud-hosted, polished UI, strong collaboration features.
- **Neptune.ai**: SaaS platform with Sacred migration path.
- **DVC**: Data version control with Git-like semantics.

Sacred remains usable but is no longer where the community's momentum lies. New projects starting today would be unlikely to choose Sacred over MLflow or W&B unless they specifically need its config system.

## Where Sacred is Stronger

- **Configuration management**: Sacred's config scope system (Python functions as config, automatic injection by name, named configs, command-line overrides, change tracking) is more expressive than artisan's Pydantic Params for interactive experimentation and rapid iteration.
- **Zero-infrastructure start**: `FileStorageObserver` requires nothing -- no database, no setup. Just run the script and results are saved to a directory. This is genuinely useful for prototyping.
- **Simplicity for single experiments**: If you have one script that trains a model and you want to log configs, metrics, and save the model file, Sacred does this with minimal boilerplate. No pipeline definition, no operation classes, no type system.
- **Flexible storage backends**: Choose between MongoDB (powerful querying), filesystem (zero setup), TinyDB (local with content-addressed files), SQL (enterprise), or S3 (cloud). Attach multiple observers simultaneously.
- **Established community knowledge**: Years of blog posts, tutorials, and Stack Overflow answers. Integration with Neptune for visualization.
- **Source code and dependency capture**: Automatic snapshotting of experiment source code and package versions is built in. Useful for reproducibility even without version control discipline.

## Where Artisan is Stronger

- **Provenance depth**: Two-layer provenance (micro artifact chains + macro step DAG) with W3C PROV alignment. Sacred has no data lineage between experiments at all -- each run is an island.
- **Content-addressed artifacts**: `artifact_id = xxh3_128(content)` gives automatic deduplication, deterministic caching, and immutable provenance edges. Sacred's artifacts are identified by filename, not content.
- **Typed artifact system**: Data, Metric, FileRef, Config, ExecutionConfig with schemas and roles vs. Sacred's opaque file blobs.
- **Pipeline orchestration**: DAG-based multi-step workflows with automatic input resolution. Sacred has no pipeline concept.
- **Batch processing**: Operations natively consume and produce lists of artifacts. Sacred processes one experiment invocation at a time.
- **HPC execution**: Built-in SLURM support via submitit, local process pool, sandbox isolation. Sacred's `--queue` flag is a stub that requires users to build their own workers.
- **Deterministic caching**: Same inputs + same params = cache hit, automatically. Sacred re-runs everything every time.
- **Storage consistency**: Delta Lake with ACID transactions and a staging-commit pattern designed for shared NFS filesystems. Sacred's FileStorageObserver has no concurrency protection; MongoObserver delegates this to MongoDB.
- **Active development**: Artisan is under active development. Sacred is in maintenance mode with infrequent releases and a growing backlog of unresolved issues.
- **Modern Python**: Pydantic models, type hints throughout, modern packaging. Sacred was designed in the Python 2/3 transition era and has accumulated technical debt in its config system.

## Key Differentiators Summary

| Dimension | Sacred | Artisan |
|---|---|---|
| **Core paradigm** | Experiment tracker (log what happened) | Pipeline framework (orchestrate + track derivations) |
| **Provenance model** | Per-run metadata (config, metrics, source) | Artifact-level derivation chains + step DAG (W3C PROV) |
| **Cross-experiment lineage** | None | Structural (automatic `wasDerivedFrom` edges) |
| **Artifact identity** | Filename / name string | Content hash (`xxh3_128`) |
| **Artifact typing** | Opaque files | Typed system (Data, Metric, FileRef, Config, etc.) |
| **Deduplication** | Partial (MD5 for resources in Mongo; HashFS in TinyDB) | Global (content-addressed, stored once) |
| **Configuration** | Config scopes, injection, named configs, CLI overrides | Pydantic BaseModel params, programmatic |
| **Execution** | Single function, decorator-driven | Multi-step pipeline DAG, batch-native |
| **Caching** | None | Deterministic (content-hash-based) |
| **HPC support** | None (manual `--queue` stub) | SLURM via submitit, process pool |
| **Storage** | MongoDB / filesystem / TinyDB / SQL / S3 | Delta Lake with ACID transactions |
| **Concurrency safety** | Depends on backend (MongoDB: yes; filesystem: no) | Staging-commit pattern for NFS |
| **Batch processing** | No | Native (operations on lists of artifacts) |
| **Maintenance status** | Maintenance mode (~4.2K stars, infrequent releases) | Active development |
| **Best suited for** | Quick single-experiment logging, prototyping | Multi-step scientific/computational pipelines |
| **First released** | ~2014 (SciPy paper 2017) | Active development (current) |
