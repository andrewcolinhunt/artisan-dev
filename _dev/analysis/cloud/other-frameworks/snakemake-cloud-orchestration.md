# Snakemake Cloud Orchestration: Architecture Analysis

How Snakemake 8+ abstracts orchestration across local, HPC, and cloud compute
backends, and what Artisan can learn from the design.

---

## Executor Plugin System

Snakemake 8 decomposed all execution backends into independently versioned
**executor plugins**, each published as a separate PyPI package
(`snakemake-executor-plugin-slurm`, `-kubernetes`, `-googlebatch`, `-aws-batch`,
`-flux`, etc.). The core framework defines a stable interface package
(`snakemake-interface-executor-plugins`) that all plugins implement.

An executor plugin subclasses either a local or remote base class and must
implement three methods:

- **`run_job(job)`** -- Submit a single job. The plugin translates Snakemake's
  job abstraction (rule name, inputs, outputs, resources, shell command) into
  the backend's native submission API (sbatch, kubectl, Batch API call, etc.),
  then calls `self.report_job_submission(job_info)` with an optional
  `external_job_id`.
- **`async check_active_jobs(active_jobs)`** -- Poll running jobs. Yields
  still-running jobs; calls `self.report_job_success()` or
  `self.report_job_error()` for completed ones. A built-in
  `status_rate_limiter` prevents API flooding.
- **`cancel_jobs(active_jobs)`** -- Terminate active jobs on interrupt.

Executor plugins declare behavioral traits through a `CommonSettings` dataclass:

| Setting | Purpose | Example |
|---------|---------|---------|
| `non_local_exec` | Remote vs local execution | `True` for SLURM, K8s |
| `implies_no_shared_fs` | No shared filesystem available | `True` for K8s, cloud batch |
| `job_deploy_sources` | Deploy workflow sources to storage before execution | `True` for K8s |
| `pass_default_storage_provider_args` | Forward storage config to workers | `True` for cloud |
| `auto_deploy_default_storage_provider` | Auto-install storage plugin on workers | `True` for cloud |
| `init_seconds_before_status_checks` | Delay before first poll | Tuned per backend |

This is directly analogous to Artisan's `WorkerTraits` / `OrchestratorTraits`,
but Snakemake's version is more granular -- it separately encodes filesystem
assumptions, source deployment needs, and storage passthrough rather than
bundling them into a single `shared_filesystem` boolean.

Plugins may also define custom `ExecutorSettings` dataclasses with fields that
bind to environment variables, support help text, and declare whether they are
required. This lets each plugin expose backend-specific knobs (e.g., SLURM
partition, K8s namespace, GCP project) without polluting the core interface.

---

## Storage Plugins & Remote I/O

Storage is a separate plugin axis. Plugins exist for S3 (also covers MinIO),
GCS, Azure Blob, FTP, HTTP, and SFTP, each implementing the
`snakemake-interface-storage-plugins` contract.

A storage plugin provides two classes:

- **`StorageProvider`** -- Global state: credentials, connection pooling, rate
  limiting. Defines `rate_limiter_key()` (e.g., hostname) and
  `default_max_requests_per_second()` for throttling.
- **`StorageObject`** -- Per-file operations. Inherits from a capability
  hierarchy:
  - `StorageObjectRead`: `exists()`, `mtime()`, `size()`, `retrieve_object()`,
    `local_footprint()`
  - `StorageObjectWrite`: `store_object()`, `remove()` (requires Read)
  - `StorageObjectGlob`: `list_candidate_matches()` (wildcard expansion)
  - `StorageObjectTouch`: `touch()` (timestamp update)

Read-only providers (HTTP) omit the Write/Touch bases.

**Default storage provider.** The flag `--default-storage-provider s3
--default-storage-prefix s3://mybucket/` makes all inputs/outputs resolve
against S3 by default. Rules do not need to name the storage explicitly --
Snakemake rewrites local paths to remote URIs transparently. This is the key
design that enables the same Snakefile to run locally with POSIX paths or on
Kubernetes with S3, with zero rule changes.

**IOCache.** The `inventory()` method lets plugins batch-prefetch metadata
(existence, size, mtime) for an entire directory listing in one API call, then
cache it via `IOCacheStorageInterface`. This avoids N+1 HEAD requests during DAG
resolution.

**Retry decorator.** All fallible I/O methods are wrapped with
`@retry_decorator` for automatic retry on transient failures.

---

## Container & Code Delivery

Snakemake 8 replaced the old `--use-singularity` / `--use-conda` flags with a
unified `--software-deployment-method` that accepts `conda`, `apptainer`, or
both. Per-rule container images are declared via a `container:` directive
accepting `docker://` and `shub://` URIs.

For cloud/Kubernetes execution without a shared filesystem, Snakemake handles
code delivery via two mechanisms:

- **Source deployment** (`job_deploy_sources=True`): Snakemake uses git to query
  all necessary source files (Snakefile, scripts, configs) and either encodes
  them into the Kubernetes job spec or uploads them to the default storage
  provider before execution.
- **Container images**: The Kubernetes executor pulls the specified container
  image directly. On Google Cloud Batch and AWS Batch, the container image is
  specified in the job definition and pulled by the managed service.

The isolation semantics differ by runtime: Docker passes no host environment
variables; Apptainer/Singularity passes everything. Snakemake's
`pass_envvar_declarations_to_cmd` setting in CommonSettings controls how
environment variables are forwarded to job commands to bridge this gap.

---

## Resource Configuration

Resources are declared per-rule via the `resources:` and `threads:` directives:

```python
rule align:
    threads: 8
    resources:
        mem_mb=16000,
        disk_mb=50000,
        runtime=120,       # minutes
        gpu=1,
        tmpdir="/tmp"
```

Four resources are "standard" and directly consumed by executors: `mem_mb`,
`disk_mb`, `runtime`, and `tmpdir`. Executors translate these into native flags
(e.g., SLURM's `--mem`, `--time`, `--gres=gpu`; Kubernetes resource requests;
Batch vCPU/memory specifications).

Resources can reference the `attempt` counter for progressive scaling on retry:

```python
resources:
    mem_mb=lambda wildcards, attempt: 8000 * attempt
```

A `--default-resources` flag and workflow-specific profiles (YAML) set
cluster-wide defaults and per-rule overrides without editing the Snakefile,
enabling the same workflow to run on a laptop (2 threads, 4GB) or a cluster
(32 threads, 128GB) by swapping the profile.

---

## Caching

Snakemake's **between-workflow caching** uses a Merkle-tree hash scheme. For
each cache-eligible job, the SHA-256 hash is computed over:

- Rule source code (the shell/script content)
- All `params:` values
- Raw input file content (for root inputs) or upstream hashes (for derived
  inputs)
- Software environment (conda YAML hash or container image URI), unless
  `cache: "omit-software"` is specified

Output files are stored in a central cache directory keyed by this hash.
Subsequent runs with identical inputs/code/params skip execution entirely.

**Eligibility constraints**: Only rules with a single output file (or directory)
or `multiext` outputs qualify. Rules must use `params:` for all parameters
(no direct wildcard/config references in shell commands). The `cache:` directive
marks eligible rules with values `"all"`, `"omit-software"`, or `True`.

The cache is filesystem-based (a shared directory). Cross-backend caching works
if backends share the same cache directory (e.g., via NFS or a storage plugin),
but there is no built-in cloud-native cache store.

---

## Error Handling

**Retries.** Per-rule `retries: N` defines how many times a failed job is
re-attempted. The `attempt` counter (1-indexed) is available in resource lambdas,
enabling progressive resource scaling (double memory on retry, extend walltime).

**Group jobs.** Multiple rules can be grouped into a single job submission
(`group:` directive) to reduce scheduling overhead. If a group job fails, the
entire group is retried. There have been known issues with group jobs on
preemptible cloud VMs -- if the VM is preempted mid-group, all constituent rules
must be re-run.

**Executor-level error reporting.** The `check_active_jobs` polling loop
distinguishes between job success, job failure, and still-running. Executors
call `report_job_error()` with failure details. Snakemake's core scheduler
handles the retry logic, partial DAG re-execution, and propagation of failure
to downstream rules.

**Incomplete output cleanup.** If a job fails, Snakemake marks its output files
as incomplete and removes them before retry to prevent downstream rules from
consuming corrupt partial outputs.

---

## Data Staging

Data staging is the composition of the executor and storage plugin systems.
When an executor declares `implies_no_shared_fs=True`, Snakemake automatically
stages data:

- **Before job execution**: Input files are retrieved from the storage provider
  to a local staging path on the worker. The storage plugin's
  `retrieve_object()` handles the download.
- **After job execution**: Output files are uploaded from the local staging path
  to the storage provider via `store_object()`.

**Access pattern optimization.** Input files can be annotated with access
patterns: `access.sequential`, `access.random`, or `access.multi`. If a file is
accessed sequentially by a single job, the storage provider may mount or stream
it on-demand rather than downloading it fully. For random-access or multi-job
inputs, Snakemake forces a full download before execution.

**Local path transparency.** Within a rule's shell command, all paths are local
POSIX paths -- the staging is invisible. The `local_path()` method on
StorageObject returns the staging location. This means rule authors never write
S3/GCS URIs in their commands.

---

## Learnings for Artisan

**Separate the executor and storage plugin axes.** Snakemake's most powerful
design decision is that executor plugins and storage plugins are orthogonal.
Any executor can compose with any storage provider. Artisan's `BackendBase`
currently bundles execution and storage assumptions together. Splitting these
into independent abstractions (an `ExecutorBackend` for dispatch and a
`StorageBackend` for I/O) would let cloud backends compose freely: SLURM +
POSIX, SLURM + S3, Kubernetes + S3, Kubernetes + GCS, etc.

**Richer traits beyond `shared_filesystem`.** Snakemake's `CommonSettings` has
six boolean flags where Artisan has essentially one. Adding traits like
`requires_source_deployment`, `requires_storage_passthrough`, and
`supports_env_forwarding` would make the backend contract more precise and let
the orchestrator auto-configure staging behavior without backend-specific
conditional logic.

**Default storage provider pattern.** Snakemake's `--default-storage-provider`
flag transparently rewrites all paths to remote URIs. Artisan's POSIX-coupled
`pathlib.Path` usage is the main blocker for cloud. Rather than rewriting all
path handling, Artisan could introduce a `StorageProvider` abstraction with a
`local_path()` / `retrieve()` / `store()` interface, defaulting to a passthrough
POSIX provider. This would let existing code work unchanged while enabling S3/GCS
providers to handle staging transparently.

**Access-pattern-aware staging.** Snakemake's sequential/random/multi access
annotations let the storage layer optimize transfer (streaming vs full download).
Artisan's `MaterializationMode` and `HydrationMode` on InputSpec/OutputSpec are
a natural place to encode similar hints, so the storage layer could decide
between full download, lazy loading, or streaming.

**Merkle-tree caching is compatible with Artisan's content addressing.** Artisan
already content-addresses artifacts by hash. Extending this to include operation
code + params in the hash (as Snakemake does) would enable between-pipeline
caching. The Delta Lake store is already well-suited to serve as the cache
backend since artifacts are keyed by hash.

**Progressive resource scaling on retry.** Snakemake's `attempt` counter in
resource lambdas is a simple, practical pattern. Artisan's `ExecutionConfig`
could expose a similar counter so operations can request more memory/time on
retry without manual intervention.

**Incomplete output cleanup.** Snakemake deletes partial outputs on failure
before retry. Artisan's atomic-commit model (workers stage, orchestrator commits)
already handles this implicitly -- uncommitted staging files are discarded. This
is a genuine architectural advantage worth preserving as cloud backends are added.

---

Sources:
- [Snakemake executor plugin interface (GitHub)](https://github.com/snakemake/snakemake-interface-executor-plugins)
- [Snakemake storage plugin interface (GitHub)](https://github.com/snakemake/snakemake-interface-storage-plugins)
- [Snakemake plugin catalog](https://snakemake.github.io/snakemake-plugin-catalog/index.html)
- [Snakemake executor plugins documentation](https://snakemake.readthedocs.io/en/v9.18.1/executing/executors.html)
- [Snakemake storage support documentation](https://snakemake.readthedocs.io/en/v8.8.0/snakefiles/storage.html)
- [Snakemake between-workflow caching](https://snakemake.readthedocs.io/en/stable/executing/caching.html)
- [Snakemake rules and resources](https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html)
- [Snakemake deployment and reproducibility](https://snakemake.readthedocs.io/en/stable/snakefiles/deployment.html)
- [Snakemake Kubernetes executor](https://snakemake.github.io/snakemake-plugin-catalog/plugins/executor/kubernetes.html)
- [Snakemake Google Cloud Batch executor](https://snakemake.github.io/snakemake-plugin-catalog/plugins/executor/googlebatch.html)
- [Snakemake AWS Batch executor](https://snakemake.github.io/snakemake-plugin-catalog/plugins/executor/aws-batch.html)
- [Snakemake SLURM executor](https://snakemake.github.io/snakemake-plugin-catalog/plugins/executor/slurm.html)
- [Snakemake S3 storage plugin](https://snakemake.github.io/snakemake-plugin-catalog/plugins/storage/s3.html)
- [Snakemake Azure Kubernetes tutorial](https://andreas-wilm.github.io/2020-06-08-snakemake-on-ask/)
