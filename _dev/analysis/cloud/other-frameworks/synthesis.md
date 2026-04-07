# Cross-Framework Cloud Orchestration Report

How seven workflow frameworks handle orchestration across different cloud compute
resources, and what Artisan should learn from each.

Frameworks analyzed: Nextflow, Snakemake, Flyte, Metaflow, Dagster, Prefect, Ray.
Hamilton analysis from prior work is also incorporated.

---

## Framework Summary

| Framework | Origin | Language | Primary Domain | Cloud Model |
|-----------|--------|----------|----------------|-------------|
| Nextflow | Seqera | Groovy/JVM | Bioinformatics | 21 executors; config-driven backend selection; Fusion FS for POSIX-over-S3 |
| Snakemake | Köster Lab | Python | Bioinformatics | Plugin-based executors + orthogonal storage plugins; auto-staging |
| Flyte | Union.ai | Python + Go | ML/Data | Kubernetes-native; shared object store data plane; typed task plugins |
| Metaflow | Netflix | Python | ML | Decorator-based compute targeting; content-addressed datastore |
| Dagster | Elementl | Python | Data Engineering | I/O managers decouple ops from storage; run launcher + executor split |
| Prefect | Prefect | Python | General | Task runners (intra-flow) vs work pools (inter-flow); shallow compute abstraction |
| Ray | Anyscale | Python | Distributed Compute | Object store for inter-task data; logical resource scheduling; compute layer, not orchestrator |
| Hamilton | DAGWorks | Python | Feature Engineering | Thin adapter layer delegating to Ray/Dask/Spark; no built-in storage or persistence |

---

## How Each Framework Abstracts Backends

### The spectrum: thin adapter → full platform

Frameworks sit on a spectrum from "thin dispatch wrapper" to "full platform with
typed data plane."

```
Thin adapter                                              Full platform
  │                                                              │
  Hamilton ─── Prefect ─── Dagster ─── Metaflow ─── Snakemake ─── Nextflow ─── Flyte
  (adapter     (task       (executor   (decorator   (executor +   (executor +   (typed tasks +
   per-node)    runner)     + I/O mgr)  + datastore) storage       work dir +    object store +
                                                     plugins)      Fusion FS)    data catalog)
```

**Hamilton** delegates everything to the backend. No storage, no failure handling,
no provenance. Simple but leaves users to solve the hard problems.

**Prefect** provides a clean task runner interface for intra-flow parallelism but
has no mechanism for per-task cloud infrastructure. Cloud provisioning lives in
work pools (flow-scoped, Cloud-only for serverless). The task runner and work pool
abstractions solve different problems at different scopes.

**Dagster** adds the I/O manager layer, which is the key innovation: ops produce
and consume Python objects; I/O managers handle serialization and storage. This
decouples ops from storage entirely. The executor handles dispatch; the I/O
manager handles persistence. Two orthogonal axes.

**Metaflow** adds a content-addressed datastore that works identically across
local and cloud. The decorator model (`@batch`, `@kubernetes`, `@resources`) gives
per-step backend control with a single CLI flag to switch everything to cloud.

**Snakemake** separates execution and storage into fully orthogonal plugin axes.
Any executor composes with any storage provider. Six behavioral traits (vs
Artisan's one `shared_filesystem` boolean) encode backend differences precisely.

**Nextflow** has the most mature multi-backend story: 21 executors, automatic data
staging between backends, the work-directory abstraction that's identical on NFS
and S3, and the Fusion filesystem for transparent POSIX access to object storage.

**Flyte** goes furthest: a Kubernetes-native control plane with a shared object
store as the data plane. Every task communicates through typed references to
objects in S3/GCS. The type system (FlyteFile, StructuredDataset) handles
serialization, transfer, and deserialization automatically. Backend plugins can be
developed independently via the agent/connector framework.

---

## Cross-Cutting Patterns

### Storage abstraction is the universal prerequisite

Every framework that successfully supports cloud compute has a storage abstraction
layer. The specific implementations differ, but the pattern is consistent:

| Framework | Storage Abstraction | Interface |
|-----------|-------------------|-----------|
| Nextflow | Work directory (local or S3) + Fusion FS | Transparent POSIX via FUSE, or automatic stage-in/stage-out |
| Snakemake | `StorageProvider` + `StorageObject` plugins | `retrieve()` / `store()` / `exists()` / `mtime()` per file |
| Flyte | Object store (S3/GCS) + typed references | `FlyteFile` / `FlyteDirectory` / `StructuredDataset` auto-offload |
| Metaflow | `DataStoreStorage` ABC | `save_bytes` / `load_bytes` / `list_content` / `is_file` |
| Dagster | I/O managers | `handle_output(context, obj)` / `load_input(context)` |
| Prefect | Result storage blocks | Opt-in; "bring your own shared storage" model |
| Ray | Distributed object store (shared memory) | `ray.put()` / `ray.get()` — volatile, intra-session only |
| Hamilton | None | Delegates entirely to backend |

**The lesson:** Frameworks without a storage abstraction (Hamilton, Prefect for
task-level) cannot support heterogeneous cloud backends. The abstraction doesn't
need to be complex — Metaflow's is just five methods — but it must exist.

**Artisan's position:** The POSIX-path coupling (`pathlib.Path` in
`RuntimeEnvironment`, `StagingArea`, `StagingManager`, `DeltaCommitter`) is the
single largest blocker. The fix is a storage interface. The existing analysis
proposes `TransferStrategy` (upload/download units and staged files). The
cross-framework evidence suggests a slightly broader abstraction — closer to
Snakemake's `StorageProvider` or Metaflow's `DataStoreStorage` — that covers not
just staging transfer but also Delta Lake access from cloud workers.

### Execution and storage are orthogonal axes

Snakemake's strongest design insight: executor plugins and storage plugins are
independent. Any executor composes with any storage provider. Nextflow achieves
this differently (the work directory abstraction plus Fusion), but the effect is
the same: execution and storage decisions are decoupled.

Artisan's `BackendBase` currently bundles execution traits with implicit storage
assumptions. Splitting the backend into an executor concern (how to dispatch
units) and a storage concern (how to transfer data) would make it natural to
compose: SLURM + POSIX, SLURM + S3, Kubernetes + S3, Modal + Volume, etc.

### Backend behavioral traits need more than one boolean

| Framework | Trait Mechanism | Granularity |
|-----------|----------------|-------------|
| Artisan (current) | `WorkerTraits.shared_filesystem` | 1 boolean |
| Snakemake | `CommonSettings` dataclass | 6 booleans: `non_local_exec`, `implies_no_shared_fs`, `job_deploy_sources`, `pass_default_storage_provider_args`, `auto_deploy_default_storage_provider`, `init_seconds_before_status_checks` |
| Nextflow | Per-executor configuration | Implicit in executor implementation |
| Flyte | Task type → plugin handler dispatch | Plugin determines all behavior |

Snakemake's granular traits let the framework auto-configure staging behavior
based on what the executor needs. Adding traits like `requires_source_deployment`
and `supports_storage_passthrough` to Artisan's `WorkerTraits` would reduce
backend-specific conditional logic in the orchestrator.

### Content-addressed caching is an advantage — if path-independent

| Framework | Cache Key | Cross-Backend? |
|-----------|-----------|----------------|
| Artisan | `xxh3_128(op_name + sorted_artifact_ids + params + config)` | Yes (content-addressed) |
| Nextflow | `hash(session_id + script + container + input_paths + sizes + mtimes)` | No (path-dependent) |
| Snakemake | `SHA-256(code + params + input_content_or_upstream_hashes + software_env)` | Partially (content-based, but FS-based cache store) |
| Flyte | `task_name + cache_version + interface_hash + input_literals` | Yes (within deployment) |
| Metaflow | Step-level resume (skip completed steps) | No (step identity, not content) |
| Dagster | `code_version + upstream_data_versions` | Last-value only |
| Hamilton | Local-only `.hamilton_cache/` | No (local filesystem, broken in distributed mode) |

Artisan's content-addressed caching is genuinely stronger than most alternatives.
The key is to preserve this as cloud backends are added — cache keys must remain
path-independent (hash by content, not by location). This is already the design;
it just needs to survive the storage abstraction work.

### Code delivery converges on container images

| Framework | Code Delivery Mechanism |
|-----------|----------------------|
| Nextflow | Shell scripts in container; Wave builds images on-demand |
| Snakemake | Source deployment to storage or K8s job spec; container pull |
| Flyte | ImageSpec per task; content-hash dedup |
| Metaflow | Tarball of source uploaded to datastore; CLI re-invocation |
| Dagster | Per-op container image tags; Dagster Pipes for external processes |
| Prefect | Build/push/pull deployment model |
| Ray | Pre-installed package in container; runtime_env for development |

Two patterns dominate: **bake into container** (Nextflow, Flyte, Dagster) and
**ship source separately** (Metaflow, Snakemake). Baking is more robust for
production; shipping source is faster for development iteration.

Artisan's planned approach — a base `artisan-worker` image with framework +
deps, operation code baked in or mounted — aligns with industry practice. Flyte's
ImageSpec (content-hash dedup, Dockerfile-free builds) is the most sophisticated
version of this pattern.

### Dynamic resource escalation on retry is standard

Nextflow, Snakemake, and Dagster all support increasing resources (memory, time)
on retry. The pattern:

```
memory = base_memory * attempt_number
```

This is critical for cloud backends where OOM kills are common and pre-allocating
maximum resources is wasteful. Artisan should support this in `ExecutionConfig` or
`ResourceConfig`.

### Hybrid pipelines require a shared data plane

Every framework that supports mixed backends (local + cloud, different cloud
backends in one pipeline) requires a **single canonical data location** accessible
from all backends:

- **Nextflow**: `-work-dir` for local, `-bucket-dir` for cloud, with automatic
  transfer between them
- **Flyte**: Shared object store (S3/GCS) as the universal data plane
- **Metaflow**: Shared datastore (S3/GCS) accessible from both local and remote
- **Dagster**: I/O managers that can read/write from cloud storage

Artisan's existing design doc reaches the same conclusion: "Use a single canonical
storage location (S3 or a shared filesystem) for Delta Lake tables. All backends
read from and write to the same Delta Lake."

---

## Per-Framework Learnings for Artisan

### From Nextflow

- **Work directory as unit of isolation.** Every task gets a self-contained,
  hex-hashed directory that is structurally identical on NFS and S3. Artisan's
  staging model is similar but path-coupled. Abstracting the "task directory" to
  work on both local paths and object-store prefixes is the minimal change for
  cloud support.
- **TaskHandler + TaskMonitor split.** Separate the submission mechanism (how to
  submit/poll one unit) from capacity management (how many, how fast, polling
  intervals). Artisan's `BackendBase` conflates these.
- **Fusion validates the abstraction approach.** Nextflow built a FUSE filesystem
  to solve the POSIX-vs-S3 gap. Artisan doesn't need a filesystem, but the lesson
  is that abstracting storage behind an interface (rather than rewriting all paths
  to URIs) is viable and less invasive.
- **Caching is backend-local.** Nextflow's cache doesn't port across backends
  because hashes include paths. Artisan's content-addressed caching is already
  better positioned — preserve this.

### From Snakemake

- **Orthogonal executor + storage plugins.** The single most important design
  pattern. Decouple "how to dispatch" from "how to transfer data." This enables
  free composition of any executor with any storage provider.
- **Richer behavioral traits.** Six booleans vs Artisan's one. Source deployment,
  storage passthrough, and env forwarding needs vary independently from filesystem
  sharing.
- **Default storage provider pattern.** `--default-storage-provider s3` rewrites
  all paths transparently. Artisan could introduce a similar mechanism at the
  `PipelineConfig` level.
- **Access-pattern-aware staging.** Sequential vs random vs multi-access
  annotations let the storage layer optimize transfers. Artisan's `InputSpec`
  could carry similar hints.

### From Flyte

- **Object store as universal data plane.** Tasks never share filesystems; they
  share typed references to objects in S3/GCS. This is the cleanest multi-backend
  data exchange model.
- **Agent/Connector framework.** Stateless gRPC services implementing
  `create`/`get`/`delete` for long-running jobs. This validates Artisan's planned
  `DispatchHandle` with `dispatch()`/`is_done()`/`collect()`/`cancel()`.
- **`map_task` with `min_success_ratio`.** Partial failure tolerance at the batch
  level. Artisan's `FailurePolicy` already provides this; Flyte validates the
  approach.
- **Per-task ImageSpec with content-hash dedup.** Only rebuilds when dependencies
  change. Worth considering for Artisan's worker image management.
- **Separate metadata from data.** Control plane DB for metadata; object store
  for content. Artisan partially does this (Delta Lake = metadata + data in
  Parquet), but decoupling them for cloud would unlock remote Delta Lake.

### From Metaflow

- **`DataStoreStorage` is the minimal viable storage abstraction.** Five methods:
  `save_bytes`, `load_bytes`, `list_content`, `path_join`, `is_file`.
  Implementations for local, S3, GCS, Azure. Simple and sufficient.
- **Content-addressed blob store validates Artisan's design.** SHA-1 with
  dedup-before-write. Artisan's `xxh3_128` is faster and equally sound.
- **Code packaging via tarball + re-invocation.** Simple, effective, and avoids
  the container rebuild cycle for development.
- **Safety rails are essential.** `--max-workers` and `--max-num-splits` prevent
  accidental resource exhaustion. Artisan should add similar caps for cloud
  backends.
- **Decorator-based backend selection is ergonomic.** `@resources` (what you need)
  separate from `@batch`/`@kubernetes` (where to run). Per-step backend overrides
  are useful for mixed local/cloud pipelines.

### From Dagster

- **Split run dispatch from step dispatch.** Run launcher (where the orchestrator
  process lives) vs executor (how steps within a run are distributed). Artisan's
  `BackendBase` conflates both. Splitting would make cloud backends cleaner.
- **I/O managers decouple ops from storage.** `handle_output`/`load_input` behind
  a string key resolved at build time. The strongest version of the storage
  abstraction pattern.
- **`FROM_FAILURE` retry strategy.** Re-run from the point of failure, reusing
  committed artifacts from prior steps. Artisan's Delta Lake store already
  supports this; adding it as a formal pipeline-level retry mode is low-effort.
- **Environment-aware resource binding.** A dictionary of resource sets keyed by
  environment name. Selected at build time via env var. Simple and powerful.

### From Prefect

- **Task runners work for local/SLURM; break down for serverless.** Task runners
  cannot provision cloud infrastructure. Work pools can, but they operate at the
  wrong granularity (flow-scoped, not task-scoped) and require Prefect Cloud for
  serverless targets.
- **The `DispatchHandle` refactor is validated.** Keep Prefect task runners for
  local and SLURM (they work well). Implement cloud backends directly against
  native APIs. This avoids a Prefect Cloud dependency and the architectural
  mismatch between task runners and cloud provisioning.
- **Container startup latency is real.** ECS/Fargate cold starts are documented
  as a significant issue. Artisan's batching model (grouping artifacts into
  execution units) already amortizes this, but backend implementations should
  expose tuning for batch granularity vs startup overhead.

### From Ray

- **Object store vs filesystem staging is a false choice.** Ray's object store
  is for transient intra-session data. Artisan's staging-commit pattern is for
  durable artifacts. They're complementary. A Ray backend should use the object
  store internally and stage final outputs to Delta Lake.
- **Ray is a backend, not a compute adapter.** The Flyte/Prefect/Airflow
  ecosystem treats Ray as a compute layer beneath an orchestrator. Artisan should
  do the same: `RayBackend` as a `BackendBase` implementation.
- **Actors solve warm-model loading.** Ray actors load models in `__init__` and
  reuse across invocations. A Ray backend could use actors for operations with
  expensive setup, keeping models warm across batches within a step.
- **Ephemeral per-run clusters.** KubeRay's `RayJob` CRD creates and tears down
  clusters around jobs. This matches Artisan's run-oriented model.
- **Fault tolerance is complementary.** Ray handles intra-step retries and object
  reconstruction. Artisan handles cross-step durability via atomic commits. No
  overlap.

### From Hamilton

- **Per-node dispatch granularity is too fine.** Hamilton submits every function
  as a separate distributed task. Serialization overhead dominates for small
  functions. Artisan's `ExecutionUnit` batching avoids this.
- **Local-only caching breaks in distributed mode.** Hamilton's cache is
  filesystem-local. Artisan's Delta-Lake-backed caching is already
  storage-agnostic — a genuine advantage.
- **`@with_columns` compilation validates composites.** Hamilton compiles sub-DAGs
  into single Spark operations. Artisan's collapsed composites do the same thing
  for worker invocations. This is a validated architectural pattern.
- **No framework-level failure handling is a gap.** Hamilton delegates all
  resilience to backends. Artisan's per-unit result tracking and failure policies
  are ahead here.

---

## Consolidated Recommendations

Organized by the concern areas from Artisan's existing cloud analysis docs.

### Storage & Data Transfer (Gap 1 — High Severity)

**Introduce a storage abstraction layer.** Every successful framework has one.
The interface should cover:
- Staging I/O (write/read Parquet files to/from staging directories)
- Delta Lake access (URI-based `DeltaTable` paths)
- Unit transfer (upload/download pickled ExecutionUnits)

Metaflow's `DataStoreStorage` (5 methods) is the right complexity target.
Snakemake's orthogonal storage plugins are the right architecture. Dagster's
I/O managers are the right level of abstraction for operations.

Implementation: `StorageProvider` ABC with `LocalStorageProvider` (passthrough)
and `S3StorageProvider` / `GCSStorageProvider`. Injected into
`RuntimeEnvironment`. Workers use `provider.read()`/`provider.write()` instead
of `Path.read_bytes()`/`Path.write_bytes()`.

### Dispatch Mechanism (Gap 2 — Medium Severity)

**Implement `DispatchHandle` and keep Prefect only where it fits.** The Prefect
analysis confirms that task runners work for local/SLURM but not for serverless
cloud. Flyte's agent/connector pattern validates the `dispatch()`/`is_done()`/
`collect()`/`cancel()` interface.

- Local backend: wrap `ProcessPoolTaskRunner` in a `DispatchHandle`
- SLURM backend: wrap `SlurmTaskRunner` in a `DispatchHandle`
- Modal backend: implement `DispatchHandle` using `modal.Function.map()`
- Kubernetes backend: implement using Prefect's `KubernetesTaskRunner` or
  direct `kubectl` API
- Ray backend: implement using `ray.remote()` task submission

### Backend Traits (Enhancement)

**Expand `WorkerTraits` beyond `shared_filesystem`.** Drawing from Snakemake's
`CommonSettings`:

```python
@dataclass
class WorkerTraits:
    shared_filesystem: bool
    requires_source_deployment: bool   # code must be shipped to workers
    requires_storage_passthrough: bool # storage config forwarded to workers
    supports_env_forwarding: bool      # env vars available in workers
    worker_id_env_var: str | None
```

### Resource Configuration (Enhancement)

**Add dynamic resource escalation on retry.** Nextflow and Snakemake both
support `memory = base * attempt`. Artisan's `ResourceConfig` or
`ExecutionConfig` should expose an attempt counter:

```python
resources = ResourceConfig(
    memory_gb=lambda attempt: 8 * attempt,
    time_limit=lambda attempt: "4h" if attempt == 1 else "12h",
)
```

### Error Handling (Enhancement)

**Add `FROM_FAILURE` pipeline-level retry.** Dagster's pattern: on re-run, skip
steps whose outputs are already committed. Artisan's Delta Lake store already
supports this — completed artifacts persist. The pipeline just needs to check
the steps table and skip completed steps on restart.

**Add explicit concurrency caps for cloud backends.** Metaflow's `--max-workers`
prevents accidental cost explosions. Artisan should add this to cloud backend
configuration.

### Hybrid Execution (Architecture)

**Plan for a single canonical data plane.** All frameworks that support hybrid
execution (mixed backends in one pipeline) converge on this: one shared storage
location (S3, Delta Lake, or equivalent) accessible from all backends. Worker
scratch space is local; durable artifacts are in the shared store.

This aligns with the existing design doc recommendation: Delta Lake on S3 as
the canonical store, with `TransferStrategy` handling ephemeral staging I/O.

---

## Priority Ordering

Based on cross-framework evidence, the implementation phases from the existing
`cloud_compute_backends.md` are well-ordered. Adjusted priorities:

| Phase | What | Validated By |
|-------|------|-------------|
| **Phase 1** | `DispatchHandle` refactor | Prefect (breaks for serverless), Flyte (agent pattern), all frameworks |
| **Phase 2** | Storage abstraction (`StorageProvider`) | Every framework — this is the universal prerequisite |
| **Phase 3** | Kubernetes backend (NFS PVC — shared FS) | Nextflow, Flyte (closest to SLURM, lowest risk) |
| **Phase 4** | Modal backend (Volume or S3) | Metaflow (decorator model), Ray (warm actors for models) |
| **Phase 5** | AWS Batch / Lambda | Nextflow (mature Batch integration), Snakemake (Batch executor) |
| **Enhancement** | Expanded traits, resource escalation, safety caps | Snakemake (traits), Nextflow (escalation), Metaflow (caps) |

Phases 1 and 2 can proceed in parallel (they're independent). Phase 2 is
arguably more impactful — the storage abstraction unblocks everything — but
Phase 1 is smaller and independently valuable for cancellation support.

---

## Sources

Individual framework reports with full citations are in the sibling files:

- `nextflow-orchestration.md`
- `snakemake-cloud-orchestration.md`
- `flyte-orchestration-analysis.md`
- `metaflow-cloud-orchestration.md`
- `dagster-orchestration-research.md`
- `prefect-orchestration-model.md`
- `ray-orchestration-across-cloud.md`
- `../hamilton-cloud-execution.md` (prior analysis)
