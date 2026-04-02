# Design: Cloud Compute Backends

**Date:** 2026-04-01
**Status:** Analysis

---

## Summary

Artisan's backend abstraction (`BackendBase`) was designed for extensibility,
and the two existing backends (LOCAL, SLURM) validate the architecture for
local and HPC compute. This document analyzes how different cloud compute
paradigms map to the current abstractions, identifies the gaps that block
cloud adoption, and proposes a layered strategy for closing them.

The core finding: **the backend interface itself is cloud-ready; the storage
and data transfer layers are not.** Adding a Kubernetes or Modal backend is
straightforward once operations can read inputs and write outputs through
something other than a POSIX shared filesystem.

---

## Cloud Compute Taxonomy

Cloud compute services differ along five axes that matter for artisan:

| Axis | Question |
|------|----------|
| **Lifecycle** | Ephemeral function invocation vs long-running container vs persistent VM? |
| **Filesystem** | Shared (NFS/EFS/Volume) vs isolated (local `/tmp` only)? |
| **Code delivery** | Pre-built container image vs serialized function vs installed package? |
| **Resource model** | Serverless (auto-scale, pay-per-use) vs provisioned (fixed pool)? |
| **GPU support** | Native GPU scheduling or CPU-only? |

The following sections describe each cloud compute paradigm along these axes,
then map it to artisan's current abstractions.

---

### Managed Batch (AWS Batch, Google Cloud Batch)

**How it works.** A job queue backed by a compute environment (EC2 instances,
spot, or Fargate). You submit container-based jobs with resource
requirements. The service manages instance scaling, queue priority, and job
scheduling. Jobs run in Docker containers with configurable vCPU, memory,
and GPU.

| Axis | Value |
|------|-------|
| Lifecycle | Container runs to completion (minutes to hours) |
| Filesystem | Isolated by default; EFS/FSx mountable |
| Code delivery | Docker image |
| Resource model | Queue-managed (auto-scaling compute environment) |
| GPU | Yes (P-series, G-series instances) |

**Mapping to artisan.** Conceptually closest to SLURM. A job queue
replaces `sbatch`, array jobs map to SLURM job arrays, resource specs
translate directly. `BackendBase` subclass with a Prefect
`AWSBatchTaskRunner` or custom dispatcher. `ResourceConfig.extra` can carry
Batch-specific settings (compute environment, job queue name, spot vs
on-demand).

**What works today:**
- Backend interface fits naturally
- `ResourceConfig` maps to container overrides (vcpus, memory, gpuCount)
- `ExecutionConfig` batching model applies unchanged
- Prefect has an ECS/Batch task runner

**What doesn't:**
- Without EFS mount, workers can't read pickled units or write staging files
  to a shared path. Units and results need to flow through S3.
- `RuntimeEnvironment` paths are `pathlib.Path` — no S3 URI support
- Worker process must have artisan + operation code in the container image

---

### Serverless Functions (AWS Lambda, Google Cloud Functions)

**How it works.** Deploy a function as a zip or container image (up to 10 GB).
The platform auto-scales instances per invocation. Cold starts range from
100 ms to 10+ seconds. Execution limits: 15 min (Lambda), 60 min (Cloud
Functions 2nd gen). Memory up to 10 GB. No persistent storage beyond `/tmp`
(up to 10 GB ephemeral).

| Axis | Value |
|------|-------|
| Lifecycle | Ephemeral invocation (seconds to 15 min) |
| Filesystem | Isolated `/tmp` only (10 GB max) |
| Code delivery | Container image or zip package |
| Resource model | Serverless (per-invocation billing, auto-scale to 1000+) |
| GPU | No |

**Mapping to artisan.** Each `ExecutionUnit` maps to one Lambda invocation.
The function loads the unit from S3, runs the lifecycle, writes outputs to
S3. This bypasses Prefect entirely — you'd use `boto3.invoke()` or
SQS-triggered Lambda, not a Prefect TaskRunner.

**What works today:**
- The per-unit execution model maps cleanly to per-invocation
- `ExecutionUnit` is self-contained (operation + inputs + config)
- The 3-phase lifecycle (preprocess/execute/postprocess) runs in any Python process
- Content-addressed artifacts are serialization-friendly

**What doesn't:**
- 15-minute timeout is a hard constraint — some operations run longer
- No GPU support
- No shared filesystem — all data transfer through S3
- Cold starts add per-unit latency (matters for many small units)
- Dispatch mechanism is not Prefect — needs a different dispatch path
- Worker needs artisan importable in the Lambda container image
- Input materialization writes to local disk — fine with `/tmp`, but the
  operation must fit in 10 GB ephemeral storage

**Best suited for:** Many small, fast, CPU-only operations (data validation,
config generation, metric calculation, filtering). Not suitable for
long-running compute or GPU workloads.

---

### Modal

**How it works.** Python-native serverless compute. Functions are decorated
with `@modal.function()`, specifying image, GPU, memory, secrets, and
volumes. Modal serializes the function + arguments via cloudpickle, runs
them on cloud GPUs or CPUs. Container images are built from a declarative
`Image` spec and cached for fast cold starts (~100 ms). `modal.Volume`
provides a shared network filesystem. Pay per second of compute.

| Axis | Value |
|------|-------|
| Lifecycle | Function invocation (seconds to hours) |
| Filesystem | `modal.Volume` (shared, persistent) or isolated |
| Code delivery | Cloudpickle serialization + declarative Image |
| Resource model | Serverless (auto-scale, per-second billing) |
| GPU | Yes — first-class (A10G, A100, H100, etc.) |

**Mapping to artisan.** Modal's programming model is closest to what artisan
already does: pickle a unit of work, send it to a worker, collect results.
Modal's `.map()` / `.starmap()` parallel exactly Prefect's
`execute_unit_task.map()`.

**What works today:**
- `ExecutionUnit` pickle serialization is compatible with cloudpickle
- Modal's `.map()` replaces Prefect's task mapping
- `ResourceConfig` maps to Modal's `gpu`, `memory`, `cpu` parameters
- `modal.Volume` provides a shared filesystem — staging model works
  unchanged if workers mount the same volume
- Fast cold starts minimize per-unit overhead

**What doesn't:**
- Modal has its own task runner — wrapping it in Prefect adds friction.
  Either bypass Prefect (Modal-native dispatch) or accept the impedance
  mismatch.
- Worker code must be importable within the Modal container image. Modal's
  `Image.pip_install()` or `Image.copy_local_dir()` handles this, but the
  operation author's code must also be installable.
- If not using `modal.Volume`, need S3 for data transfer
- `modal.Volume` ties the deployment to Modal's infrastructure
- Modal functions need to be defined at module scope (decorated), which
  means either a generic `execute_unit_task` function deployed to Modal,
  or a code-generation step

**Best suited for:** GPU-heavy pipelines (ML training, inference, protein
folding). Modal's GPU access and fast cold starts make it ideal for
workloads that need GPUs but don't justify maintaining a SLURM cluster.

---

### Serverless Containers (AWS Fargate, Google Cloud Run Jobs)

**How it works.** Run a container image without managing servers. Fargate
runs ECS tasks; Cloud Run Jobs run containers to completion. You specify
vCPU and memory. Execution can run for hours. No shared filesystem by
default (Fargate can mount EFS; Cloud Run has no persistent mount).

| Axis | Value |
|------|-------|
| Lifecycle | Container runs to completion (minutes to hours) |
| Filesystem | Isolated by default; EFS mountable (Fargate only) |
| Code delivery | Docker image |
| Resource model | Serverless (per-vCPU-second billing) |
| GPU | Cloud Run: yes (L4, A100). Fargate: no. |

**Mapping to artisan.** Similar to Managed Batch but without the built-in job
queue. Each batch of units becomes a container invocation. Prefect has an
`ECSTaskRunner` for Fargate.

**What works today:**
- Backend interface fits
- Prefect ECS task runner exists
- Resource mapping is straightforward
- Long execution times accommodate most operations

**What doesn't:**
- Same filesystem gap as Managed Batch — S3 or EFS for data transfer
- No GPU on Fargate (Cloud Run has GPUs but is GCP-only)
- Container image must include artisan + operation code

**Best suited for:** Variable-length jobs that don't need GPUs (Fargate) or
GCP-native GPU workloads (Cloud Run).

---

### Kubernetes (EKS, GKE, AKS, self-hosted)

**How it works.** Run containers as Pods in a cluster. For batch work, use
`Job` resources with `completions` and `parallelism` (analogous to SLURM
job arrays). Persistent Volumes (PVs) backed by NFS, EFS CSI driver, or
cloud-native block storage provide shared storage. GPU nodes are accessed
via node selectors and resource requests.

| Axis | Value |
|------|-------|
| Lifecycle | Pod runs to completion (minutes to hours) |
| Filesystem | Shared via PersistentVolumeClaim (NFS, EFS, GCS FUSE) |
| Code delivery | Docker image |
| Resource model | Provisioned cluster (with optional autoscaler) |
| GPU | Yes (via node selectors + resource requests) |

**Mapping to artisan.** The closest architectural match to SLURM. NFS-backed
PVCs provide a shared filesystem, so the existing staging model works with
minimal changes. Kubernetes `Job` with `completions=N` maps to SLURM job
arrays. Prefect has a `KubernetesTaskRunner`.

**What works today:**
- Backend interface fits perfectly
- `shared_filesystem=True` with NFS PVCs — staging model works as-is
- Prefect Kubernetes task runner exists
- `ResourceConfig` maps to Pod resource requests/limits
- GPU via node selectors and `nvidia.com/gpu` resource requests
- `WorkerTraits.worker_id_env_var` can use `JOB_COMPLETION_INDEX`

**What doesn't:**
- Worker container image must include artisan + operation code
- Kubernetes cluster management is a separate concern
- Pod scheduling latency can be significant without warm node pools

**Best suited for:** Teams already running Kubernetes. Provides SLURM-like
shared-filesystem semantics with cloud elasticity. The lowest-friction
cloud backend to implement.

---

### Ray

**How it works.** Distributed compute framework with a task/actor model and
built-in object store for zero-copy data transfer between tasks. Runs on any
infrastructure (local, Kubernetes, AWS, GCP). `@ray.remote` decorates
functions for distributed execution. The object store handles intermediate
data without filesystem I/O.

| Axis | Value |
|------|-------|
| Lifecycle | Task or actor (seconds to hours) |
| Filesystem | Depends on deployment (shared on NFS/k8s, not on multi-cloud) |
| Code delivery | Installed package + runtime env, or container image |
| Resource model | Provisioned cluster (with KubeRay autoscaler) |
| GPU | Yes (resource annotations) |

**Mapping to artisan.** Ray replaces both the task runner (Prefect) and
potentially the staging mechanism (object store for in-memory transfer).
The object store could eliminate filesystem I/O for intermediate artifacts
in composites.

**What works today:**
- Per-unit task mapping (`ray.remote` → `execute_unit_task`)
- `ResourceConfig` maps to Ray resource annotations
- GPU scheduling is native

**What doesn't:**
- Different enough from Prefect that it doesn't fit `_build_prefect_flow()`
- Would need its own dispatch path (not wrapped in Prefect)
- Object store semantics differ from filesystem staging
- Ray cluster management is a separate concern

**Best suited for:** Teams already using Ray, or workloads that benefit from
the object store (large intermediate data, iterative algorithms).

---

## Current Architecture: Cloud-Relevant Abstractions

### What's already cloud-ready

**Backend interface.** `BackendBase` is an ABC with pluggable traits and a
`create_flow()` method. Adding a new backend is: subclass, set traits,
implement `create_flow()`, register. The interface imposes no
filesystem assumptions.

**Orchestrator-worker split.** The `ExecutionUnit` is a sealed, self-contained
package (operation instance + input artifact IDs + cache key + step number).
Workers execute units identically regardless of where they run. This is
exactly the right abstraction for cloud dispatch.

**Trait system.** `WorkerTraits` and `OrchestratorTraits` already encode the
key behavioral differences between backends (`shared_filesystem`,
`worker_id_env_var`, `staging_verification_timeout`). Cloud backends declare
their traits and the framework adapts.

**Pickle serialization.** `ExecutionUnit` objects are pickled for dispatch.
This works for any backend that can transfer a pickle file (S3, Modal
volumes, container env). Modal's cloudpickle is a superset.

**Container environment specs.** `DockerEnvironmentSpec` and
`ApptainerEnvironmentSpec` already wrap tool commands in containers. These
compose naturally with cloud backends — a Kubernetes worker runs a Docker
image, and the operation's tool invocation runs inside another container
layer if needed.

**Content-addressed artifacts.** Artifacts are identified by content hash,
not file path. This is storage-agnostic by design — the hash is the same
whether the artifact lives on a local disk, NFS, or S3.

### What's not cloud-ready

**Filesystem-coupled storage.** Every path in the system is a
`pathlib.Path`. The three `RuntimeEnvironment` path fields
(`delta_root_path`, `staging_root_path`, `working_root_path`) assume POSIX
semantics. The staging layer (`StagingArea`, `StagingManager`,
`DeltaCommitter`) operates exclusively on local paths. Delta Lake reads
and writes go through `deltalake.DeltaTable(path)` where path is a local
directory.

This is the single largest gap. Every cloud backend without a shared
filesystem needs an alternative data transfer mechanism.

**Prefect-coupled dispatch.** `_build_prefect_flow()` constructs a Prefect
`@flow` with a `TaskRunner`. This works for backends with Prefect
TaskRunners (Kubernetes, ECS, Batch) but doesn't accommodate native
dispatch for Modal, Lambda, or Ray. The planned `DispatchHandle` refactor
(see `dispatch_handle.md`) would decouple dispatch from Prefect.

**Implicit code availability.** Workers assume `from artisan.operations...`
is importable. Local and SLURM satisfy this because they share a filesystem
and Python environment. Cloud workers need the code bundled into a
container image or serialized alongside the unit.

---

## Gap Analysis

### Gap 1: Storage and Data Transfer

**Severity: High — blocks all non-shared-filesystem backends.**

The storage layer assumes POSIX paths at every level:

| Component | Path usage | Cloud impact |
|-----------|-----------|--------------|
| `RuntimeEnvironment` | `delta_root_path`, `staging_root_path`, `working_root_path` as `Path` | Can't point to S3/GCS |
| `StagingArea` | `Path.mkdir()`, `pl.DataFrame.write_parquet(path)` | Workers can't stage to remote storage |
| `StagingManager` | `Path.glob()`, `pl.read_parquet(path)` | Orchestrator can't discover remote staged files |
| `DeltaCommitter` | `DeltaTable(path)` for reads and writes | Works with S3 URIs if using deltalake-rs S3 support |
| `ArtifactStore` | `DeltaTable(path)` for reads | Same as above |
| Input materialization | Writes artifacts to local `materialized_inputs/` dir | Fine — workers have local `/tmp` |
| Output staging | Writes Parquet to `staging_root / step_N / ...` | Breaks if staging_root is not local or shared |
| NFS fsync/verification | `os.fsync()`, `os.listdir()` cache invalidation | Not applicable to cloud storage |

**Key insight:** `deltalake-rs` (the Rust Delta Lake library used by the
Python `deltalake` package) already supports S3, GCS, and Azure Blob
natively. `DeltaTable("s3://bucket/delta")` works today. The gap is not in
Delta Lake itself but in the staging and transfer layers around it.

### Gap 2: Dispatch Mechanism

**Severity: Medium — blocks Modal, Lambda, Ray; does not block k8s or Batch.**

The current dispatch path is: `BackendBase.create_flow()` → Prefect `@flow`
→ `execute_unit_task.map()` → `_collect_results()`. Backends that have
Prefect TaskRunners (Kubernetes, ECS, Batch) can plug in directly. Backends
with their own task mapping (Modal `.map()`, Lambda `invoke()`, Ray
`.remote()`) cannot.

The planned `DispatchHandle` interface (`dispatch_handle.md`) already
addresses this: `dispatch()` / `is_done()` / `collect()` / `cancel()`
decouples the dispatch mechanism from Prefect. A `ModalDispatchHandle`
would call `modal_function.map()` in `dispatch()` and poll
`modal.FunctionCall.get()` in `collect()`.

**Recommendation:** Implement `DispatchHandle` before cloud backends. It is
a prerequisite for non-Prefect backends and independently valuable for
cancellation support.

### Gap 3: Worker Environment and Code Delivery

**Severity: Medium — affects all cloud backends.**

Workers must be able to `import` the operation classes. On local/SLURM, this
is satisfied by the shared filesystem and Python environment. Cloud workers
need:

- A container image with artisan installed
- Operation code either baked into the image or delivered alongside the unit

This is not a framework gap per se — it's an operational concern. But the
framework should make it easy to build worker images and validate that
operations are importable in the target environment.

---

## Strategy: Three Storage Models

Rather than a single abstraction that tries to cover all backends, the
analysis reveals three natural storage models, each suited to different
backend categories:

### Model A: Shared Filesystem

**Backends:** LOCAL, SLURM, Kubernetes (NFS PVC), Managed Batch (EFS),
Modal (Volume)

**How it works:** The current model. Workers and orchestrator share a
filesystem. Workers write staged Parquet to a shared path. Orchestrator
reads and commits to Delta Lake. `shared_filesystem=True` in traits.

**Changes needed:** None for Kubernetes with NFS PVC. For EFS/Modal Volume,
the mount path may differ from local paths, but `Path` semantics still
apply.

### Model B: Object Storage with Local Sandbox

**Backends:** Lambda, Fargate (without EFS), Cloud Run, Managed Batch
(without EFS)

**How it works:** Workers use local `/tmp` as their sandbox (input
materialization, execute_dir, postprocess_dir) — this is unchanged. The
new behavior is in how units arrive and how staged outputs leave:

- **Unit delivery:** Orchestrator uploads pickled units to object storage
  (S3/GCS). Workers download from object storage on startup.
- **Staging upload:** After the lifecycle completes, workers upload staged
  Parquet files to object storage instead of writing to a shared path.
- **Orchestrator collection:** Orchestrator reads staged files from object
  storage and commits to Delta Lake (which can also live on object
  storage via deltalake-rs).

The key abstraction is a **`TransferStrategy`** that handles the
upload/download of units and staged files:

```python
class TransferStrategy(ABC):
    """Moves data between orchestrator and workers."""

    @abstractmethod
    def upload_units(self, local_path: Path) -> str:
        """Upload pickled units, return a URI workers can fetch."""

    @abstractmethod
    def download_units(self, uri: str, local_path: Path) -> None:
        """Download pickled units to a local path on the worker."""

    @abstractmethod
    def upload_staging(self, local_dir: Path) -> str:
        """Upload staged Parquet directory, return a URI."""

    @abstractmethod
    def download_staging(self, uri: str, local_dir: Path) -> None:
        """Download staged Parquet to a local directory."""
```

Implementations: `LocalTransfer` (no-op, paths are already shared),
`S3Transfer`, `GCSTransfer`, `ModalVolumeTransfer`.

This strategy is owned by the backend and injected into
`RuntimeEnvironment` (or a new field on it). Workers call
`transfer.download_units()` on startup and `transfer.upload_staging()`
after the lifecycle. The orchestrator calls `transfer.download_staging()`
before commit.

**Changes needed:**
- New `TransferStrategy` ABC and implementations
- Worker bootstrap code calls `download_units()` / `upload_staging()`
- Orchestrator collection calls `download_staging()` before commit
- `RuntimeEnvironment` gains a transfer config field (serializable)
- `StagingManager` gains an object-storage discovery path

### Model C: Framework-Managed Object Store

**Backends:** Ray (object store), future in-memory backends

**How it works:** The framework manages an in-memory or distributed object
store that replaces filesystem staging entirely. Artifacts flow as
serialized objects, not files. This is a more fundamental change and is
mainly relevant for Ray's object store or future streaming architectures.

**Changes needed:** Significant — alternative staging path that bypasses
Parquet files entirely. Out of scope for initial cloud support.

---

## Backend Designs

### Kubernetes Backend

**Priority: Highest.** Closest to SLURM. Shared filesystem via NFS PVC.
Prefect has a Kubernetes TaskRunner.

```python
class KubernetesBackend(BackendBase):
    name = "kubernetes"
    worker_traits = WorkerTraits(
        worker_id_env_var="JOB_COMPLETION_INDEX",
        shared_filesystem=True,  # assumes NFS PVC
    )
    orchestrator_traits = OrchestratorTraits(
        shared_filesystem=True,
        staging_verification_timeout=30.0,
    )

    def create_flow(self, resources, execution, step_number, job_name):
        from prefect_kubernetes import KubernetesTaskRunner

        task_runner = KubernetesTaskRunner(
            image=self._worker_image,
            cpu_request=str(resources.cpus),
            memory_request=f"{resources.memory_gb}Gi",
            gpu_limit=resources.gpus or None,
            namespace=self._namespace,
            service_account_name=self._service_account,
            volumes=self._volume_mounts,  # NFS PVC
            job_name=f"s{step_number}-{job_name}",
            **dict(resources.extra),
        )
        return self._build_prefect_flow(task_runner)
```

**Storage model:** A (shared filesystem via NFS PVC). No changes to staging
or commit.

**Prerequisites:**
- Worker Docker image with artisan + operations installed
- Kubernetes cluster with NFS-backed PVC
- Prefect Kubernetes integration (`prefect-kubernetes`)

**Configuration surface:**
- `worker_image` — Docker image for worker pods
- `namespace` — Kubernetes namespace
- `service_account` — for RBAC
- `volume_mounts` — PVC mounts for Delta root, staging, working dirs
- `node_selector` / `tolerations` — for GPU node targeting
- `ResourceConfig.extra` — escape hatch for k8s-specific settings

---

### Modal Backend

**Priority: High.** Python-native GPU compute. Fast cold starts. Native
`.map()` parallel.

```python
class ModalBackend(BackendBase):
    name = "modal"
    worker_traits = WorkerTraits(
        worker_id_env_var=None,  # Modal manages worker identity
        shared_filesystem=True,  # if using modal.Volume
    )
    orchestrator_traits = OrchestratorTraits(
        shared_filesystem=True,  # volume is eventually consistent
        staging_verification_timeout=10.0,
    )

    def create_flow(self, resources, execution, step_number, job_name):
        # Returns a callable that uses modal.Function.map()
        # instead of Prefect task mapping
        ...
```

**Storage model:** A (shared filesystem via `modal.Volume`) or B (S3
transfer). Modal Volumes provide a POSIX-like shared filesystem that
workers and the orchestrator can both access. If the orchestrator runs
locally (not on Modal), it accesses the volume via the Modal client.

**Dispatch model:** Modal-native, not Prefect. A `ModalDispatchHandle`
wraps `modal.Function.map()`:

```python
class ModalDispatchHandle(DispatchHandle):
    def dispatch(self, units_path, runtime_env):
        units = _load_units(Path(units_path))
        self._call = self._modal_fn.map.aio(
            [(unit, runtime_env) for unit in units]
        )

    def is_done(self):
        return self._call.done()

    def collect(self):
        return list(self._call.result())

    def cancel(self):
        self._call.cancel()
```

**Prerequisites:**
- `DispatchHandle` refactor (to bypass Prefect)
- Modal account and token
- Modal `Image` definition with artisan + operations
- Modal `Volume` for shared storage (or S3 transfer strategy)

**Configuration surface:**
- `image` — Modal Image spec (or pre-built image name)
- `gpu` — GPU type string (`"A10G"`, `"A100"`, `"H100"`)
- `memory` — memory in MB
- `cpu` — CPU count
- `volume_name` — Modal Volume for shared storage
- `secrets` — Modal Secret references
- `ResourceConfig.extra` — escape hatch for Modal-specific settings

---

### AWS Batch Backend

**Priority: Medium.** Natural SLURM replacement for AWS users.

```python
class AWSBatchBackend(BackendBase):
    name = "aws_batch"
    worker_traits = WorkerTraits(
        worker_id_env_var="AWS_BATCH_JOB_ARRAY_INDEX",
        shared_filesystem=False,  # unless EFS mounted
    )
    orchestrator_traits = OrchestratorTraits(
        shared_filesystem=False,
    )
```

**Storage model:** B (S3 transfer) or A (if EFS mounted — then
`shared_filesystem=True`).

**Dispatch model:** Prefect `ECSTaskRunner` or direct `boto3` Batch API.
Array jobs map to SLURM job arrays.

**Prerequisites:**
- `TransferStrategy` for S3 (if not using EFS)
- `DispatchHandle` (if not using Prefect)
- Worker Docker image in ECR
- AWS Batch compute environment + job queue

---

### Lambda Backend

**Priority: Lower.** Best for many small, fast operations.

```python
class LambdaBackend(BackendBase):
    name = "lambda"
    worker_traits = WorkerTraits(
        worker_id_env_var=None,
        shared_filesystem=False,
    )
    orchestrator_traits = OrchestratorTraits(
        shared_filesystem=False,
    )
```

**Storage model:** B (S3 transfer). No shared filesystem option.

**Dispatch model:** Non-Prefect. Orchestrator invokes Lambda functions via
`boto3.invoke()` (synchronous, up to 1000 concurrent) or SQS-triggered
(asynchronous, higher throughput). A `LambdaDispatchHandle` manages
concurrent invocations and collects results.

**Hard constraints:**
- 15-minute execution timeout
- No GPU
- 10 GB ephemeral storage
- 10 GB container image size
- 256 KB synchronous payload (units must go through S3)

**Validation:** `LambdaBackend.validate_operation()` should reject
operations with GPU requirements or estimated execution times > 15 min.

---

## Implementation Phases

Each phase is independently shippable and builds on the previous.

### Phase 1: DispatchHandle Refactor

Decouple dispatch from Prefect by implementing the `DispatchHandle`
interface described in `dispatch_handle.md`. This is a prerequisite for
Modal, Lambda, and Ray backends, and independently valuable for
cancellation support.

**Scope:** `BackendBase`, `LocalBackend`, `SlurmBackend`, `step_executor`,
`pipeline_manager`. No new backends.

**Dependency:** None (independent of storage work).

### Phase 2: Kubernetes Backend

Implement `KubernetesBackend` using storage model A (NFS PVC). This
requires no storage abstraction work — the existing staging model works
unchanged with a shared filesystem.

**Scope:** New `KubernetesBackend` class, registration in `Backend`
namespace, documentation. Worker Docker image build process.

**Dependency:** None (can use `_build_prefect_flow()` with Prefect's
Kubernetes TaskRunner before DispatchHandle lands, or DispatchHandle after).

### Phase 3: Transfer Strategy

Implement the `TransferStrategy` abstraction for object storage backends.
`LocalTransfer` (no-op), `S3Transfer`, `GCSTransfer`. Modify worker
bootstrap and orchestrator collection to use the transfer strategy.

**Scope:** New `TransferStrategy` ABC, `S3Transfer` implementation,
modifications to worker dispatch/collection code, `RuntimeEnvironment`
transfer config.

**Dependency:** None (independent of DispatchHandle).

### Phase 4: Modal Backend

Implement `ModalBackend` using storage model A (Modal Volume) or B (S3
transfer). Requires `DispatchHandle` for non-Prefect dispatch.

**Scope:** New `ModalBackend` and `ModalDispatchHandle`, Modal Image
definition, documentation.

**Dependency:** Phase 1 (DispatchHandle).

### Phase 5: AWS Batch / Lambda Backends

Implement `AWSBatchBackend` and optionally `LambdaBackend` using storage
model B (S3 transfer).

**Scope:** New backend classes, dispatch handles, documentation.

**Dependency:** Phase 1 (DispatchHandle) + Phase 3 (TransferStrategy).

---

## Cross-Cutting Concerns

### Worker Image Management

All cloud backends need a Docker image with artisan + operation code. The
framework should provide:

- A base `artisan-worker` image with the framework installed
- Documentation for building operation-specific images (extending the base)
- Validation that operations are importable in the target image

The image is not a framework concern — it's an operational concern. But
helper tooling (a `Dockerfile` template, a `build-worker-image` CLI
command) would reduce friction.

### Delta Lake on Object Storage

`deltalake-rs` supports S3, GCS, and Azure natively via
`DeltaTable("s3://bucket/path")`. The storage options (credentials, region,
endpoint) are passed via `storage_options` dict. This means the orchestrator
can commit to a cloud-hosted Delta Lake without changes to `DeltaCommitter`
beyond accepting URI strings alongside `Path` objects.

Workers would read from Delta Lake (for `ArtifactStore` lookups during
input hydration) via the same mechanism. The `delta_root_path` in
`RuntimeEnvironment` would accept a URI string in addition to a `Path`.

### Cost Model

Different backends have different cost profiles:

| Backend | Billing model | Cost driver |
|---------|--------------|-------------|
| LOCAL | Hardware amortization | Fixed |
| SLURM | Allocation hours | Time × nodes |
| Kubernetes | Node hours (+ autoscaler) | Time × nodes |
| Modal | Per-second GPU/CPU | Compute seconds |
| AWS Batch | Per-vCPU-second + spot pricing | Compute seconds |
| Lambda | Per-invocation + per-GB-second | Invocations × duration |

The framework doesn't need to model costs, but documentation should help
users choose backends based on workload characteristics:

- **Many small, fast ops** → Lambda (cheapest per-invocation)
- **GPU-heavy, variable load** → Modal (no idle cost, fast cold start)
- **Steady-state GPU cluster** → Kubernetes or SLURM (amortized cost)
- **Burst CPU workloads** → AWS Batch with spot (cheapest per-vCPU-hour)

### Hybrid Pipelines

The existing per-step backend override (`pipeline.run(...,
backend=Backend.SLURM)`) already supports hybrid pipelines. A pipeline
could run data ingestion locally, GPU inference on Modal, and metric
aggregation on Lambda:

```python
pipeline.run(IngestData, inputs=files, backend=Backend.LOCAL)
pipeline.run(RunInference, inputs=..., backend=Backend.MODAL)
pipeline.run(CalcMetrics, inputs=..., backend=Backend.LAMBDA)
```

This works today at the API level. The implementation challenge is that
different backends may use different storage locations (local disk vs S3).
The `TransferStrategy` must handle cross-backend data flow — e.g., the
Modal step's outputs are on a Modal Volume, and the Lambda step needs them
on S3.

**Resolution:** Use a single canonical storage location (S3 or a shared
filesystem) for Delta Lake tables. All backends read from and write to the
same Delta Lake, regardless of where their staging happens. The transfer
strategy only handles staging files (ephemeral); Delta Lake is the source
of truth and is accessible from all backends.

---

## Comparison: Current vs Target Architecture

### Current

```
Orchestrator (local Python process)
    │
    ├── Resolve inputs (Delta Lake on local disk)
    ├── Batch into ExecutionUnits
    ├── Pickle units to shared filesystem
    ├── Dispatch via Prefect @flow
    │       │
    │       ├── Local: ProcessPool (same machine)
    │       └── SLURM: sbatch job array (shared NFS)
    │
    ├── Workers read units from shared filesystem
    ├── Workers write staged Parquet to shared filesystem
    ├── Orchestrator reads staged files from shared filesystem
    └── Commit to Delta Lake (local disk)
```

### Target

```
Orchestrator (local Python process OR cloud-hosted)
    │
    ├── Resolve inputs (Delta Lake on local disk OR object storage)
    ├── Batch into ExecutionUnits
    ├── Transfer units via TransferStrategy
    │       │
    │       ├── Local/SLURM/K8s: write to shared filesystem
    │       └── Cloud: upload to S3/GCS
    │
    ├── Dispatch via DispatchHandle
    │       │
    │       ├── Local: ProcessPool
    │       ├── SLURM: sbatch
    │       ├── Kubernetes: Job (Prefect or direct)
    │       ├── Modal: .map() (native)
    │       ├── AWS Batch: SubmitJob (Prefect or direct)
    │       └── Lambda: invoke() (direct)
    │
    ├── Workers download units via TransferStrategy
    ├── Workers execute locally (sandbox in /tmp)
    ├── Workers upload staged Parquet via TransferStrategy
    ├── Orchestrator downloads staged files via TransferStrategy
    └── Commit to Delta Lake (local disk OR object storage)
```

The operation lifecycle (preprocess/execute/postprocess) is unchanged.
Operations always work with local paths in their sandbox. The framework
handles data transfer transparently.

---

## Open Questions

- **Delta Lake location for hybrid pipelines.** If the orchestrator runs
  locally but workers run on Modal/Lambda, where does Delta Lake live? Local
  disk requires workers to have network access back to the orchestrator.
  S3-hosted Delta Lake is more natural but requires all backends (including
  local) to go through S3. Should the framework support both, with a
  `delta_storage` config?

- **Operation code delivery.** Should the framework help build worker
  images (e.g., a `artisan build-image` CLI), or is this purely the user's
  responsibility? How do users test that their operations are importable in
  the target container?

- **Transfer strategy performance.** S3 upload/download adds latency per
  unit. For many small units (Lambda), the transfer overhead may dominate
  execution time. Should the framework batch-transfer multiple units'
  staged files in a single S3 operation?

- **Modal Volume vs S3.** Modal Volumes provide POSIX semantics but are
  Modal-specific. S3 is universal but requires the transfer strategy.
  Should ModalBackend default to Volume (simpler) or S3 (more portable)?

- **Credential management.** Cloud backends need credentials (AWS keys,
  Modal tokens, k8s kubeconfig). Should the framework manage these, or
  delegate to the environment (env vars, IAM roles, service accounts)?
  Current approach for SLURM is "the environment just works" — same
  philosophy should apply to cloud.

- **Streaming pipeline interaction.** The streaming pipeline design
  (`streaming_pipeline_execution.md`) assumes `DispatchHandle` and a step
  scheduler. Cloud backends with variable latency (cold starts, queue wait)
  interact differently with priority scheduling than resource-managed
  backends (LOCAL, SLURM_INTRA). How does the step scheduler adapt its
  dispatch strategy for queue-managed cloud backends?

- **Prefect dependency.** Adding non-Prefect backends (Modal, Lambda, Ray)
  means Prefect is no longer required for all dispatch paths. Should
  Prefect become an optional dependency, installed only when using
  Prefect-based backends (LOCAL, SLURM, Kubernetes)?
