# Ray: Orchestration Across Cloud Compute Resources

How Ray handles distributed compute, data movement, cluster deployment, resource
scheduling, fault tolerance, and model serving -- and what this means for
Artisan's cloud backend strategy.

---

## Task Model & Object Store

Ray's compute model rests on three primitives: **tasks** (stateless `@ray.remote`
functions), **actors** (stateful `@ray.remote` classes), and **objects** (immutable
shared data identified by `ObjectRef`).

A call to `process.remote(data)` does not return a result -- it returns an
`ObjectRef`, a handle to a future value stored in Ray's distributed object store.
The caller can pass that ref directly to another task without ever materializing
the data locally. This is the key architectural insight: **data moves between
tasks via the object store, not via filesystem I/O**.

The object store is a per-node shared-memory segment (originally based on Apache
Arrow's Plasma). On each node:

- Objects created by `ray.put()` or task returns are written into shared memory.
- Local reads of numpy arrays are zero-copy (backed by the shared memory segment
  directly via memory-mapped files).
- Cross-node reads trigger automatic transfer: the requesting node's object store
  pulls the data from the owning node.
- Serialization uses Pickle 5 with out-of-band buffers for numpy/Arrow data,
  enabling efficient handling of large arrays without copying.

Objects are **immutable once sealed** and **reference-counted**. When no
references remain, memory is reclaimed. Under memory pressure, objects are
evicted via LRU and can spill to local disk. By default, the object store gets
30% of available memory; worker heap gets the other 70%.

Objects are **not content-addressed**. Two identical computations on the same
input produce different `ObjectRef`s. There is no deduplication, no cross-run
identity, and no mechanism to ask "has this been computed before?" This is a
fundamental difference from Artisan's model.

---

## Cluster Deployment

Ray runs on local machines, cloud VMs, and Kubernetes with a consistent
programming model.

**Local**: `ray.init()` starts a single-node cluster in-process. No
configuration needed. Good for development and testing.

**Cloud VMs (Ray Cluster Launcher)**: The `ray up` CLI reads a YAML config
specifying a cloud provider (AWS, GCP, Azure), instance types, and min/max
nodes. It provisions a head node, starts the GCS and scheduler, then adds worker
nodes on demand. The cluster launcher handles SSH, instance lifecycle, and
autoscaling.

**Kubernetes (KubeRay)**: The production-grade path. KubeRay is a Kubernetes
operator providing three CRDs:

- **RayCluster**: Manages the lifecycle of a Ray cluster (head + worker pods).
- **RayJob**: Submits a one-shot job to a cluster, optionally creating and
  tearing down the cluster around it.
- **RayService**: Manages Ray Serve deployments with rolling updates.

KubeRay autoscaling operates as a sidecar in the head pod. It monitors logical
resource demand (not physical utilization), scales worker pods by adjusting the
`replicas` field on the RayCluster CR, and relies on the Kubernetes cluster
autoscaler to provision underlying nodes when pod scheduling fails. Scale-down
identifies idle workers (no active tasks, actors, or referenced objects) and
drains them after a configurable idle timeout (default 60s). Recent versions
(v2.10+) add per-instance lifecycle tracing and graceful draining to prevent
task disruption during termination.

Three autoscaling layers interact: (1) Ray library-level scaling (e.g., Ray
Serve adjusting replica count), (2) Ray autoscaler adjusting pod count via the
RayCluster CR, and (3) Kubernetes node autoscaler provisioning physical nodes.

As of GKE v1.33 and KubeRay v1.4, in-place pod resizing allows Ray workers to
vertically scale resources in seconds rather than the minutes required for new
pod provisioning.

---

## Resource Management

Ray's resource model is **logical, not physical**. Resources are key-value pairs
(name to float quantity) used purely for scheduling admission control. Declaring
`num_cpus=2` on a task means the scheduler reserves two CPU slots -- it does not
pin cores or enforce CPU isolation. The OS schedules threads freely across
physical cores.

Default logical resources detected at startup: CPUs (matching physical count),
GPUs (matching physical count), memory (70% of available). Custom resources are
arbitrary labels (e.g., `{"special_hardware": 1}`) declared at node startup and
requested by tasks.

Key characteristics:

- **Fractional resources**: Tasks can request `num_cpus=0.5` for IO-bound work.
  Precision is 0.0001. GPU requests above 1.0 must be integers.
- **GPU isolation**: Via `CUDA_VISIBLE_DEVICES` environment variable set per
  worker process. Not a hard boundary -- just a convention that ML frameworks
  respect.
- **Scheduling constraint**: The sum of logical resource requirements of all
  concurrent tasks/actors on a node cannot exceed the node's total logical
  resources. This is the **only** enforcement mechanism -- there is no cgroup
  isolation, no memory fencing, no CPU pinning.
- **Placement groups**: Bundle resource requirements that must be co-located on
  the same node or spread across nodes, useful for distributed training.

**Contrast with SLURM**: SLURM allocates exclusive node/core/GPU slots with hard
isolation via cgroups. A SLURM job owns its allocation. Ray tasks share a node
cooperatively, relying on logical accounting rather than physical partitioning.
This makes Ray more flexible for fine-grained task parallelism but less
appropriate for workloads that require strict resource isolation (e.g., MPI jobs
that assume exclusive node access).

---

## Batch Processing

Ray Data provides a streaming execution engine for large-scale data processing
and batch inference. It processes data in **blocks** (chunks of rows) without
materializing entire datasets in memory.

The core operation is `map_batches()`:

- **Stateless mode** (function): Uses Ray tasks. Each batch is independent.
  Simple and efficient for pure transformations.
- **Stateful mode** (callable class): Uses Ray actors via `ActorPoolStrategy`.
  The class `__init__` loads expensive state (models, lookup tables) once, and
  the `__call__` method processes batches. This is the pattern for batch
  inference -- the model loads once per actor, not once per batch.

Streaming execution applies backpressure to prevent upstream operators from
overwhelming downstream consumers or exhausting memory. Data flows through
operator stages with bounded buffering.

Recent additions (Ray 2.44+): native vLLM integration for LLM batch inference,
SGLang support (2.45), cross-node model parallelism for large models, and
support for arbitrary accelerators (TPUs, non-NVIDIA hardware).

---

## Fault Tolerance

Ray provides three levels of fault tolerance:

**Task retries**: When a worker dies mid-task (crash or node failure), Ray
re-executes the task on another worker. Non-actor tasks default to 3 retries
(`max_retries`). Actor tasks default to 0 retries but can be configured.

**Object reconstruction via lineage**: When an object is lost (node failure,
eviction), Ray first searches for copies on other nodes. If none exist, it
re-executes the task that created the object, recursively reconstructing
dependencies. This is lineage-based recovery -- the driver retains task
descriptions (bounded to 1GB by default via `RAY_max_lineage_bytes`) for
potential re-execution. Tasks are assumed deterministic and idempotent.

**Limitations**:

- Objects created by `ray.put()` cannot be reconstructed (no task lineage).
- Owner failure (the worker that created the `ObjectRef`) causes
  `OwnerDiedError` -- remaining copies are cleaned up and the object is
  unrecoverable.
- Actor state is not checkpointed by default. Actor reconstruction recreates
  the actor from scratch (re-runs `__init__`), losing accumulated state unless
  the application implements its own checkpointing.
- No durable execution: if the entire cluster fails, all progress is lost.

**Contrast with Artisan**: Artisan's atomic commit to Delta Lake means completed
work survives any failure. Ray's lineage reconstruction is intra-session only --
it recovers from partial failures during a run but provides no cross-run
durability.

---

## Model Lifecycle

Ray Serve deploys ML models as autoscaling HTTP endpoints backed by Ray actors.

A **deployment** is a class whose `__init__` loads the model and whose
`__call__` handles requests. At runtime, each deployment has N **replicas** --
separate actor processes, each holding the model in memory. This is the "warm
container" model: the model loads once at replica startup, then handles many
requests without reloading.

Key mechanisms:

- **Autoscaling**: Replica count adjusts based on request queue depth. Scale-up
  provisions new actors (with model loading cost); scale-down terminates idle
  replicas after a timeout.
- **Multi-model composition**: Deployments call other deployments via
  `DeploymentHandle`, enabling pipelines (preprocessing -> inference ->
  postprocessing) as a graph of warm services.
- **Request batching**: Serve can batch incoming requests for vectorized
  inference on GPU.

The model lifecycle is: deployment defined -> replicas created as actors ->
`__init__` loads model into memory -> replicas serve requests indefinitely ->
scale-down or redeployment terminates replicas.

This pattern -- persistent actors holding loaded models -- is relevant for
Artisan operations that require expensive initialization (loading ML models,
connecting to databases). Ray Serve's approach to this is mature and
battle-tested.

---

## Integration Patterns

The ecosystem has converged on a clear separation: **orchestrators manage
workflow structure; Ray manages distributed compute**.

**Flyte + Ray**: Flyte's RayJobConfig plugin spins up an ephemeral Ray cluster
per Flyte task, dispatches work, collects results, and tears down the cluster.
Flyte handles workflow sequencing, caching, artifact versioning, retry logic, and
provenance. Ray handles intra-task parallelism. Data flows through Flyte's
artifact storage between steps; Ray processes batches internally within a step.

**Prefect + Ray**: The `prefect-ray` integration allows Prefect task runs to
execute as Ray remote tasks. Prefect manages flow structure, scheduling, and
observability. Ray provides the parallel execution backend.

**Airflow + Ray**: The Ray provider for Airflow lets DAG tasks submit Ray jobs.
Airflow manages scheduling and dependency resolution; Ray executes the compute.

**The pattern**: In all cases, Ray is a **compute backend**, not a pipeline
orchestrator. It does not manage cross-step data flow, artifact persistence,
caching, provenance, or workflow-level retries. These responsibilities belong
to the orchestration layer above it.

---

## Learnings for Artisan

### Object store vs filesystem staging

Ray's object store eliminates filesystem I/O between tasks within a session. Data
moves through shared memory (intra-node) or network transfer (inter-node) without
ever touching disk. This gives Ray excellent performance for fine-grained task
parallelism with high data throughput.

But the object store is **volatile**. Nothing persists beyond the cluster
lifetime. There is no content addressing, no deduplication, no provenance. This
is exactly the gap Artisan fills.

The tradeoff is clear: **Artisan should not try to replicate the object store
for intra-session data movement**. Instead, the planned `TransferStrategy`
abstraction should handle moving staged artifacts between workers and durable
storage (Delta Lake, object stores). The object store is an optimization for
transient data within a computation; Artisan's staging-to-commit pipeline is for
durable, addressable artifacts.

### Ray as a backend, not a compute adapter

The Flyte/Prefect/Airflow integration pattern is instructive. Ray is always the
compute layer underneath an orchestrator. **Artisan's orchestrator should treat
Ray the same way it treats SLURM: as a `BackendBase` implementation**.

A `RayBackend` would:

- Accept `ExecutionUnit`s from the orchestrator (same interface as LOCAL and
  SLURM backends).
- Submit each unit as a Ray remote task (or actor for stateful operations).
- Use Ray's object store for data movement between co-scheduled tasks within a
  step, but stage final outputs as Parquet for the orchestrator to commit.
- Leverage Ray's autoscaling for elastic compute without Artisan needing to
  manage cluster sizing.

This is cleaner than making Ray a "compute adapter" within workers. The backend
abstraction already exists; Ray fits it naturally.

### Resource model mapping

Artisan's two-level batching (`artifacts_per_unit` x `units_per_worker`) maps
well to Ray's resource model. Each `ExecutionUnit` becomes a Ray task with
declared resource requirements (CPU, GPU, memory, custom). Ray's scheduler
handles placement. This replaces SLURM's job-array-based dispatch with
fine-grained task-level scheduling, which is better suited to heterogeneous
workloads and elastic clusters.

### Warm workers for expensive initialization

Ray Serve's actor-based model loading pattern (load in `__init__`, reuse across
invocations) solves a problem Artisan will face with ML-heavy operations. A Ray
backend could use actors instead of tasks for operations with expensive setup,
keeping models warm across batches within a step. This avoids per-unit model
loading overhead without requiring Artisan to manage the lifecycle itself.

### Fault tolerance gap

Ray's lineage reconstruction handles transient failures within a run but provides
no cross-run durability. Artisan already solves this: completed steps are
committed atomically to Delta Lake and cached for future runs. A Ray backend
would rely on Ray for intra-step fault tolerance (task retries, object
reconstruction) and on Artisan's commit model for cross-step durability. The two
are complementary, not redundant.

### Ephemeral clusters

The Flyte pattern of spinning up per-job Ray clusters is worth considering. For
cloud deployment, Artisan could launch a Ray cluster per pipeline run (via
KubeRay's RayJob CRD), eliminating long-running cluster management. The cluster
autoscales during the run and is torn down afterward. This matches Artisan's
run-oriented model better than a persistent Ray cluster.
