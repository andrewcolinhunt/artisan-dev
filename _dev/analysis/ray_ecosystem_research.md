# Ray Ecosystem Research

Comprehensive analysis of the Ray ecosystem for comparison with Artisan.

---

## Ray Core

### Architecture

Ray is a general-purpose distributed computing framework built on three
primitives: **tasks** (stateless functions), **actors** (stateful classes), and
**objects** (immutable shared data). These map to a worker-based execution model
where a driver process submits work to a cluster of worker processes.

**Key components:**

- **Raylet**: Per-node agent that manages local scheduling and object store
- **Global Control Store (GCS)**: Centralized key-value store (backed by Redis,
  now internal) for cluster metadata, actor locations, and object ownership
- **Object Store**: Per-node shared-memory store (originally Plasma/Arrow) for
  immutable objects with zero-copy reads via memory mapping
- **Scheduler**: Hybrid bottom-up scheduling -- tasks are scheduled locally when
  possible, spilling to other nodes via the GCS when resources are constrained

### Task and Actor Model

Tasks are decorated Python functions executed remotely:

```python
@ray.remote
def process(data):
    return transform(data)

ref = process.remote(data)  # Returns ObjectRef, not result
result = ray.get(ref)       # Blocks until result available
```

Actors are decorated classes with mutable state:

```python
@ray.remote
class Counter:
    def __init__(self):
        self.count = 0
    def increment(self):
        self.count += 1
        return self.count
```

### Object Store

- Objects are **immutable** once sealed -- no mutation after creation
- Stored in shared memory (zero-copy reads for numpy arrays via Pickle 5)
- Objects are **not content-addressed** -- identified by ObjectRef (essentially
  a unique ID assigned at creation time)
- Eviction via LRU when memory pressure occurs; lineage-based reconstruction
  can re-execute tasks to recover evicted objects
- No persistence -- objects exist only in memory during cluster lifetime

### Fault Tolerance

- **Task fault tolerance**: Failed tasks can be retried (configurable
  `max_retries`). Ray tracks lineage metadata to re-execute tasks that produced
  lost objects.
- **Actor fault tolerance**: Actors can be restarted on failure with
  configurable `max_restarts`. State is lost unless checkpointed externally.
- **Object fault tolerance**: If an object is evicted or its node dies, Ray can
  reconstruct it by re-executing its lineage (the chain of tasks that produced
  it). This is lineage-based reconstruction, not provenance tracking.

### Scheduling

- Resource-based: tasks declare CPU/GPU/memory/custom resource requirements
- Strategies: DEFAULT (locality-aware), SPREAD (distribute evenly),
  placement groups (co-locate related tasks/actors)
- No content-aware scheduling or artifact-aware placement

---

## Ray Data

### Dataset Abstraction

Ray Data provides a `Dataset` class representing a distributed collection of
"blocks" (Arrow tables or numpy arrays). Execution is lazy -- transformations
build a logical plan that is optimized and executed only when results are
consumed.

### Streaming Execution

- Three-stage pipeline: logical planning, physical planning with optimization,
  streaming execution with backpressure
- Operators are connected in a pipeline where each operator's output feeds the
  next operator's input -- does not wait for one stage to complete before
  starting the next
- Enables processing datasets larger than cluster memory
- Resource-aware: CPU preprocessing and GPU inference can run concurrently

### Pipeline Model

- **Linear chains**: Well-supported. Map, filter, flat_map, etc.
- **Fan-out / branching**: Limited. Known issue where fan-out stages cause
  downstream operators to run sequentially rather than in parallel
  (ray-project/ray#54430)
- **Multi-input (joins/merges)**: Not a first-class concept. Ray Data is
  oriented toward single-stream transformations, not DAG-shaped dataflows.

### Data Management

- No artifact identity or content addressing
- No built-in caching between pipeline runs -- re-executes from source each time
- No provenance tracking of data transformations
- Data is ephemeral (in-memory blocks), not durable artifacts
- Can read/write from cloud storage (S3, GCS) and local/NFS, but this is I/O,
  not artifact management

---

## Ray Train

### Distributed Training

Ray Train coordinates distributed training across multiple workers with:

- Data-parallel training (PyTorch DDP, FSDP, DeepSpeed, Megatron)
- Automatic gradient synchronization
- Fault recovery with configurable retries

### Checkpointing

- Workers call `ray.train.report(metrics, checkpoint=...)` to save checkpoints
- Supports distributed checkpointing: each worker uploads its own shard in
  parallel (avoids gathering to single node)
- Async checkpoint upload available (`CheckpointUploadMode.ASYNC`) to avoid
  blocking GPU training
- Storage: cloud storage (S3, GCS) or shared filesystems (NFS, EFS)
- Checkpoints are identified by trial/run ID and step number -- **not
  content-addressed**

### What It Doesn't Do

- No automatic experiment reproducibility (must manually log hyperparams, code
  versions, etc.)
- No artifact lineage (which dataset version produced which model checkpoint)
- No content-addressed model storage
- Integrates with MLflow/W&B for experiment tracking, but this is external

---

## Ray Tune

### Architecture

Ray Tune provides hyperparameter optimization with:

- **Search space**: Define parameter distributions
- **Trainable**: Function or class that runs one trial
- **Search algorithm**: Guides parameter selection (random, Bayesian, etc.)
- **Scheduler**: Early-stops bad trials, adjusts hyperparameters mid-run

### Search Algorithms

- Built-in: random search, grid search
- Integrations: Optuna, Ax, BayesOpt, BOHB, Nevergrad, HyperOpt
- BOHB combines Bayesian Optimization with HyperBand for both intelligent
  search and early termination

### Schedulers

- ASHA (Asynchronous Successive Halving): Aggressively terminates
  low-performing trials
- PBT (Population Based Training): Clones good trials, mutates hyperparameters
- PB2 (Population Based Bandits): PBT with Gaussian Process-guided perturbation
- MedianStoppingRule: Stops trials performing below median

### Storage and Results

- Trial results and checkpoints stored to configured storage path
- Supports cloud storage and shared filesystems
- Results accessible via `ResultGrid` API after tuning completes
- No content-addressed storage of trial artifacts
- No built-in cross-experiment deduplication or caching

---

## Ray Serve

### Architecture

- **Controller**: Manages deployment lifecycle, autoscaling decisions
- **HTTP Proxy**: Uvicorn-based server (one per node by default) that routes
  requests to replicas
- **Replicas**: Actor instances that execute deployment code (e.g., model
  inference)

### Autoscaling

- Application-level autoscaler on top of Ray's cluster autoscaler
- Scales replicas based on queue depth (`target_ongoing_requests`)
- Ray cluster autoscaler provisions new nodes if resources are insufficient
- Custom autoscaling policies supported (since Ray 2.51)

### Model Composition

- Deployment graphs allow multi-model composition (ensemble, routing, etc.)
- Supports dynamic request routing and model multiplexing

### Relevance to Artisan

Ray Serve is a serving/inference framework -- fundamentally different from
Artisan's domain. It has no artifact management, provenance, or pipeline
concepts. Mentioned for completeness.

---

## Ray Workflows (Deprecated)

### Status

**Deprecated as of March 2025. Scheduled for removal in a future Ray version.**

### What It Was

Ray Workflows provided durable DAG execution on top of Ray Core:

- Steps were Ray tasks decorated with `@workflow.step`
- DAGs were built by composing step outputs
- Execution was deferred until `workflow.run(dag)` was called
- Each step was durably persisted to storage (local or cloud)
- Failed steps could be retried; interrupted workflows could be resumed by ID
- Supported conditional branching (dynamic DAGs)

### Why It Mattered (and Why It Was Killed)

Ray Workflows was the closest Ray came to Artisan-style pipeline execution with
durability. Its deprecation signals that Anyscale views durable orchestration as
outside Ray's scope -- teams are directed toward external orchestrators (Flyte,
Airflow, Prefect) that use Ray as an execution backend.

### Implications

Without Workflows, Ray has **no built-in durable pipeline execution**. Users
must either:

- Accept ephemeral execution (lose progress on cluster failure)
- Use external orchestrators for durability
- Build custom checkpointing logic

---

## Ray on SLURM / HPC

### Deployment Model

Ray on SLURM uses a **head-worker architecture**:

- SLURM allocates nodes
- One node runs the Ray head (GCS, dashboard, driver)
- Other nodes run Ray workers that connect to the head
- User script runs on the head node

This is fundamentally different from SLURM's native job model where each
`srun`/`sbatch` is an independent job with its own resource allocation.

### `ray symmetric-run` (Ray 2.49+)

Addresses the impedance mismatch with HPC:

- Launches the same entrypoint on every node (like `mpirun`/`torchrun`)
- Starts a Ray cluster implicitly across allocated nodes
- More natural for HPC users who expect symmetric execution

### Limitations for HPC

- **Single cluster per allocation**: Ray expects to own the entire SLURM
  allocation. Running multiple Ray jobs on the same nodes requires careful port
  management.
- **No native SLURM job submission**: Ray doesn't submit SLURM jobs itself --
  it runs *within* a SLURM allocation. An external script or orchestrator must
  handle `sbatch`.
- **No SLURM-native scheduling**: Ray's internal scheduler is separate from
  SLURM. You can't leverage SLURM's job dependencies, array jobs, or
  heterogeneous job steps.
- **Ephemeral clusters**: Ray cluster dies when the SLURM allocation ends. No
  persistence across allocations without external orchestration.
- **No multi-allocation pipelines**: Can't natively run a pipeline where
  step A uses one SLURM allocation and step B uses another.

### Third-Party Tools

- **SlurmRay**: Community tool for distributing Ray tasks across SLURM clusters
- **NeMo-Run**: NVIDIA's tool for Ray on Kubernetes or SLURM

---

## Critical Comparison: Ray vs Artisan Design Principles

### Provenance / Lineage

| Aspect | Ray | Artisan |
|--------|-----|---------|
| **Tracking granularity** | Task-level lineage for object reconstruction (fault tolerance only) | Full provenance graph with operation, input, and output tracking |
| **Persistence** | Lineage is ephemeral (lost when cluster dies) | Provenance persisted to storage, queryable across runs |
| **Scope** | Internal implementation detail, not user-facing | First-class user-facing feature with visualization |
| **Cross-run** | None natively. Anyscale (commercial) added OpenLineage-based tracking in 2025, but only on their platform | Built-in across all runs |
| **Content** | Tracks "which task produced which object" for reconstruction | Tracks full data flow: which artifacts, operations, parameters |

**Verdict**: Ray has no meaningful provenance. Its lineage is a fault-tolerance
mechanism, not a scientific reproducibility feature. Anyscale's commercial
lineage tracking is platform-locked and limited to dataset/model reads/writes.

### Artifact Persistence and Caching

| Aspect | Ray | Artisan |
|--------|-----|---------|
| **Storage model** | Ephemeral in-memory object store | Persistent content-addressed artifact store |
| **Identity** | ObjectRef (opaque unique ID, location-based) | Content hash (deterministic, portable) |
| **Caching** | LRU eviction from object store; no cross-run caching | Content-addressed deduplication; automatic cross-run cache hits |
| **Persistence** | Objects lost when cluster dies | Artifacts persist indefinitely |
| **Deduplication** | None | Automatic via content addressing |

**Verdict**: Ray treats data as ephemeral compute intermediates. Artisan treats
data as first-class persistent artifacts with identity.

### Content-Addressed Storage

Ray has **no content-addressed storage**. Objects are identified by
auto-generated ObjectRefs. Two identical computations produce two separate
objects with different refs. There is no mechanism to detect that two objects
have the same content.

### Operation/Task Definition

| Aspect | Ray | Artisan |
|--------|-----|---------|
| **Definition** | `@ray.remote` decorator on any function/class | `OperationDefinition` base class with declared inputs/outputs/specs |
| **Type safety** | None at framework level (Python types only) | Schema-based input/output validation |
| **Metadata** | None (just a function) | Operation metadata, version, specs |
| **Composability** | Ad-hoc (call remote functions from remote functions) | `CompositeDefinition` with formal composition model |

### Multi-Step Pipelines with Branching

| Aspect | Ray | Artisan |
|--------|-----|---------|
| **Pipeline model** | No built-in pipeline abstraction (Workflows was deprecated) | First-class pipeline with steps, sources, branching, merging |
| **Branching** | Manual (user code decides which tasks to call) | Declarative pipeline topology |
| **Merging** | Manual | Built-in merge operations |
| **Diamond patterns** | User must handle manually | Supported natively |
| **Execution** | Eager task submission | Orchestrated step execution with batching |

### HPC / SLURM Integration

| Aspect | Ray | Artisan |
|--------|-----|---------|
| **SLURM model** | Runs inside a single SLURM allocation | Native SLURM job submission per pipeline step |
| **Multi-allocation** | Not supported | Pipeline steps can use different allocations |
| **Resource management** | Ray's internal scheduler (parallel to SLURM) | Delegates to SLURM scheduler |
| **Job dependencies** | Not leveraged | Can leverage SLURM job dependencies |
| **Array jobs** | Not leveraged | Supports batching via SLURM array jobs |

### Reproducibility

| Aspect | Ray | Artisan |
|--------|-----|---------|
| **Deterministic identity** | No (ObjectRefs are random/location-based) | Content-addressed hashing ensures identical inputs produce identical IDs |
| **Parameter tracking** | External (MLflow, W&B) | Built-in via execution config and provenance |
| **Code versioning** | External | Part of operation definition |
| **Result caching** | None across runs | Automatic via content-addressed cache lookup |
| **Audit trail** | None natively | Full provenance graph |

### Data Management vs Compute Management

This is the fundamental architectural difference:

- **Ray is compute-centric**: It manages distributed computation (scheduling,
  resources, communication) and treats data as transient intermediates that flow
  between compute tasks. Data has no identity, no persistence guarantees, and no
  tracking beyond what's needed for fault tolerance.

- **Artisan is artifact-centric**: It manages data artifacts as first-class
  entities with identity (content hash), provenance (who produced what from
  what), and persistence (durable storage). Computation is the mechanism that
  produces artifacts, but artifacts are the primary objects of concern.

---

## Summary Assessment

### What Ray Does Well

- Elastic distributed compute with minimal API surface
- GPU-aware scheduling and zero-copy data sharing
- Rich ecosystem for ML training, tuning, and serving
- Good performance for stateless parallel workloads
- Active development and large community

### What Ray Does Not Do

- **No artifact management**: No content addressing, no deduplication, no
  persistent artifact identity
- **No provenance**: No tracking of data lineage across runs or even within
  runs (beyond fault-tolerance reconstruction)
- **No reproducibility guarantees**: No deterministic artifact identity, no
  automatic caching, no parameter tracking
- **No durable pipelines**: Workflows was deprecated; no replacement. Must use
  external orchestrators.
- **No native HPC integration**: Runs inside SLURM allocations but doesn't
  leverage SLURM's scheduling, dependencies, or array jobs
- **No pipeline abstraction**: No declarative way to define multi-step
  pipelines with branching, merging, or diamond patterns

### Complementarity

Ray and Artisan address fundamentally different concerns. Ray could
theoretically serve as an execution backend for Artisan (replacing or
supplementing the SLURM backend) for workloads that benefit from Ray's actor
model and GPU scheduling. However, Ray provides none of Artisan's core value
propositions: artifact identity, provenance, content-addressed caching, or
pipeline orchestration.

The closer competitors to Artisan in the orchestration space are tools like
**Flyte** (which offers type-safe workflows, caching, and lineage) and
**Pachyderm** (which offers content-addressed data versioning). Even these lack
Artisan's combination of content-addressed storage, full provenance tracking,
and HPC-native execution.

---

## Sources

- [Ray Core Key Concepts](https://docs.ray.io/en/latest/ray-core/key-concepts.html)
- [Ray Core Walkthrough](https://docs.ray.io/en/latest/ray-core/walkthrough.html)
- [Ray Actors](https://docs.ray.io/en/latest/ray-core/actors.html)
- [Ray Memory Management](https://docs.ray.io/en/latest/ray-core/scheduling/memory-management.html)
- [Ray Scheduling Resources](https://docs.ray.io/en/latest/ray-core/scheduling/resources.html)
- [Ray Fault Tolerance](https://docs.ray.io/en/latest/ray-core/fault-tolerance.html)
- [Ray Objects](https://docs.ray.io/en/latest/ray-core/objects.html)
- [Ray Serialization](https://docs.ray.io/en/latest/ray-core/objects/serialization.html)
- [Ray Data Overview](https://docs.ray.io/en/latest/data/data.html)
- [Ray Data Internals](https://docs.ray.io/en/latest/data/data-internals.html)
- [Ray Data Key Concepts](https://docs.ray.io/en/latest/data/key-concepts.html)
- [Ray Data DeepWiki](https://deepwiki.com/ray-project/ray/3-ray-data)
- [Ray Train Checkpoints](https://docs.ray.io/en/latest/train/user-guides/checkpoints.html)
- [Ray Train V2 Announcement](https://www.anyscale.com/blog/ray-train-v2-unified-distributed-training-on-ray)
- [Ray Train Persistent Storage](https://docs.ray.io/en/latest/train/user-guides/persistent-storage.html)
- [Ray Tune Overview](https://docs.ray.io/en/latest/tune/index.html)
- [Ray Tune Key Concepts](https://docs.ray.io/en/latest/tune/key-concepts.html)
- [Ray Tune Schedulers](https://docs.ray.io/en/latest/tune/api/schedulers.html)
- [Ray Tune Search Algorithms](https://docs.ray.io/en/latest/tune/api/suggestion.html)
- [Ray Serve Architecture](https://docs.ray.io/en/latest/serve/architecture.html)
- [Ray Serve Autoscaling](https://docs.ray.io/en/latest/serve/autoscaling-guide.html)
- [Ray Workflows Deprecation Discussion](https://discuss.ray.io/t/ray-workflows-deprecated/22132)
- [Ray Workflows API Comparisons](https://docs.ray.io/en/master/workflows/comparison.html)
- [Ray on SLURM](https://docs.ray.io/en/latest/cluster/vms/user-guides/community/slurm.html)
- [Ray Symmetric Run (vLLM Blog)](https://blog.vllm.ai/2025/11/22/ray-symmetric-run.html)
- [Anyscale Lineage Tracking Announcement](https://www.anyscale.com/blog/announcing-lineage-tracking-ray-managed-anyscale)
- [Flyte vs Ray vs Flyte+Ray](https://www.union.ai/blog-post/flyte-vs-ray-vs-flyte-ray-choosing-the-right-tool-for-distributed-ai-workflows)
- [Ray's Anatomy (Monadical)](https://monadical.com/posts/the-essential-guide-to-rays-anatomy.html)
- [Plasma Object Store](https://ray-project.github.io/2017/08/08/plasma-in-memory-object-store.html)
- [Ray Data Fan-Out Issue #54430](https://github.com/ray-project/ray/issues/54430)
- [Ray Original Paper](https://ar5iv.labs.arxiv.org/html/1712.05889)
