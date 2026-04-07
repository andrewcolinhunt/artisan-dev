# Metaflow: Cloud Orchestration Research

Research into how Metaflow (Netflix/Outerbounds) handles orchestration across
different cloud compute resources, with learnings relevant to Artisan's planned
cloud backend expansion.

---

## Compute Abstraction Model

Metaflow abstracts compute backends through Python **step decorators**. The three
key decorators are `@batch` (AWS Batch), `@kubernetes`, and `@resources`. They
operate at step granularity -- each step in a flow can target a different backend.

The **`@resources` decorator** is backend-agnostic. It declares cpu, memory, gpu,
disk, and shared_memory requirements without specifying where to run. The actual
compute layer is selected either by a backend-specific decorator (`@batch`,
`@kubernetes`) on the step, or at the command line via `--with batch` or
`--with kubernetes`. When both `@resources` and a backend decorator specify the
same parameter, Metaflow takes the **maximum** of both values, ensuring the step
gets sufficient resources regardless of which decorator set them.

The **backend-specific decorators** (`@batch`, `@kubernetes`) extend resources
with platform-specific knobs. `@batch` exposes AWS-specific options like
`queue`, `iam_role`, `execution_role`, `inferentia`/`trainium` chip counts, EFA
network devices, ephemeral storage, and log driver configuration. `@kubernetes`
exposes `namespace`, `service_account`, `node_selector`, `tolerations`,
`labels`, `annotations`, `persistent_volume_claims`, `gpu_vendor`, QoS class,
and security context. These decorators are substantial -- `BatchDecorator` alone
has ~30 parameters.

The key design insight: **resources are declared orthogonally to the compute
target**. A step decorated only with `@resources(cpu=4, memory=8192)` runs
locally by default. Add `--with kubernetes` on the command line and the same step
runs on Kubernetes with those resources. Add `@batch` directly to the step and
it always runs on Batch. This separation means users can develop locally, then
target cloud without code changes.

Internally, both `BatchDecorator` and `KubernetesDecorator` inherit from
`StepDecorator`, which provides lifecycle hooks (`step_init`, `task_pre_step`,
`task_post_step`, `task_exception`, `task_finished`). The decorators intercept
step execution to launch the task in a container, wait for completion, and
propagate results back through the datastore.

---

## Data Store & Artifacts

Metaflow's datastore is a **three-layer architecture**:

**DataStoreStorage** is the lowest layer -- an abstract file/directory interface
with `save_bytes`, `load_bytes`, `list_content`, `path_join`, `path_split`,
`is_file`. Implementations exist for local filesystem, S3, Azure Blob Storage,
and Google Cloud Storage. This is the key abstraction that makes Metaflow
storage-agnostic.

**ContentAddressedStore** sits on top of DataStoreStorage. It hashes blobs with
SHA-1 and stores them at `{prefix}/{hash[:2]}/{hash}`. Duplicate content is
automatically deduplicated -- before writing, it checks `is_file` and skips
blobs that already exist. Blobs can be stored raw (directly accessible by URI)
or packed (gzip-compressed, only accessible via `load_blob`). This is
structurally similar to Artisan's content-addressed artifact storage.

**TaskDataStore** provides the task-level API. It stores three things: artifacts
(via ContentAddressedStore), metadata (JSON files in a hierarchical path
`flow/run/step/task/`), and logs. Artifacts are pickled, keyed by name, and
stored in the content-addressed store. Metadata is name-indexed (not content-
indexed). The done marker pattern (`DONE.lock`) signals task completion.

**How `self.artifact_name` works**: When user code assigns to `self.x` in a
step, Metaflow intercepts the attribute assignment, pickles the value, and
persists it through `TaskDataStore.save_artifacts`. Subsequent steps access
`self.x` and the value is transparently loaded from the datastore. This works
identically whether the task ran locally or in the cloud -- the datastore
abstraction handles the difference.

**Data movement between steps**: When a local step produces data that a remote
step consumes (or vice versa), Metaflow handles serialization/deserialization
through the datastore. There's no direct data passing between steps -- everything
goes through the persistent store. This is the same pattern as Artisan's staging
and commit model, but Metaflow uses pickle rather than Parquet/Delta Lake.

---

## Code Packaging

When a step executes remotely, Metaflow must ship user code to the worker. The
mechanism works as follows:

The orchestrator **snapshots all source code** into a tarball (the "code
package"), uploads it to the datastore (e.g., S3), and records the package URL
and SHA hash. The remote container downloads and unpacks this tarball before
executing the step.

Metaflow re-invokes the **same flow file with different CLI arguments** to
isolate individual step execution. It uses Click-based command routing so that
`python flow.py step train --task-id 42` executes just the `train` step for
task 42. The container entrypoint is essentially the same Python script, just
with different arguments.

For **dependency management**, Metaflow supports `@pypi`, `@conda`, and
recently `uv`. When these decorators are present, Metaflow resolves
dependencies locally, caches the resolved packages in the datastore, and
rehydrates them in the remote container using `mamba`. This avoids parallel
`pip install` storms when launching many containers. The resolved environment
is cross-platform-aware: a macOS client can resolve Linux-targeted packages for
remote containers.

The `FEAT_ALWAYS_UPLOAD_CODE_PACKAGE` config flag controls whether code is
uploaded on every run or only when changed.

---

## Resource Configuration

Resources are configured at three levels:

**Step-level decorators**: `@resources(cpu=4, memory=8192, gpu=1)` declares
requirements. `@batch(cpu=8, queue="gpu-queue")` or `@kubernetes(cpu=8,
node_selector="gpu=true")` adds platform specifics.

**Global defaults via environment variables**: `METAFLOW_BATCH_JOB_QUEUE`,
`METAFLOW_KUBERNETES_NAMESPACE`, `METAFLOW_BATCH_CONTAINER_IMAGE`, etc. These
are the fallbacks when decorators don't specify values.

**Maximum-wins merging**: When `@resources` and a backend decorator both specify
a value, Metaflow takes the maximum. This means `@resources(memory=4096)` +
`@batch(memory=8192)` yields 8192 MB. The logic lives in a shared
`compute_resource_attributes` utility.

`@resources` has **no effect on local execution**. This is by design -- local
runs use whatever the machine has. Resources only matter when a compute backend
is active.

---

## Parallel Execution

Metaflow provides two parallelism patterns:

**`foreach` (embarrassingly parallel)**: A step can set `self.next(step,
foreach='items')` where `items` is a list on `self`. Metaflow spawns one task
per element. On cloud backends, each task becomes a separate container (Batch
job or Kubernetes pod). The `--max-workers` flag (default 16) limits concurrent
tasks. The `--max-num-splits` flag (default 100) guards against accidentally
spawning too many tasks. These limits are adjustable. The fan-in step (join)
receives all results and can aggregate them. The number of parallel branches is
determined at runtime, not design time, enabling data-driven parallelism.

**`@parallel` (gang-scheduled distributed compute)**: For workloads requiring
inter-task communication (e.g., distributed training), `@parallel` launches
multiple nodes that execute simultaneously and can communicate. It designates
one node as the control node and communicates its address to workers via
environment variables. This integrates with framework-specific decorators like
`@torchrun` and `@deepspeed`. Under the hood on AWS Batch, it uses multi-node
jobs; on Kubernetes, gang scheduling.

The `foreach` pattern maps closely to Artisan's batched dispatch model. The
`@parallel` pattern has no Artisan equivalent (and likely isn't needed for
bioinformatics pipelines, which are embarrassingly parallel).

---

## Resume & Retry

**`@retry(times=N)`**: Retries a failed task up to N times (default 3). Waits 2
minutes between retries for remote tasks. Designed for transient failures
(network issues, spot instance preemption). Each retry is a new attempt with an
incremented attempt ID, and the TaskDataStore tracks attempts via
`{attempt_id}.attempt.json` markers.

**`@catch`**: Used with `@retry`, executes a no-op task after all retries are
exhausted, allowing the flow to continue past a failed step. The catch stores
the exception so downstream steps can inspect it.

**`--resume`**: Re-runs a flow from the point of failure. Successfully completed
steps are **cloned** (results reused from the datastore) rather than re-
executed. This is keyed on flow name and step name -- if you change the code of
a previously-successful step, its old results are still reused (by design, to
enable iterating on the failing step). Resume creates a new run ID but links
back to the original run for cloned steps.

The resume mechanism is task-granularity, not artifact-granularity. This is a
key difference from Artisan's content-addressed caching, which is finer-grained
(individual artifact deduplication). Metaflow's resume is more like "skip
successfully completed steps" than "reuse matching artifacts."

---

## Hybrid Execution

The local-to-cloud transition in Metaflow is designed to be a **one-flag
change**:

```python
# Local execution
python flow.py run

# Cloud execution (command-line override)
python flow.py run --with kubernetes

# Cloud execution (decorator on step)
@kubernetes(cpu=4, memory=8192)
def train(self): ...
```

**Mixed local/remote steps** are supported. Steps without a compute decorator
run locally; steps with `@batch` or `@kubernetes` run remotely. Metaflow handles
data movement between environments automatically through the datastore. This
enables a pattern where cheap orchestration steps run locally while expensive
compute steps run in the cloud.

The `--with` CLI flag applies the specified backend to **all steps** in the
flow. Per-step decorators provide finer control. Both can coexist -- a step with
`@batch` ignores a global `--with kubernetes` flag.

The transition is not fully zero-friction in practice. You need:
- Cloud infrastructure configured (IAM roles, job queues or K8s namespaces)
- A shared datastore (S3/GCS/Azure) accessible from both local and cloud
- Docker images (optionally custom) available to the compute backend
- The Metaflow metadata service running (for production deployments)

But the user code itself requires **zero changes**.

---

## Learnings for Artisan

**Decorator-based backend selection is the right pattern.** Metaflow's
separation of `@resources` (what you need) from `@batch`/`@kubernetes` (where to
run it) is clean. Artisan's `BackendBase` ABC approach is analogous but
configured at the pipeline level, not per-step. Artisan could consider per-step
backend overrides for mixed local/cloud workflows.

**Storage abstraction is the critical layer.** Metaflow's `DataStoreStorage`
(path_join, save_bytes, load_bytes, list_content, is_file) is the foundation
that makes everything else work. Artisan's POSIX-path-coupled storage is the
single biggest blocker for cloud execution. The fix is a similar abstract
storage interface with local-filesystem and object-store implementations. The
Metaflow model demonstrates that a simple file/directory abstraction (not a
database abstraction) is sufficient.

**Content-addressed storage translates well.** Metaflow uses SHA-1 hashed blobs
with deduplication checks before writes, structurally identical to Artisan's
xxh3_128-based approach. This validates Artisan's design. The key difference is
Metaflow pickles Python objects while Artisan uses Parquet -- Artisan's approach
is better for scientific data (queryable, typed, interoperable).

**Code packaging via tarball + CLI re-invocation is simple and effective.**
Metaflow packages source into a tarball, uploads to the datastore, and
re-invokes the same script with different arguments on the worker. Artisan could
adopt this pattern: package the user's operation modules, upload to cloud
storage, and have cloud workers import and execute them. The
`FEAT_ALWAYS_UPLOAD_CODE_PACKAGE` flag shows this needs a caching strategy.

**Maximum-wins resource merging is pragmatic.** When multiple decorators specify
resources, taking the maximum avoids under-provisioning. Artisan's
`ExecutionConfig` could adopt this when merging per-operation defaults with
per-pipeline overrides.

**Resume at task granularity vs. artifact granularity.** Metaflow's resume
replays from the last failed step, reusing entire step results. Artisan's
content-addressed caching is more granular (individual artifacts). This is an
advantage for Artisan -- scientific pipelines benefit from artifact-level
deduplication rather than step-level replay.

**`--max-workers` and `--max-num-splits` are essential safety rails.** Artisan's
two-level batching (`artifacts_per_unit` x `units_per_worker`) provides
similar control but should add explicit caps to prevent accidental resource
exhaustion when moving to cloud backends where costs scale linearly.

**The metadata service is a separate concern.** Metaflow runs a metadata service
for production deployments that tracks flows, runs, steps, tasks, and artifacts.
Artisan's Delta Lake store already serves this role, but cloud execution may
need a lightweight coordination service for run tracking and locking.
