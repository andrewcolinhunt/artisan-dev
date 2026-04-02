# Flyte Orchestration Analysis

How Flyte (by Union.ai) handles orchestration across different cloud compute
resources, with specific learnings for Artisan's planned cloud backend
abstraction.

---

## Task Plugin Architecture

Flyte abstracts compute backends through a layered plugin system. A backend
extension has three components:

- **Interface specification**: A Protobuf (or OpenAPI) schema defining the
  `custom` field on a `TaskTemplate`. This is an unstructured JSON blob that
  carries backend-specific configuration (e.g., Spark driver/executor counts,
  Ray cluster topology). The task template is the serialized, language-agnostic
  representation of a task.

- **Flytekit SDK plugin**: A Python class extending `PythonTask` or
  `PythonFunctionTask`. During serialization, its `get_custom()` method
  populates the custom field with the Protobuf structure. Users interact with
  this layer via decorators and config objects.

- **FlytePropeller backend plugin**: The server-side component that actually
  executes tasks. FlytePropeller has a `PluginRegistry` that maps task type
  strings to plugin handlers. When Propeller encounters a task of type "spark",
  the registry dispatches to the Spark plugin, which creates SparkApplication
  CRDs on Kubernetes.

Three plugin handler categories exist: **Kubernetes operator plugins** (Spark,
PyTorch, TensorFlow, Ray, Dask, MPI), **Web API plugins** (async or sync
external service calls), and **Core plugins** (everything else).

### The Agent/Connector Evolution

Flyte's original plugin system required writing Go code for FlytePropeller,
which limited contributions from the Python-centric ML community. The **Flyte
Agents framework** (now GA) replaces this with stateless gRPC services:

- **Async agents** implement `create`, `get`, `delete` for long-running jobs
  (Snowflake queries, SageMaker training, Databricks jobs).
- **Sync agents** implement a single `do` method for request/response services
  (API calls, inference endpoints).

Agents are deployed as separate Kubernetes pods and communicate with
FlytePropeller via gRPC. Because they use a Protobuf interface, agents can be
implemented in any language, tested locally (no cluster needed), and updated
independently of the core system.

### Full Integration Catalog

Native backend plugins: Spark, Ray, Dask, Kubeflow PyTorch, Kubeflow
TensorFlow, MPI/Horovod. External service plugins: AWS Batch, AWS Athena, Hive,
Flyte Interactive. Agent-based connectors: SageMaker, BigQuery, Databricks,
Snowflake, Airflow, Slurm, ChatGPT, OpenAI Batch, Perian, Memory Machine Cloud,
Sensor. Flytekit-only plugins: MLflow, Weights & Biases, Pandera, DuckDB, ONNX,
Papermill, and others.

---

## Data Handling & Type System

Flyte enforces a strongly-typed interface system where every task declares input
and output types, validated at compile time and runtime.

### Metadata vs Raw Data Separation

Flyte separates **metadata** (stored in a relational database as the control
plane) from **raw data** (stored in an object store: S3, GCS, or Azure Blob).
Task outputs are serialized and uploaded to the configured blob store. Only
references (URIs) are passed through the control plane. This means:

- The orchestrator never handles large data directly.
- Tasks on different backends exchange data via the shared object store.
- The raw data prefix is configurable per execution, launch plan, or globally.

### Core Types

- **FlyteFile**: A reference to a single file in object storage. Tasks receive a
  local path after automatic download; outputs are automatically uploaded. The
  fsspec library provides streaming access without full materialization.
- **FlyteDirectory**: Same pattern for directories.
- **StructuredDataset**: An abstract DataFrame type that bridges Pandas, Spark,
  Polars, etc. Flytekit auto-converts between DataFrame types using a
  codec/encoder registry. Stored as Parquet in the blob store. Supports
  column-level type checking via Pandera integration.
- **Dataclasses**: Native Python dataclasses with nested Flyte types
  (FlyteFile, StructuredDataset, etc.) are fully supported.

### Automatic Offloading

When a task returns a `pandas.DataFrame`, Flytekit detects the return type
annotation, serializes it to Parquet, uploads to the blob store, and replaces it
with a StructuredDataset reference. The downstream task's input annotation
triggers automatic download and deserialization. This is completely transparent
to user code.

---

## Container & Image Management

Flyte runs tasks in containers. By default, all tasks in a workflow share a
single container image, but **ImageSpec** allows per-task customization:

```python
custom_image = ImageSpec(
    name="my-ml-image",
    packages=["torch==2.0", "transformers"],
    cuda="12.1",
    python_version="3.11",
    registry="ghcr.io/myorg",
)

@task(container_image=custom_image)
def train_model(data: StructuredDataset) -> FlyteFile:
    ...
```

ImageSpec builds images without Dockerfiles. Before building, Flytekit checks
the registry for an existing image with the same content hash, only rebuilding
when dependencies change. Features include CUDA support, custom pip index URLs,
APT package installation, and pip/conda package caching across builds.

For tasks that need arbitrary containers (non-Python, legacy tools), **raw
containers** allow specifying any Docker image with explicit input/output
mappings.

---

## Resource Configuration

Resources are declared per task via the `@task` decorator:

```python
@task(
    requests=Resources(cpu="2", mem="8Gi"),
    limits=Resources(cpu="4", mem="16Gi", gpu="1"),
    accelerator=GPUAccelerator("nvidia-tesla-a100"),
)
def gpu_task(data: FlyteFile) -> FlyteFile:
    ...
```

- **requests/limits**: Follow Kubernetes semantics. CPU in cores or millicores,
  memory in standard units, GPU as integer counts.
- **Accelerator**: Named GPU types (`A100`, `T4`, `V100`) via the
  `flytekit.extras.accelerators` module. Custom accelerator strings are also
  supported for non-standard hardware.
- **Portability**: Resource declarations are Kubernetes-native. On AWS Batch,
  the plugin translates resources to Batch job definitions. The user-facing API
  is the same regardless of backend.

Resources can also be overridden at call sites using `.with_overrides()`,
enabling the same task definition to run with different resource profiles in
different workflow contexts.

---

## Caching

Flyte caching is opt-in per task via `cache=True` and `cache_version`:

```python
@task(cache=True, cache_version="1.0")
def expensive_compute(data: StructuredDataset) -> StructuredDataset:
    ...
```

### Cache Key Composition

The cache key is derived from: (1) the task's fully-qualified name, (2) the
`cache_version` string, (3) a hash of the input/output type interface, and (4)
the literal representation of all input values. For primitive types, the literal
value is hashed directly. For file-like types (FlyteFile, StructuredDataset),
users can supply a custom `HashMethod` that hashes file contents rather than
URIs.

### Data Catalog

Cache entries are stored in a **Data Catalog** service separate from the
metadata store. When a cached task is invoked, FlytePropeller queries the
catalog before scheduling execution. Cache hits skip execution entirely and
return the stored output references.

### Cross-Environment Behavior

Caching works across executions within the same Flyte deployment. The
`cache_serialize=True` option prevents concurrent executions of the same cache
key (useful for expensive tasks that might be triggered simultaneously).

---

## Map Tasks & Batching

`map_task` applies a single task function across a list of inputs, creating one
execution node that fans out internally:

```python
@task
def process_item(item: FlyteFile) -> FlyteFile:
    ...

@workflow
def pipeline(items: list[FlyteFile]) -> list[FlyteFile]:
    return map_task(process_item, concurrency=10, min_success_ratio=0.9)(items=items)
```

### Execution Backends

Two array plugins ship with Flyte: **k8s-array** (default, launches a pod per
item) and **aws-array** (maps to AWS Batch array jobs). The plugin choice is
transparent to the user.

### Configuration

- **concurrency**: Maximum parallel executions. If inputs exceed this, batches
  run serially. Unspecified means unbounded parallelism.
- **min_success_ratio**: Fraction of items that must succeed for the map task to
  be marked successful (e.g., 0.9 = tolerate 10% failures). This enables
  partial-failure tolerance at the batch level.

### Resource Inheritance

Map task items inherit the resources of the underlying task definition. Each
item runs in its own pod (k8s-array) or array job slot (aws-array).

---

## Error Handling

### Error Classification

Flyte distinguishes two error categories:

- **SYSTEM errors**: Infrastructure failures (node crashes, OOM kills, network
  issues). Always treated as recoverable. Retried against a system retry budget
  configured at the platform level (`MaxWorkflowRetries`).
- **USER errors**: Exceptions in user code. Non-recoverable by default. To make
  a user error retryable, raise `FlyteRecoverableException`. To explicitly
  prevent retries, raise `FlyteNonRecoverableException`.

### Retry Configuration

```python
@task(retries=3, timeout=timedelta(minutes=30))
def flaky_task(x: int) -> int:
    ...
```

`retries=3` means 4 total attempts (1 initial + 3 retries). Maximum is 10.
Timeout applies independently to each attempt. Advanced timeout configuration
separates `max_runtime` from `max_queued_time`.

### Interruptible Tasks (Spot Instances)

Tasks marked `interruptible=True` can run on preemptible/spot instances. Spot
preemptions count against the system retry budget, not user retries. On the
final system retry, Flyte automatically schedules the task on a non-preemptible
instance, guaranteeing eventual completion.

### Transient Failure Recovery

System-level transient failures (FlytePropeller restarts, etcd timeouts) are
handled transparently. Propeller retries the atomic operation rather than
restarting the entire task.

---

## Multi-Backend Workflows

A single Flyte workflow can mix task types freely. Each task declares its own
backend via its type and configuration:

```python
@task(task_config=Spark(spark_conf={...}), limits=Resources(mem="32Gi"))
def spark_etl(raw: FlyteDirectory) -> StructuredDataset:
    ...

@task(limits=Resources(gpu="1"), accelerator=GPUAccelerator("nvidia-tesla-a100"))
def train(data: StructuredDataset) -> FlyteFile:
    ...

@task
def evaluate(model: FlyteFile, data: StructuredDataset) -> float:
    ...

@workflow
def pipeline(raw: FlyteDirectory) -> float:
    data = spark_etl(raw=raw)
    model = train(data=data)
    return evaluate(model=model, data=data)
```

FlytePropeller dispatches each task to the appropriate plugin handler. Data
flows between tasks via the shared object store, so a Spark task's output
(uploaded as Parquet to S3) is seamlessly consumed by a GPU Kubernetes task
(downloaded from S3). The user sees only typed Python function calls.

This works because the data plane (object store) is decoupled from the compute
plane (Kubernetes pods, Spark clusters, AWS Batch jobs, external services).

---

## Learnings for Artisan

### Storage Abstraction is the Foundation

Flyte's ability to mix backends hinges entirely on its object-store-based data
layer. Tasks never share filesystems; they share references to objects in S3/GCS.
Artisan's POSIX-path-coupled storage is the single biggest blocker for cloud
backends. The priority should be introducing a storage abstraction layer (likely
`fsspec`-based, matching Flyte's choice) that allows artifact content to live in
object stores while maintaining Delta Lake metadata locally or remotely.

### Declarative Resource Configuration Transfers Well

Flyte's `Resources` + `Accelerator` pattern maps cleanly onto Artisan's
`WorkerTraits` / execution config. Artisan could adopt a similar declarative
model where operations declare their resource needs, and the backend translates
to infrastructure-specific settings (Kubernetes requests, SLURM `--gres`, AWS
Batch job definitions). This avoids backend-specific configuration leaking into
operation definitions.

### The Agent/Connector Pattern for External Services

Flyte's evolution from compiled-in Go plugins to gRPC agents is instructive.
Artisan's Prefect-coupled dispatch could evolve toward a similar connector
pattern: stateless services that implement a standard interface (submit, poll,
cleanup). This would allow backend plugins to be developed and tested
independently.

### Map Tasks Validate Two-Level Batching

Flyte's `map_task` with `concurrency` and `min_success_ratio` validates
Artisan's two-level batching design. The key difference is that Flyte's map
tasks have no concept of "artifacts per unit" --- they're 1:1 mapping. Artisan's
`artifacts_per_unit` x `units_per_worker` gives finer control over granularity,
which is genuinely useful for HPC workloads where pod startup cost is high.

### Caching by Content Hash is Proven

Flyte's cache key = (task name + version + interface hash + input literals)
closely mirrors Artisan's content-addressed hashing. The `cache_version` field
is worth noting: it provides an explicit escape hatch when task logic changes
but inputs don't. Artisan's hash-based caching already handles this implicitly
(code changes produce different hashes), which is arguably more robust.

### Per-Task Container Images are Essential for Cloud

Flyte's ImageSpec solves a real problem: different tasks need different
dependencies. Artisan currently assumes worker code is importable in the local
environment. For cloud backends, Artisan will need either an ImageSpec-like
system or a convention for mapping operations to pre-built images.

### Separate Metadata from Data Storage

Flyte's clean separation of metadata (control plane DB) from raw data (object
store) is directly applicable. Artisan already partially does this with Delta
Lake (Parquet files = raw data, Delta log = metadata), but the path coupling
means both must be colocated on a POSIX filesystem. Decoupling these so the
Delta log can reference objects in cloud storage would unlock remote execution.

---

## Sources

- [Backend Plugins Architecture](https://www.union.ai/docs/v1/flyte/architecture/extending-flyte/backend-plugins/)
- [Flyte Agents Framework](https://www.union.ai/blog-post/flyte-agents-framework)
- [Integrations Catalog](https://www.union.ai/docs/v1/flyte/integrations/)
- [Data Management Concepts](https://docs.flyte.org/en/latest/concepts/data_management.html)
- [StructuredDataset](https://docs.flyte.org/en/latest/user_guide/data_types_and_io/structureddataset.html)
- [FlyteFile](https://docs-legacy.flyte.org/en/v1.13.0/user_guide/data_types_and_io/flytefile.html)
- [FlyteDirectory](https://docs-legacy.flyte.org/en/latest/user_guide/data_types_and_io/flytedirectory.html)
- [ImageSpec](https://docs.flyte.org/en/latest/user_guide/customizing_dependencies/imagespec.html)
- [Customizing Task Resources](https://docs.flyte.org/en/latest/user_guide/productionizing/customizing_task_resources.html)
- [GPU Configuration](https://docs-legacy.flyte.org/en/latest/user_guide/productionizing/configuring_access_to_gpus.html)
- [Accelerators API](https://docs-legacy.flyte.org/en/v1.14.1/api/flytekit/extras.accelerators.html)
- [Caching](https://docs-legacy.flyte.org/en/latest/user_guide/development_lifecycle/caching.html)
- [Map Tasks](https://docs-legacy.flyte.org/en/v1.15.0/user_guide/advanced_composition/map_tasks.html)
- [Map Tasks Blog](https://flyte.org/blog/map-tasks-in-flyte)
- [Retries and Timeouts](https://www.union.ai/docs/v2/flyte/user-guide/task-configuration/retries-and-timeouts/)
- [Build Indestructible Pipelines](https://www.union.ai/blog-post/build-indestructible-pipelines-with-flyte)
- [Spark Plugin](https://www.union.ai/docs/v1/flyte/integrations/native-backend-plugins/k8s-spark-plugin/)
- [AWS Batch Plugin](https://www.union.ai/docs/v1/flyte/integrations/external-service-backend-plugins/aws-batch-plugin/)
- [Kubernetes Plugins](https://www.union.ai/docs/v1/flyte/deployment/flyte-plugins/kubernetes-plugins/)
- [Flyte on GitHub](https://github.com/flyteorg/flyte)
