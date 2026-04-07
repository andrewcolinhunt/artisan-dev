# Dagster Orchestration Model: Research for Artisan

Research into how Dagster abstracts compute backends, storage, configuration,
containerized execution, caching, dynamic fan-out, and error handling. Focused on
extracting patterns relevant to Artisan's planned cloud backend work.

---

## Executor Model

Every Dagster job run is governed by an **executor** that controls how ops/assets
within that run are scheduled onto compute. The executor is selected at the job
level and is responsible for orchestrating step execution after the run worker
process has been allocated by the run launcher.

**Built-in executors:**

| Executor | Substrate | Mechanism |
|----------|-----------|-----------|
| `in_process_executor` | Same process | Sequential, for debugging |
| `multiprocess_executor` | Local subprocesses | Default for production; one process per step |
| `celery_executor` | Celery workers | Steps submitted as Celery tasks; horizontal scaling |
| `celery_docker_executor` | Docker containers via Celery | Each step launches a Docker container |
| `celery_k8s_job_executor` | Kubernetes pods via Celery | Each step spawns an ephemeral K8s Job; Celery handles queuing/priority |
| `k8s_job_executor` | Kubernetes pods directly | Each step runs in its own pod without Celery overhead |
| `dask_executor` | Dask cluster | Steps submitted as Dask tasks |

**Key architectural split:** Dagster separates the **run launcher** (which
allocates the run worker process) from the **executor** (which dispatches
individual steps within that run). This two-level dispatch means the same job
definition works across environments -- you change the executor config, not the
ops.

The executor interface is pluggable. Custom executors can target arbitrary
substrates. The executor receives the compiled execution plan (a DAG of steps
with resolved dependencies) and is responsible for executing steps in dependency
order, respecting concurrency limits and retry policies.

**Relevance to Artisan:** Artisan's `BackendBase` conflates run-level and
step-level dispatch. Dagster's two-level split (run launcher + executor) is a
cleaner abstraction. The executor only needs to know how to run a single step
and report its status -- the framework handles dependency resolution, plan
compilation, and result collection.

---

## I/O Manager System

I/O managers are Dagster's abstraction for **decoupling ops/assets from storage
backends**. They separate "what to compute" from "where to read/write."

**Interface:** A custom I/O manager implements two methods:

- `handle_output(context, obj)` -- serialize and persist the output of an op
- `load_input(context, upstream_output)` -- deserialize and return a
  previously persisted output for use as input to a downstream op

The `context` objects carry metadata: asset key, partition, run ID, step key,
type annotations, and resource configuration.

**Built-in I/O managers:**

| Manager | Storage | Serialization |
|---------|---------|---------------|
| `FilesystemIOManager` | Local disk | Pickle |
| `InMemoryIOManager` | Process memory | None (pass-through) |
| `S3PickleIOManager` | AWS S3 | Pickle |
| `GCSPickleIOManager` | Google Cloud Storage | Pickle |
| `SnowflakePandasIOManager` | Snowflake | DataFrame |
| `BigQueryPandasIOManager` | BigQuery | DataFrame |
| `DuckDBPandasIOManager` | DuckDB | DataFrame |

**How assets select their I/O manager:** Assets declare an `io_manager_key`
string (e.g., `@asset(io_manager_key="snowflake_io_manager")`). This key is
resolved at runtime against the resource bindings in `Definitions`. If omitted,
the default I/O manager is used.

**Data flow:** When op A produces output consumed by op B, the executor calls
A's I/O manager's `handle_output` to persist, then B's I/O manager's
`load_input` to hydrate. The ops never directly touch storage -- they return
Python objects and receive Python objects.

**Relevance to Artisan:** This is directly applicable. Artisan's storage is
currently POSIX-path-coupled. An I/O manager-style abstraction (with
`stage_output` / `load_input` methods on a swappable backend) would decouple
operations from storage. The key insight is that the storage key is declared on
the operation/asset, not baked into the operation's code. Artisan's existing
`StagingManager` already handles staging and commit; refactoring it behind an
abstract interface that can target S3/GCS/Delta Lake would follow this pattern.

---

## Resource System

Resources provide **dependency-injected access to external services** -- database
connections, API clients, cloud credentials, etc. They are the mechanism for
environment-specific configuration.

**Definition:** Resources subclass `ConfigurableResource` (a Pydantic-based
class). Configuration fields are typed attributes:

```python
class MyDB(ConfigurableResource):
    host: str
    port: int = 5432
    password: str  # resolved from EnvVar at runtime
```

**Injection:** Ops and assets declare resource dependencies via type-annotated
parameters. The framework injects the configured instance at runtime:

```python
@asset
def my_asset(db: MyDB) -> pd.DataFrame:
    return db.query("SELECT ...")
```

**Environment swapping:** The recommended pattern creates a dictionary of
resource sets keyed by environment name, then selects based on an environment
variable:

```python
resources = {
    "local": {"io_manager": FilesystemIOManager(), "db": MyDB(host="localhost")},
    "production": {"io_manager": S3PickleIOManager(...), "db": MyDB(host="prod-db")},
}
deployment = os.getenv("DAGSTER_DEPLOYMENT", "local")
defs = Definitions(assets=[...], resources=resources[deployment])
```

Configuration values support `EnvVar("SECRET_NAME")` for runtime resolution,
keeping secrets out of code.

**Relevance to Artisan:** Artisan doesn't have a resource injection system.
Operations access external resources ad-hoc or via constructor parameters. A
lightweight resource system -- even just a typed config bag resolved at pipeline
build time -- would enable clean environment swapping. The `Definitions`-level
binding pattern (mapping string keys to resource instances) is simple and
powerful.

---

## Container & Code Delivery

Dagster supports containerized step execution at multiple levels:

**Docker executor:** `celery_docker_executor` launches each step as a Docker
container. Container configuration (image, volumes, networks) is set at the
executor level and can be overridden per op.

**Kubernetes executors:** Both `k8s_job_executor` and `celery_k8s_job_executor`
run each step in an ephemeral Kubernetes pod. The key difference is that the
Celery variant adds a queuing/priority layer.

**Per-op container configuration:** Ops tag themselves with
`dagster-k8s/config` to specify per-step Kubernetes resource requests, container
images, volumes, and pod metadata:

```python
@op(tags={
    "dagster-k8s/config": {
        "container_config": {
            "image": "my-special-image:latest",
            "resources": {"requests": {"cpu": "2", "memory": "4Gi"}},
        }
    }
})
def heavy_compute(): ...
```

Tags merge recursively with executor-level defaults: step-level tags override
broader deployment and job-level settings through a precedence hierarchy. You can
set `"merge_behavior": "SHALLOW"` to replace rather than combine values.

**Dagster Pipes:** For launching arbitrary external processes (containers, Spark
jobs, Kubernetes jobs), Dagster Pipes provides a protocol for the external
process to report events, metadata, and outputs back to the Dagster orchestrator.

**Relevance to Artisan:** Artisan's SLURM backend already tags operations with
resource requirements (CPUs, memory, partition). The same pattern extends to
cloud: operations declare resource requirements via tags/config, and the backend
translates those to container specs. The per-op tag override pattern (with merge
semantics) is a well-tested approach.

---

## Caching & Memoization

Dagster's memoization system is **version-based**, not content-addressed:

**Code versions:** Each asset can declare a `code_version` string. If not set,
Dagster defaults to the run ID (meaning every run is unique).

**Data versions:** Dagster automatically computes data versions by hashing the
code version with input data versions. Users can also supply custom data
versions via `Output(data_version=DataVersion("..."))` for cases involving
randomness or cosmetic refactors.

**Skip logic:** Before materializing an asset, Dagster checks: is the code
version unchanged AND are all upstream data versions unchanged? If so, skip
materialization and return the cached value. This is "last-value" caching -- only
the most recent materialization is cached, not a full content-addressed store.

**MemoizableIOManager:** The `FilesystemIOManager` subclasses
`MemoizableIOManager`, which adds a `has_output` method. This method checks
whether a previously stored output exists and is still valid given current
versions. Custom I/O managers can implement this interface to participate in
memoization.

**Observable source assets:** For external data, `@observable_source_asset`
defines a function that computes a data version by examining the external source
(e.g., hashing file contents). When the data version changes, downstream assets
are marked as out-of-date.

**Relevance to Artisan:** Artisan uses content-addressed hashing (hash of inputs
+ config + code) for caching, which is stronger than Dagster's version-based
approach. Artisan's model already handles the "full store" case. The interesting
takeaway is Dagster's `observable_source_asset` pattern for external data --
Artisan could use a similar mechanism for ingest operations that monitor external
data sources.

---

## Dynamic Execution

Dynamic fan-out uses `DynamicOutput` to produce a variable number of outputs at
runtime, each with a unique `mapping_key`:

```python
@op(out=DynamicOut())
def generate_chunks(data):
    for i, chunk in enumerate(split(data)):
        yield DynamicOutput(chunk, mapping_key=str(i))
```

**Map:** `chunks.map(process_chunk)` clones the downstream op for each dynamic
output, creating parallel branches. Each clone is identified by its mapping key.

**Collect:** `results.collect()` gathers all dynamic branches into a single
list, performing a fan-in.

**Chaining:** Multiple `.map()` calls can be chained. Non-dynamic arguments can
be passed via lambda closures alongside dynamic ones.

**Executor interaction:** Dynamic outputs are resolved at runtime, so the
executor must handle the dynamic expansion of the execution plan. All built-in
executors support dynamic graphs. The multiprocess and Kubernetes executors run
each mapped instance as a separate step (and therefore a separate process or
pod).

**Async executor (2025):** A community `dagster-async-executor` was built for
I/O-bound fan-outs (e.g., hundreds of concurrent API calls) using async/await
within a single worker process, without requiring separate containers per branch.

**Relevance to Artisan:** Artisan already handles batched execution via
`ExecutionUnit` arrays and job arrays on SLURM. The `map`/`collect` pattern is
analogous to Artisan's batching, but expressed at the op level rather than the
orchestrator level. The key difference: in Dagster, the op decides how to fan
out; in Artisan, the orchestrator decides batching. Dagster's approach gives ops
more control over parallelism granularity.

---

## Error Handling

Dagster provides two levels of retry and two mechanisms at each level:

**Op-level retries (within a run):**

- `RetryPolicy(max_retries=3, delay=0.2, backoff=Backoff.EXPONENTIAL, jitter=Jitter.PLUS_MINUS)` -- declarative policy attached to an op or job
- `RetryRequested(max_retries=1, seconds_to_wait=1)` -- manual exception for conditional retry logic
- Retry policies can be set per-op, per-op-invocation (`.with_retry_policy()`), or as a job-wide default (`op_retry_policy`)

**Run-level retries (new run on failure):**

- Configured globally; when a run fails, a new run is automatically kicked off
- Default strategy is `FROM_FAILURE`: successful ops are skipped, their outputs reused for downstream ops
- Alternative strategy `ALL_STEPS`: re-executes everything
- `FROM_FAILURE` requires an I/O manager that can access outputs from prior runs (cloud storage works; local filesystem in containers does not)

**Interaction between levels:** When both op and run retries are active, op retry
counts reset per retried run. The `retry_on_asset_or_op_failure` setting can
disable run-level retries for step failures (keeping them only for process
crashes).

**Partial failure:** Failed ops mark downstream ops as skipped. The execution
plan records which steps succeeded, failed, or were skipped. On `FROM_FAILURE`
retry, only the failed step and its downstream dependents are re-executed.

**Relevance to Artisan:** Artisan currently handles retries at the SLURM job
level. The two-level retry model (step-level + run-level) is worth adopting. The
`FROM_FAILURE` strategy is particularly relevant -- it requires that the storage
backend can serve outputs from partial runs, which Artisan's Delta Lake store
already supports (committed artifacts persist across runs). Adding a declarative
`RetryPolicy` to `OperationDefinition` would be straightforward.

---

## Learnings for Artisan

**Split run dispatch from step dispatch.** Dagster's run launcher + executor
split cleanly separates "where does the run worker live" from "how are steps
distributed." Artisan's `BackendBase` should evolve toward this: a run-level
launcher (local process, SLURM allocation, cloud VM) and a step-level executor
(process pool, SLURM job array, K8s pods). This makes it natural to add cloud
backends without rewriting the orchestrator.

**Introduce a storage abstraction layer.** Dagster's I/O manager pattern --
`handle_output` / `load_input` behind a string key resolved at `Definitions`
time -- is the proven way to decouple operations from storage. Artisan's
`StagingManager` and artifact store should sit behind an abstract interface that
can target local Parquet, S3-backed Delta Lake, or cloud object storage. The key
is that operations never see file paths -- they produce and consume typed Python
objects.

**Add environment-aware resource binding.** A simple resource dictionary keyed by
environment name (local/slurm/cloud), resolved at pipeline build time via an
environment variable, would let the same pipeline definition run across
environments with zero code changes. This is more structured than ad-hoc
configuration.

**Use tags for compute resource declarations.** Per-operation tags
(CPU, memory, GPU, container image) that the executor translates into
backend-specific resource requests is a battle-tested pattern. Artisan already
does this for SLURM; formalizing it as a first-class tag system makes cloud
extension natural.

**Consider version-based staleness detection for external data.** Dagster's
`observable_source_asset` pattern -- a function that computes a data version
for external data -- could complement Artisan's content-addressed caching for
ingest operations that monitor external file systems.

**Adopt two-level retry with FROM_FAILURE.** Artisan's Delta Lake store already
persists partial results, making `FROM_FAILURE` retries feasible. Adding a
declarative `RetryPolicy` to operations (with exponential backoff and jitter)
and a run-level retry strategy would improve resilience, especially for
long-running cloud pipelines where transient failures are common.

**Dynamic fan-out at the op level.** Dagster lets ops declare their own fan-out
via `DynamicOutput`, giving operations control over parallelism granularity.
This is worth considering as a complement to Artisan's orchestrator-driven
batching -- some operations know better than the orchestrator how to partition
their work.

---

## Sources

- [I/O managers](https://docs.dagster.io/guides/build/io-managers)
- [External resources](https://docs.dagster.io/guides/build/external-resources)
- [Defining resources](https://docs.dagster.io/guides/build/external-resources/defining-resources)
- [Dev to prod transition](https://docs.dagster.io/guides/operate/dev-to-prod)
- [Asset versioning and caching](https://docs.dagster.io/guides/build/assets/asset-versioning-and-caching)
- [Dynamic graphs](https://docs.dagster.io/concepts/ops-jobs-graphs/dynamic-graphs)
- [Op retries](https://docs.dagster.io/guides/build/ops/op-retries)
- [Run retries](https://docs.dagster.io/deployment/execution/run-retries)
- [Executing on Celery](https://docs.dagster.io/deployment/execution/celery)
- [Celery + Kubernetes](https://docs.dagster.io/deployment/oss/deployment-options/kubernetes/kubernetes-and-celery)
- [Kubernetes customization](https://docs.dagster.io/deployment/oss/deployment-options/kubernetes/customizing-your-deployment)
- [Dagster Docker integration](https://dagster.io/integrations/dagster-docker)
- [Software-defined assets](https://dagster.io/blog/software-defined-assets)
- [Async execution](https://dagster.io/blog/when-sync-isnt-enough)
