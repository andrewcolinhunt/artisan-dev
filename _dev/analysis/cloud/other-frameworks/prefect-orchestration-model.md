# Prefect Orchestration Model: Cloud Compute Analysis

Research into how Prefect 3.x handles orchestration across different cloud
compute resources, and what this means for Artisan's use of Prefect.

---

## Task Runner Model

Task runners are pluggable execution strategies for tasks within a flow. The
`TaskRunner` base class defines two core methods: `submit()` (execute a single
task, return a `PrefectFuture`) and `map()` (execute multiple tasks, return a
`PrefectFutureList`). Task runners are context managers with `__enter__` /
`__exit__` lifecycle hooks for resource management.

**Built-in runners:**

| Runner | Mechanism | Use Case |
|--------|-----------|----------|
| `ThreadPoolTaskRunner` | Python `ThreadPoolExecutor` | I/O-bound concurrency (default) |
| `ProcessPoolTaskRunner` | Python `multiprocessing` | CPU-bound parallelism, GIL bypass |
| `PrefectTaskRunner` | API-backed scheduling to remote `TaskWorker` processes | Distributed task execution |

**Third-party runners (integration packages):**

| Runner | Package | Mechanism |
|--------|---------|-----------|
| `DaskTaskRunner` | `prefect-dask` | Dask distributed cluster |
| `RayTaskRunner` | `prefect-ray` | Ray cluster |
| `SlurmTaskRunner` | `prefect-submitit` | SLURM job arrays via submitit |

The task runner interface is clean but narrow. A runner receives a task callable
and parameters, submits it for execution, and returns a future. It does not
manage infrastructure provisioning, code deployment, or result storage -- those
are separate concerns.

**Key architectural point:** Task runners control *intra-flow* parallelism. They
determine how tasks within a single flow execution are distributed. They do not
control where the flow itself runs.

---

## Work Pool Model

Work pools are Prefect's abstraction for *inter-flow* infrastructure management.
They determine where entire flow runs execute, not individual tasks within a
flow. This is a fundamentally different scope than task runners.

**Three categories:**

| Type | Worker Required | How It Works |
|------|-----------------|--------------|
| **Hybrid** | Yes (user runs a worker process) | Worker polls work pool, provisions infrastructure, executes flows. Maximum control. |
| **Push** | No | Prefect Cloud submits directly to serverless infrastructure. No polling. |
| **Managed** | No | Prefect manages everything, including infrastructure. Cloud-only. |

**Supported work pool types (Prefect Cloud):**

Process, Docker, Kubernetes, AWS ECS, AWS ECS Push, Azure Container Instances,
Azure Container Instances Push, Google Cloud Run, Google Cloud Run V2, Google
Vertex AI, Google Cloud Run Push, Modal Push, Coiled, Prefect Managed.

**Self-hosted Prefect Server supports:** Process, Docker, Kubernetes, AWS ECS,
Azure Container Instances, Google Cloud Run, Google Cloud Run V2, Google Vertex
AI. Notably: no push pools, no Modal, no managed pools.

**Infrastructure blocks (deprecated):** The old model used typed infrastructure
blocks (e.g., `DockerContainer`, `KubernetesJob`) attached directly to
deployments. These were deprecated in March 2024 and removed in September 2024,
replaced entirely by work pools and workers.

---

## Result Storage

Prefect's result system is opt-in. By default, task/flow results are not
persisted -- they live in memory during the flow run and are discarded. Enabling
persistence unlocks caching and transaction features.

**Storage backends:** Local filesystem (default: `~/.prefect/storage/`), S3,
GCS, Azure Blob via integration storage blocks.

**Serializers:** `pickle` (default, base64-encoded), `json`, `compressed/pickle`,
`compressed/json`, plus custom `ResultSerializer` implementations.

**Configuration:** Per-task/flow decorator arguments (`result_storage=`,
`result_serializer=`), or global via `PREFECT_DEFAULT_RESULT_STORAGE_BLOCK` and
`PREFECT_RESULTS_PERSIST_BY_DEFAULT`.

**Cross-node results:** For distributed runners (Dask, Ray, PrefectTaskRunner),
result storage must be accessible from all execution environments. Prefect does
not handle data transfer -- it assumes the configured storage block (e.g., S3)
is reachable from everywhere. This is a "bring your own shared storage" model.

**Relevance to Artisan:** Artisan does not use Prefect's result storage at all.
Artisan manages its own artifact storage, staging, and commit cycle. Prefect
results pass only lightweight dicts (success/error/run_id) between orchestrator
and workers.

---

## Code Deployment

Prefect 3.x uses a build/push/pull deployment model defined in `prefect.yaml`
or programmatically via `Flow.from_source(...).deploy(...)`.

**Three deployment steps:**
- **Build:** Package code (e.g., build Docker image)
- **Push:** Upload to storage (e.g., push image to registry, upload to S3/GCS)
- **Pull:** At execution time, fetch code (e.g., `git_clone`, pull from cloud storage, `set_working_directory`)

**Key constraint:** Pull steps execute every time a deployment runs through a
worker. This means code must be retrievable from wherever the flow runs.

**Relevance to Artisan:** Artisan does not use Prefect deployments. Artisan
flows run in the same process (local) or on a shared filesystem (SLURM). For
cloud backends without shared filesystems, code packaging becomes a real
concern -- but this is a problem Prefect's deployment model solves at the
*flow* level, not the *task* level.

---

## Parallel Execution

**`task.map()`** submits one task run per element. Internally: resolve futures
in parameters, split iterable vs static (unmapped) parameters, validate equal
lengths, submit each element individually. All iterable parameters must have
the same length.

**Scaling characteristics by runner:**

| Runner | Map Scale | Bottleneck |
|--------|-----------|------------|
| `ThreadPoolTaskRunner` | Hundreds (GIL-bound) | Python GIL for CPU work |
| `ProcessPoolTaskRunner` | Tens to hundreds | OS process overhead, pickling |
| `DaskTaskRunner` | 50,000+ | Dask scheduler + API state updates |
| `RayTaskRunner` | Thousands+ | Ray cluster capacity |
| `PrefectTaskRunner` | Thousands+ | API throughput (horizontally scalable) |

**Concurrency control:** Global concurrency limits via Prefect server, tag-based
task run limits (enforced in 30-second windows, not strict concurrency).

**Cloud execution latency:** The dominant issue for cloud-based parallel
execution is container cold-start time. A GitHub discussion (PrefectHQ/prefect
#18976) documents this thoroughly: ECS Fargate cold-starts are especially slow
(image pull + dependency install + container scheduling). Recommendations include
baking self-contained images, using EC2-backed ECS with warm pools, and using
larger task granularity to amortize startup overhead.

---

## Error Handling

**Retries:** Configured per-task via `retries=` and `retry_delay_seconds=`
decorator arguments. Supports fixed delays, custom delay lists, and
`exponential_backoff()`. When a task fails, Prefect transitions it to a retry
state and re-executes.

**Timeouts:** `timeout_seconds=` on tasks. Raises `TaskRunTimeoutError` and
transitions to Failed/TimedOut. Composable with retries.

**Partial failures with `.map()`:** Each mapped task is independent. If 3 of
100 mapped tasks fail, the other 97 succeed normally. The flow can inspect
individual future states. There is no built-in "fail the whole batch if N
fail" mechanism -- that is application logic.

**Relevance to Artisan:** Artisan already handles partial failures in its own
`_collect_results` function, converting exceptions to failure dicts. Artisan
does not currently use Prefect's retry mechanism -- it wraps execution in
try/except and returns success/error dicts.

---

## Limitations and Gaps

**Task runners are local-scope only.** A task runner controls parallelism
within a single flow run on a single machine (or connected cluster for
Dask/Ray). It cannot provision cloud infrastructure. If you want tasks to run
on ECS or Cloud Run, you need work pools -- but work pools operate at the
*flow* level, not the *task* level.

**No task-level cloud dispatch.** Prefect has no mechanism to run individual
tasks on separate cloud infrastructure (e.g., "run this task on a GPU ECS
container, that task on a CPU container"). The `PrefectTaskRunner` distributes
to `TaskWorker` processes, but those workers are generic -- they do not
provision per-task infrastructure.

**Modal is Cloud-only.** The Modal push work pool requires Prefect Cloud. There
is no Modal worker for self-hosted Prefect Server (GitHub issue #20218, opened
January 2026, still open). This means OSS Prefect users cannot use Modal
through Prefect's native abstractions.

**Push pools require Prefect Cloud.** All push work pools (ECS Push, Cloud Run
Push, Azure ACI Push, Modal Push) are Prefect Cloud features. Self-hosted
Prefect Server only supports hybrid (worker-based) pools.

**Container startup latency.** For cloud work pools, each flow run incurs
container cold-start. This makes fine-grained task-per-container patterns
impractical. The recommended pattern is coarse-grained flows with intra-flow
parallelism via task runners.

**Two-level indirection for cloud parallelism.** To run parallel tasks on cloud
infrastructure, you need: (a) a work pool + worker to provision the container
for the flow, then (b) a task runner inside that container for task-level
parallelism. These are configured separately and interact in non-obvious ways.

**Thread context variable issues.** The `ThreadPoolTaskRunner` creates new
threads for tasks that modify `contextvars` (e.g., via `prefect.tags`), which
can hit OS thread limits under high-throughput `.map()` calls.

---

## Learnings for Artisan

**Artisan's current Prefect integration is shallow and well-positioned.** Artisan
uses Prefect only for its task runner `.map()` dispatch -- creating a flow,
mapping `execute_unit_task` over units, collecting results. It does not use
Prefect deployments, result storage, work pools, or the Prefect API server.
This is a thin integration surface.

**The task runner interface is the right abstraction level for Artisan.** What
Artisan needs is: "take N units, dispatch them to some compute backend, collect
results." That is exactly what `TaskRunner.map()` does. The interface is simple
(`submit`/`map` + futures), and third-party runners (Dask, Ray, submitit) plug
in cleanly.

**But the task runner model breaks down for serverless cloud compute.** Task
runners assume they control execution within a running process or connected
cluster. They do not provision infrastructure. Modal, Cloud Run, and ECS
require infrastructure provisioning per invocation -- which Prefect handles at
the *work pool* level, not the *task runner* level. There is no
`ModalTaskRunner` or `CloudRunTaskRunner` because the task runner abstraction
is the wrong level for that problem.

**The DispatchHandle abstraction makes sense.** Artisan should define its own
dispatch interface that maps to the same contract as Prefect task runners
(`dispatch(units) -> futures`) but is not coupled to Prefect. This allows:

- **Local/SLURM:** Continue using Prefect task runners underneath (they work
  well here). The DispatchHandle wraps the Prefect flow.
- **Modal/serverless:** Implement DispatchHandle directly using Modal's native
  API (`modal.Function.map()`), bypassing Prefect entirely. No Prefect task
  runner exists for Modal, and the work pool model is Cloud-only and
  flow-scoped (wrong granularity).
- **Dask/Ray clusters:** Either wrap the Prefect Dask/Ray task runners, or call
  the Dask/Ray APIs directly. Both are viable.

**Do not adopt Prefect work pools.** Work pools solve a different problem
(deploying and scheduling entire flow runs across infrastructure). Artisan's
pipeline steps are not independent deployments -- they are sequential stages in
a single pipeline run. The work pool model adds complexity (workers, polling,
deployment configuration) without solving Artisan's actual need (dispatching
batched units to compute).

**Result storage is irrelevant.** Artisan's artifact storage, staging, and
commit model is purpose-built for provenance-tracked scientific artifacts.
Prefect's result storage is a generic key-value cache. There is no benefit to
adopting it.

**Container startup latency matters for cloud backends.** The Prefect community
documents real issues with cold-start latency on ECS/Fargate. Artisan's
batching model (grouping artifacts into execution units) is well-suited to
amortize this -- but backend implementations should expose tuning knobs for
batch granularity vs. startup overhead.

**Summary recommendation:** Keep Prefect as a dependency for local and SLURM
backends where task runners work well. For cloud/serverless backends, bypass
Prefect and use native APIs behind a DispatchHandle interface. This avoids
depending on Prefect Cloud (commercial) and sidesteps the architectural
mismatch between task runners (intra-flow) and cloud provisioning (per-invocation).

---

## Sources

- [Prefect Task Runners Concepts](https://docs.prefect.io/v3/concepts/task-runners)
- [Prefect Task Runners Guide](https://docs.prefect.io/3.0/develop/task-runners)
- [Task Runners Deep Analysis (DeepWiki)](https://deepwiki.com/PrefectHQ/prefect/3.3-task-runners)
- [Prefect Work Pools Concepts](https://docs.prefect.io/v3/concepts/work-pools)
- [Prefect Work Pools Configuration](https://docs.prefect.io/v3/deploy/infrastructure-concepts/work-pools)
- [Prefect Workers](https://docs.prefect.io/v3/deploy/infrastructure-concepts/workers)
- [Prefect Results (Advanced)](https://docs.prefect.io/v3/advanced/results)
- [Prefect Deployments](https://docs.prefect.io/v3/concepts/deployments)
- [Prefect Serverless How-To](https://docs.prefect.io/v3/how-to-guides/deployment_infra/serverless)
- [Prefect Retries How-To](https://docs.prefect.io/v3/how-to-guides/workflows/retries)
- [Modal Work Pool Changelog](https://www.prefect.io/changelog/modal-workers)
- [Modal OSS Support Request (GitHub #20218)](https://github.com/PrefectHQ/prefect/issues/20218)
- [ECS Latency Discussion (GitHub #18976)](https://github.com/PrefectHQ/prefect/discussions/18976)
- [Prefect Task Mapping at Scale (Blog)](https://www.prefect.io/blog/beyond-loops-how-prefect-s-task-mapping-scales-to-thousands-of-parallel-tasks)
- [Prefect Global Concurrency Limits](https://docs.prefect.io/v3/how-to-guides/workflows/global-concurrency-limits)
- [Prefect SDK Reference: task_runners](https://reference.prefect.io/prefect/task_runners/)
