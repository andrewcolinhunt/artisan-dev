# Prefect Alternatives: Execution Layer Comparison

Analysis of alternatives to Prefect as artisan's execution dispatch layer.
Artisan owns pipeline orchestration (step sequencing, caching, provenance,
artifact storage). The question is what dispatches batched execution units
to compute backends and collects results.

---

## Current State: How Artisan Uses Prefect

Artisan's Prefect integration is shallow. The entire coupling surface is:

| File | What it uses |
|------|-------------|
| `dispatch.py` | `@task` decorator on `execute_unit_task` |
| `local.py` | `ProcessPoolTaskRunner`, `@flow`, `unmapped`, `.map()` |
| `slurm.py` | `@flow`, `unmapped`, `.map()`, plus `SlurmTaskRunner` from `prefect-submitit` |
| `slurm_intra.py` | Same as slurm.py (srun mode via `prefect-submitit`) |
| `prefect_server.py` | Server discovery, health checks, version compat (~300 lines) |

The pattern in every backend is identical:

```python
@flow(task_runner=some_runner)
def step_flow():
    futures = execute_unit_task.map(units, runtime_env=unmapped(env))
    return _collect_results(futures)
```

Artisan does NOT use: Prefect deployments, work pools, result storage,
retries, caching, scheduling, or the Prefect API/UI for anything essential.

What Prefect actually provides to artisan today:
- **Local**: A `ProcessPoolExecutor` wrapper with `.map()` semantics
- **SLURM**: The `prefect-submitit` bridge (submitit + Prefect task runner interface)
- **Observability**: The Prefect UI shows flow/task runs (optional, not required)
- **Server**: Required only for SLURM (workers need a coordination endpoint)

---

## Option 1: Roll Your Own (concurrent.futures + submitit)

Replace Prefect with direct use of `concurrent.futures.ProcessPoolExecutor`
for local and `submitit.AutoExecutor` for SLURM.

### What changes

**Local backend**: Replace `ProcessPoolTaskRunner` + `@flow` + `@task` +
`.map()` with a plain `ProcessPoolExecutor.map()` or `executor.submit()`
loop. The `SIGINTSafeProcessPoolTaskRunner` becomes a
`ProcessPoolExecutor` with `initializer=_ignore_sigint` (which it already
wraps internally).

**SLURM backend**: Replace `SlurmTaskRunner` (from `prefect-submitit`) with
direct `submitit.AutoExecutor` calls. submitit already exposes a
`concurrent.futures`-compatible API: `executor.submit(fn, *args)` returns a
`submitit.Job` (which is a `Future`). `executor.map_array(fn, args_list)`
submits SLURM job arrays.

**Server**: Eliminated entirely. submitit uses the shared filesystem for
coordination (pickle files for input/output). No server process, no port,
no discovery, no health checks.

### Gains

- **Remove ~300 lines** of server management code (`prefect_server.py`)
- **Remove prefect dependency** (~100+ transitive deps including httpx,
  sqlalchemy, pydantic-settings, typer, etc.)
- **Remove PostgreSQL dependency** (currently needed for Prefect server)
- **Simpler SLURM setup**: No `prefect-start` / `prefect-stop` dance.
  submitit just writes pickle files to NFS and calls sbatch.
- **Fewer failure modes**: No server crashes, no version mismatches, no
  stale discovery files, no `SettingsContext` propagation
- **Easier testing**: No need to mock Prefect flows/tasks in unit tests
- **submitit is already a dependency** (via prefect-submitit)

### Losses

- **Prefect UI observability**: No flow/task run dashboard. Would need to
  build custom logging/progress reporting (Rich console output is already
  present, so the gap is smaller than it sounds).
- **prefect-submitit bridge**: This package handles the
  submitit-to-Prefect-future translation and adds features like
  `units_per_worker` batching. Those features would need to be replicated
  (~50-100 lines) or contributed upstream to submitit.
- **Future extensibility via TaskRunner interface**: Prefect's TaskRunner
  is a known interface that third-party tools target. Without it, each new
  backend is a bespoke implementation (but artisan's `BackendBase` +
  `DispatchHandle` already provides this abstraction).

### Verdict

**Strong candidate.** The Prefect integration is thin enough that removing
it is straightforward. The main risk is losing the `units_per_worker`
batching from prefect-submitit, which would need reimplementation. The
architecture already has `BackendBase` and `DispatchHandle` as the real
abstraction layer -- Prefect is an implementation detail inside two
backends.

---

## Option 2: Dask (dask.distributed + dask-jobqueue)

Replace Prefect's task dispatch with Dask's distributed scheduler.

### How it works

- **Local**: `dask.distributed.LocalCluster` creates a scheduler + workers
  on the local machine. `client.map(fn, items)` returns futures.
- **SLURM**: `dask_jobqueue.SLURMCluster` submits SLURM jobs that start
  Dask workers. The scheduler coordinates work distribution. Supports
  adaptive scaling (`cluster.adapt(minimum=1, maximum=100)`).
- **Cloud**: `dask-cloudprovider` has AWS Fargate, GCP, Azure adapters.
  `dask-kubernetes` for K8s.

### Gains

- **Unified scheduler**: One programming model for local, SLURM, and cloud.
  `client.map()` works the same everywhere.
- **Dashboard**: Real-time task execution dashboard (Bokeh-based) with
  worker status, task progress, memory usage, task graph visualization.
  Built-in, no separate server needed.
- **Adaptive scaling on SLURM**: `cluster.adapt()` automatically requests
  and releases SLURM nodes based on workload.
- **Well-established in scientific Python**: Widely used at national labs
  (NCAR, NERSC), in xarray/pangeo ecosystems. Mature SLURM support.
- **Lighter than Prefect**: No server requirement. The scheduler runs
  in-process or as a lightweight local process.

### Losses / Concerns

- **Overkill for artisan's pattern**: Artisan dispatches independent,
  embarrassingly-parallel units. Dask's strength is its task graph scheduler
  for complex dependency chains. Using `client.map()` on independent tasks
  underutilizes Dask.
- **Scheduler overhead**: Dask's distributed scheduler is a long-running
  process (lightweight, but still a process). For local execution, this is
  heavier than a plain `ProcessPoolExecutor`.
- **Dependency weight**: `dask` + `distributed` + `dask-jobqueue` is a
  non-trivial dependency tree (tornado, cloudpickle, msgpack, etc.).
  Lighter than Prefect, but not minimal.
- **Different failure model**: Dask handles retries and worker failures at
  the scheduler level. Artisan would need to reconcile this with its own
  error handling (or disable Dask's retry logic).
- **dask-jobqueue maintenance**: Last commit December 2025. Functional but
  not rapidly evolving. The SLURMCluster API is stable.

### Verdict

**Viable but overfit.** Dask brings real value if artisan's execution model
evolves toward complex inter-task dependencies or if adaptive SLURM scaling
is needed. For the current "map N independent units" pattern, it adds
complexity without proportional benefit. The dashboard is nice but doesn't
justify the dependency.

---

## Option 3: Ray

Replace Prefect with Ray's distributed runtime.

### How it works

- **Local**: `ray.init()` starts a local Ray cluster. `ray.remote(fn)` +
  `fn.remote(arg)` dispatches tasks. `ray.get(refs)` collects results.
- **SLURM**: Start a Ray head node on one SLURM node, connect workers from
  other nodes. Community scripts and `slurmray` package assist with setup.
  Not as turnkey as dask-jobqueue or submitit.
- **Cloud**: Ray has first-class Kubernetes support (KubeRay), AWS/GCP
  integration, and Anyscale (managed Ray cloud). Strong cloud story.

### Gains

- **High-performance task dispatch**: Millions of tasks/second with
  sub-millisecond latency. Far more throughput than artisan needs, but
  headroom is never a problem.
- **Shared object store**: Ray's plasma store enables zero-copy data
  sharing between tasks. Useful if units share large inputs.
- **Ecosystem**: Ray Tune, Ray Train, Ray Serve, Ray Data provide
  higher-level tools if needed. Strong in ML/AI communities.
- **Cloud-native**: Best-in-class Kubernetes deployment. If artisan moves
  to cloud clusters, Ray is well-positioned.

### Losses / Concerns

- **Heavy runtime**: Ray starts a head node, object store, GCS (Global
  Control Service), and dashboard. Significant overhead for "run 10 tasks
  in parallel on my laptop." Process creation overhead is notable for small
  task counts.
- **SLURM integration is community-maintained**: No official `SlurmCluster`
  equivalent to dask-jobqueue. The documented approach is manual: start Ray
  head via sbatch, connect workers via srun. `slurmray` (v8.8.0, Jan 2026)
  helps but is third-party.
- **Complexity**: Ray's programming model (actors, object refs, placement
  groups) is powerful but complex for artisan's simple map-collect pattern.
- **Dependency weight**: Ray pulls in grpcio, protobuf, aiohttp, and many
  more. One of the heavier options.
- **Cultural mismatch**: Ray is designed for long-running distributed
  applications and ML training. Artisan's "batch N units, collect results,
  move to next step" is a fraction of what Ray offers.

### Verdict

**Too heavy for the execution layer role.** Ray would make sense if artisan
were building distributed training or serving pipelines. For dispatching
embarrassingly-parallel execution units, Ray's overhead and complexity are
not justified. However, Ray remains relevant as a potential **cloud compute
adapter** (Option C in the cloud analysis) -- shipping individual compute
kernels to a Ray cluster via `ray.remote()` is a clean 30-line adapter.

---

## Option 4: Parsl

A Python parallel scripting library designed specifically for HPC and
scientific workflows.

### How it works

- **Local**: `ThreadPoolExecutor` or `HighThroughputExecutor` with
  `LocalProvider`. Tasks are Python functions decorated with `@python_app`.
- **SLURM**: `HighThroughputExecutor` with `SlurmProvider`. Parsl submits
  pilot jobs via sbatch, starts workers inside them, and dispatches tasks
  to workers via ZMQ. Supports `nodes_per_block`, `cores_per_node`,
  `mem_per_node`, `walltime`.
- **Cloud**: Providers for AWS, GCP, Azure, Kubernetes. Also supports
  HTCondor, PBS/Torque, Cobalt, GridEngine.

### Gains

- **Built for this exact problem**: Parsl's design center is "dispatch
  Python functions to HPC resources and collect results." This is precisely
  what artisan needs.
- **Broadest HPC support**: SLURM, PBS, HTCondor, Cobalt, GridEngine,
  Kubernetes, clouds -- all through a unified provider/executor model.
  More scheduler support than any other option.
- **HighThroughputExecutor**: Pilot-job model with hierarchical scheduling.
  Workers stay alive across tasks (amortizes startup). Scales to thousands
  of nodes and tens of thousands of tasks/second.
- **Lightweight**: Smaller dependency footprint than Prefect, Dask, or Ray.
  Core is pure Python + ZMQ.
- **concurrent.futures compatible**: `@python_app` returns an
  `AppFuture` (subclass of `Future`). Familiar API.
- **Active NSF-funded development**: Used at NERSC, ALCF, OLCF.
  Specifically designed for DOE supercomputers. Actively maintained
  through 2025-2026 with recent releases.

### Losses / Concerns

- **Less known outside HPC**: Parsl is well-known at national labs but has
  lower visibility in the broader Python ecosystem compared to Dask or Ray.
- **App decorator model**: Parsl expects `@python_app` decorated functions.
  Artisan would need to wrap `execute_unit_task` with this decorator or use
  Parsl's executor API more directly.
- **Monitoring**: Parsl has a monitoring database (SQLite/PostgreSQL) and a
  basic web dashboard, but it's less polished than Prefect's or Dask's UI.
- **Workflow-level features**: Parsl has its own DAG execution engine for
  chaining apps. Artisan doesn't need this -- it only needs the
  executor/provider dispatch layer.
- **Pilot job model adds a layer**: The HighThroughputExecutor runs
  interchange processes and worker pools. For local execution, this is
  heavier than a plain ProcessPoolExecutor.

### Verdict

**Interesting for HPC-heavy deployments.** If artisan needs to support many
HPC schedulers (not just SLURM), Parsl's provider model is the most
complete. However, for the current scope (local + SLURM), the benefits over
direct submitit are marginal. Parsl's sweet spot is when you need portable
HPC execution across multiple scheduler types.

---

## Option 5: Nextflow / Snakemake

Pipeline-level workflow managers from the bioinformatics community.

### Why they don't fit

These are **pipeline orchestrators**, not execution layers. They own the
full pipeline: DAG definition, step execution, data flow, caching, and
(often) containerization. Artisan is also a pipeline orchestrator. Using
Nextflow or Snakemake under artisan would mean artisan delegates entire
pipeline execution to them, which defeats the purpose.

| Concern | Nextflow | Snakemake |
|---------|----------|-----------|
| Language | Groovy DSL (not Python) | Python-based DSL |
| Execution model | Process-per-task (fork/exec) | Process-per-rule (shell/script) |
| SLURM support | Excellent (native executor) | Excellent (cluster profiles) |
| Cloud support | AWS Batch, Google Life Sciences, Azure Batch | Kubernetes, cloud execution via profiles |
| Python integration | Poor (Groovy-centric) | Good (Python DSL), but rules are shell-oriented |
| Embedding as library | Not designed for it | Not designed for it |

Both are designed to be the top-level orchestrator. Neither provides a
clean API for "take this Python function and run it on SLURM" -- they
expect to own the workflow definition.

**Snakemake** is closer to embeddable (Python-based), but its core
abstraction is file-based rules (`input: / output: / shell:`), which
maps poorly to artisan's artifact-centric model.

### Verdict

**Wrong layer.** These compete with artisan at the pipeline level, not the
execution dispatch level. Irrelevant for this decision.

---

## Option 6: Apache Airflow

The heavyweight enterprise workflow orchestrator.

### Why it doesn't fit

- **Massive dependency footprint**: SQLAlchemy, Flask, Celery, Redis or
  RabbitMQ for the executor, PostgreSQL for metadata. Far heavier than
  Prefect.
- **Infrastructure requirements**: Webserver, scheduler, metadata database,
  and optionally Celery workers -- all as long-running services.
- **DAG-file model**: Workflows defined as Python files that Airflow's
  scheduler periodically parses. Not designed for programmatic, in-process
  pipeline construction.
- **No HPC support**: No native SLURM integration. Community operators
  exist but are not well-maintained. Airflow's model assumes containerized
  cloud or VM-based execution.
- **Scheduling-centric**: Designed for recurring batch ETL jobs, not
  interactive/ad-hoc scientific pipelines.
- **Overhead**: Documented 30-50% infrastructure overhead for worker
  provisioning. Startup latency measured in seconds to minutes.

### Verdict

**Categorically wrong.** Airflow solves enterprise ETL scheduling at scale.
It adds enormous complexity without solving any problem artisan has. Worse
in every dimension than the current Prefect setup.

---

## Option 7: Simple (ProcessPoolExecutor + submitit, no framework)

The minimal approach: use Python stdlib for local, submitit for SLURM,
nothing else.

### Implementation sketch

```python
# Local dispatch
class LocalDispatchHandle(DispatchHandle):
    def dispatch(self, units, runtime_env):
        with ProcessPoolExecutor(
            max_workers=self._max_workers,
            mp_context=multiprocessing.get_context("spawn"),
            initializer=_ignore_sigint,
        ) as pool:
            futures = [pool.submit(execute_unit, u, runtime_env) for u in units]
            self._results = [f.result() for f in futures]

# SLURM dispatch
class SlurmDispatchHandle(DispatchHandle):
    def dispatch(self, units, runtime_env):
        executor = submitit.AutoExecutor(folder=self._log_folder)
        executor.update_parameters(
            slurm_partition=self._partition,
            slurm_time=self._time_limit,
            mem_gb=self._mem_gb,
            gpus_per_node=self._gpus,
            cpus_per_task=self._cpus,
        )
        jobs = executor.map_array(execute_unit, units,
                                   [runtime_env] * len(units))
        self._results = [j.result() for j in jobs]
```

### What this requires

- Reimplement `units_per_worker` batching from prefect-submitit (~50 lines)
- Move the `@task` decorator off `execute_unit_task` (it becomes a plain
  function)
- Delete `prefect_server.py` entirely
- Replace `SIGINTSafeProcessPoolTaskRunner` with a plain
  `ProcessPoolExecutor` (the SIGINT-safe initializer is already there)
- Update tests to remove Prefect flow/task mocking

### What this preserves

- `BackendBase` and `DispatchHandle` abstractions (unchanged)
- `execute_unit_task` logic (unchanged -- just loses the `@task` decorator)
- All artifact storage, staging, commit, provenance (unchanged)
- All pipeline orchestration logic (unchanged)

### Gains

Same as Option 1, plus:
- **Minimum viable dependency**: stdlib + submitit. No framework opinions.
- **Maximum debuggability**: A `ProcessPoolExecutor` is the simplest
  parallel construct in Python. `submitit.Job` is a thin Future wrapper.
  No framework magic between your code and the OS.
- **Clear upgrade path**: If a framework proves necessary later, the
  `DispatchHandle` interface accepts any implementation. Adding a Dask,
  Ray, or Parsl backend later is additive, not a migration.

### Losses

Same as Option 1 (no UI dashboard), plus:
- **Less battle-tested SLURM integration**: prefect-submitit adds error
  handling, log capture, and worker batching that raw submitit doesn't
  have. These need explicit reimplementation.

### Verdict

**Recommended starting point.** This is the natural consequence of
artisan's architecture: `BackendBase` + `DispatchHandle` already define the
abstraction. Prefect is an implementation detail that can be replaced with
~100 lines of `ProcessPoolExecutor` and `submitit.AutoExecutor` code. The
architecture is already framework-agnostic; the implementation should match.

---

## Comparison Matrix

| Criterion | Prefect (current) | Simple (PPE+submitit) | Dask | Ray | Parsl | Nextflow/Snakemake | Airflow |
|-----------|------------------|----------------------|------|-----|-------|-------------------|---------|
| **Dependency weight** | Heavy (~100+ transitive) | Minimal (stdlib + submitit) | Medium | Heavy | Light-medium | N/A | Very heavy |
| **Local multiprocess** | ProcessPoolTaskRunner | ProcessPoolExecutor | LocalCluster | ray.init() | ThreadPool/HTE | N/A (wrong layer) | N/A |
| **SLURM dispatch** | prefect-submitit | submitit direct | dask-jobqueue | Community scripts | SlurmProvider | Native executor | No native support |
| **Cloud execution** | Work pools (Cloud only) | None (add later) | dask-cloudprovider | KubeRay, Anyscale | AWS/GCP/K8s providers | AWS Batch, GLS | K8s, ECS, Cloud Composer |
| **Observability** | Prefect UI (excellent) | Custom logging (Rich) | Dask dashboard (good) | Ray dashboard (good) | Monitoring DB (basic) | Nextflow Tower | Airflow UI |
| **Server required** | Yes (for SLURM) | No | No (scheduler in-process) | Yes (head node) | No | No | Yes (multiple services) |
| **Suitable as sub-layer** | Yes (current proof) | Yes (simplest) | Yes (embeddable) | Partially (heavy runtime) | Yes (executor API) | No (wants to be top) | No (wants to be top) |
| **Lines to implement** | 0 (exists) | ~100-150 | ~150-200 | ~200-300 | ~150-200 | N/A | N/A |
| **HPC scheduler breadth** | SLURM only | SLURM only | SLURM, PBS, SGE | SLURM (community) | SLURM, PBS, HTCondor, Cobalt, SGE, K8s | SLURM, PBS, SGE, K8s | None native |

---

## Recommendation

### Short term: Decouple from Prefect (Option 7 / Simple)

The Prefect integration surface is small enough (~30 lines of actual
Prefect API calls across 3 backends) that removal is low-risk. The gains
are meaningful:

- Eliminate the Prefect server requirement for SLURM
- Remove ~100+ transitive dependencies
- Remove PostgreSQL requirement
- Simplify SLURM setup (no prefect-start/stop)
- Eliminate server-related failure modes entirely

The `DispatchHandle` abstraction already insulates the rest of artisan from
the execution layer. Swapping Prefect for direct `ProcessPoolExecutor` +
`submitit` is a backend-internal change.

### Key implementation tasks

- New `LocalDispatchHandle` using `ProcessPoolExecutor` directly
- New `SlurmDispatchHandle` using `submitit.AutoExecutor` directly
- Port `units_per_worker` batching from prefect-submitit (~50 lines)
- Port SLURM log capture from prefect-submitit future objects
- Delete `prefect_server.py`
- Remove `@task` decorator from `execute_unit_task`
- Update `pixi.toml`: remove prefect, prefect-submitit, postgresql deps
- Update docs referencing Prefect setup

### Medium term: Add cloud backends

When cloud execution is needed, add backend implementations behind
`DispatchHandle`:

- **Modal**: `ModalDispatchHandle` using `modal.Function.map()` (~100 lines)
- **Ray (as compute adapter)**: For shipping compute kernels to GPU
  clusters via `ray.remote()` (~30 lines, per cloud analysis Option C)
- **Dask**: Only if adaptive SLURM scaling or complex task graphs become
  necessary

Each is an additive change -- a new `BackendBase` subclass. No migration
from the simple approach required.

### What NOT to do

- Do not adopt Airflow, Nextflow, or Snakemake (wrong layer)
- Do not adopt Ray or Dask as the primary execution layer (overkill for
  the current pattern; keep them as potential cloud compute adapters)
- Do not build a custom dashboard (Rich console output + pipeline result
  tables in Delta Lake provide sufficient observability for now)

---

## Sources

- [Parsl documentation](https://parsl.readthedocs.io/en/stable/)
- [Parsl GitHub](https://github.com/Parsl/parsl)
- [Parsl at NERSC](https://docs.nersc.gov/jobs/workflow/parsl/)
- [Dask HPC deployment](https://docs.dask.org/en/stable/deploying-hpc.html)
- [dask-jobqueue SLURMCluster](https://jobqueue.dask.org/en/latest/generated/dask_jobqueue.SLURMCluster.html)
- [dask-jobqueue GitHub](https://github.com/dask/dask-jobqueue)
- [Ray SLURM deployment](https://docs.ray.io/en/latest/cluster/vms/user-guides/community/slurm.html)
- [slurmray PyPI](https://pypi.org/project/slurmray/)
- [submitit GitHub](https://github.com/facebookincubator/submitit)
- [submitit blog post (Meta)](https://ai.meta.com/blog/open-sourcing-submitit-a-lightweight-tool-for-slurm-cluster-computation/)
- [Prefect open source](https://www.prefect.io/prefect/open-source)
- [prefect-client (lightweight)](https://pypi.org/project/prefect-client/)
- [Nextflow vs Snakemake comparison](https://tasrieit.com/blog/nextflow-vs-snakemake-comprehensive-comparison-of-workflow-management)
- [Bioinformatics pipeline frameworks 2025](https://www.tracer.cloud/resources/bioinformatics-pipeline-frameworks-2025)
- [Airflow HPC limitations](https://avik-datta-15.medium.com/how-to-setup-apache-airflow-on-hpc-cluster-ea2575764b43)
- [Airflow hidden costs (Prefect blog)](https://www.prefect.io/blog/hidden-costs-apache-airflow)
- [Nvidia acquires SchedMD/SLURM](https://www.nextplatform.com/2025/12/18/nvidia-nearly-completes-its-control-freakery-with-slurm-acquisition/)
