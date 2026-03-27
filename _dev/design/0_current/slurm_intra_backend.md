# Design: SLURM Intra-Allocation Backend

**Date:** 2026-03-20
**Status:** Draft
**Repo scope:** `prefect-submitit` + `artisan`

---

## Context

Artisan pipelines run on HPC clusters via SLURM. The current `Backend.SLURM`
submits each step's work as independent `sbatch` jobs (or job arrays) to the
SLURM queue. This works well for long-running tasks but creates unnecessary
overhead when a user already has resources allocated:

```bash
salloc --nodes=4 --gpus-per-node=8 --cpus-per-node=64
# 32 GPUs, 256 CPUs are now reserved and idle, waiting for work
```

With the current backend, each pipeline task still goes through the SLURM
queue via `sbatch`, even though the resources are already in hand. This means:

- **Queue latency per task** — seconds to minutes of wait, multiplied across
  hundreds of tasks
- **sbatch overhead** — each submission is a SLURM API call
- **Resource fragmentation** — jobs may land outside the allocation

The intra-allocation backend eliminates this by distributing work directly to
allocated resources using `srun`, SLURM's built-in intra-allocation task
launcher.

---

## Design Constraints

**Must support multi-node allocations.** A typical ML workload allocates 2-8
nodes. Single-node-only would leave most allocated resources idle, defeating
the purpose. This constraint eliminates `submitit.LocalExecutor` and Python's
`ProcessPoolExecutor` as dispatch mechanisms — both are single-node only.

**Must use `srun` for task dispatch.** Within a SLURM allocation, `srun` is
the only mechanism that distributes work across multiple allocated nodes
without going through the queue. It also provides GPU binding via
`--gres=gpu:N`, cgroup isolation, and proper SLURM accounting — all for free.

**Must integrate with the existing Prefect dispatch flow.** Artisan's
`BackendBase._build_prefect_flow()` constructs a Prefect `@flow` that maps
`execute_unit_task` across units using a `TaskRunner`. The new backend must
produce a Prefect-compatible `TaskRunner` so this machinery works unchanged.

**Must use shared filesystem for data exchange.** In a multi-node allocation,
the head process and workers communicate through NFS (same as `Backend.SLURM`).
The existing pickle-based unit serialization and result collection work as-is.

**Must reuse submitit's pickle/result protocol where possible.** submitit
already solves "serialize callable → launch process → collect result from
pickle." Reimplementing this from scratch would be error-prone. The srun-based
approach should follow the same protocol, diverging only in how the process is
launched.

**No new external dependencies.** `srun` is available on any SLURM cluster.
submitit and prefect-submitit are existing dependencies.

---

## Design Decisions

### `srun` over `submitit.LocalExecutor`

submitit's `LocalExecutor` handles GPU isolation automatically via
`visible_gpus` and has an existing Prefect bridge through `prefect-submitit`'s
local execution mode. This was the original preferred approach.

However, `LocalExecutor` hardcodes single-node execution:
- `_submit_command` sets `SUBMITIT_LOCAL_JOB_NUM_NODES="1"`
- `_internal_update_parameters` raises `ValueError` if `nodes > 1`
- `start_controller` spawns a local subprocess, no cross-node capability

Since multi-node is a hard requirement, `srun` is the necessary dispatch
mechanism. `srun --exclusive` within an allocation runs immediately (no queue),
distributes across all allocated nodes, and SLURM handles GPU binding via
`--gres`.

### New `ExecutionMode.SRUN` in prefect-submitit

Rather than building a completely new TaskRunner, extend the existing
`SlurmTaskRunner` with a third execution mode. The runner already handles
two modes (`SLURM` for sbatch, `LOCAL` for subprocess). Adding `SRUN` follows
the established pattern and reuses the existing future/submission infrastructure.

The key difference between modes is only how the process is launched:

| Mode    | Launch      | Queue? | Multi-node? | GPU isolation      |
| ------- | ----------- | ------ | ----------- | ------------------ |
| `SLURM` | `sbatch`    | Yes    | Yes         | SLURM scheduler    |
| `LOCAL`  | subprocess  | No     | No          | `CUDA_VISIBLE_DEVICES` env var |
| `SRUN`  | `srun`      | No     | Yes         | `--gres=gpu:N`     |

### submitit's pickle protocol for srun workers

submitit's execution pattern is:

```
Orchestrator                          Worker (remote node)
    │                                     │
    ├─ pickle(callable) → NFS             │
    ├─ launch process ──────────────────► │
    │                                     ├─ unpickle callable
    │                                     ├─ execute
    │                                     ├─ pickle(result) → NFS
    │  poll for result file  ◄─────────────┤
    ├─ unpickle result                    │
```

The `srun` mode reuses this exactly. submitit's `_submit` module
(`python -m submitit.core._submit <folder>`) is the worker-side entry point
that loads the pickle, runs it, and writes the result. We invoke it via `srun`
instead of via a local subprocess or sbatch script.

### New `SlurmIntraBackend` in artisan

A new `BackendBase` subclass that creates a `SlurmTaskRunner` with
`execution_mode="srun"`. Follows the exact same pattern as `SlurmBackend`
(which uses `execution_mode="slurm"`). The backend declares
`shared_filesystem=True` traits for NFS behavior.

---

## Architecture

### How it fits together

```
artisan                          prefect-submitit              SLURM
───────                          ────────────────              ─────

SlurmIntraBackend                SlurmTaskRunner
  .create_flow()                   execution_mode="srun"
      │                                │
      ├─ constructs TaskRunner         │
      ├─ calls _build_prefect_flow()    │
      │                                │
      ▼                                │
  Prefect @flow                         │
    execute_unit_task.map() ──────────►│
                                       ├─ pickle callable to NFS
                                       ├─ srun --exclusive ... python -m submitit.core._submit
                                       │      │
                                       │      ├──► Node 0, GPU 0: unpickle → execute → pickle result
                                       │      ├──► Node 1, GPU 2: unpickle → execute → pickle result
                                       │      └──► Node 3, GPU 7: unpickle → execute → pickle result
                                       │
                                       ├─ poll for result pickles on NFS
                                       ├─ return SrunPrefectFuture per task
                                       │
  _collect_results()  ◄────────────────┘
  commit staged parquet
```

### Changes by repo

**prefect-submitit:**

- `constants.py` — add `SRUN = "srun"` to `ExecutionMode`
- `runner.py` — add srun path in `__enter__`, `submit`, and `map`
- `futures/srun.py` — new `SrunPrefectFuture` wrapping an srun subprocess
- `srun.py` (new) — srun launch helpers (build command, manage subprocess)

**artisan:**

- `orchestration/backends/slurm_intra.py` — new `SlurmIntraBackend` subclass
- `orchestration/backends/__init__.py` — register `Backend.SLURM_INTRA`

Nothing else changes in artisan. `execute_unit_task`, `_build_prefect_flow`,
`ResourceConfig`, `ExecutionConfig`, and the step executor are all untouched.

---

## prefect-submitit Changes

### ExecutionMode

```python
class ExecutionMode(StrEnum):
    SLURM = "slurm"
    LOCAL = "local"
    SRUN = "srun"           # NEW: within existing allocation
```

### SlurmTaskRunner — srun mode entry

In `__enter__`, the srun mode doesn't create a submitit executor. Instead it
prepares a working directory for pickle I/O:

```python
elif self.execution_mode == ExecutionMode.SRUN:
    self._srun_folder = Path(self.log_folder) / "srun"
    self._srun_folder.mkdir(parents=True, exist_ok=True)
    # Validate we're inside an allocation
    if not os.environ.get("SLURM_JOB_ID"):
        raise RuntimeError(
            "execution_mode='srun' requires an active SLURM allocation "
            "(SLURM_JOB_ID not set). Use salloc or sbatch first."
        )
```

### SlurmTaskRunner.submit — srun path

The submit method, in srun mode, follows submitit's pickle protocol manually:

```python
# 1. Serialize the callable (same cloudpickle pattern as SLURM mode)
wrapped_call = cloudpickle_wrapped_call(run_task_in_slurm, env=env, **submit_kwargs)

# 2. Write pickle to shared NFS
job_folder = self._srun_folder / uuid.uuid4().hex
job_folder.mkdir()
pickle_path = job_folder / "submitted.pkl"
with open(pickle_path, "wb") as f:
    cloudpickle.dump(DelayedSubmission(wrapped_call), f)

# 3. Launch via srun (non-blocking)
cmd = self._build_srun_command(job_folder)
proc = subprocess.Popen(cmd, stdout=..., stderr=...)

# 4. Return future that polls for result pickle
return SrunPrefectFuture(proc, job_folder, task_run_id, ...)
```

### srun command construction

```python
def _build_srun_command(self, job_folder: Path) -> list[str]:
    cmd = [
        "srun",
        "--exclusive",       # get dedicated resources within allocation
        "-n1",               # one task
        "--kill-on-bad-exit=1",
    ]
    if self.gpus_per_node > 0:
        cmd.extend(["--gres", f"gpu:{self.gpus_per_node}"])
    if self.mem_gb:
        cmd.extend(["--mem", f"{self.mem_gb}G"])
    if self.slurm_kwargs.get("cpus_per_task"):
        cmd.extend(["--cpus-per-task", str(self.slurm_kwargs["cpus_per_task"])])

    # Use submitit's worker entry point
    cmd.extend([
        sys.executable, "-u", "-m", "submitit.core._submit",
        str(job_folder),
    ])
    return cmd
```

`srun --exclusive` within an allocation:
- Runs immediately (no queue)
- SLURM picks a node with available resources
- `--gres=gpu:N` binds specific GPUs and sets `CUDA_VISIBLE_DEVICES`
- Provides cgroup memory/CPU isolation
- Proper SLURM accounting within the allocation

### SrunPrefectFuture

Wraps an srun subprocess and polls for result pickle on NFS. Similar to
`SlurmPrefectFuture` but tracks a `subprocess.Popen` instead of a
`submitit.Job`:

```python
class SrunPrefectFuture(PrefectFuture):
    def __init__(self, proc, job_folder, task_run_id, poll_interval, max_poll_time):
        self._proc = proc
        self._job_folder = job_folder
        self._result_pickle = job_folder / "result.pkl"
        ...

    def wait(self, timeout=None):
        # Poll for process completion + result pickle on NFS
        self._proc.wait(timeout=timeout)
        if self._proc.returncode != 0:
            raise SlurmJobFailed(f"srun exited with code {self._proc.returncode}")

    def result(self, timeout=None, raise_on_failure=True):
        self.wait(timeout)
        # Read result from NFS (same pickle format as submitit)
        with open(self._result_pickle, "rb") as f:
            state = cloudpickle.loads(pickle.load(f))
        return state.result() if hasattr(state, "result") else state

    def cancel(self):
        self._proc.terminate()

    def logs(self):
        stdout = (self._job_folder / "stdout.txt").read_text()
        stderr = (self._job_folder / "stderr.txt").read_text()
        return stdout, stderr
```

The result pickle path and format must match what `submitit.core._submit`
writes. submitit writes results to `<folder>/<job_id>/result.pkl` — need to
verify the exact path convention and whether `_submit` can be pointed at an
arbitrary folder.

### SlurmTaskRunner.map — srun path

The map method launches N concurrent srun processes. Unlike sbatch job arrays
(which are a single submission), srun tasks are launched individually:

```python
# In srun mode, map launches concurrent srun processes
futures = []
for i, wrapped_call in enumerate(wrapped_calls):
    job_folder = self._srun_folder / f"task_{i}_{uuid.uuid4().hex[:8]}"
    job_folder.mkdir()
    _serialize(wrapped_call, job_folder)
    proc = subprocess.Popen(self._build_srun_command(job_folder), ...)
    futures.append(SrunPrefectFuture(proc, job_folder, task_run_ids[i], ...))
```

SLURM internally queues srun requests within the allocation when resources
are fully utilized, so launching all N concurrently is safe — excess tasks
block in srun until a slot opens.

---

## artisan Changes

### SlurmIntraBackend

```python
class SlurmIntraBackend(BackendBase):
    """Execute within an existing SLURM allocation via srun."""

    name = "slurm_intra"
    worker_traits = WorkerTraits(
        worker_id_env_var=None,
        shared_filesystem=True,
    )
    orchestrator_traits = OrchestratorTraits(
        shared_filesystem=True,
        staging_verification_timeout=60.0,
    )

    def create_flow(self, resources, execution, step_number, job_name):
        from prefect_submitit import SlurmTaskRunner

        slurm_kwargs = dict(resources.extra)

        task_runner = SlurmTaskRunner(
            execution_mode="srun",
            time_limit=resources.time_limit,
            mem_gb=resources.memory_gb,
            gpus_per_node=resources.gpus,
            units_per_worker=execution.units_per_worker,
            slurm_job_name=f"s{step_number}_{job_name}",
            **slurm_kwargs,
        )
        return self._build_prefect_flow(task_runner)

    def capture_logs(self, results, staging_root, failure_logs_root, operation_name):
        from artisan.orchestration.engine.dispatch import _patch_worker_logs
        _patch_worker_logs(results, staging_root, failure_logs_root, operation_name)

    def validate_operation(self, operation):
        if not os.environ.get("SLURM_JOB_ID"):
            warnings.warn(
                f"Backend 'slurm_intra' selected for {operation.name!r} but "
                f"SLURM_JOB_ID is not set. Are you inside an salloc/sbatch?",
                stacklevel=2,
            )
```

This mirrors `SlurmBackend.create_flow()` almost exactly — same parameter
mapping, same `_build_prefect_flow` call. The only difference is
`execution_mode="srun"` instead of the implicit `"slurm"`.

### Registration

```python
# orchestration/backends/__init__.py
from artisan.orchestration.backends.slurm_intra import SlurmIntraBackend

class Backend:
    LOCAL = LocalBackend()
    SLURM = SlurmBackend()
    SLURM_INTRA = SlurmIntraBackend()

_REGISTRY = {b.name: b for b in [Backend.LOCAL, Backend.SLURM, Backend.SLURM_INTRA]}
```

### User-facing API

```python
from artisan.orchestration.backends import Backend

# Inside an salloc session (4 nodes, 8 GPUs each):
pipeline.run(
    MyGPUOp,
    inputs=artifacts,
    backend=Backend.SLURM_INTRA,
    resources=ResourceConfig(gpus=1, cpus=4, memory_gb=16),
    execution=ExecutionConfig(units_per_worker=1),
)
# → 32 srun tasks across 4 nodes, 1 GPU each, zero queue wait
```

---

## Open Questions

**submitit `_submit` module compatibility.** The srun approach invokes
`python -m submitit.core._submit <folder>` as the worker entry point. Need
to verify:
- Does `_submit` accept an arbitrary folder path?
- What exact file layout does it expect (pickle filename, result path)?
- Can we rely on this as a stable interface, or is it an internal detail?

If `_submit` doesn't work as a standalone entry point for srun, we may need
a thin wrapper script that handles the pickle load/execute/save cycle
directly.

**Worker ID propagation.** With `worker_id_env_var=None`, all srun workers
report `worker_id=0` in provenance. `srun` sets `SLURM_PROCID` (global rank)
and `SLURM_LOCALID` (node-local rank) in each task's environment. We could
use `SLURM_PROCID` as the worker ID env var for provenance tracking.

**Concurrency control.** When mapping 1000 tasks onto a 32-GPU allocation,
all 1000 `srun` processes are launched but only 32 run concurrently — the
rest block in srun waiting for resources. This is correct behavior, but
launching 1000 subprocesses on the head node has memory overhead. May need
a semaphore or batch launching strategy for very large task counts.

**Batching (units_per_worker > 1).** The batched submission path in
`prefect-submitit` groups multiple items into one job. This should work
with srun mode (one srun per batch), but needs verification.

**Log capture.** srun stdout/stderr can be captured by the `Popen` object
or redirected to files. Need to decide whether to use submitit's log paths
or manage logs directly in `SrunPrefectFuture`.

**Cleanup.** Pickle files accumulate in the srun working directory. Should
they be cleaned up after result collection, or left for debugging?
