# Design: SLURM Intra-Allocation Backend

**Date:** 2026-03-20
**Status:** Draft
**Repo:** `artisan`
**Related:** [srun Execution Mode (prefect-submitit)](srun_execution_mode.md)

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
launcher. This doc covers the artisan side: a new `SlurmIntraBackend` that
wires into the `srun` execution mode provided by `prefect-submitit`.

---

## Design Constraints

**Must integrate with the existing Prefect dispatch flow.** Artisan's
`BackendBase._build_prefect_flow()` constructs a Prefect `@flow` that maps
`execute_unit_task` across units using a `TaskRunner`. The new backend must
produce a Prefect-compatible `TaskRunner` so this machinery works unchanged.

**Must use shared filesystem for data exchange.** In a multi-node allocation,
the head process and workers communicate through NFS (same as `Backend.SLURM`).
The existing pickle-based unit serialization and result collection work as-is.

---

## Design Decision: New `SlurmIntraBackend`

A new `BackendBase` subclass that creates a `SlurmTaskRunner` with
`execution_mode="srun"`. Follows the exact same pattern as `SlurmBackend`
(which uses `execution_mode="slurm"`). The backend declares
`shared_filesystem=True` traits for NFS behavior.

Nothing else changes in artisan. `execute_unit_task`, `_build_prefect_flow`,
`ResourceConfig`, `ExecutionConfig`, and the step executor are all untouched.

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
      ├─ calls _build_prefect_flow()   │
      │                                │
      ▼                                │
  Prefect @flow                        │
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

---

## Changes

### File overview

- `orchestration/backends/slurm_intra.py` — new `SlurmIntraBackend` subclass
- `orchestration/backends/__init__.py` — register `Backend.SLURM_INTRA`

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
# -> 32 srun tasks across 4 nodes, 1 GPU each, zero queue wait
```

---

## Open Questions

**Worker ID propagation.** With `worker_id_env_var=None`, all srun workers
report `worker_id=0` in provenance. `srun` sets `SLURM_PROCID` (global rank)
and `SLURM_LOCALID` (node-local rank) in each task's environment. We could
use `SLURM_PROCID` as the worker ID env var for provenance tracking.
