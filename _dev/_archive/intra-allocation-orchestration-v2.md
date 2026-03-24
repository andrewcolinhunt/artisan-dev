# Analysis: Intra-Allocation Orchestration (v2)

**Date:** 2026-03-20
**Status:** Updated analysis (supersedes 2026-01-27 version)
**Author:** Claude

---

## Problem

When a user obtains a large SLURM allocation (`salloc --gpus=32 --cpus=200`),
the current `Backend.SLURM` still submits individual `sbatch` jobs per task.
This wastes the pre-allocated resources:

- **Queue latency** — each task waits in the SLURM queue despite resources
  being already reserved
- **Submission overhead** — `sbatch` calls accumulate for many small tasks
- **Resource fragmentation** — jobs may land on different nodes

The goal is a "head node" pattern: a master process inside the allocation
distributes work to pre-allocated resources with zero queue wait.

```
Current (sbatch per task):

  Login Node ──sbatch──► SLURM Queue ──► Node A (GPU 0-3)
             ──sbatch──►              ──► Node B (GPU 4-7)

Desired (intra-allocation):

  ┌─────── Single Allocation (32 GPUs) ───────┐
  │  Head Process ──► Worker 0 (GPU 0)         │
  │                ──► Worker 1 (GPU 1)         │
  │                ──► ...                      │
  │                ──► Worker 31 (GPU 31)       │
  └────────────────────────────────────────────┘
```

---

## Current Architecture (as of 2026-03-20)

### Backend ABC

`BackendBase` (`orchestration/backends/base.py`) defines the interface:

```python
class BackendBase(ABC):
    name: ClassVar[str]
    worker_traits: ClassVar[WorkerTraits]      # worker-side behavior
    orchestrator_traits: ClassVar[OrchestratorTraits]  # post-dispatch behavior

    @abstractmethod
    def create_flow(self, resources, execution, step_number, job_name) -> Callable
    @abstractmethod
    def capture_logs(self, results, staging_root, failure_logs_root, operation_name)
    def validate_operation(self, operation)  # optional hook
```

A concrete helper `_build_prefect_flow(task_runner)` constructs the standard
Prefect flow that pickles units to disk, loads them in the flow, and maps
`execute_unit_task` across them.

### Existing Backends

| Backend        | TaskRunner                          | Worker ID Source           | Filesystem |
| -------------- | ----------------------------------- | -------------------------- | ---------- |
| `LocalBackend` | `SIGINTSafeProcessPoolTaskRunner`   | None (always 0)            | Local      |
| `SlurmBackend` | `SlurmTaskRunner` (prefect-submitit)| `SLURM_ARRAY_TASK_ID`      | Shared NFS |

### Traits System

Each backend declares two frozen dataclasses:

- **`WorkerTraits`**: `worker_id_env_var`, `shared_filesystem` — serialized to
  workers via `RuntimeEnvironment`
- **`OrchestratorTraits`**: `shared_filesystem`, `staging_verification_timeout`
  — controls post-dispatch behavior (NFS polling, fsync)

### Worker Execution

`execute_unit_task` (`orchestration/engine/dispatch.py`) is a Prefect `@task`
that is completely backend-agnostic. It reads `worker_id` from the env var
specified in `RuntimeEnvironment`, then routes to the appropriate executor
(creator, curator, or composite).

### Resource Model

`ResourceConfig` declares hardware needs portably: `cpus`, `memory_gb`, `gpus`,
`time_limit`, `extra` (escape hatch dict). Each backend translates these to
its native format.

`ExecutionConfig` controls batching: `artifacts_per_unit` (level 1),
`units_per_worker` (level 2), `max_workers`, `estimated_seconds`, `job_name`.

---

## Key Discovery: prefect-submitit Already Has Local Mode

The `prefect-submitit` package (our own Prefect wrapper for submitit) already
provides an `execution_mode="local"` on `SlurmTaskRunner`:

```python
# runner.py lines 112-137
def __enter__(self) -> Self:
    if self.execution_mode == ExecutionMode.LOCAL:
        self._executor = submitit.LocalExecutor(folder=self.log_folder)
        self._executor.update_parameters(
            timeout_min=self._parse_time_to_minutes(self.time_limit),
        )
    else:
        self._executor = submitit.AutoExecutor(...)
```

And submitit's `LocalExecutor` natively supports GPU isolation:

```python
# submitit LocalExecutor.update_parameters() accepts:
#   visible_gpus: Sequence[int]  — specific GPU indices to make visible
#   gpus_per_node: int           — number of GPUs per task
#   tasks_per_node: int          — number of concurrent tasks
```

submitit automatically sets `CUDA_VISIBLE_DEVICES` per subprocess worker
when `visible_gpus` is provided (local.py line 209). The entire GPU isolation
problem is already solved in our dependency stack — we just need to thread
the parameter through.

**This eliminates the need for a custom `GPUAwareProcessPoolTaskRunner`.** The
Prefect ↔ submitit bridge already exists and handles futures, result
collection, log capture, and job arrays in local mode.

---

## Revised Implementation Plan

The implementation spans two repos with minimal changes in each.

### Change 1: prefect-submitit — expose `visible_gpus`

Add `visible_gpus` parameter to `SlurmTaskRunner.__init__()` and pass it
through to `LocalExecutor.update_parameters()` when in local mode.

**`runner.py`** — constructor gains one new parameter:

```python
def __init__(
    self,
    # ... existing params ...
    visible_gpus: list[int] | None = None,  # NEW
    **slurm_kwargs: Any,
):
    self.visible_gpus = visible_gpus
```

**`runner.py`** — `__enter__` passes it through in local mode:

```python
if self.execution_mode == ExecutionMode.LOCAL:
    self._executor = submitit.LocalExecutor(folder=self.log_folder)
    params = {"timeout_min": self._parse_time_to_minutes(self.time_limit)}
    if self.visible_gpus is not None:
        params["visible_gpus"] = self.visible_gpus
        params["gpus_per_node"] = 1  # or from config
    self._executor.update_parameters(**params)
```

**Estimated scope:** ~10-15 lines changed in `runner.py`.

### Change 2: artisan — new `SlurmIntraBackend`

**New file: `orchestration/backends/slurm_intra.py`**

```python
import os
import warnings
from collections.abc import Callable
from pathlib import Path

from artisan.orchestration.backends.base import (
    BackendBase, OrchestratorTraits, WorkerTraits,
)
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.operation_config.resource_config import ResourceConfig


def _detect_visible_gpus() -> list[int]:
    """Detect GPUs available in the current allocation."""
    cuda_visible = os.environ.get("CUDA_VISIBLE_DEVICES", "")
    if cuda_visible:
        return [int(g) for g in cuda_visible.split(",")]
    slurm_gpus = os.environ.get("SLURM_GPUS", "")
    if slurm_gpus:
        return list(range(int(slurm_gpus)))
    return [0]


class SlurmIntraBackend(BackendBase):
    """Execute within an existing SLURM allocation (no sbatch).

    Uses prefect-submitit's local execution mode with submitit's
    LocalExecutor, which spawns subprocesses and manages
    CUDA_VISIBLE_DEVICES automatically via the visible_gpus parameter.
    """

    name = "slurm_intra"
    worker_traits = WorkerTraits(
        worker_id_env_var=None,
        shared_filesystem=True,
    )
    orchestrator_traits = OrchestratorTraits(
        shared_filesystem=True,
        staging_verification_timeout=60.0,
    )

    def create_flow(
        self,
        resources: ResourceConfig,
        execution: ExecutionConfig,
        step_number: int,
        job_name: str,
    ) -> Callable[[str, RuntimeEnvironment], list[dict]]:
        from prefect_submitit import SlurmTaskRunner

        visible_gpus = _detect_visible_gpus()
        gpus_per_task = resources.gpus or 1

        task_runner = SlurmTaskRunner(
            execution_mode="local",
            time_limit=resources.time_limit,
            visible_gpus=visible_gpus,
            units_per_worker=execution.units_per_worker,
            slurm_job_name=f"s{step_number}_{job_name}",
        )
        return self._build_prefect_flow(task_runner)

    def capture_logs(
        self,
        results: list[dict],
        staging_root: Path,
        failure_logs_root: Path | None,
        operation_name: str,
    ) -> None:
        """Local subprocesses — logs are in orchestrator stdout."""

    def validate_operation(self, operation) -> None:
        if not os.environ.get("SLURM_JOB_ID"):
            warnings.warn(
                f"Backend 'slurm_intra' selected for {operation.name!r} but "
                f"SLURM_JOB_ID is not set. Are you inside an salloc/sbatch?",
                stacklevel=2,
            )
```

**Registration in `orchestration/backends/__init__.py`** (~5 lines):

```python
from artisan.orchestration.backends.slurm_intra import SlurmIntraBackend

class Backend:
    LOCAL = LocalBackend()
    SLURM = SlurmBackend()
    SLURM_INTRA = SlurmIntraBackend()

_REGISTRY = {b.name: b for b in [Backend.LOCAL, Backend.SLURM, Backend.SLURM_INTRA]}
```

**Estimated scope:** ~80 lines new file + ~5 lines registration.

### No Other Changes Needed

- `execute_unit_task` — unchanged (already backend-agnostic)
- `_build_prefect_flow` — unchanged (works with any Prefect TaskRunner)
- `ResourceConfig` — unchanged (`gpus` field already exists)
- `ExecutionConfig` — unchanged (`max_workers` already exists)
- Step executor — unchanged (calls `backend.create_flow()` polymorphically)

### User-Facing API

```python
from artisan.orchestration.backends import Backend

pipeline.run(
    MyGPUOp,
    inputs=artifacts,
    backend=Backend.SLURM_INTRA,  # or "slurm_intra"
    resources=ResourceConfig(gpus=1, cpus=4, memory_gb=16),
    execution=ExecutionConfig(max_workers=32),
)
```

---

## Why This Is Better Than the Original Options

The original analysis (and the first draft of v2) treated the submitit ↔
Prefect bridge as a gap. But `prefect-submitit` already closes that gap:

| Concern                      | Already handled by                     |
| ---------------------------- | -------------------------------------- |
| Prefect future compatibility | `SlurmPrefectFuture` wraps submitit jobs |
| Result collection            | `_collect_results` works with any future |
| Job arrays in local mode     | `SlurmTaskRunner.map()` local path      |
| Log capture                  | submitit `LocalExecutor` writes logs    |
| GPU isolation                | submitit `visible_gpus` → `CUDA_VISIBLE_DEVICES` |

The custom `GPUAwareProcessPoolTaskRunner` from the earlier sketch is
unnecessary — it reinvents what submitit already does.

---

## Open Questions

- **Multi-node allocations**: submitit's `LocalExecutor` is single-node.
  Multi-node `salloc` would need a different approach (srun-based or Dask).
  Is single-node sufficient for v1?
- **`gpus_per_task` threading**: How should `resources.gpus` map to submitit's
  `gpus_per_node` in local mode? Currently `gpus_per_node` means "per task"
  in `LocalExecutor` context — need to verify this.
- **`tasks_per_node` vs `max_workers`**: submitit's `LocalExecutor` has
  `tasks_per_node` which controls concurrency. How does this interact with
  `execution.max_workers`? They may be the same knob.
- **Environment detection**: Should there be an `auto` mode that picks
  `SLURM_INTRA` when inside an allocation and `SLURM` otherwise?
- **Worker ID tracking**: With `worker_id_env_var=None`, all workers report
  `worker_id=0`. submitit may set its own env vars in subprocesses that we
  could use.
- **Log capture**: submitit `LocalExecutor` writes stdout/stderr to files.
  Should `capture_logs` read these (like `SlurmBackend` does) rather than
  being a no-op?

---

## References

- [submitit LocalExecutor source](https://github.com/facebookincubator/submitit/blob/main/submitit/local/local.py)
- [prefect-submitit runner.py](file:///Users/andrewhunt/git/prefect-submitit/src/prefect_submitit/runner.py)
- [Artisan BackendBase](file:///Users/andrewhunt/git/artisan-dev/src/artisan/orchestration/backends/base.py)
- [SLURM srun within allocation](https://slurm.schedmd.com/srun.html)
