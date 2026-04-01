# Analysis: Intra-Allocation Orchestration

**Date:** 2026-01-27 **Status:** Research Complete **Author:** Claude

> **Note:** This analysis references the old `ComputeBackend` enum, which has
> since been replaced by the `Backend` namespace (`BackendBase` ABC). The
> intra-allocation backend is now `Backend.SLURM_INTRA` (a `SlurmIntraBackend`
> instance). See `design_backend_abstraction.md`.

---

## Executive Summary

This document analyzes options for orchestrating pipeline tasks **within a
single large Slurm allocation** (e.g., 32 GPUs, 200 CPUs) rather than submitting
individual jobs to the Slurm queue. The goal is to enable a "head node" pattern
where a master process distributes work to allocated resources with minimal
latency and overhead.

**Key Finding:** The existing codebase architecture is well-designed for this
extension. The `ComputeBackend` enum provides a clean abstraction point, and the
worker-side execution logic (`run_execution`) is already execution-model
agnostic.

---

## Problem Statement

### Current Behavior

The `SlurmTaskRunner` submits **new Slurm jobs** for each task (or batch of
tasks):

```
┌─────────────────┐         ┌──────────────────────────────┐
│  Login Node     │ sbatch  │     Slurm Queue              │
│  (Prefect Head) │ ──────► │  Job1, Job2, Job3...         │
└─────────────────┘         └──────────────────────────────┘
```

This works well for long-running tasks but has drawbacks:

- **Queue latency:** Each task waits in the Slurm queue
- **Submission overhead:** `sbatch` calls add up for many small tasks
- **Resource fragmentation:** Jobs may land on different nodes with varying
  availability

### Desired Behavior

When a user has a large allocation (e.g., `salloc --gpus=32 --cpus=200`), they
want to:

```
┌─────────────────────────────────────────────────────────┐
│           Single Large Slurm Allocation (32 GPUs)       │
│  ┌───────────────┐                                      │
│  │  Head Process │ ──► Worker 0 (GPU 0)                 │
│  │  (Orchestrator)│ ──► Worker 1 (GPU 1)                │
│  │               │ ──► ...                              │
│  │               │ ──► Worker 31 (GPU 31)               │
│  └───────────────┘                                      │
└─────────────────────────────────────────────────────────┘
```

**Benefits:**

- Zero queue latency (resources already allocated)
- Lower submission overhead (subprocess/srun vs sbatch)
- Guaranteed resource availability
- Predictable wall time

---

## Research Findings

### 1. submitit Executors

The codebase already uses
[submitit](https://github.com/facebookincubator/submitit) for Slurm integration.
submitit provides three executors:

| Executor        | Purpose                                           | GPU Control              |
| --------------- | ------------------------------------------------- | ------------------------ |
| `AutoExecutor`  | Auto-detects environment, uses Slurm if available | `gpus_per_node`          |
| `SlurmExecutor` | Direct Slurm submission                           | `gpus_per_node`, `gres`  |
| `LocalExecutor` | Subprocess-based local execution                  | `visible_gpus` parameter |

**Key insight:** `LocalExecutor` with `visible_gpus` can distribute work across
GPUs within an existing allocation:

```python
from submitit import LocalExecutor

executor = LocalExecutor(folder="logs")
executor.update_parameters(
    visible_gpus=list(range(32)),  # All 32 GPUs
    tasks_per_node=32,              # One task per GPU
    timeout_min=60
)
jobs = [executor.submit(task, item) for item in items]
```

### 2. Slurm `srun --exclusive`

Within an allocation, `srun --exclusive` acts as a "mini-scheduler":

```bash
# Inside an sbatch script or salloc session
srun --exclusive -n1 --gres=gpu:1 python worker.py &
srun --exclusive -n1 --gres=gpu:1 python worker.py &
wait
```

This provides:

- Automatic GPU isolation via `CUDA_VISIBLE_DEVICES`
- Resource accounting within the allocation
- Proper signal handling

### 3. Dask SLURMRunner (Batch Pattern)

[Dask-jobqueue](https://jobqueue.dask.org/) provides a "Batch Runner" pattern
where scheduler and workers run within a single allocation:

```python
from dask_jobqueue.slurm import SLURMRunner
from dask.distributed import Client

cluster = SLURMRunner()  # Auto-detects allocation
client = Client(cluster)
# All computation happens within the allocation
```

**Pros:** Mature, well-tested, good Prefect integration via `DaskTaskRunner`
**Cons:** Additional dependency, different execution model

### 4. Prefect Task Runners

Prefect 3 provides several built-in task runners:

| Task Runner             | Parallelism Model | Use Case                |
| ----------------------- | ----------------- | ----------------------- |
| `ThreadPoolTaskRunner`  | Threading         | I/O-bound tasks         |
| `ProcessPoolTaskRunner` | Multiprocessing   | CPU-bound tasks         |
| `DaskTaskRunner`        | Dask distributed  | Large-scale distributed |

These can be used within an allocation without any Slurm-specific code.

---

## Current Architecture Analysis

### ComputeBackend Abstraction

The `ComputeBackend` enum in `src/pipelines/schemas/enums.py` is the **single
decision point** for execution routing:

```python
class ComputeBackend(Enum):
    LOCAL = "local"      # ThreadPool execution
    SLURM = "slurm"      # Job array submission
```

### Dispatch Flow

In `src/pipelines/orchestration/step_executor.py`, the `dispatch_to_workers()`
function routes based on backend:

```python
if compute_backend == ComputeBackend.SLURM:
    flow = create_step_flow_slurm(operation, compute_options)
else:
    flow = create_step_flow_local(max_workers=4)
```

### Worker-Side Execution

The `run_execution()` function in
`src/pipelines/execution/executor_transform.py` is **already execution-model
agnostic**. It doesn't care how it was invoked (Slurm job, local thread, etc.).

### Existing worker_id Support

The infrastructure already supports worker identification:

```python
def run_execution(unit, config, worker_id=0):  # Parameter exists
    execution_run_id = generate_execution_run_id(
        spec_id=unit.execution_spec_id,
        timestamp=timestamp_start,
        worker_id=worker_id  # Included in provenance
    )
```

Currently underutilized (always 0), but the hooks are in place.

---

## Implementation Options

### Option A: New ComputeBackend Enum Value

**Approach:** Add `INTRA_ALLOCATION` as a new compute backend with dedicated
flow factory.

**Changes Required:**

1. **enums.py** (~3 lines)

   ```python
   class ComputeBackend(Enum):
       LOCAL = "local"
       SLURM = "slurm"
       INTRA_ALLOCATION = "intra_allocation"  # NEW
   ```

2. **prefect_flows.py** (~40-60 lines)

   ```python
   def create_step_flow_intra_allocation(
       operation: OperationDefinition,
       num_workers: int = None,
       visible_gpus: list[int] = None,
   ) -> Callable:
       """Create flow that distributes work within existing allocation."""

       @flow(task_runner=ProcessPoolTaskRunner(max_workers=num_workers))
       def intra_allocation_flow(units, config):
           return process_task.map(units, unmapped(config))

       return intra_allocation_flow
   ```

3. **step_executor.py** (~5 lines)
   ```python
   elif compute_backend == ComputeBackend.INTRA_ALLOCATION:
       flow = create_step_flow_intra_allocation(operation, **compute_options)
   ```

**Pros:**

- Clean separation of concerns
- Full observability in Prefect UI
- Explicit user intent

**Cons:**

- New enum value to document
- Slightly more code

**Estimated effort:** ~100-150 lines

---

### Option B: Enhanced LOCAL Backend

**Approach:** Extend `ComputeBackend.LOCAL` with GPU-aware configuration.

**Changes Required:**

1. **prefect_flows.py** (~20 lines)

   ```python
   def create_step_flow_local(
       max_workers: int = 4,
       visible_gpus: list[int] = None,  # NEW
       use_processes: bool = False,      # NEW
   ) -> Callable:
       if use_processes:
           runner = ProcessPoolTaskRunner(max_workers=max_workers)
       else:
           runner = ThreadPoolTaskRunner(max_workers=max_workers)
       # ... rest unchanged
   ```

2. **Usage:**
   ```python
   pipeline.step(
       op,
       compute_backend=ComputeBackend.LOCAL,
       compute_options={
           "max_workers": 32,
           "visible_gpus": list(range(32)),
           "use_processes": True,
       }
   )
   ```

**Pros:**

- Minimal code changes
- No new enum value
- Backwards compatible

**Cons:**

- Overloads meaning of "LOCAL"
- Less explicit about execution model

**Estimated effort:** ~30-50 lines

---

### Option C: submitit LocalExecutor Integration

**Approach:** Use `submitit.LocalExecutor` with `visible_gpus` parameter within
`SlurmTaskRunner`.

**Changes Required:**

1. **slurm_taskrunner.py** (~30 lines)

   ```python
   def __init__(
       self,
       # ... existing params
       visible_gpus: list[int] | None = None,  # NEW
   ):
       if visible_gpus is not None and compute_backend == ComputeBackend.LOCAL:
           self._executor = submitit.LocalExecutor(folder=self.log_folder)
           self._executor.update_parameters(
               visible_gpus=visible_gpus,
               tasks_per_node=len(visible_gpus),
           )
   ```

2. **Usage:**
   ```python
   @flow(task_runner=SlurmTaskRunner(
       compute_backend="local",
       visible_gpus=list(range(32)),
   ))
   def my_flow():
       return heavy_task.map(items)
   ```

**Pros:**

- Leverages existing submitit integration
- Consistent with current patterns
- GPU isolation handled automatically

**Cons:**

- Mixes concerns in SlurmTaskRunner
- Name becomes misleading ("Slurm" but running locally)

**Estimated effort:** ~50-80 lines

---

### Option D: Dask-Based Solution

**Approach:** Use `DaskTaskRunner` with `SLURMRunner` or `LocalCluster`.

**Changes Required:**

1. **New dependency:** `prefect-dask`, `dask-jobqueue`

2. **prefect_flows.py** (~40 lines)

   ```python
   from prefect_dask import DaskTaskRunner
   from dask.distributed import Client, LocalCluster

   def create_step_flow_dask_local(num_workers: int, threads_per_worker: int = 1):
       cluster = LocalCluster(
           n_workers=num_workers,
           threads_per_worker=threads_per_worker,
       )
       client = Client(cluster)

       @flow(task_runner=DaskTaskRunner(address=client.scheduler.address))
       def dask_flow(units, config):
           return process_task.map(units, unmapped(config))

       return dask_flow
   ```

**Pros:**

- Battle-tested distributed computing
- Rich dashboard and monitoring
- Handles complex dependency graphs

**Cons:**

- Additional dependencies
- Different mental model
- More complex debugging

**Estimated effort:** ~100-150 lines + dependency management

---

### Option E: srun Subprocess Wrapper

**Approach:** Create a task runner that uses `srun --exclusive` for GPU
isolation.

**Changes Required:**

1. **New file: srun_taskrunner.py** (~200 lines)

   ```python
   class SrunTaskRunner(TaskRunner):
       """Distributes tasks via srun within an allocation."""

       def submit(self, task, parameters, ...):
           proc = subprocess.Popen([
               "srun", "--exclusive", "-n1", f"--gres=gpu:{self.gpus_per_task}",
               "python", "-c", f"import cloudpickle; ..."
           ])
           return SrunFuture(proc)
   ```

**Pros:**

- Native Slurm resource management
- Proper GPU isolation
- Works with heterogeneous nodes

**Cons:**

- More complex implementation
- Subprocess management overhead
- Serialization complexity

**Estimated effort:** ~200-300 lines

---

## Comparison Matrix

| Option            | Code Changes   | New Dependencies | GPU Isolation | Observability | Complexity |
| ----------------- | -------------- | ---------------- | ------------- | ------------- | ---------- |
| A: New Backend    | ~100-150 lines | None             | Manual        | Full          | Medium     |
| B: Enhanced LOCAL | ~30-50 lines   | None             | Manual        | Full          | Low        |
| C: submitit Local | ~50-80 lines   | None             | Automatic     | Full          | Low        |
| D: Dask-Based     | ~100-150 lines | prefect-dask     | Automatic     | Rich          | High       |
| E: srun Wrapper   | ~200-300 lines | None             | Native        | Custom        | High       |

---

## Recommendations

### For Minimal Code Changes: Option B or C

Both options require <100 lines of changes and leverage existing infrastructure:

- **Option B** if you want to keep using Prefect's native task runners
- **Option C** if you want automatic GPU isolation via submitit

### For Clean Architecture: Option A

If the feature will be used frequently and needs to be clearly documented, a
dedicated `ComputeBackend.INTRA_ALLOCATION` makes the intent explicit.

### For Maximum Scalability: Option D

If you anticipate complex workflows with many dependencies and need rich
monitoring, Dask provides the most mature distributed computing foundation.

### Not Recommended: Option E

The `srun` wrapper approach adds significant complexity without proportional
benefits. The other options achieve similar results with less code.

---

## Suggested Implementation Path

1. **Phase 1:** Implement Option B (Enhanced LOCAL)
   - Quick win with minimal changes
   - Validates the approach works for your workloads

2. **Phase 2:** If Phase 1 is successful, consider Option A
   - Refactor to dedicated backend for clarity
   - Add proper documentation and examples

3. **Phase 3:** Evaluate Dask if scaling needs grow
   - Only if ProcessPool-based approach hits limits

---

## Open Questions

1. **GPU Assignment Strategy:** Should GPUs be assigned round-robin, or should
   users specify explicit mappings?

2. **Error Handling:** How should partial failures be handled? (Some workers
   succeed, others fail)

3. **Resource Heterogeneity:** What if different tasks need different GPU
   counts?

4. **Prefect UI:** Should intra-allocation tasks appear as individual task runs
   or as a single batched run?

5. **Environment Detection:** Should the system auto-detect when running inside
   an allocation and switch modes?

---

## References

- [submitit GitHub](https://github.com/facebookincubator/submitit)
- [Dask HPC documentation](https://docs.dask.org/en/stable/deploying-hpc.html)
- [Dask-jobqueue SLURMCluster](https://jobqueue.dask.org/en/latest/generated/dask_jobqueue.SLURMCluster.html)
- [Prefect Task Runners](https://docs.prefect.io/3.0/develop/task-runners)
- [Slurm srun documentation](https://slurm.schedmd.com/srun.html)
- [Princeton Slurm guide](https://researchcomputing.princeton.edu/support/knowledge-base/slurm)
