# Design: DispatchHandle

**Date:** 2026-03-24 (updated 2026-04-02)
**Status:** Active

---

## Summary

Replace the fire-and-forget callable returned by `BackendBase.create_flow()`
with a `DispatchHandle` that gives the orchestration layer control over
in-flight backend work: start, poll, collect results, and cancel.

Serves two purposes:

- **Cancellation of in-flight backend work.** Pipeline-level cancellation
  already exists (`PipelineManager.cancel()`, signal handling,
  `record_step_cancelled`), but it only prevents future steps from
  starting — it cannot stop work already dispatched to a backend.
  `DispatchHandle.cancel()` gives each backend a way to stop its own
  in-flight work (SLURM: `scancel`, local: cancel event, K8s: delete pods).

- **Non-blocking dispatch for the step scheduler.** The streaming
  step scheduler cannot block one thread per in-flight task. It needs to
  fire off backend work and poll for completion. `dispatch()` / `is_done()`
  / `collect()` provide this.

- **Handle-owned unit transport for cloud backends.** Cloud backends
  (Modal, Ray, Lambda) send units through their native dispatch
  mechanism, not through a shared filesystem. The handle owns unit
  transport — `dispatch()` takes units directly and decides how to
  deliver them. See `cloud-storage-design.md` for the full storage
  design.

---

## Interface

```python
class DispatchHandle(ABC):
    @abstractmethod
    def dispatch(
        self,
        units: list[ExecutionUnit | ExecutionComposite],
        runtime_env: RuntimeEnvironment,
    ) -> None:
        """Start execution, return immediately.

        The handle owns unit transport — it decides how to deliver
        units to workers. Shared-FS handles write a pickle file.
        Cloud handles send units through their native mechanism
        (Modal .map() args, Ray object store, etc.).
        """

    @abstractmethod
    def is_done(self) -> bool:
        """Non-blocking completion check. Thread-safe."""

    @abstractmethod
    def collect(self) -> list[dict]:
        """Get results. Valid only after is_done() returns True."""

    @abstractmethod
    def cancel(self) -> None:
        """Cancel in-flight work. Thread-safe, idempotent."""

    def run(
        self,
        units: list[ExecutionUnit | ExecutionComposite],
        runtime_env: RuntimeEnvironment,
    ) -> list[dict]:
        """Execute the step. Blocks until completion or cancellation.

        Concrete template method: dispatch() + poll is_done() + collect().
        """
        self.dispatch(units, runtime_env)
        while not self.is_done():
            time.sleep(0.1)
        return self.collect()
```

`run()` is a concrete template method. Non-streaming pipelines use `run()`.
The step scheduler uses the non-blocking methods.

**State machine:** `dispatch()` must be called exactly once. Calling
`is_done()` or `collect()` before `dispatch()` raises `RuntimeError`.
Calling `dispatch()` twice raises `RuntimeError`. `cancel()` is valid in
any state (no-op if not dispatched or already done). If `dispatch()` itself
fails (e.g. SLURM submission error), it raises immediately — the handle
is not recoverable.

**Unit transport is handle-owned.** The previous design passed a
`units_path` (file path to pickled units). The handle now receives units
directly and decides how to deliver them:

- **LocalDispatchHandle:** passes units through ProcessPool
  (multiprocessing pickle serialization, same as current behavior)
- **SlurmDispatchHandle:** writes pickle to shared filesystem via
  fsspec, workers read from the same path
- **ModalDispatchHandle:** passes units as arguments to
  `modal.Function.map()`
- **K8sDispatchHandle:** writes pickle to fsspec (NFS PVC or S3),
  workers read via fsspec
- **RayDispatchHandle:** puts units in Ray object store

`_save_units()` and `_load_units()` move from `dispatch.py` into the
shared-FS handle implementations. `dispatch.py` retains
`execute_unit_task` and `_collect_results`.

---

## Backend implementations

### LocalDispatchHandle

- `dispatch(units, runtime_env)` enters the Prefect task runner context,
  passes units directly to `execute_unit_task.map()` (no pickle file —
  multiprocessing serializes them), and stores the resulting Prefect
  futures.
- `is_done()` checks whether all Prefect futures have resolved.
- `collect()` gathers results from Prefect futures via `_collect_results`,
  then exits the task runner context.
- `cancel()` sets a `threading.Event`. The `_collect_results` call fills
  unresolved futures with cancellation markers. The task runner context
  exit shuts down the process pool.

### SlurmDispatchHandle

- `dispatch(units, runtime_env)` writes units to a pickle file on the
  shared filesystem (via fsspec), then submits via `srun`/`sbatch`.
  Returns immediately after submission.
- `is_done()` checks job status (sacct or job state polling).
- `collect()` gathers results from completed futures, captures SLURM logs.
- `cancel()` calls `scancel --name <job_name>` (works even before job IDs
  are available, because `SlurmTaskRunner` sets `--job-name`). Also cancels
  by ID for any captured job IDs.

`scancel --name` is the key mechanism — it cancels by the job name already
set in `SlurmBackend.create_flow`, so it works before submitit returns IDs
and harmlessly no-ops for non-existent names.

---

## Cancellation-aware result collection

`_collect_results` in `dispatch.py` already collects futures in parallel
via `ThreadPoolExecutor`. Add a `cancel_event` parameter: when set, fill
remaining unresolved futures with cancellation markers instead of waiting.

The cancel event flows from `DispatchHandle` → `collect()` →
`_collect_results` (in `engine/dispatch.py`).

---

## Refactoring `_build_prefect_flow` and `dispatch.py`

`BackendBase._build_prefect_flow()` currently builds a Prefect `@flow`
closure that loads units, maps `execute_unit_task`, and calls
`_collect_results` — all in one blocking call. Both `LocalBackend` and
`SlurmBackend` delegate to it.

With `DispatchHandle`, these steps are split across `dispatch()` and
`collect()`, so the monolithic flow closure no longer fits.

**Removed from `BackendBase`:** `_build_prefect_flow()`. No
backwards-compat shim — call sites are updated in the same change.

**Moved from `dispatch.py` into handle implementations:**
`_save_units()` and `_load_units()`. These are only needed by shared-FS
handles (SLURM, K8s+NFS) that write pickle files. The local handle
doesn't need them (multiprocessing serializes directly). Cloud handles
don't need them (units travel through native transport).

**Stays in `dispatch.py`:** `execute_unit_task` (the Prefect `@task`
that runs one unit) and `_collect_results` (parallel future collection).
These are reused by all Prefect-based handles.

Each handle's `dispatch()` enters the task runner context and calls
`execute_unit_task.map()`. Each handle's `collect()` calls
`_collect_results(futures, cancel_event)`.

---

## Pipeline-level cancellation

`PipelineManager` already has `cancel()`, `_cancel_event`, signal handling
(SIGINT/SIGTERM), `record_step_cancelled`, and the "cancelled" step state.
The new work is wiring `cancel()` to also call `handle.cancel()` on active
dispatch handles, so that in-flight backend work is stopped — not just
future steps.

- `PipelineManager` tracks active `DispatchHandle` instances
- `cancel()` iterates active handles and calls `handle.cancel()`
- The pipeline manager doesn't know or care what `cancel()` does
  internally — the backend handles cleanup

**Interaction with existing cancel checks.** `execute_step` already checks
`cancel_event` between phases (before dispatch, before commit). Those
checks remain — they prevent wasted work when cancellation happens between
phases. `handle.cancel()` is complementary: it stops work *during* the
dispatch phase. Both are needed.

---

## Updated BackendBase interface

```python
class BackendBase(ABC):
    @abstractmethod
    def create_dispatch_handle(
        self,
        resources: ResourceConfig,
        execution: ExecutionConfig,
        step_number: int,
        job_name: str,
    ) -> DispatchHandle:
        """Build a configured dispatch handle for this backend."""
```

Return type changes from `Callable` to `DispatchHandle`. Method renamed
from `create_flow` to `create_dispatch_handle` (it no longer returns a
flow). Call sites change:

```python
# Before
step_flow = backend.create_flow(...)
units_path = _save_units(units, staging_root, step_number)
results = step_flow(units_path=str(units_path), runtime_env=runtime_env)

# After
handle = backend.create_dispatch_handle(...)
results = handle.run(units=units_to_dispatch, runtime_env=runtime_env)
```

The orchestrator no longer calls `_save_units()` — the handle owns unit
transport.

---

## Scope

| File | Change |
|------|--------|
| `orchestration/backends/base.py` | `DispatchHandle` ABC, rename `create_flow` → `create_dispatch_handle`, remove `_build_prefect_flow` |
| `orchestration/backends/local.py` | `LocalDispatchHandle` (units passed in-memory via ProcessPool) |
| `orchestration/backends/slurm.py` | `SlurmDispatchHandle` with scancel + pickle-to-fs unit transport |
| `orchestration/engine/dispatch.py` | Move `_save_units`/`_load_units` to handle implementations, add `cancel_event` param on `_collect_results` |
| `orchestration/engine/step_executor.py` | Use `handle.run(units, runtime_env)` instead of `step_flow(units_path, runtime_env)`, remove `_save_units` call |
| `orchestration/pipeline_manager.py` | Track active dispatch handles, wire `cancel()` to call `handle.cancel()` |

---

## Open Questions

- **Prefect future compatibility.** Do Prefect futures support
  `concurrent.futures.as_completed()`? If not, the ThreadPoolExecutor
  wrapper in `_collect_results` is the fallback.
- **Partial commit on cancellation.** Should a cancelled step commit the
  units that succeeded before cancellation? Current behavior: incomplete
  step → nothing committed → resume re-runs entire step.
- **Context manager.** Should `PipelineManager` support `with` for
  automatic cancel-on-exception + finalize? Cleaner for scripts; explicit
  `finalize()` needed for notebooks.

---

## Related docs

- `cloud-deployment.md` — overall cloud deployment design (references
  this doc for dispatch abstraction)
- `cloud-storage-design.md` — fsspec-based storage design that motivated
  handle-owned unit transport
