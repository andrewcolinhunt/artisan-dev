# Design: DispatchHandle

**Date:** 2026-03-24
**Status:** Draft

---

## Summary

Replace the fire-and-forget callable returned by `BackendBase.create_flow()`
with a `DispatchHandle` that gives the orchestration layer control over
in-flight backend work: start, poll, collect results, and cancel.

Serves two purposes:

- **Cancellation.** Currently there is no way to cancel in-flight backend
  work. `create_flow()` returns a callable — once called, the caller is
  blocked until all futures resolve. `DispatchHandle.cancel()` gives each
  backend a way to stop its own work (SLURM: `scancel`, local: cancel
  event, K8s: delete pods).

- **Non-blocking dispatch for the step scheduler.** The streaming
  step scheduler cannot block one thread per in-flight task. It needs to
  fire off backend work and poll for completion. `dispatch()` / `is_done()`
  / `collect()` provide this.

---

## Interface

```python
class DispatchHandle(ABC):
    def run(self, units_path: str, runtime_env: RuntimeEnvironment) -> list[dict]:
        """Execute the step. Blocks until completion or cancellation."""

    def dispatch(self, units_path: str, runtime_env: RuntimeEnvironment) -> None:
        """Start execution, return immediately."""

    def is_done(self) -> bool:
        """Non-blocking completion check."""

    def collect(self) -> list[dict]:
        """Get results. Valid only after is_done() returns True."""

    def cancel(self) -> None:
        """Cancel in-flight work. Thread-safe, idempotent."""
```

`run()` is equivalent to `dispatch()` + poll `is_done()` + `collect()`.
Non-streaming pipelines use `run()`. The step scheduler uses the non-blocking
methods.

---

## Backend implementations

### LocalDispatchHandle

- `dispatch()` submits units to `ProcessPoolExecutor` via Prefect flow,
  stores the futures.
- `is_done()` checks whether all futures have resolved.
- `collect()` gathers results from futures via `_collect_results`.
- `cancel()` sets cancel event. Prefect handles pool shutdown when the
  flow exits.

### SlurmDispatchHandle

- `dispatch()` submits via `srun`/`sbatch`. Returns immediately after
  submission.
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

The cancel event flows from `DispatchHandle` → `_build_prefect_flow` →
`_collect_results`.

---

## Pipeline-level cancellation

With `DispatchHandle`, `PipelineManager.cancel()` is straightforward:

- Set `_cancel_event`
- Call `cancel()` on active dispatch handles
- Pending steps see the cancel event and skip

Signal handling (SIGINT, SIGTERM) calls `cancel()`. The pipeline manager
doesn't know or care what `cancel()` does internally — the backend handles
cleanup.

### Cancelled step state

Add `record_step_cancelled` to `StepTracker`. Cancelled steps are
distinguishable from failed steps. `load_completed_steps` treats
"cancelled" like "failed" — not eligible for cache hits.

---

## Updated BackendBase interface

```python
class BackendBase(ABC):
    @abstractmethod
    def create_flow(
        self,
        resources: ResourceConfig,
        execution: ExecutionConfig,
        step_number: int,
        job_name: str,
    ) -> DispatchHandle:
        """Build a configured dispatch handle for this backend."""
```

Return type changes from `Callable` to `DispatchHandle`. Call sites change:

```python
# Before
step_flow = backend.create_flow(...)
results = step_flow(units_path=..., runtime_env=...)

# After
handle = backend.create_flow(...)
results = handle.run(units_path=..., runtime_env=...)
```

---

## Scope

| File | Change |
|------|--------|
| `orchestration/backends/base.py` | `DispatchHandle` ABC, updated `create_flow` return type |
| `orchestration/backends/local.py` | `LocalDispatchHandle` |
| `orchestration/backends/slurm.py` | `SlurmDispatchHandle` with scancel |
| `orchestration/engine/dispatch.py` | `cancel_event` param on `_collect_results` |
| `orchestration/engine/step_executor.py` | Use `handle.run()`, check cancel_event between phases |
| `orchestration/engine/step_tracker.py` | Add "cancelled" state |
| `orchestration/pipeline_manager.py` | `cancel()`, signal handling, dispatch handle tracking |

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
