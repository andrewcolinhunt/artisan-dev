# Design: Cancel-Aware Result Collection

**Date:** 2026-03-12  **Status:** Draft  **Author:** Claude + ach94

---

## Problem

After pressing Ctrl+C during a SLURM pipeline, the cancel event is set and
`finalize()` returns a summary, but the process hangs for up to minutes until
all SLURM jobs finish. The step thread stays alive inside `_collect_results()`,
blocked on `SlurmPrefectFuture.wait()` which polls squeue every 5s until the
job completes or times out.

Observed timeline from `cancel_demo_slurm.py`:

```
16:53:31  Ctrl+C → cancel event set
16:54:02  finalize() returns (gave up on future after 5s timeout)
16:54:34  step thread finally exits (SLURM job finished on cluster)
          Logs "Collected results from 1 futures" AFTER summary printed
```

The process doesn't exit until 16:54:34 because `_run()` is a non-daemon
thread blocked inside the Prefect flow.

## Root Cause

`_collect_results()` in `dispatch.py` has no cancel awareness. The call chain:

```
_run()                                    [pipeline-step thread]
  → execute_step()
    → _execute_creator_step()
      → step_flow(units_path, runtime_env) ← blocking Prefect @flow call
        → _collect_results(futures)         ← blocks here
          → _get_one(f) per future
            → SlurmPrefectFuture.result()
              → .wait()                     ← polls squeue every 5s forever
```

The cancel_event exists at the `_execute_creator_step` level but is never
passed through to the flow or result collection. There is no mechanism to
interrupt the `.wait()` polling loop from outside.

## Key Observations

- `SlurmArrayPrefectFuture.cancel()` already exists — it runs
  `scancel <array_job_id>` and returns True/False
- `SlurmPrefectFuture.wait()` already treats `CANCELLED` as a terminal
  failure state — it raises `SlurmJobFailed` immediately
- `_get_one()` already catches all exceptions and converts them to failure
  dicts
- `_collect_results()` already uses a `ThreadPoolExecutor` for parallel
  collection

So if we call `.cancel()` on the SLURM futures when the cancel event fires,
the entire chain unblocks naturally:

```
cancel_event fires
  → future.cancel() calls scancel
    → SLURM jobs enter CANCELLED state
      → .wait() sees CANCELLED → raises SlurmJobFailed
        → _get_one catches it → returns failure dict
          → _collect_results returns
            → step_flow returns → _run returns → thread exits
```

## Design

Thread the cancel_event from `_execute_creator_step` through the flow closure
into `_collect_results`. Add a cancel-watcher daemon thread that calls
`future.cancel()` on SLURM futures when the event fires.

### Change 1: `_collect_results` — cancel watcher

`src/artisan/orchestration/engine/dispatch.py`

```python
def _cancel_futures(futures: list) -> None:
    """Call cancel() on futures that support it (SLURM futures)."""
    seen: set[str] = set()
    for f in futures:
        # SlurmArrayPrefectFuture.cancel() scancels the whole array —
        # deduplicate by array_job_id to avoid redundant scancel calls
        job_id = getattr(f, "array_job_id", None)
        if job_id and job_id in seen:
            continue
        if job_id:
            seen.add(job_id)
        if hasattr(f, "cancel"):
            try:
                f.cancel()
            except Exception:
                pass


def _collect_results(
    futures: list,
    cancel_event: threading.Event | None = None,
) -> list[dict]:
    if not futures:
        return []

    results: list[dict | None] = [None] * len(futures)
    max_workers = min(len(futures), 32)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        submitted = {pool.submit(_get_one, f): i for i, f in enumerate(futures)}

        # Daemon thread: when cancel fires, scancel all SLURM jobs
        if cancel_event is not None:
            def _watch():
                cancel_event.wait()
                _cancel_futures(futures)
            watcher = threading.Thread(target=_watch, daemon=True)
            watcher.start()

        for cf in as_completed(submitted):
            idx = submitted[cf]
            results[idx] = cf.result()

    # ... rest unchanged ...
```

The cancel watcher is a daemon thread — if the main process exits, it dies.
When cancel fires, it scancels all SLURM jobs. The `.wait()` loops in each
`_get_one` thread see `CANCELLED` state and resolve within one poll interval
(~5s).

For `SlurmBatchedItemFuture`, it delegates `.cancel()` doesn't exist directly,
but the watcher checks for `cancel()` via `hasattr`, so non-SLURM futures
(local ProcessPool) are skipped harmlessly.

### Change 2: `_build_prefect_flow` — pass cancel_event

`src/artisan/orchestration/backends/base.py`

```python
def _build_prefect_flow(self, task_runner, cancel_event=None):
    @flow(task_runner=task_runner)
    def step_flow(units_path, runtime_env):
        units = _load_units(Path(units_path))
        futures = execute_unit_task.map(units, runtime_env=unmapped(runtime_env))
        return _collect_results(futures, cancel_event=cancel_event)
    return step_flow
```

### Change 3: `create_flow` — accept cancel_event

`src/artisan/orchestration/backends/local.py` and `slurm.py`

Add `cancel_event: threading.Event | None = None` parameter to `create_flow`
in both backends. Pass through to `_build_prefect_flow`.

The `BackendBase` ABC signature also gets the parameter (with default None).

### Change 4: `_execute_creator_step` — pass cancel_event to flow

`src/artisan/orchestration/engine/step_executor.py`

```python
step_flow = backend.create_flow(
    operation.resources,
    operation.execution,
    step_number,
    job_name=...,
    cancel_event=cancel_event,   # NEW
)
```

Same change for the composite step path.

## Files to modify

| File | Change |
|------|--------|
| `src/artisan/orchestration/engine/dispatch.py` | `_cancel_futures`, cancel_event param on `_collect_results` |
| `src/artisan/orchestration/backends/base.py` | cancel_event param on `_build_prefect_flow` and `create_flow` ABC |
| `src/artisan/orchestration/backends/local.py` | cancel_event param on `create_flow` |
| `src/artisan/orchestration/backends/slurm.py` | cancel_event param on `create_flow` |
| `src/artisan/orchestration/engine/step_executor.py` | Pass cancel_event to `create_flow` (creator + composite paths) |

## What this does NOT do

- No `FlowHandle` refactor (that's the `pipeline-cancellation-full.md` design)
- No `scancel --name` (requires the FlowHandle to own the job name)
- No active SLURM monitoring thread
- No changes to `finalize()` or `PipelineManager`

This is the minimal change to make the step thread exit promptly on cancel.
The full FlowHandle design remains the future path.

## Testing

### Unit tests
- `_cancel_futures` calls `.cancel()` on futures that have it, deduplicates
  by `array_job_id`
- `_collect_results` with pre-set cancel_event returns promptly (mock futures)
- `create_flow` passes cancel_event through to `_build_prefect_flow`

### Manual verification
```bash
# SLURM: Ctrl+C should scancel the array job and exit within ~10s
~/.pixi/bin/pixi run python _dev/demos/slurm-cancel/demo_slurm_cancel.py
# Verify with squeue that jobs are cancelled
```

### CI
```bash
~/.pixi/bin/pixi run -e dev fmt
~/.pixi/bin/pixi run -e dev test-unit
```
