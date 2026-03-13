# Design: Pipeline Cancellation — High-Impact Changes

**Date:** 2026-03-11  **Status:** Draft  **Author:** Claude + ach94

**Supersedes:** `pipeline-cancellation.md` (full design retained for future
reference; this doc scopes the first deliverable).

---

## Problem

Cancelling a running pipeline is unreliable and slow:

- **External `scancel` takes ~1 hour for large arrays.** `_collect_results()`
  in `dispatch.py` blocks sequentially on each `f.result()`. For a 300-job
  array, each cancelled job must individually timeout through submitit's
  polling before the next one is checked: ~300 × polling interval.

- **Ctrl+C leaves everything in a bad state.** `KeyboardInterrupt` propagates
  up through `PipelineManager` without calling `finalize()`. The
  `ThreadPoolExecutor` isn't shut down, SLURM jobs keep running, and storage
  may be inconsistent.

- **"Cancelled" is invisible.** `StepTracker` records running/completed/
  failed/skipped. There's no way to distinguish "failed because cancelled"
  from "failed because of a bug" when reviewing pipeline history.

---

## Approach

Three independent, high-impact changes that each ship standalone — no
interface refactors (`FlowHandle`, `BackendBase` changes) required.

---

## Change A: Parallel result collection

**File:** `src/artisan/orchestration/engine/dispatch.py`

### Problem

`_collect_results` iterates futures sequentially:

```python
# Current (dispatch.py:110)
def _collect_results(futures: list) -> list[dict]:
    results = []
    for f in futures:
        try:
            results.append(f.result())
        except Exception as exc:
            results.append({"success": False, "error": format_error(exc), ...})
    ...
    return results
```

When SLURM jobs are cancelled, each `f.result()` blocks until submitit's
polling detects the cancellation. With 300 futures collected one at a time,
this takes ~1 hour.

### Solution

Collect futures concurrently using `ThreadPoolExecutor` + `as_completed`.
All futures resolve in parallel, so total wait ≈ one polling interval
regardless of job count.

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def _collect_results(futures: list) -> list[dict]:
    """Collect results from Prefect futures, converting exceptions to failure dicts."""
    def _get_one(f):
        try:
            return f.result()
        except Exception as exc:
            logger.error(
                "Future raised during result collection: %s: %s",
                type(exc).__name__, exc,
            )
            return {
                "success": False,
                "error": format_error(exc),
                "item_count": 1,
                "execution_run_ids": [],
            }

    results = [None] * len(futures)
    max_workers = min(len(futures), 32)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        submitted = {pool.submit(_get_one, f): i for i, f in enumerate(futures)}
        for cf in as_completed(submitted):
            results[submitted[cf]] = cf.result()

    logger.info("Collected results from %d futures", len(results))

    # Best-effort SLURM log capture (unchanged)
    for future, result in zip(futures, results, strict=False):
        _capture_slurm_logs(future, result)

    return results
```

**Why `min(len(futures), 32)`:** Each thread just blocks on `f.result()` —
no CPU work. 32 is more than enough concurrency. Creating 300 threads for
300 futures wastes resources.

**Result ordering preserved:** The `submitted` dict maps each
`concurrent.futures.Future` back to its index, so `results[i]` corresponds
to `futures[i]` regardless of completion order.

**SLURM log capture unchanged:** The existing `_capture_slurm_logs` loop
runs after all results are collected, same as before.

### Impact

External `scancel` on a 300-job array goes from ~1 hour to seconds. No
interface changes, no new dependencies, no behavioral changes for the
success path.

---

## Change B: "Cancelled" step state in StepTracker

**File:** `src/artisan/orchestration/engine/step_tracker.py`

### Problem

`StepTracker` has four states: running, completed, failed, skipped. When a
pipeline is interrupted, steps are left as "running" (no terminal state
recorded) or marked "failed" — indistinguishable from actual bugs.

### Solution

Add `record_step_cancelled` following the pattern of `record_step_failed`:

```python
def record_step_cancelled(
    self,
    start_record: StepStartRecord,
    reason: str = "Pipeline cancelled",
) -> None:
    """Write a 'cancelled' row for a step that was interrupted.

    Args:
        start_record: The original StepStartRecord for metadata.
        reason: Human-readable cancellation reason.
    """
    row = {
        "step_run_id": start_record.step_run_id,
        "step_spec_id": start_record.step_spec_id,
        "pipeline_run_id": self._pipeline_run_id,
        "step_number": start_record.step_number,
        "step_name": start_record.step_name,
        "status": "cancelled",
        "operation_class": start_record.operation_class,
        "params_json": start_record.params_json,
        "input_refs_json": start_record.input_refs_json,
        "compute_backend": start_record.compute_backend,
        "compute_options_json": start_record.compute_options_json,
        "output_roles_json": start_record.output_roles_json,
        "output_types_json": start_record.output_types_json,
        "total_count": None,
        "succeeded_count": None,
        "failed_count": None,
        "timestamp": datetime.now(UTC),
        "duration_seconds": None,
        "error": reason,
        "dispatch_error": None,
        "commit_error": None,
        "metadata": None,
    }
    df = pl.DataFrame([row], schema=STEPS_SCHEMA)
    self._write_row(df)
```

**Cache behavior:** `check_cache` filters on `status == "completed"`, so
cancelled steps are automatically excluded. No change needed.

**Resume behavior:** `load_completed_steps` filters on
`status.is_in(["completed", "skipped"])`. Cancelled steps are excluded —
they'll be re-run on resume. No change needed.

### Impact

Pipeline history shows which steps were cancelled vs failed. Enables
downstream tooling (dashboards, `list_runs`) to report cancellations
accurately. This method is wired up by Change C.

---

## Change C: `PipelineManager.cancel()` with signal handling

**Files:**
- `src/artisan/orchestration/pipeline_manager.py`
- `src/artisan/orchestration/engine/step_executor.py` (cancel check between phases)

### Problem

`PipelineManager` has no `cancel()` method. Ctrl+C raises
`KeyboardInterrupt` which propagates uncontrolled — `finalize()` never
runs, the `ThreadPoolExecutor` isn't shut down, and no terminal state is
recorded for the in-flight step.

### Solution

Add a `_cancel_event` (`threading.Event`) to `PipelineManager`. Signal
handlers set the event; the step executor checks it between phases; pending
steps are skipped.

This does **not** cancel in-flight SLURM jobs. It stops the pipeline from
dispatching new steps and ensures a clean exit. Combined with Change A
(parallel collection), the current step finishes quickly when jobs are
externally cancelled.

#### PipelineManager changes

**`__init__`** — add cancel event, store signal handler refs:

```python
self._cancel_event = threading.Event()
self._prev_sigint = None
self._prev_sigterm = None
```

**`cancel()`** — public method, thread-safe, idempotent:

```python
def cancel(self) -> None:
    """Cancel the running pipeline.

    Sets the cancel event so in-flight steps exit promptly and
    pending steps are skipped. Does not cancel backend jobs — use
    scancel or similar for that.
    """
    if self._cancel_event.is_set():
        return
    logger.warning("Pipeline cancellation requested")
    self._cancel_event.set()
```

**Signal handling** — installed when step execution begins, restored in
`finalize()`:

```python
def _install_signal_handlers(self) -> None:
    try:
        self._prev_sigint = signal.signal(signal.SIGINT, self._handle_signal)
        self._prev_sigterm = signal.signal(signal.SIGTERM, self._handle_signal)
    except ValueError:
        pass  # Not in main thread (e.g., Jupyter)

def _handle_signal(self, signum, frame) -> None:
    logger.warning("Received signal %s, cancelling pipeline", signum)
    self.cancel()

def _restore_signal_handlers(self) -> None:
    try:
        if self._prev_sigint is not None:
            signal.signal(signal.SIGINT, self._prev_sigint)
        if self._prev_sigterm is not None:
            signal.signal(signal.SIGTERM, self._prev_sigterm)
    except (ValueError, AttributeError):
        pass
```

Signal handlers only set the event — no subprocess calls, no complex logic.
This is safe because `threading.Event.set()` is signal-safe in CPython.

**`submit()`** — check cancel event before dispatching:

The existing `_stopped` check (line 915) already skips steps when the
pipeline is stopped. Add a parallel check for `_cancel_event`:

```python
# At the top of submit(), after validation
if self._cancel_event.is_set():
    # Same skip logic as _stopped, but with cancel-specific metadata
    skipped_result = StepResult(
        step_name=step_name,
        step_number=step_number,
        ...
        metadata={"skipped": True, "skip_reason": "cancelled"},
    )
    ...
    return StepFuture(...)
```

**`finalize()`** — restore signals, handle cancelled state:

```python
def finalize(self) -> dict[str, Any]:
    self._restore_signal_handlers()

    cancelled = self._cancel_event.is_set()

    for step_num, future in self._active_futures.items():
        try:
            future.result(timeout=5.0 if cancelled else None)
        except Exception as exc:
            logger.error("Step %d future failed during finalize: %s", step_num, exc)

    self._executor.shutdown(wait=True)
    ...
```

When cancelled, `finalize()` uses a short timeout on active futures instead
of blocking indefinitely. The executor is still shut down cleanly.

#### Step executor changes

**`execute_step`** — accept and pass through cancel event:

`execute_step` is the public entry point called by `PipelineManager`. It
routes to `_execute_creator_step` or `_execute_curator_step` based on
operation type. Both need `cancel_event`:

```python
def execute_step(
    operation_class, inputs, params, backend, ...,
    cancel_event: threading.Event | None = None,
) -> StepResult:
    ...
    if is_curator_operation(operation):
        return _execute_curator_step(..., cancel_event=cancel_event)
    return _execute_creator_step(..., cancel_event=cancel_event)
```

**`_execute_creator_step`** — check cancel event between phases:

```python
def _execute_creator_step(
    operation, inputs, backend, ...,
    cancel_event: threading.Event | None = None,
) -> StepResult:
    # PHASE 1: DISPATCH
    ...

    # Check before expensive execution
    if cancel_event and cancel_event.is_set():
        return _cancelled_result(step_name, step_number, start_record, step_tracker)

    # PHASE 2: EXECUTE
    step_flow = backend.create_flow(...)
    results = step_flow(units_path=..., runtime_env=...)

    # Check before commit
    if cancel_event and cancel_event.is_set():
        return _cancelled_result(step_name, step_number, start_record, step_tracker)

    # PHASE 3: COMMIT
    ...
```

**`_execute_curator_step`** — same cancel checks:

```python
def _execute_curator_step(
    operation, inputs, ...,
    cancel_event: threading.Event | None = None,
) -> StepResult:
    # --- resolve_inputs phase ---
    ...

    # Check before execution
    if cancel_event and cancel_event.is_set():
        return _cancelled_result(step_name, step_number, start_record, step_tracker)

    # --- execute phase ---
    staging_result = _run_curator_in_subprocess(unit, runtime_env)
    ...

    # Check before commit
    if cancel_event and cancel_event.is_set():
        return _cancelled_result(step_name, step_number, start_record, step_tracker)

    # --- commit phase ---
    ...
```

Curator steps are typically fast (local execution), but cancel checks are
still needed for consistency — a long-running curator (e.g., large merge)
should respect cancellation at the same phase boundaries as creator steps.

The cancel check happens at two points in both paths:
- **Before execute:** Avoids dispatching work to the backend (creator) or
  spawning a subprocess (curator).
- **Before commit:** Avoids committing results from a cancelled run. The
  step is recorded as "cancelled" in the tracker.

There is no check *during* execution — the step flow / subprocess runs to
completion (or external cancellation via `scancel`). This is the deliberate
simplification that avoids the `FlowHandle` refactor.

**`_cancelled_result` helper:**

```python
def _cancelled_result(step_name, step_number, start_record, step_tracker):
    step_tracker.record_step_cancelled(start_record)
    return StepResult(
        step_name=step_name,
        step_number=step_number,
        success=False,
        total_count=0,
        succeeded_count=0,
        failed_count=0,
        output_roles=frozenset(),
        output_types={},
        metadata={"cancelled": True},
    )
```

### Cancellation flow

```
Ctrl+C (or pipeline.cancel())
  → _handle_signal sets _cancel_event
  → Current step's Phase 2 continues (step_flow runs to completion)
    → _collect_results finishes quickly (parallel, Change A)
  → Cancel check before Phase 3 → skip commit, record "cancelled"
  → Next submit() sees _cancel_event → skip, return immediately
  → finalize() restores signals, shuts down executor, returns summary
```

### Impact

- Ctrl+C produces a clean exit with `finalize()` always running
- Pipeline history shows cancelled steps distinctly
- No interface changes to `BackendBase` or `create_flow`
- Combined with Change A, the practical wall-clock time for cancellation
  is short: current step's futures resolve in parallel, then everything
  stops cleanly

---

## What this does NOT do

These three changes deliberately avoid:

- **Cancelling in-flight backend jobs.** The pipeline stops dispatching and
  exits cleanly, but SLURM jobs from the current step run to completion
  unless the user also runs `scancel`. This is the trade-off for avoiding
  the `FlowHandle` refactor.

- **Partial commits.** A cancelled step commits nothing. On resume, the
  entire step re-runs. This keeps the implementation simple and storage
  consistent.

- **`BackendBase` interface changes.** `create_flow` still returns a
  callable. The `FlowHandle` abstraction from the full design remains a
  future option for backend-aware cancellation (e.g., automatic `scancel`).

---

## Shipping order

| Order | Change | Scope | Dependencies |
|-------|--------|-------|-------------|
| First | A: Parallel result collection | `dispatch.py` only | None |
| Second | B: "Cancelled" step state | `step_tracker.py` only | None |
| Third | C: `PipelineManager.cancel()` | `pipeline_manager.py`, `step_executor.py` | Uses B |

A and B are independent and could ship in either order (or together). C
depends on B for recording the cancelled state.

---

## Testing strategy

### Change A

- Unit test: mock futures that resolve in varying order, verify results
  maintain correct index ordering
- Unit test: mock futures that raise exceptions, verify failure dicts
- Integration test (`@pytest.mark.slow`): run a small Prefect flow with
  the local backend, cancel some futures, verify parallel collection
  completes faster than sequential would

### Change B

- Unit test: `record_step_cancelled` writes correct row to delta table
- Unit test: `check_cache` excludes cancelled steps
- Unit test: `load_completed_steps` excludes cancelled steps

### Change C

- Unit test: `cancel()` sets event, is idempotent
- Unit test: `submit()` skips steps when cancel event is set
- Unit test: `finalize()` returns cleanly after cancellation
- Unit test: `_execute_creator_step` returns cancelled result when event
  is set between phases
- Unit test: `_execute_curator_step` returns cancelled result when event
  is set between phases
- Unit test: `execute_step` passes `cancel_event` through to both code paths
- Integration test: run a multi-step pipeline, cancel mid-execution,
  verify clean exit and correct step states in delta table

---

## Files affected

| File | Change |
|------|--------|
| `src/artisan/orchestration/engine/dispatch.py` | Parallel `_collect_results` |
| `src/artisan/orchestration/engine/step_tracker.py` | `record_step_cancelled` |
| `src/artisan/orchestration/pipeline_manager.py` | `cancel()`, signal handling, cancel-aware `submit()` and `finalize()` |
| `src/artisan/orchestration/engine/step_executor.py` | `cancel_event` param, checks between phases |

---

## Open questions

- **Prefect future compatibility with `ThreadPoolExecutor`.** The parallel
  collection wraps each `f.result()` call in its own thread. This should
  work regardless of whether Prefect futures support
  `concurrent.futures.as_completed()` directly — but needs verification.

- **Context manager support.** Should `PipelineManager` also support
  `with pipeline:` syntax where `__exit__` calls `cancel()` on exception
  and `finalize()` always? Useful for scripts, but the current explicit
  `finalize()` call is needed for notebooks. Could be a follow-up.

- **`_stopped` vs `_cancel_event` interaction.** Both cause step skipping
  but for different reasons. `_stopped` is set when upstream produces
  empty outputs (automatic, not an error). `_cancel_event` is user-initiated.
  They should remain separate flags with separate skip reasons.
