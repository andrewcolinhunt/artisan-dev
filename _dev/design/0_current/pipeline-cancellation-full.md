# Design: Pipeline and Job Cancellation

**Date:** 2026-03-11  **Status:** Draft  **Author:** Claude + ach94

---

## Problem

Cancelling a running pipeline is unreliable and slow:

- **No programmatic cancellation.** `PipelineManager` has no `cancel()` or
  `stop()` method. The only way to halt a pipeline is Ctrl+C, which leaves
  SLURM jobs running and skips `finalize()`.

- **External `scancel` is slow.** When a user cancels a 300-job SLURM array
  with `scancel`, it takes ~1 hour for all cancellations to propagate. This is
  because `_collect_results()` blocks sequentially on each `f.result()` —
  each cancelled job must individually timeout or error out through submitit's
  polling before the next one is checked.

- **No backend-aware cancellation.** Artisan never calls `scancel`, `squeue`,
  or any SLURM CLI tool. Job lifecycle is fully delegated to
  `prefect_submitit`, with no mechanism to proactively cancel or monitor
  jobs. The `BackendBase` ABC has no cancellation interface — `create_flow()`
  returns a fire-and-forget callable with no handle to stop it.

- **No "cancelled" step state.** `StepTracker` only records
  running/completed/failed/skipped — there's no way to distinguish "failed
  because cancelled" from "failed because of a bug."

### Current call chain on Ctrl+C

```
KeyboardInterrupt
  → PipelineManager: propagates up, finalize() never called
    → ThreadPoolExecutor: not shut down
      → _execute_creator_step: blocked in step_flow()
        → Prefect flow: blocked in _collect_results()
          → f.result(): waiting on submitit future
            → SLURM job: still running, never scancel'd
```

### Current call chain on external scancel

```
User runs: scancel <array_job_id>

_collect_results() iterating futures sequentially:
  → f.result() for future 0: submitit detects CANCELLED (polling interval)
  → f.result() for future 1: submitit detects CANCELLED (polling interval)
  → ... × 300 futures, each waiting for submitit's poll cycle
  → Total time: ~300 × polling_interval
```

---

## Goals

- **Pipeline-level cancellation** that is clean, fast, and leaves storage in a
  consistent state.
- **Backend-scoped job cleanup** — each backend knows how to cancel its own
  in-flight work (SLURM: `scancel`, local: kill processes, future backends:
  their own mechanism).
- **Fast detection of externally cancelled jobs** — when a user runs `scancel`
  directly, the pipeline should detect this in seconds, not minutes.
- **Observable state** — cancelled steps should be distinguishable from failed
  steps in the step tracker and pipeline results.

## Non-goals

- Real-time progress reporting (separate concern, useful but orthogonal).
- Graceful mid-unit cancellation (workers can be killed; we don't need
  cooperative cancellation within an operation's `execute()` method).
- Automatic retry after cancellation (use the existing resume mechanism).

---

## Current architecture (relevant pieces)

| Component | Role | Cancellation awareness |
|-----------|------|----------------------|
| `PipelineManager` | Orchestrates steps via `ThreadPoolExecutor(1)` | `_stopped` flag exists but only for empty-inputs propagation |
| `_execute_creator_step` (in `step_executor.py`) | Three-phase: dispatch → execute → commit | No cancellation; `step_flow()` is a blocking call |
| `BackendBase` | ABC with `create_flow`, `capture_logs`, `validate_operation` | No cancellation interface |
| `BackendBase.create_flow` | Returns `Callable[[str, RuntimeEnvironment], list[dict]]` | Fire-and-forget — no handle to stop |
| `BackendBase._build_prefect_flow` | Builds Prefect flow, calls `_collect_results` inside | Sequential future collection, no cancellation check |
| `SlurmBackend` | Configures `SlurmTaskRunner`, delegates to `_build_prefect_flow` | No `scancel`, no job ID tracking |
| `LocalBackend` | Configures `ProcessPoolTaskRunner` | No process termination on cancel |
| `dispatch._collect_results` | Iterates futures calling `f.result()` sequentially | Bottleneck: sequential blocking |
| `StepTracker` | Records step lifecycle to Delta Lake | States: running, completed, failed, skipped |
| `StepFuture` | Wraps `concurrent.futures.Future` | No `cancel()` method |
| `external_tools.py` | Subprocess cleanup on interrupt | Handles SIGTERM/SIGKILL for child processes |

### The core interface gap

`BackendBase.create_flow()` returns a plain callable:

```python
def create_flow(self, ...) -> Callable[[str, RuntimeEnvironment], list[dict]]:
```

Once `step_flow(units_path, runtime_env)` is called, there is no handle to
the in-flight work. The caller is blocked until all futures resolve. The
backend cannot be asked to cancel because there's nothing to cancel *on*.

This is the fundamental design gap: **cancellation must be a backend
responsibility** because only the backend knows how to stop its own workers
(SLURM: `scancel`, local: kill processes, K8s: delete pods), but the
current interface gives the backend no way to express that.

---

## Design

### Core abstraction: `FlowHandle`

Replace the fire-and-forget callable with a richer object that gives the
caller a cancellation handle while the backend retains ownership of cleanup.

```python
class FlowHandle(ABC):
    """Handle to an in-flight step execution.

    Returned by BackendBase.create_flow(). The caller starts execution
    with run(), and can cancel at any time from another thread.
    """

    @abstractmethod
    def run(self, units_path: str, runtime_env: RuntimeEnvironment) -> list[dict]:
        """Execute the step. Blocks until completion or cancellation.

        Returns:
            Result dicts for each unit.

        Raises:
            CancelledError: If cancel() was called during execution.
        """
        ...

    @abstractmethod
    def cancel(self) -> None:
        """Cancel in-flight work. Thread-safe, idempotent.

        Backend-specific cleanup: SLURM calls scancel, local kills
        processes, etc. Must be safe to call from any thread (signal
        handler, PipelineManager.cancel(), etc.).

        After cancel(), run() should return or raise promptly.
        """
        ...
```

This is the key insight: **the backend owns both dispatch and cancellation**.
The pipeline manager doesn't need to know about `scancel` vs process
killing — it just calls `handle.cancel()`.

### Backend implementations

#### `SlurmFlowHandle`

```python
class SlurmFlowHandle(FlowHandle):
    def __init__(self, task_runner, job_name: str):
        self._task_runner = task_runner
        self._job_name = job_name
        self._cancel_event = threading.Event()
        self._slurm_job_ids: list[str] = []

    def run(self, units_path, runtime_env):
        # Build and run Prefect flow
        flow = self._build_flow()
        # The flow internally populates self._slurm_job_ids
        # as futures come back from submitit
        return flow(units_path=units_path, runtime_env=runtime_env)

    def cancel(self):
        self._cancel_event.set()
        # scancel by job name — works even before we have job IDs,
        # because SlurmTaskRunner sets --job-name
        try:
            subprocess.run(
                ["scancel", "--name", self._job_name],
                timeout=10, capture_output=True,
            )
        except Exception:
            logger.debug("scancel --name %s failed", self._job_name, exc_info=True)

        # Also cancel by ID for any IDs we've captured
        for job_id in self._slurm_job_ids:
            try:
                subprocess.run(
                    ["scancel", str(job_id)],
                    timeout=10, capture_output=True,
                )
            except Exception:
                pass
```

Using `scancel --name <job_name>` is the key trick here. We already set
`slurm_job_name=f"s{step_number}_{job_name}"` in `SlurmBackend.create_flow`.
This means we can cancel jobs *before* submitit returns job IDs, and even
cancel jobs that haven't been submitted yet (SLURM rejects the scancel
harmlessly for non-existent names).

#### `LocalFlowHandle`

```python
class LocalFlowHandle(FlowHandle):
    def __init__(self, task_runner):
        self._task_runner = task_runner
        self._cancel_event = threading.Event()

    def run(self, units_path, runtime_env):
        flow = self._build_flow()
        return flow(units_path=units_path, runtime_env=runtime_env)

    def cancel(self):
        self._cancel_event.set()
        # ProcessPoolTaskRunner: Prefect handles worker shutdown
        # when the flow is cancelled. The cancel_event causes
        # _collect_results to stop waiting.
```

For local, cancellation is simpler — the process pool lives in the same
machine. The main mechanism is the cancel event interrupting result
collection. Prefect's ProcessPoolTaskRunner handles worker cleanup when
the flow exits.

### Cancellation-aware result collection

The `_collect_results` function (inside `_build_prefect_flow`) needs two
changes:

**Parallel collection** — so one slow future doesn't block the rest:

```python
def _collect_results(futures, cancel_event=None):
    def _get_one(f):
        try:
            return f.result()
        except Exception as exc:
            return {"success": False, "error": format_error(exc), ...}

    with ThreadPoolExecutor(max_workers=len(futures)) as pool:
        result_futures = {pool.submit(_get_one, f): i for i, f in enumerate(futures)}
        results = [None] * len(futures)
        for cf in as_completed(result_futures):
            idx = result_futures[cf]
            results[idx] = cf.result()
            if cancel_event and cancel_event.is_set():
                # Fill remaining with cancellation markers
                for i, r in enumerate(results):
                    if r is None:
                        results[i] = {
                            "success": False,
                            "error": "Cancelled",
                            "item_count": 1,
                            "execution_run_ids": [],
                        }
                break
    return results
```

**Cancel event check** — so `cancel()` interrupts result collection
promptly rather than waiting for all futures to time out.

The cancel event flows from `FlowHandle` into `_collect_results` via the
Prefect flow closure:

```python
# In BackendBase._build_prefect_flow
def _build_prefect_flow(self, task_runner, cancel_event=None):
    @flow(task_runner=task_runner)
    def step_flow(units_path, runtime_env):
        units = _load_units(Path(units_path))
        futures = execute_unit_task.map(units, runtime_env=unmapped(runtime_env))
        return _collect_results(futures, cancel_event=cancel_event)
    return step_flow
```

### Updated `BackendBase` interface

```python
class BackendBase(ABC):
    # ... existing ClassVars ...

    @abstractmethod
    def create_flow(
        self,
        resources: ResourceConfig,
        execution: ExecutionConfig,
        step_number: int,
        job_name: str,
    ) -> FlowHandle:
        """Build a configured flow handle for this backend.

        Returns:
            FlowHandle with run() and cancel() methods.
        """
        ...

    # capture_logs and validate_operation unchanged
```

The return type changes from `Callable` to `FlowHandle`. This is the only
breaking change to the backend interface. Existing call sites change from:

```python
# Before
step_flow = backend.create_flow(...)
results = step_flow(units_path=..., runtime_env=...)

# After
flow_handle = backend.create_flow(...)
results = flow_handle.run(units_path=..., runtime_env=...)
```

### Pipeline-level cancellation

With `FlowHandle` in place, `PipelineManager.cancel()` is simple:

```python
class PipelineManager:
    def __init__(self, ...):
        ...
        self._cancel_event = threading.Event()
        self._active_flow_handle: FlowHandle | None = None

    def cancel(self) -> None:
        """Cancel the running pipeline.

        In-flight backend jobs are cancelled. Pending steps are skipped.
        """
        if self._cancel_event.is_set():
            return
        logger.warning("Pipeline cancellation requested")
        self._cancel_event.set()
        if self._active_flow_handle:
            self._active_flow_handle.cancel()
```

The pipeline manager doesn't know or care what `cancel()` does internally —
the backend handles it.

#### Signal handling

```python
def _install_signal_handlers(self) -> None:
    try:
        self._prev_sigint = signal.signal(signal.SIGINT, self._handle_interrupt)
        self._prev_sigterm = signal.signal(signal.SIGTERM, self._handle_interrupt)
    except ValueError:
        pass  # Not in main thread (e.g., Jupyter)

def _handle_interrupt(self, signum, frame):
    logger.warning("Received signal %s, cancelling pipeline", signum)
    self.cancel()

def _restore_signal_handlers(self) -> None:
    try:
        signal.signal(signal.SIGINT, self._prev_sigint)
        signal.signal(signal.SIGTERM, self._prev_sigterm)
    except (ValueError, AttributeError):
        pass
```

#### Context manager

```python
with PipelineManager.create(...) as pipeline:
    pipeline.run(OpA, ...)
    pipeline.run(OpB, ...)
    # __exit__ calls cancel() on exception, finalize() always
```

#### Step tracker: "cancelled" state

Add `record_step_cancelled`. `load_completed_steps` treats "cancelled" like
"failed" — not eligible for cache hits or as a successful predecessor.

### Flow handle lifecycle

```
PipelineManager.submit()
  → backend.create_flow() returns FlowHandle
  → self._active_flow_handle = flow_handle
  → _execute_creator_step:
      Phase 1 (DISPATCH): check cancel_event, prepare units
      Phase 2 (EXECUTE):  flow_handle.run(units_path, runtime_env)
      Phase 3 (COMMIT):   check cancel_event, commit results
  → self._active_flow_handle = None
```

On cancellation (from any thread):

```
PipelineManager.cancel()
  → self._cancel_event.set()
  → self._active_flow_handle.cancel()
      → SlurmFlowHandle: scancel --name <job_name>
      → LocalFlowHandle: set cancel_event (result collection stops)
  → _collect_results sees cancel_event, fills remaining with "Cancelled"
  → step_flow returns partial results
  → step_executor checks cancel_event, skips commit
  → step_tracker records "cancelled"
  → next step sees cancel_event, returns immediately
```

### Future backends

The `FlowHandle` abstraction scales to any backend:

| Backend | `cancel()` implementation |
|---------|--------------------------|
| SLURM | `scancel --name <job_name>` + cancel event |
| Local (ProcessPool) | Cancel event (Prefect handles pool shutdown) |
| Kubernetes (future) | `kubectl delete job <name>` + cancel event |
| Cloud Batch (future) | API call to cancel batch job + cancel event |

Each backend brings its own cleanup logic. The pipeline manager and step
executor are completely backend-agnostic — they only interact with
`FlowHandle.run()` and `FlowHandle.cancel()`.

---

## Improvement: Parallel result collection

Independent of the `FlowHandle` refactor, `_collect_results` should collect
futures in parallel. This is the biggest bang-for-buck fix:

**Before:** Sequential — 300 cancelled SLURM jobs × ~12s polling interval
= ~1 hour.

**After:** Parallel — all 300 futures resolve concurrently, total time ≈
1 polling interval ≈ seconds.

This can ship first, before the `FlowHandle` refactor, as a standalone
change to `dispatch.py`.

---

## Improvement: Active SLURM job monitoring (optional)

A background thread in `SlurmFlowHandle` that polls `squeue` to detect
externally cancelled jobs:

```python
def _monitor_loop(self):
    while not self._cancel_event.wait(5.0):
        # squeue --name <job_name> --format=%T --noheader
        # If all jobs are CANCELLED/COMPLETED/FAILED, set cancel_event
        ...
```

This would detect external `scancel` within 5 seconds regardless of job
count. Trade-off: adds complexity and SLURM CLI dependency from the head
node. May be unnecessary if parallel result collection is fast enough.

---

## Recommendation

**Ship in order:**

- **Parallel result collection first** — standalone change to `dispatch.py`.
  Biggest impact on the "scancel took an hour" problem. No interface changes.

- **`FlowHandle` + `PipelineManager.cancel()` second** — the real feature.
  Changes `BackendBase` interface, adds cancellation to both backends,
  signal handling, context manager, "cancelled" step state.

- **Active SLURM monitoring only if needed** — evaluate after parallel
  collection is shipped. If it makes external scancel fast enough, the
  monitor adds complexity for diminishing returns.

---

## Open questions

- **Prefect future compatibility.** Do Prefect futures support
  `concurrent.futures.as_completed()`? If not, the ThreadPoolExecutor
  wrapper (each thread blocks on one `f.result()`) is the fallback.

- **Job ID availability.** When does `prefect_submitit` expose the SLURM job
  array ID? The `scancel --name` approach sidesteps this for cancellation,
  but we may still want IDs for monitoring and logging.

- **Context manager vs explicit finalize.** Should `PipelineManager` support
  both? Context manager is cleaner for scripts; explicit `finalize()` is
  needed for interactive notebook use.

- **Partial commit on cancellation.** Should a cancelled step commit the
  units that succeeded before cancellation? Makes resume more efficient but
  adds complexity. Current behavior: incomplete step → nothing committed →
  resume re-runs the entire step.

- **`FlowHandle` thread safety.** `cancel()` is called from signal handlers
  or other threads while `run()` is blocked. The implementation must be
  thread-safe. `threading.Event` handles the coordination; `scancel` is
  idempotent. But Prefect flow internals may not be safe to interrupt — need
  to verify.

---

## Files affected

| File | Change | Phase |
|------|--------|-------|
| `orchestration/engine/dispatch.py` | Parallel result collection, cancel_event param | Parallel collection |
| `orchestration/backends/base.py` | `FlowHandle` ABC, updated `create_flow` return type | FlowHandle |
| `orchestration/backends/slurm.py` | `SlurmFlowHandle` with scancel | FlowHandle |
| `orchestration/backends/local.py` | `LocalFlowHandle` | FlowHandle |
| `orchestration/pipeline_manager.py` | `cancel()`, signal handling, context manager, flow handle tracking | FlowHandle |
| `orchestration/engine/step_executor.py` | Check cancel_event between phases in `_execute_creator_step`, pass to flow handle | FlowHandle |
| `orchestration/engine/step_tracker.py` | Add "cancelled" state | FlowHandle |
| `orchestration/step_future.py` | Optional: add `cancel()` delegation | FlowHandle |
