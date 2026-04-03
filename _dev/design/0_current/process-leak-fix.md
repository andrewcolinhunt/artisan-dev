# Design: Fix Process/Thread Leaks in Tutorial Notebooks

**Date:** 2026-04-03
**Status:** Draft
**Trigger:** User report — running 4–5 tutorial notebooks fills `top` with
Python processes, requiring `pkill`. "Resource temporarily unavailable" errors
in subprocesses.

---

## Problem

Running tutorial notebooks sequentially on a shared server (Jupyter via
`pixi run jupyter notebook`) causes Python processes and threads to accumulate
until the system hits resource limits (`EAGAIN` from `fork()`). The user must
`pkill` to recover.

This is **not** caused by pixi or the Jupyter launch method. The leak is
inside the Python kernels themselves.

---

## Root Causes

### RC-1: PipelineManager has no cleanup safety net

`PipelineManager.__init__` creates a `ThreadPoolExecutor(max_workers=1)` at
`pipeline_manager.py:535`. The **only** place this executor is shut down is
inside `finalize()` at line 2078:

```python
self._executor.shutdown(wait=not cancelled, cancel_futures=cancelled)
```

There is no `__del__`, no `__enter__`/`__exit__`, no `close()`, and no
`atexit` handler. If `finalize()` is never called — exception, user interrupt,
oversight — the executor thread persists for the lifetime of the kernel.

### RC-2: Tutorial notebooks with unfinalized PipelineManagers

Three notebooks create PipelineManagers in code cells without calling
`finalize()`:

| Notebook | creates | finalizes | leaked |
|---|---|---|---|
| `execution/04-error-visibility.ipynb` | 5 | 3 | **2** |
| `execution/07-slurm-execution.ipynb` | 4 | 2 | **2** |
| `execution/10-slurm-intra-execution.ipynb` | 4 | 2 | **2** |

The leaked pipelines are typically in error-demonstration cells (fail-fast
examples, validation demos, debug config examples) where the pipeline is
created to show a concept but never finalized.

Across all 24 tutorials: 62 `PipelineManager.create()` calls, 56
`finalize()` calls, 6 leaked.

### RC-3: `activate_server()` leaks Prefect SettingsContext

Every `PipelineManager.create()` call triggers `activate_server()`
(`prefect_server.py:265`), which does:

```python
new_ctx = SettingsContext(profile=ctx.profile, settings=new_settings)
new_ctx.__enter__()  # __exit__() is never called
```

Each call pushes a new context onto Prefect's context stack. Over 62
pipeline creations across a full tutorial run, this creates 62 stacked
context objects that are never popped.

### RC-4: Prefect internal services accumulate per kernel

Prefect uses singleton `_QueueServiceBase` instances (EventsWorker, etc.)
that spawn daemon `WorkerThread`s and register with a global
`_active_services` set. These are created lazily on first flow invocation
and persist for the kernel lifetime. Visible in notebook output as:

```
EventsWorker - Still processing items: 3 items remaining...
```

Not a direct leak per-se (they're singletons), but they add to the
thread/resource baseline of each kernel.

### RC-5: Idle Jupyter kernels accumulate

Jupyter does not kill idle kernels by default. When a user moves from
notebook to notebook, old kernels persist with all their leaked resources.
After 4–5 notebooks, there are 4–5 kernel processes, each carrying
leaked ThreadPoolExecutor threads, stacked SettingsContexts, and Prefect
background services.

### Cumulative effect

On a shared server with per-user process/thread limits:

```
4–5 kernels × (leaked ThreadPoolExecutor threads
              + Prefect WorkerThread daemons
              + stacked SettingsContext objects)
= enough accumulated state to exhaust fork() limits → EAGAIN
```

### What is NOT leaking

Per-step `ProcessPoolExecutor` workers (the actual pipeline compute
processes) are properly managed. Prefect's flow engine enters the task
runner as a context manager via `ExitStack`:

```python
task_runner = stack.enter_context(self.flow.task_runner.duplicate())
```

The `SIGINTSafeProcessPoolTaskRunner.__exit__` shuts down all pools with
`wait=True`. The curator subprocess pool in `_run_curator_in_subprocess`
uses a `with` block. External tool subprocesses use `try/except
BaseException` with process group cleanup. These are all properly scoped.

---

## Fixes

### Fix A: Add cleanup safety net to PipelineManager

**File:** `src/artisan/orchestration/pipeline_manager.py`

Add three layers of defense so that a missed `finalize()` can't leak:

**A1. `__del__` method** — last-resort cleanup when the object is garbage
collected:

```python
def __del__(self) -> None:
    if hasattr(self, "_executor") and self._executor is not None:
        self._executor.shutdown(wait=False)
```

`wait=False` avoids blocking the GC thread. This catches the common case
where a notebook cell creates a pipeline, uses it, but forgets to finalize.

**A2. Context manager protocol** — for users who want deterministic cleanup:

```python
def __enter__(self) -> PipelineManager:
    return self

def __exit__(self, exc_type, exc_val, exc_tb) -> None:
    if not self._finalized:
        self.finalize()
```

Enables:
```python
with PipelineManager.create(...) as pipeline:
    pipeline.run(...)
    pipeline.run(...)
# finalize() called automatically
```

This is opt-in; existing code continues to work without `with`.

**A3. `atexit` handler** — catches the case where the kernel shuts down
with live PipelineManagers:

```python
import atexit
import weakref

def _cleanup_executor(ref: weakref.ref) -> None:
    executor = ref()
    if executor is not None:
        executor.shutdown(wait=False)

# In __init__:
atexit.register(_cleanup_executor, weakref.ref(self._executor))
```

Uses `weakref` to avoid preventing garbage collection of the executor.

**Tracking `_finalized` state:** Add a `self._finalized: bool = False` flag
set in `finalize()`, checked by `__exit__` and `__del__` to avoid double
shutdown.

### Fix B: Fix `activate_server()` SettingsContext leak

**File:** `src/artisan/orchestration/prefect_server.py`

The current approach enters a SettingsContext and never exits it, stacking
contexts on every `PipelineManager.create()`. Since the intent is to set a
global setting, not to scope it, we can skip the SettingsContext entirely
and rely on the env var:

```python
def activate_server(info: PrefectServerInfo) -> None:
    os.environ["PREFECT_API_URL"] = info.url
    # No SettingsContext manipulation — Prefect reads the env var
    # on each flow invocation via get_current_settings().
    mode = "Cloud" if _is_cloud_url(info.url) else "self-hosted"
    logger.info("Prefect %s: %s (source: %s)", mode, info.url, info.source)
```

**Risk:** Prefect may cache its settings and not re-read the env var.
Needs verification: create a pipeline, change `PREFECT_API_URL`, create
another pipeline, confirm the second one uses the new URL.

**Fallback if env-var-only doesn't work:** Track the SettingsContext and
exit it before entering a new one:

```python
_active_settings_ctx: SettingsContext | None = None

def activate_server(info: PrefectServerInfo) -> None:
    global _active_settings_ctx
    os.environ["PREFECT_API_URL"] = info.url
    try:
        from prefect.context import SettingsContext
        # Exit previous context if one exists
        if _active_settings_ctx is not None:
            _active_settings_ctx.__exit__(None, None, None)
        ctx = SettingsContext.get()
        if ctx is not None:
            new_api = ctx.settings.api.model_copy(update={"url": info.url})
            new_settings = ctx.settings.model_copy(update={"api": new_api})
            _active_settings_ctx = SettingsContext(
                profile=ctx.profile, settings=new_settings
            )
            _active_settings_ctx.__enter__()
    except Exception:
        pass
    ...
```

### Fix C: Fix unfinalized tutorial notebooks

**Files:** Three notebooks need `finalize()` calls added.

**`execution/04-error-visibility.ipynb`** — Two pipelines leak:
- Cell 17 (fail-fast demo): wraps `pipeline.run()` in `try/except
  RuntimeError` but never finalizes. Add `finally: pipeline.finalize()`.
- Cell 25 (validation demo): wraps `PipelineManager.create()` +
  `pipeline.run()` in `try/except` but never finalizes. Add `finally:
  pipeline.finalize()`.

**`execution/07-slurm-execution.ipynb`** — Two pipelines leak:
- Cell 18 (overrides demo): creates pipeline, runs steps, never
  finalizes. Add `finalize()` call.
- Cell 28 (debug config demo): creates pipeline with `preserve_working=True`,
  never runs steps or finalizes. Add `finalize()` call (or remove the
  code cell if it's purely illustrative).

**`execution/10-slurm-intra-execution.ipynb`** — Two pipelines leak:
- Cell 19 (overrides demo): same pattern as 07. Add `finalize()`.
- Cell 27 (debug config demo): same pattern as 07. Add `finalize()`.

**Pattern for error-demo cells:** Use `try/finally`:

```python
pipeline = PipelineManager.create(...)
try:
    pipeline.run(...)  # expected to raise
except SomeError as e:
    print(f"Got expected error: {e}")
finally:
    pipeline.finalize()
```

---

## Scope and Non-Goals

**In scope:**
- PipelineManager cleanup safety net (Fix A)
- SettingsContext leak (Fix B)
- Unfinalized tutorial notebooks (Fix C)

**Not in scope:**
- Prefect internal service lifecycle (Prefect's responsibility, singletons
  are working as designed)
- Jupyter idle kernel management (user/admin configuration)
- Adding a `tutorial_cleanup()` helper to `tutorial_setup` (the safety
  net in Fix A makes this unnecessary)

---

## Validation

- Run all 24 tutorial notebooks sequentially in a single Jupyter session
- After each notebook, check thread/process counts:
  ```python
  import threading, os
  print(f"Threads: {threading.active_count()}")
  os.system("pstree -p $PPID")
  ```
- Verify thread count stays bounded (should not grow with each notebook)
- Verify `resource temporarily unavailable` errors are gone
- Run existing test suite: `pixi run -e dev test`

---

## Implementation Order

- Fix A (safety net) — structural fix, prevents all future leaks
- Fix C (notebooks) — immediate relief for the current user
- Fix B (SettingsContext) — correctness fix, lower urgency
