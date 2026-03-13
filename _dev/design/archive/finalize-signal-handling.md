# Design: Graceful SIGINT Handling During Pipeline Execution

**Date:** 2026-03-12  **Status:** Draft  **Author:** Claude + ach94

**Predecessor:** [cancellation-hang.md](cancellation-hang.md) — fixed
problems 1–4 (cancel-aware predecessor wait, early closure bailout,
composite cancel check, `cancel_futures=True`). This doc addresses the
remaining gaps that cause `cancel_demo.py` to crash instead of shutting
down cleanly.

---

## Problem

After the cancellation-hang fixes, pressing Ctrl+C during
`cancel_demo.py` produces noisy tracebacks and takes ~54 seconds to
exit instead of shutting down gracefully within a few seconds.

### What the user sees

```bash
pixi run python docs/tutorials/execution/cancel_demo.py
# Press Ctrl+C ~4 seconds in
```

- "Exception in thread Thread-1" with a full `KeyboardInterrupt`
  traceback from deep inside Prefect/distributed import chain
- Signal handler continues to fire on subsequent Ctrl+C but cannot
  force-kill — user must wait ~54s for the process to exit
- Final output shows 0 steps completed (the running step's result is
  lost to `BrokenProcessPool`)

### What we want

- First Ctrl+C: cancel event set, finalize detects within ~500ms,
  short grace period for active work, clean summary printed
- Second Ctrl+C: signal handlers restored (user is warned)
- Third Ctrl+C: process dies immediately (default Python behavior)
- Total time from first Ctrl+C to exit: <10s

---

## Execution Model

Understanding the three layers is essential for reasoning about where
SIGINT lands and what can respond to it.

```
┌─────────────────────────────────────────────────────────────────────┐
│ Main thread                                                        │
│                                                                    │
│   submit() ──► _wait_for_predecessors() ──► executor.submit(_run)  │
│   ...                                                              │
│   finalize() ──► future.result() ──► executor.shutdown()           │
│                                                                    │
│   Signal handlers live here (Python requirement).                  │
│   _cancel_event is set here.                                       │
└────────────────────────────┬────────────────────────────────────────┘
                             │ ThreadPoolExecutor(max_workers=1)
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Worker thread ("pipeline-step")                                    │
│                                                                    │
│   _run() closure                                                   │
│     ├── checks _cancel_event at start (bail if set)                │
│     ├── calls execute_step()                                       │
│     │     ├── checks _cancel_event at phase boundaries             │
│     │     ├── creator path: invokes Prefect flow (blocks)          │
│     │     └── curator path: spawns subprocess (blocks)             │
│     └── catches Exception, returns StepResult                      │
│                                                                    │
│   Cannot receive signals. Only way to interrupt: _cancel_event.    │
└────────────────────────────┬────────────────────────────────────────┘
                             │ ProcessPoolExecutor (Prefect or direct)
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Child process (same process group)                                 │
│                                                                    │
│   execute_unit_task()  (dispatch.py, via Prefect ProcessPoolTaskRunner) │
│   run_curator_flow()   (curator, via _run_curator_in_subprocess)       │
│                                                                    │
│   No access to _cancel_event (threading.Event, process-local).     │
│   RECEIVES SIGINT from terminal (same process group).              │
│   Default handler: raises KeyboardInterrupt.                       │
└─────────────────────────────────────────────────────────────────────┘
```

| Layer | Cancel mechanism | SIGINT behavior |
|-------|-----------------|-----------------|
| Main thread | `_handle_signal` → `cancel()` → `_cancel_event.set()` | Custom handler catches SIGINT, never raises KeyboardInterrupt |
| Worker thread | Checks `_cancel_event` at phase boundaries | Cannot receive signals (non-main thread) |
| Child process | None — runs to completion or is killed | OS delivers SIGINT → `KeyboardInterrupt` raised |

---

## Root Cause Analysis

There are four independent problems. Each contributes to the poor
cancellation behavior; they must all be fixed for a clean shutdown.

### Problem A: Signal handlers restored before `finalize()` waits on futures

**Status: FIXED** (current branch)

`finalize()` previously restored signal handlers *before* entering the
future-wait loop. Once handlers were restored, Ctrl+C
raised `KeyboardInterrupt` instead of calling `cancel()`.

Since Wait steps have no inputs, all 10 `submit()` calls complete
instantly (no `_wait_for_predecessors` blocking). The main thread
enters `finalize()` within milliseconds. The custom handler was active
only during the submit loop — microseconds.

**Fix applied:** Move `_restore_signal_handlers()` to after executor
shutdown. Replace the blocking `future.result(timeout=...)` with a
cancel-aware polling loop (matching `_wait_for_predecessors` pattern):
poll with `timeout=0.5`, check `_cancel_event` each iteration.

### Problem B: SIGINT delivered to child processes in the process group

When Ctrl+C is pressed in a terminal, SIGINT is sent to the **entire
foreground process group** — parent AND all child processes. Even with
Fix A working correctly:

- **Parent** catches SIGINT via `_handle_signal`, sets `_cancel_event`
- **Child process** receives raw SIGINT, raises `KeyboardInterrupt`

The child process has no `_cancel_event` access (it's a
`threading.Event`, process-local). The only thing that can stop the
child is the signal itself, and the default handler crashes the process.

This produces the traceback output:

```
concurrent.futures.process._RemoteTraceback:
"""
Traceback (most recent call last):
  ...deep inside Prefect/distributed import chain...
KeyboardInterrupt
"""
```

Followed by the process pool's internal thread crashing:

```
Exception in thread Thread-1:
...
KeyboardInterrupt
```

### Problem C: No second-SIGINT escape hatch

Once `_handle_signal` is installed, every Ctrl+C calls `cancel()`
(idempotent after the first). The user cannot force-kill the process
without `kill -9` or closing the terminal. The original SIGINT handler
(which would raise `KeyboardInterrupt`) is stored in `_prev_sigint`
but not restored until `finalize()` completes.

In the observed output, the user pressed Ctrl+C **19 times** between
10:11:10 and 10:11:17, each producing a "cancelling" warning but no
other effect.

### Problem D: Blocking waits with no cancel awareness in execution paths

Several blocking calls have no timeout and no cancel check:

| Location | Blocking call | Impact |
|----------|--------------|--------|
| `_collect_results` in dispatch.py | `future.result()` — no timeout (via `_get_one` helper) | Blocks executor thread until all Prefect tasks finish |
| `_run_curator_in_subprocess` in step_executor.py | `future.result()` — no timeout | Blocks executor thread until curator subprocess finishes |
| `step_flow()` call in `_execute_creator_step` | Synchronous Prefect flow invocation | Blocks executor thread until entire Prefect flow completes |

When Ctrl+C kills a child process, the result is either a delayed
exception (process pool detects the dead worker) or a hang (if the
pool itself breaks). The executor thread is stuck in `execute_step()`
with no way to check `_cancel_event` and bail out.

This means `finalize()` has to wait for the executor thread to finish,
which waits for the Prefect flow, which waits for the dead/broken
process pool. The 5s grace timeout in finalize applies to the
`_active_futures` (which are `StepFuture`s wrapping the
`ThreadPoolExecutor` futures), but the thread itself is stuck inside
the Prefect flow.

---

## Interaction Diagram: Current (Broken) Behavior

```
Time ─────────────────────────────────────────────────────►

Main thread:
  submit x10 ─► finalize() ──► poll future ──────────────────────────► exit
                                 │ (0.5s)
                                 ├── cancel_event set (Ctrl+C)
                                 ├── else: future.result(timeout=5.0)
                                 │         ↑ BUT: future is the StepFuture
                                 │           wrapping _run(), which is stuck
                                 │           inside execute_step() → Prefect
                                 │         This 5s timeout works on the
                                 │         ThreadPoolExecutor future, not the
                                 │         Prefect subprocess.
                                 └── executor.shutdown()
                                      ↑ shutdown(wait=False) returns quickly
                                        BUT: atexit handlers wait for the
                                        subprocess pool to terminate

Worker thread (pipeline-step):
  _run() ──► execute_step() ──► step_flow() ──────────────────────────────►
                                  │ (blocks until Prefect flow completes)
                                  │
                                  ▼ Prefect flow: futures blocked on
                                    broken process pool. Takes 30-60s
                                    for Prefect to detect & propagate
                                    the BrokenProcessPool error.

Child process:
  execute_unit_task() ──► time.sleep(30) ──► SIGINT ──► KeyboardInterrupt
                                                         │
                                                         ▼
                                              Fix B2: caught, → RuntimeError
                                              BUT: Prefect's own wrapper code
                                              also gets KeyboardInterrupt
                                              (before our code runs)
```

The Fix B2 catch in `execute_unit_task` doesn't help when the
`KeyboardInterrupt` occurs in Prefect's process worker infrastructure
*before* `execute_unit_task` is called (as seen in the traceback:
the error is in `_process_worker` → `_run_serialized_call` →
`hydrated_context` → Prefect/distributed import chain).

---

## Proposed Fixes

### Fix 1: Escalating signal handler (solves Problem C)

On the second SIGINT, restore default handlers so the user can
force-kill on the third Ctrl+C.

```python
def _handle_signal(self, signum: int, frame: Any) -> None:
    sig_name = signal.Signals(signum).name
    if self._cancel_event.is_set():
        # Second signal: restore defaults so next Ctrl+C kills process
        logger.warning(
            "Pipeline '%s': received second %s — restoring default handlers. "
            "Press Ctrl+C again to force exit.",
            self._config.name, sig_name,
        )
        self._restore_signal_handlers()
        return
    logger.warning(
        "Pipeline '%s': received %s — cancelling.",
        self._config.name, sig_name,
    )
    self.cancel()
```

**Why this is important:** This is the standard escalation pattern
(used by pytest, Django, Celery, etc.). Without it, the user is
trapped — they can see the process is stuck but cannot kill it cleanly.

### Fix 2: Ignore SIGINT in child processes (solves Problem B)

Prevent child processes from receiving SIGINT entirely. The parent
handles SIGINT and coordinates shutdown; children should not see it.

**Challenge:** Prefect's `ProcessPoolTaskRunner` creates its own
`ProcessPoolExecutor` internally without exposing an `initializer`
parameter. We cannot inject `signal.signal(SIGINT,
SIG_IGN)` into the child process startup.

#### Option 2a: Subclass ProcessPoolTaskRunner

Override `__enter__` to create the `ProcessPoolExecutor` with an
initializer that ignores SIGINT:

```python
class SIGINTSafeProcessPoolTaskRunner(ProcessPoolTaskRunner):
    def __enter__(self):
        super().__enter__()
        # Re-create executor with SIGINT-ignoring initializer
        if self._executor is not None:
            self._executor.shutdown(wait=False)
        mp_context = multiprocessing.get_context("spawn")
        self._executor = ProcessPoolExecutor(
            max_workers=self._max_workers,
            mp_context=mp_context,
            initializer=_ignore_sigint,
        )
        return self
```

**Pros:** Clean, deterministic, prevents the problem at source.
**Cons:** Couples to Prefect internals (`_executor`, `_max_workers`).
Fragile across Prefect version upgrades.

#### Option 2b: Block SIGINT in parent before spawning

Use `signal.pthread_sigmask` (Linux only) to block SIGINT before
Prefect creates its process pool, then unblock after. The child
inherits the blocked signal mask.

```python
# In _build_prefect_flow or create_flow
old_mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
task_runner.__enter__()  # creates ProcessPoolExecutor
signal.pthread_sigmask(signal.SIG_SETMASK, old_mask)
```

**Pros:** Doesn't depend on Prefect internals.
**Cons:** Linux-only (`pthread_sigmask` not available on macOS in all
Python builds). Race condition window. Complex to integrate with
Prefect's context manager flow.

#### Option 2c: Ignore SIGINT in the execute_unit_task wrapper

Have the subprocess set `SIG_IGN` as its first action:

```python
@task
def execute_unit_task(unit, runtime_env):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    # ... rest of function
```

**Pros:** Simple, no Prefect subclassing needed.
**Cons:** There's a race window — SIGINT can arrive *before*
`execute_unit_task` runs (during Prefect's own subprocess setup).
This is exactly what we see in the traceback. The `KeyboardInterrupt`
occurs in Prefect's `_process_worker` → `hydrated_context` → import
chain, before our task function even starts.

#### Option 2d: Catch KeyboardInterrupt + accept subprocess death

Keep the Fix B2 catch (`except KeyboardInterrupt: raise
RuntimeError(...)`) as defense in depth, but accept that SIGINT
during Prefect setup code will still crash the subprocess. Handle the
crash gracefully on the parent side by catching `BrokenProcessPool`
in the creator path (currently only caught in the curator path).

```python
# In _execute_creator_step, around the step_flow() call
try:
    results = step_flow(units_path=str(units_path), runtime_env=runtime_env)
except BrokenProcessPool:
    logger.warning("Step %d: worker process killed during cancellation", step_number)
    results = []
    succeeded, failed = 0, len(units_to_dispatch)
```

**Pros:** Robust — works regardless of where the `KeyboardInterrupt`
hits. No Prefect subclassing.
**Cons:** Still produces "Exception in thread Thread-1" stderr output
from the process pool's internal thread. The subprocess death is
expected during cancel but looks like an error.

#### Option 2e: Subclass + catch as defense in depth (recommended)

Combine 2a and 2d:

- **Primary:** Subclass `ProcessPoolTaskRunner` with SIGINT-ignoring
  initializer. Under normal operation, children never see SIGINT.
- **Defense in depth:** Catch `BrokenProcessPool` in the creator
  path. If the subclass approach breaks due to Prefect version change,
  or if SIGINT arrives during the race window before the initializer
  runs, the parent still handles it cleanly.
- **Keep Fix B2:** The `except KeyboardInterrupt` in
  `execute_unit_task` (dispatch.py) acts as a third layer of defense.

### Fix 3: Handle BrokenProcessPool in creator path (defense in depth)

The curator path already catches `BrokenProcessPool` in
`_execute_curator_step`. The creator path does not — it lets
`BrokenProcessPool` propagate as a bare exception, caught by the broad
`except Exception` with a generic "Dispatch failed" message.

Add explicit `BrokenProcessPool` handling in the creator path for a
clear log message and proper error attribution:

```python
# In _execute_creator_step, after step_flow() call
except BrokenProcessPool:
    dispatch_error = "Worker process killed (signal or OOM)"
    logger.warning(
        "Step %d (%s): %s", step_number, operation.name, dispatch_error,
    )
    succeeded, failed = 0, len(units_to_dispatch)
```

### Fix 4: Distinguish SIGINT kills from OOM in curator path

The curator path's `BrokenProcessPool` handler calls
`_format_subprocess_kill_error`, which always reports "likely OOM"
regardless of the actual cause. When the kill is from SIGINT during
cancellation, this is misleading.

```python
except BrokenProcessPool:
    if cancel_event is not None and cancel_event.is_set():
        error_msg = "Curator subprocess killed during cancellation"
    else:
        error_msg = _format_subprocess_kill_error(unit)
```

### Fix 5: Add cancel-aware timeout to curator subprocess wait

`_run_curator_in_subprocess` calls `future.result()` with no timeout:

```python
def _run_curator_in_subprocess(unit, runtime_env):
    ctx = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=1, mp_context=ctx) as pool:
        future = pool.submit(run_curator_flow, unit, runtime_env, 0)
        return future.result()  # ← blocks indefinitely
```

If the subprocess ignores SIGINT (e.g., in C extension code), this
hangs forever. Pass `cancel_event` and poll:

```python
def _run_curator_in_subprocess(unit, runtime_env, cancel_event=None):
    ctx = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=1, mp_context=ctx) as pool:
        future = pool.submit(run_curator_flow, unit, runtime_env, 0)
        while True:
            try:
                return future.result(timeout=0.5)
            except TimeoutError:
                if cancel_event is not None and cancel_event.is_set():
                    # Cancel requested — let pool context manager shut down
                    raise RuntimeError("Curator interrupted by cancellation")
                continue
```

The call site in `_execute_curator_step` must pass `cancel_event`
through to `_run_curator_in_subprocess`.

---

## Which Fixes Are Already Applied

| Fix | Status | Notes |
|-----|--------|-------|
| Fix A (signal handlers through finalize) | **Done** | Current branch, committed |
| Fix B2 (KeyboardInterrupt catch in dispatch.py) | **Done** | Current branch, committed |
| Fix 1 (escalating signal handler) | Not started | |
| Fix 2e (SIGINT-safe subprocess spawning) | Not started | |
| Fix 3 (BrokenProcessPool in creator path) | Not started | |
| Fix 4 (SIGINT vs OOM in curator path) | Not started | |
| Fix 5 (cancel-aware curator subprocess wait) | Not started | |

---

## Priority and Ordering

| Fix | Impact | Effort | Priority |
|-----|--------|--------|----------|
| Fix 1: Escalating signal handler | User can force-exit (critical UX) | Trivial | **P0** |
| Fix 2e: SIGINT-safe subprocesses | Eliminates noisy tracebacks | Medium | **P0** |
| Fix 3: BrokenProcessPool in creator | Clean error on subprocess death | Small | **P1** |
| Fix 4: SIGINT vs OOM in curator | Accurate error messages | Trivial | **P2** |
| Fix 5: Cancel-aware curator wait | Prevents indefinite hang | Small | **P2** |

**Recommended implementation order:** Fix 1 → Fix 2e → Fix 3 → Fix 4 → Fix 5.

Fix 1 is essential because even after all other fixes, there will
always be edge cases where the process is stuck. The user must have
an escape hatch.

Fix 2e is the highest-impact fix for clean output — it eliminates the
root cause of the noisy tracebacks by preventing SIGINT from reaching
children.

---

## Interaction Diagram: After All Fixes

```
Ctrl+C in terminal
│
├──► Main process (main thread)
│     │
│     ▼
│    _handle_signal() [first time]
│     │
│     ▼
│    cancel() → _cancel_event.set()
│     │
│     ▼ (effects propagate)
│    finalize() poll loop: detects within 0.5s           [Fix A — done]
│    └── 5s grace timeout on active future
│    └── executor.shutdown(wait=False, cancel_futures=True)
│    └── _restore_signal_handlers()
│    └── return clean summary
│
├──► If user presses Ctrl+C again:
│     │
│     ▼
│    _handle_signal() [second time]                      [Fix 1]
│     │
│     ▼
│    _restore_signal_handlers() — default SIGINT restored
│    └── Third Ctrl+C → KeyboardInterrupt → immediate exit
│
├──► Child processes (Prefect workers)
│     │
│     ▼
│    SIGINT ignored (initializer set SIG_IGN)            [Fix 2e primary]
│    └── Operation continues running normally
│    └── Pool shutdown kills children when context manager exits
│
└──► If child somehow crashes anyway (race, OOM, etc.)
      │
      ▼
     BrokenProcessPool caught in creator path             [Fix 3]
     KeyboardInterrupt caught in execute_unit_task         [Fix B2 — done]
     └── Clean failure result, no noisy traceback
```

---

## Files to Modify

| File | Fix | Changes |
|------|-----|---------|
| `pipeline_manager.py` `_handle_signal` | Fix 1 | Add cancel-already-set branch that restores default handlers |
| `backends/local.py` | Fix 2e | Subclass `ProcessPoolTaskRunner` with SIGINT-ignoring initializer |
| `engine/step_executor.py` `_execute_creator_step` | Fix 3 | Add `except BrokenProcessPool` before `except Exception` |
| `engine/step_executor.py` `_execute_curator_step` | Fix 4 | Check `cancel_event` before reporting OOM |
| `engine/step_executor.py` `_run_curator_in_subprocess` | Fix 5 | Add cancel_event param, polling loop with timeout |
| `engine/step_executor.py` `_execute_curator_step` | Fix 5 | Pass `cancel_event` to `_run_curator_in_subprocess` |

---

## Testing Plan

### Unit tests

- **Fix 1:** Test that second signal call to `_handle_signal` restores
  `_prev_sigint` to `None` (handlers restored)
- **Fix 2e:** Test that `SIGINTSafeProcessPoolTaskRunner` creates a
  pool with an initializer (mock `ProcessPoolExecutor` and assert
  `initializer` kwarg)
- **Fix 3:** Test that `BrokenProcessPool` during creator dispatch
  produces a failed `StepResult` (not an exception)
- **Fix 5:** Test that `_run_curator_in_subprocess` with a set
  `cancel_event` raises `RuntimeError` within ~1s

### Manual verification

```bash
# Single Ctrl+C: clean exit within ~6s
pixi run python docs/tutorials/execution/cancel_demo.py

# Double Ctrl+C: "restoring default handlers" message, then exit
pixi run python docs/tutorials/execution/cancel_demo.py

# Triple Ctrl+C: immediate hard kill
pixi run python docs/tutorials/execution/cancel_demo.py
```

---

## Thread Safety Notes

No new concurrency primitives needed. All fixes use the existing
`_cancel_event` (`threading.Event`) which is thread-safe by design.
`signal.signal()` must be called from the main thread — both
`_handle_signal` and `_restore_signal_handlers` already ensure this
(they catch `ValueError` for non-main-thread calls).

---

## Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| Prefect changes `ProcessPoolTaskRunner` internals | Fix 3 (BrokenProcessPool catch) acts as fallback. Pin Prefect version in CI. |
| `initializer` ignored on some platforms | Fix B2 catch in `execute_unit_task` provides third layer of defense. |
| Escalating handler restores signals too early (user accidentally presses Ctrl+C twice) | The warning message tells the user what happened. Finalize continues to run — only a *third* Ctrl+C would kill the process. |
| `_run_curator_in_subprocess` timeout causes premature abort | Only triggers when `cancel_event` is set — normal execution uses no timeout. |
