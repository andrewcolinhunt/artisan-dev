# Design: Fix Pipeline Cancellation Hang

**Date:** 2026-03-12  **Status:** Draft  **Author:** Claude + ach94

---

## Problem

After pressing Ctrl+C, the pipeline hangs for over a minute (sometimes
indefinitely) instead of shutting down quickly. The user has to mash
Ctrl+C repeatedly to force-kill the process.

### Reproduction

```bash
pixi run python docs/tutorials/execution/cancel_demo.py
# Press Ctrl+C ~4 seconds into execution
# Expected: clean shutdown within a few seconds
# Actual: hangs, requires multiple Ctrl+C to kill
```

---

## Root Cause Analysis

There are **five independent problems** that combine to cause the hang.
Each one alone would cause bad cancellation behavior; together they make
it nearly impossible to cancel cleanly.

### Problem 1: `_wait_for_predecessors` blocks indefinitely

**The primary hang.** When `submit()` is called for a step with inputs
(e.g. `transform_0` depending on `generate_0`), the main thread blocks
in `_wait_for_predecessors` with no timeout and no cancel check:

```python
def _wait_for_predecessors(self, inputs):
    source_steps = _extract_source_steps(inputs)
    for step_num in source_steps:
        if step_num in self._active_futures:
            try:
                self._active_futures[step_num].result()  # blocks forever
            except Exception:
                logger.warning(...)
```

**Timeline of the hang:**

```
t=0.0s  submit(generate_0) → dispatched to thread pool
t=0.0s  submit(generate_1) → queued (pool has 1 worker)
t=0.0s  submit(generate_2..4) → queued
t=0.0s  submit(transform_0) → calls _wait_for_predecessors
        → main thread blocks on generate_0.result()
t=4.0s  Ctrl+C → signal handler sets _cancel_event
        → main thread is STUCK in .result() — can't check the event
        → generate_0's subprocess gets KeyboardInterrupt from process group
        → generate_0 eventually fails → _wait_for_predecessors unblocks
        → BUT generators 1-4 are still queued in the thread pool
```

After `_wait_for_predecessors` unblocks, the main thread continues
through `submit()` for `transform_0`. It hits the cache miss path and
dispatches `transform_0` to the thread pool (behind generators 1-4 in
the queue). Then `submit(transform_1)` is called, and since
`_cancel_event` is now set, it skips immediately. Same for transforms
2-4. The main thread then reaches `finalize()`.

But `finalize()` calls `future.result(timeout=5.0)` for each active
future. Generators 1-4 and transform_0 are all queued in the thread
pool. The pool runs them one at a time. Each generator runs `_run()`
which calls `execute_step()`. `execute_step` does check the cancel
event at phase boundaries (before execute, before commit), so each
queued generator should exit quickly once it starts — BUT it still has
to wait for the one ahead of it in the queue to finish.

### Problem 2: Queued `_run()` closures don't check cancel at start

When generators 1-4 are queued in the `ThreadPoolExecutor`, their
`_run()` closures capture the cancel event but don't check it before
calling `execute_step()`:

```python
def _run() -> StepResult:
    logger.info("Step %d (%s) starting...", ...)  # starts here
    start = time.perf_counter()
    try:
        result = execute_step(...)  # goes straight to execution
```

`execute_step()` does check cancel before PHASE 2 (execute) and
PHASE 3 (commit), but not at the very top. It instantiates the
operation, resolves inputs, and runs the batch_and_cache phase before
checking. For operations with many inputs, this is non-trivial work.

The fix is trivial: check `_cancel_event` at the top of `_run()` and
return a cancelled result immediately. This would make queued closures
exit in microseconds instead of running through expensive setup.

### Problem 3: `_submit_composite` has no cancel check at all

`submit()` routes composites to `_submit_composite()` at line 991,
**before** the cancel check at line 1067:

```python
def submit(self, operation, ...):
    if issubclass(operation, CompositeDefinition):
        return self._submit_composite(...)  # ← exits before cancel check

    # ... cancel check is here (line 1067) ...
    if self._cancel_event.is_set():
        ...
```

`_submit_composite()` itself has no cancel check. It checks `_stopped`
(for empty inputs) but not `_cancel_event`. If cancellation is
requested while a composite step is queued, it will fully execute.

Additionally, the composite `_run()` closure doesn't pass
`cancel_event` to `execute_composite_step()`, and that function
doesn't accept one. Composites have zero mid-execution cancellation.

### Problem 4: `finalize()` leaves threads alive after `shutdown(wait=False)`

```python
self._executor.shutdown(wait=not cancelled)
```

When cancelled, `shutdown(wait=False)` tells the executor not to wait,
but `ThreadPoolExecutor` threads are non-daemon by default. The Python
interpreter won't exit until all non-daemon threads complete. So even
though `finalize()` returns and the main function ends, the process
hangs at interpreter shutdown waiting for the worker thread.

### Problem 5: SIGINT double-delivery to subprocess process group

When Ctrl+C is pressed in a terminal, SIGINT is delivered to the
**entire foreground process group** — both the parent process and all
child processes (Prefect's `ProcessPoolExecutor` workers). The parent
catches SIGINT via the signal handler and sets `_cancel_event`. But
child processes receive raw SIGINT, causing `KeyboardInterrupt` which
kills the subprocess and results in `BrokenProcessPool`.

This isn't a hang issue per se — the creator step path handles the
resulting exception via `except Exception` at step_executor.py:826.
But it produces noisy tracebacks in the output (the "Exception in
thread Thread-1" block in the reproduction output). It also means the
cancel_event check is never reached — the subprocess dies from SIGINT
before the cancel event can signal a graceful stop.

---

## Proposed Fixes

### Fix 1: Cancel-aware `_wait_for_predecessors` (critical)

Poll with a short timeout instead of blocking indefinitely:

```python
def _wait_for_predecessors(self, inputs):
    source_steps = _extract_source_steps(inputs)
    for step_num in source_steps:
        future = self._active_futures.get(step_num)
        if future is None:
            continue
        while not self._cancel_event.is_set():
            try:
                future.result(timeout=0.5)
                break
            except TimeoutError:
                continue
            except Exception:
                logger.warning(
                    "Predecessor step %d failed — downstream will see empty inputs.",
                    step_num,
                )
                break
```

When cancel fires, the loop exits within 500ms.

**Important:** The existing cancel check in `submit()` (line 1067) runs
*before* `_wait_for_predecessors` (line 1109), so it won't be re-evaluated
after the wait returns. Add a second cancel check immediately after
`_wait_for_predecessors` in both `submit()` and `_submit_composite()`:

```python
self._wait_for_predecessors(inputs)

# Re-check after wait — cancel may have fired during the wait
if self._cancel_event.is_set():
    # ... same skip pattern as the pre-wait cancel check ...
```

This ensures that if the wait was interrupted by cancellation, the step
is skipped rather than dispatched to the thread pool.

### Fix 2: Early cancel check in `_run()` closures (important)

Add a cancel check at the top of both `_run()` closures (regular and
composite) so queued steps exit immediately:

```python
def _run() -> StepResult:
    if self._cancel_event.is_set():
        result = StepResult(
            step_name=step_name,
            step_number=step_number,
            success=True,
            total_count=0,
            succeeded_count=0,
            failed_count=0,
            output_roles=frozenset(operation.outputs.keys()),
            output_types=output_types_map,
            metadata={"cancelled": True},
        )
        self._step_results.append(result)
        self._named_steps.setdefault(result.step_name, []).append(result)
        return result
    # ... existing code (start_record is constructed below here) ...
```

Note: `record_step_cancelled` requires a `StepStartRecord`, which is
constructed further into the existing `_run()` body. At the early-exit
point we skip tracking — the step never started, so there's nothing to
record. The result is still appended to `_step_results` so `finalize()`
sees it.

This means 4 queued generators exit in microseconds instead of each
running through `execute_step()` setup.

### Fix 3: Cancel check in `_submit_composite` (important)

Add a cancel check in `_submit_composite()` mirroring the one in the
regular path. Place it after the `_stopped` check:

```python
if self._stopped:
    ...

if self._cancel_event.is_set():
    step_number = self._current_step
    # ... same skip pattern as regular submit ...
```

Propagating `cancel_event` to `execute_composite_step()` is a separate,
larger task. The pre-dispatch check is the minimum viable fix.

### Fix 4: Force-cancel executor on cancellation (nice-to-have)

Replace `shutdown(wait=False)` with cancellation of pending futures:

```python
if cancelled:
    self._executor.shutdown(wait=False, cancel_futures=True)
```

`cancel_futures=True` (Python 3.9+) cancels queued-but-not-started
work items in the thread pool. Combined with Fix 2, this means queued
closures never even start. Without it, the thread stays alive running
through the queue.

Note: this only cancels *queued* items, not the currently-running one.
The running closure still needs Fix 2 for the *next* step it might
process, and needs the cancel_event checks in `execute_step` for
mid-execution cancellation.

### Fix 5: Suppress noisy subprocess tracebacks (cosmetic)

The "Exception in thread Thread-1" traceback comes from Prefect's
`ProcessPoolTaskRunner` callback thread when the child process dies
from SIGINT. We can't prevent the subprocess from receiving SIGINT
(it's the same process group), but we can handle it more gracefully:

- Option A: Set child processes to ignore SIGINT (via
  `signal.signal(signal.SIGINT, signal.SIG_IGN)` in the initializer).
  Then only the parent's cancel_event controls shutdown. This is the
  cleanest approach but requires Prefect to support initializer kwargs.
- Option B: Accept the noisy output as inherent to process-group SIGINT
  delivery. The cancel still works correctly — it's just ugly.

**Recommendation:** Defer this. It's cosmetic and may require Prefect
upstream changes. The other fixes eliminate the hang.

---

## Fix Priority

| Fix | Impact | Effort | Priority |
|-----|--------|--------|----------|
| Fix 1: Cancel-aware `_wait_for_predecessors` | Eliminates primary hang | Small | Critical |
| Fix 2: Early cancel in `_run()` closures | Prevents queued steps from executing | Small | High |
| Fix 3: Cancel check in `_submit_composite` | Prevents composite dispatch after cancel | Small | High |
| Fix 4: `cancel_futures=True` in shutdown | Clean executor shutdown | Trivial | Medium |
| Fix 5: Suppress subprocess tracebacks | Cosmetic | Complex | Low/Deferred |

---

## Files to Modify

| File | Changes |
|------|---------|
| `pipeline_manager.py` `_wait_for_predecessors` | Poll with timeout, check cancel_event |
| `pipeline_manager.py` `submit` after `_wait_for_predecessors` | Re-check cancel_event post-wait |
| `pipeline_manager.py` `_run()` in `submit` | Cancel check at top of closure |
| `pipeline_manager.py` `_run()` in `_submit_composite` | Cancel check at top of closure |
| `pipeline_manager.py` `_submit_composite` | Cancel check before dispatch + re-check post-wait |
| `pipeline_manager.py` `finalize` | `cancel_futures=True` in shutdown |
| `test_pipeline_manager.py` | Cancel-during-predecessor-wait test |

---

## Thread Safety Notes

- `_cancel_event` is a `threading.Event` — `.is_set()` and `.set()`
  are thread-safe by design
- `_wait_for_predecessors` runs on the **main thread** inside `submit()`
- `_run()` closures run on the **ThreadPoolExecutor worker thread**
- The signal handler runs on the **main thread** (Python requirement)
- No new locks needed — all cancel checks are read-only on a thread-safe Event

---

## Testing Plan

- `test_cancel_during_predecessor_wait` — cancel while main thread is
  in `_wait_for_predecessors`, verify it unblocks within ~1s
- `test_queued_steps_exit_immediately_on_cancel` — submit multiple
  steps, cancel before they start, verify all return cancelled results
- `test_composite_skipped_on_cancel` — submit composite after cancel,
  verify it's skipped
- Existing tests: verify no regressions in `TestCancellation` class

---

## Verification

```bash
# Automated
~/.pixi/bin/pixi run -e dev test-unit

# Manual: cancel at various points, should exit within ~2 seconds
~/.pixi/bin/pixi run python docs/tutorials/execution/cancel_demo.py
```
