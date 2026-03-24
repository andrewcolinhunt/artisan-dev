# Design: Parallel Step Execution

**Date:** 2026-03-24
**Status:** Draft

---

## Summary

Enable independent pipeline steps to execute concurrently. Currently
`PipelineManager` runs steps sequentially via `ThreadPoolExecutor` with
`max_workers=1`. Steps with no data dependencies cannot overlap.

This is the foundation for streaming pipeline execution. With parallel
steps, branched pipelines stream naturally — branch 1's downstream step
starts as soon as its upstream finishes, independent of other branches.

---

## Constraints

**Dependency tracking already works.** `_extract_source_steps()` and
`StepFuture.output()` correctly model inter-step dependencies.
`_wait_for_predecessors()` blocks on the right futures. The wiring is
parallel-ready — the bottleneck is the single-threaded executor.

**Thread safety does not exist.** `_step_results` (list), `_named_steps`
(dict), `_step_registry` (dict), and `_current_step` (int) are mutated
from both the main thread and executor threads with no synchronization.
Safe only because `max_workers=1`.

**`_stopped` is pipeline-global.** When a step gets empty inputs, it sets
`self._stopped = True`, which causes all subsequent steps to skip —
including steps on independent branches that have valid inputs.

---

## Design

### Configurable parallelism

Add `max_parallel_steps` to `PipelineConfig` (default 1). The
`ThreadPoolExecutor` uses this as `max_workers`.

### Move predecessor waiting into the worker thread

Currently `_wait_for_predecessors()` runs on the main thread before
submitting to the executor. This means `submit()` blocks until all
upstream steps complete — serializing everything regardless of
`max_workers`.

Move `_wait_for_predecessors()` into the `_run()` closure. Each step waits
on its own thread. Independent steps proceed in parallel. `submit()`
returns immediately after validation, step numbering, and cache check.

### Thread safety

- `threading.Lock` for `_step_results` list mutations
- `threading.Lock` for `StepTracker._write_row()` Delta writes
- `_current_step` increment and step numbering stay on the main thread
  (inside `submit()`), so no lock needed

### Replace `_stopped` with input-resolution skip logic

Remove the pipeline-global `_stopped` flag. Instead, rely on the existing
behavior: when a step's inputs resolve to empty (because the upstream step
was skipped or produced nothing), the step skips itself. This is already
how `_execute_creator_step` handles empty inputs — it returns a skipped
result without dispatching.

Skip propagation becomes per-branch: a skipped step on branch A does not
affect branch B, because branch B's inputs resolve independently.

---

## Scope

| File | Change |
|------|--------|
| `schemas/orchestration/pipeline_config.py` | Add `max_parallel_steps: int = 1` |
| `orchestration/pipeline_manager.py` | Use `max_parallel_steps` for executor, move `_wait_for_predecessors` into `_run()`, add locks, remove `_stopped` |
| `orchestration/engine/step_tracker.py` | Add lock around `_write_row()` |
