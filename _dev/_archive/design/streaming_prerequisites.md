# Design: Streaming Prerequisites

**Date:** 2026-03-24
**Status:** Draft
**Depends on:** nothing
**Depended on by:** `streaming_pipeline_execution.md`

---

## Summary

The minimal set of changes required before streaming pipeline execution can
be implemented. All three components are independent and can be built in
parallel.

```
A (parallel step execution)
B (bulk cache lookup)
C (DispatchHandle cancellation)
```

Note: `pipeline_run_id` scoped output resolution (see `pipeline_replay.md`)
is NOT needed for streaming. Parallel branches within a single pipeline get
unique step numbers from the monotonically increasing `_current_step`
counter. `resolve_output_reference()` filters by `origin_step_number`, which
is unambiguous within a single run. `pipeline_run_id` scoping is a
replay/re-run concern, not a streaming concern.

---

## Component A: Parallel Step Execution

**Problem:** `PipelineManager` executes steps sequentially.
`ThreadPoolExecutor` has `max_workers=1`. `_wait_for_predecessors()` runs on
the main thread before the step is submitted to the executor. `_step_results`
and `StepTracker._write_row()` have no thread safety.

**Current state:** The dependency tracking (`_extract_source_steps`) and
`StepFuture.output()` already work correctly for parallelism. None of the
changes below have been implemented.

**Changes:**

- Make `ThreadPoolExecutor` max_workers configurable via
  `max_parallel_steps` on `PipelineConfig` (default 1)
- Move `_wait_for_predecessors()` into the `_run()` closure so each step
  waits on its own thread
- Add `threading.Lock` for `_step_results` list
- Add `threading.Lock` for `StepTracker._write_row()`
- Fix `_stopped` propagation — currently pipeline-global, must be replaced with input-resolution-based skip logic (empty
  input resolution already returns empty and triggers skip)

---

## Component B: Bulk Cache Lookup

**Problem:** The dispatch loop calls `cache_lookup()` once per
`ExecutionUnit` — up to 3 Delta scans per call. At scale this takes 20+
minutes. With streaming, work is dispatched in smaller increments,
amplifying the per-unit cost.

**Current state:** No `bulk_cache_lookup()` exists. The dispatch loop in
`_execute_creator_step()` checks cache serially per unit. The `CacheHit`
return value is used as a boolean — `inputs`/`outputs` from `CacheHit` are
never read.

**Changes:**

- Add `bulk_cache_lookup(spec_ids, delta_root) -> set[str]` that performs a
  single `is_in()` scan against the executions table
- Refactor the dispatch loop into three phases: compute all spec_ids, one
  bulk cache lookup, create `ExecutionUnit` objects only for cache misses

---

## Component C: DispatchHandle Cancellation

**Problem:** `BackendBase.create_flow()` returns a plain callable. Once
called, there is no way to cancel in-flight backend work. The cancel event
is checked between step phases but cannot interrupt a `step_flow()` call
blocked on SLURM results. With streaming, multiple steps run concurrently —
upstream errors or priority changes require cancelling in-flight dispatches.

**Current state:** `cancel()` method, `_cancel_event`, signal handling, and
parallel result collection already exist. The `DispatchHandle` abstraction does
not.

**Changes:**

- Add `DispatchHandle` ABC with `run()` and `cancel()` methods
- Change `BackendBase.create_flow()` return type from `Callable` to
  `DispatchHandle`
- Implement `LocalDispatchHandle` (cancel event propagation)
- Implement `SlurmDispatchHandle` (`scancel --name` + cancel event)
- Update `_execute_creator_step` to use `flow_handle.run()` and track
  active flow handle for cancellation
- Pass `cancel_event` to `_collect_results` for cancel-aware result
  collection
