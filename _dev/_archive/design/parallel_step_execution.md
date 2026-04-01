# Design: Parallel Step Execution

**Status:** Draft
**Scope:** `PipelineManager` step dispatch (`pipeline_manager.py`, `step_future.py`)
**Goal:** Enable independent pipeline steps to execute concurrently, particularly for SLURM-submitted steps that don't depend on each other

---

## Motivation

Currently, pipeline steps always execute sequentially — even when they have no
data dependencies. For example:

```python
pipeline = PipelineManager.create(...)
step0 = pipeline.submit(IngestStructure, inputs=files)
step1 = pipeline.submit(RFD3, inputs=step0.output("structure"))       # depends on step0
step2 = pipeline.submit(MPNN, inputs=step0.output("structure"))       # depends on step0, NOT step1
step3 = pipeline.submit(ScoreOp, inputs={                             # depends on step1 AND step2
    "rfd3": step1.output("structure"),
    "mpnn": step2.output("structure"),
})
```

Steps 1 and 2 are independent — both depend only on step 0. With SLURM
backends, both could be submitted to the cluster simultaneously, and step 3
would wait for both to complete. Today, step 2 waits for step 1 to finish
before even being submitted to SLURM, wasting wall-clock time.

---

## Current Architecture

### How it works today

1. `submit()` is called on the main thread
2. It calls `_wait_for_predecessors(inputs)` **on the main thread** — blocks
   until upstream `OutputReference` sources are done
3. It increments `_current_step`, records metadata, and submits the `_run()`
   closure to a `ThreadPoolExecutor(max_workers=1)`
4. Returns a `StepFuture` immediately

### Three barriers to parallelism

**Barrier 1: `max_workers=1` on the ThreadPoolExecutor** (line 401)

```python
self._executor = ThreadPoolExecutor(
    max_workers=1, thread_name_prefix="pipeline-step"
)
```

Only one step closure runs at a time. Even if two steps have no dependency,
the second queues behind the first.

**Barrier 2: `_wait_for_predecessors` runs on the main thread** (line 718)

```python
self._wait_for_predecessors(inputs)  # blocks main thread
step_number = self._current_step
# ... validation, cache check ...
cf_future = self._executor.submit(ctx.run, _run)
```

Because the wait happens before the executor submit, the main thread blocks
on each step's predecessors sequentially. Even with `max_workers=N`, the
main thread would serialize step submissions when predecessors aren't done yet.

The fix: move `_wait_for_predecessors` inside the `_run()` closure so each
step waits on its own thread.

**Barrier 3: Shared mutable state without synchronization**

Several fields are mutated from the `_run()` closure (background thread) and
from `submit()` (main thread) without locks:

| Field | Written from | Risk |
|---|---|---|
| `self._step_results.append(result)` | background thread (`_run()`, line 886/911) | Concurrent appends from multiple threads |
| `self._current_step += 1` | main thread (`submit()`, line 817) | Safe — only main thread writes this |
| `self._step_spec_ids[step_number]` | main thread (`submit()`, line 818) | Safe — only main thread writes this |

Only `_step_results` is at risk — it's appended from background threads.

### What already works

- **Dependency tracking**: `_extract_source_steps()` correctly identifies only
  the specific upstream steps referenced in `inputs`. If step 2 doesn't
  reference step 1, it won't wait for step 1.
- **`StepFuture.output()`**: Returns an `OutputReference` immediately without
  blocking — wiring is decoupled from execution.
- **`finalize()`**: Already iterates all `_active_futures` and waits for
  completion.
- **Delta Lake writes**: Each step writes to its own staging directory
  (partitioned by `step_number`), then commits via `DeltaCommitter`. The commit
  phase uses Delta Lake's transactional append — concurrent appends from
  different steps should be safe (Delta Lake handles this via optimistic
  concurrency).

---

## Proposed Changes

### Change 1: Make `max_workers` configurable

Add a `max_parallel_steps` parameter to `PipelineConfig` (or `PipelineManager.create()`),
defaulting to `1` for backward compatibility:

```python
# PipelineConfig
max_parallel_steps: int = 1

# PipelineManager.__init__
self._executor = ThreadPoolExecutor(
    max_workers=config.max_parallel_steps,
    thread_name_prefix="pipeline-step",
)
```

### Change 2: Move `_wait_for_predecessors` inside the closure

Before (current — main thread blocks):
```python
def submit(self, operation, inputs, ...):
    self._wait_for_predecessors(inputs)   # blocks main thread
    step_number = self._current_step
    ...
    def _run() -> StepResult:
        result = execute_step(...)
        return result

    cf_future = self._executor.submit(ctx.run, _run)
```

After (each step waits on its own thread):
```python
def submit(self, operation, inputs, ...):
    step_number = self._current_step      # assign immediately
    ...
    def _run() -> StepResult:
        self._wait_for_predecessors(inputs)  # blocks this thread only
        result = execute_step(...)
        return result

    cf_future = self._executor.submit(ctx.run, _run)
```

This lets the main thread continue issuing `submit()` calls without blocking,
while each background thread independently waits for its specific predecessors.

**Important**: `record_step_start` should also move inside the closure (after
the predecessor wait), since it records a "running" timestamp that should
reflect when the step actually starts executing, not when it was queued.

### Change 3: Thread-safe `_step_results`

Protect `_step_results` appends with a `threading.Lock`:

```python
def __init__(self, ...):
    ...
    self._results_lock = threading.Lock()

# In _run() closure:
with self._results_lock:
    self._step_results.append(result)
```

Alternatively, use a thread-safe collection, but a lock is simpler and the
append is not a hot path.

### Change 4: Thread-safe StepTracker writes

`StepTracker._write_row()` does Delta Lake writes. With concurrent steps,
two steps could call `_write_row()` simultaneously. Delta Lake append mode
should handle this via optimistic concurrency, but add a lock as a safety
measure:

```python
class StepTracker:
    def __init__(self, ...):
        ...
        self._write_lock = threading.Lock()

    def _write_row(self, df: pl.DataFrame) -> None:
        with self._write_lock:
            # existing write logic
```

---

## What Does NOT Change

- **`run()` behavior**: `run()` = `submit().result()` — still blocks. Users
  who call `run()` for every step get the same sequential behavior.
- **`_current_step` assignment**: Stays on the main thread in `submit()`.
  Step numbers are assigned in submission order, which is deterministic.
- **Intra-step parallelism**: Worker dispatch within a step (Prefect
  `task.map()`) is unchanged.
- **Cache semantics**: `step_spec_id` is computed from operation + step_number +
  params + upstream spec IDs — deterministic regardless of execution order.
- **`finalize()`**: Already waits for all futures.

---

## Execution Model: Before and After

### Before (sequential)

```
main thread:  submit(s0) ─ wait(s0) ─ submit(s1) ─ wait(s1) ─ submit(s2) ─ wait(s2) ─ submit(s3)
executor:     ───────────── [s0 runs] ─────────── [s1 runs] ─────── [s2 runs] ─── [s3 runs]
wall clock:   |─────── s0 ───────|─── s1 ───|─── s2 ───|────── s3 ──────|
```

### After (parallel independent steps)

```
main thread:  submit(s0) ─ submit(s1) ─ submit(s2) ─ submit(s3)  [all return immediately]
executor:     thread-1: [s0 runs] ──────────────── [s3 waits for s1,s2, then runs]
              thread-2: ───── wait(s0) [s1 runs] ──
              thread-3: ───── wait(s0) [s2 runs] ──
wall clock:   |── s0 ──|── s1 + s2 (parallel) ──|── s3 ──|
```

---

## Edge Cases and Risks

### Delta Lake commit contention

Each step's `_execute_creator_step` has a commit phase that calls
`DeltaCommitter.commit_all_tables()`. If two steps commit simultaneously, they
write to the same Delta tables (e.g., artifact_index, executions).

Delta Lake uses optimistic concurrency — concurrent appends succeed as long as
they don't conflict on the same partition. Since each step writes different
artifacts, this should be safe. However:

- **Compaction** (`_compact_step_tables`) could conflict. Mitigation: skip
  compaction for parallel steps and run it once in `finalize()`, or gate
  compaction with a lock.
- **First-write mode** in `StepTracker._write_row()` uses `mode="overwrite"` on
  first write vs `mode="append"` on subsequent writes. With parallel steps,
  two threads could both see "table doesn't exist" and race. Mitigation: use a
  lock (Change 4 above) or always use append mode with create-if-not-exists.

### Predecessor failure propagation

`_wait_for_predecessors` catches exceptions from failed predecessors and logs a
warning. With parallel steps, a failed step 1 should not prevent independent
step 2 from running — which is already the behavior since
`_wait_for_predecessors` only checks inputs for the current step.

A failed predecessor that IS a dependency will produce empty inputs for the
downstream step, which is handled by `_all_inputs_empty()` in `execute_step`.

### Step numbering semantics

Step numbers are assigned on the main thread in submission order, which remains
deterministic. However, with parallel execution, step N may complete before
step N-1. This is fine — step numbers reflect *submission* order, not
*completion* order. `_step_results` may contain results out of order, but
`finalize()` doesn't depend on ordering.

### `run()` still works

`run()` calls `submit().result()`, so it blocks the main thread. A pipeline
mixing `run()` and `submit()` would still work but wouldn't achieve full
parallelism — the `run()` calls act as synchronization barriers.

---

## User-Facing API

### Option A: Pipeline-level config (recommended)

```python
pipeline = PipelineManager.create(
    name="my_pipeline",
    base_dir="./results",
    max_parallel_steps=4,
)
```

Simple, discoverable, and consistent with existing config patterns like
`max_workers_local`.

### Option B: Per-submit override

```python
pipeline.submit(RFD3, inputs=..., parallel=True)
```

More granular but adds API complexity for minimal benefit — if steps are
independent, the framework already knows from the dependency graph.

**Recommendation: Option A.** The framework determines parallelism from the
dependency structure; users just set the concurrency limit.

---

## Implementation Plan

### Phase 1: Thread safety (no behavior change)

1. Add `threading.Lock` for `_step_results` in `PipelineManager`
2. Add `threading.Lock` for `_write_row` in `StepTracker`
3. Run existing tests — should pass with zero behavior change

### Phase 2: Move predecessor wait into closure

1. Move `_wait_for_predecessors` call from `submit()` into the `_run()` closure
2. Move `record_step_start` into the closure (after predecessor wait)
3. Keep `max_workers=1` — behavior is still sequential
4. Run existing tests

### Phase 3: Make `max_workers` configurable

1. Add `max_parallel_steps` to `PipelineConfig`
2. Wire it through to `ThreadPoolExecutor`
3. Default to `1` for backward compatibility

### Phase 4: Handle compaction

1. Skip per-step compaction when `max_parallel_steps > 1`
2. Run compaction once in `finalize()`
3. Or gate compaction with a dedicated lock

### Phase 5: Testing

1. Unit test: two independent steps with `max_parallel_steps=2` — verify
   they overlap in time (both start before either finishes)
2. Unit test: dependent step waits for predecessor — verify correct ordering
3. Unit test: failed predecessor — verify independent steps still run
4. Integration test: SLURM backend with parallel steps (if applicable)

---

## Alternatives Considered

### Full DAG scheduler

Build a dependency graph upfront and use a topological sort to schedule steps.
**Rejected** — the current submit-based API doesn't declare all steps upfront.
Steps are submitted incrementally, so a full DAG isn't available at pipeline
start. The "wait for predecessors in the closure" approach achieves the same
result without requiring upfront declaration.

### asyncio instead of threads

Replace `ThreadPoolExecutor` with `asyncio` event loop. **Rejected** — the
execution layer (Prefect, SLURM submission) is synchronous and blocking.
Wrapping blocking calls in `asyncio.to_thread` adds complexity without benefit
over the current `ThreadPoolExecutor` approach. Threads are the right tool here
since the work is I/O-bound (waiting for SLURM jobs).

### Multiprocessing

Use `ProcessPoolExecutor` instead of `ThreadPoolExecutor`. **Rejected** —
steps need access to shared state (`_active_futures`, `_step_results`,
`PipelineConfig`). Multiprocessing would require serialization of these objects
and IPC, adding significant complexity for no benefit (the parallelism is in
SLURM, not in the Python process).
