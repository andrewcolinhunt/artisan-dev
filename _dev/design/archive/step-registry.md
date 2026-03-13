# Design: Step Registry — Decouple `output()` from Step Completion

**Date:** 2026-03-12  **Status:** Draft  **Author:** Claude + ach94

---

## Problem

`pipeline.output(name, role)` fails for steps that have been submitted
but not yet completed. This makes `submit()` + `output()` wiring
unreliable — the exact sequence that users write when building async
pipelines.

### Reproduction

```python
for i in range(5):
    pipeline.submit(DataGenerator, name=f"gen_{i}", ...)

for i in range(5):
    pipeline.submit(DataTransformer,
        inputs={"data": output(f"gen_{i}", "datasets")},  # ValueError
    )
```

`output("gen_0", "datasets")` evaluates as a Python argument expression
**before** `submit()` runs. At that point, `gen_0` is still executing
in the thread pool — it hasn't been added to `_named_steps` yet.

With cancellation this is worse: cancelled-mid-execution steps may never
populate `_named_steps`, so any subsequent `output()` call for them is
permanently broken.

### Root cause

`_named_steps` conflates two concerns:

| Concern | When known | Who needs it |
|---------|-----------|--------------|
| "Does this step exist? What does it produce?" | Submit time | `output()` |
| "Did this step complete? What were the results?" | Completion time | `finalize()`, `__iter__`, logging |

For `run()` (synchronous), these coincide — the step completes before
the next line. For `submit()` (async), they don't. `output()` is gated
on completion data it doesn't use.

---

## Context

`OutputReference` is already lazy. It contains three fields:

```python
OutputReference(source_step=step_number, role="datasets", artifact_type="data")
```

No runtime data. No artifacts. No success/failure. It says "when you
execute, resolve artifacts from step N, role R." All three values come
from the operation class definition and the step counter — available at
submit time.

The actual data resolution happens later in `_wait_for_predecessors`,
which blocks on the upstream future. The reference is just a pointer.

`StepFuture` (returned by `submit()`) already has an `output()` method
that constructs references without blocking — proving the metadata is
available at submit time. The gap is that `pipeline.output(name, role)`
doesn't have access to this metadata.

---

## Design

### New data structure: `_step_registry`

A dict populated at **declaration time** — the moment `submit()` or
`run()` is called, before dispatch to the thread pool:

```python
@dataclass(frozen=True)
class _StepEntry:
    step_number: int
    output_roles: frozenset[str]
    output_types: dict[str, str | None]

_step_registry: dict[str, list[_StepEntry]]  # name -> entries
```

### Modified `output()`

```python
def output(self, name: str, role: str, *, step_number: int | None = None) -> OutputReference:
    entries = self._step_registry.get(name)
    if not entries:
        available = sorted(self._step_registry.keys()) or ["(none)"]
        msg = f"No step named '{name}'. Available: {', '.join(available)}"
        raise ValueError(msg)

    entry = entries[-1] if step_number is None else ...  # lookup by step_number
    if role not in entry.output_roles:
        ...  # existing validation

    return OutputReference(
        source_step=entry.step_number,
        role=role,
        artifact_type=entry.output_types.get(role),
    )
```

### Registration points

Every path that assigns a step number also registers in `_step_registry`:

| Path | Where | Currently registers in `_named_steps` |
|------|-------|--------------------------------------|
| `submit()` — cache hit | Line ~1098 | Yes (immediately) |
| `submit()` — cache miss (dispatch) | Line ~1163 | No (deferred to `_run()`) |
| `submit()` — stopped | Line ~995 | Yes (immediately) |
| `submit()` — cancelled | Line ~1037 | Yes (immediately) |
| `submit()` — file promotion failure | Line ~1136 | Yes (immediately) |
| `run()` | Delegates to `submit().result()` | Via `submit()` paths |
| `expand()` — cache hit | Line ~1501 | Yes (immediately) |
| `expand()` — cache miss | Line ~1515 | No (deferred to `_run()`) |
| `expand()` — stopped | Line ~1463 | Yes (immediately) |
| `from_delta()` | Line ~810 | Yes (immediately) |

The two "No" rows are the bug. They dispatch to the executor and return
a `StepFuture` without registering in `_named_steps`. The fix: register
in `_step_registry` at the same point where `step_number` is assigned,
before the executor dispatch.

### `_named_steps` unchanged

`_named_steps` continues to track **completed** `StepResult` objects.
It's used by:

- `__iter__` — iterate over completed step results
- `finalize()` — summarize pipeline execution
- Logging — step completion messages
- `from_delta()` — reconstruct pipeline state

None of these should see pending/in-flight steps. Separation is correct.

---

## Changes

| File | Change |
|------|--------|
| `pipeline_manager.py` `__init__` | Add `_step_registry: dict[str, list[_StepEntry]]` |
| `pipeline_manager.py` `submit()` | Register `_StepEntry` before executor dispatch |
| `pipeline_manager.py` `expand()` | Register `_StepEntry` before executor dispatch |
| `pipeline_manager.py` `output()` | Look up `_step_registry` instead of `_named_steps` |
| `pipeline_manager.py` `from_delta()` | Populate `_step_registry` alongside `_named_steps` |

No changes to: `OutputReference`, `StepResult`, `StepFuture`,
`step_executor.py`, `dispatch.py`, or any test that uses `run()`.

---

## Thread safety

Registration happens synchronously in the main thread inside
`submit()`/`run()`. `output()` is also called from the main thread.
No concurrent writes to `_step_registry`, no locks needed.

`_named_steps` writes happen in worker threads (inside `_run()`), but
`output()` no longer reads from it, so the race condition is eliminated.

---

## Alternatives considered

### Placeholder `StepResult` in `_named_steps`

Insert a dummy `StepResult(success=True, total_count=0, ...)` at submit
time. Replace with the real result on completion.

Rejected because:

- Creates objects that look like real results but aren't. Any code
  iterating `_named_steps` must distinguish placeholders from real
  results — a new invariant that's easy to violate.
- Two entries per step (placeholder + real) requires cleanup logic or
  replacement semantics. `steps[-1]` coincidentally works but is
  fragile.
- Category error: a declaration is not a result.

### Make `output()` block on the upstream future

Have `output()` check `_active_futures` and call `.result()` if the step
hasn't completed.

Rejected because:

- Turns a metadata lookup into a blocking call with unbounded wait time.
- Defeats the purpose of `submit()` — the user expects non-blocking
  pipeline construction.
- Can deadlock if the thread pool is full and the blocked main thread
  holds a slot.

### Require users to use `StepFuture.output()` instead

The return value of `submit()` already has a non-blocking `output()`
method. Require `sf = pipeline.submit(...); sf.output("role")`.

Rejected because:

- Forces different wiring patterns for `run()` vs `submit()`.
- `pipeline.output(name, role)` is the ergonomic, name-based API. Users
  shouldn't need to track future objects to wire steps.
- Doesn't fix the case where `output()` is called after cancellation
  for steps that started but were cancelled mid-execution.

---

## Verification

```bash
# Existing tests — no regressions
~/.pixi/bin/pixi run -e dev test-unit

# Manual: cancel_demo.py with Ctrl+C at various points
~/.pixi/bin/pixi run python docs/tutorials/execution/cancel_demo.py

# Manual: cancel_demo_slurm.py on cluster
~/.pixi/bin/pixi run python docs/tutorials/execution/cancel_demo_slurm.py
```

New tests to add:

- `test_output_before_submit_completes` — call `output()` for a
  submitted-but-not-completed step, verify `OutputReference` is returned
- `test_output_after_cancellation` — cancel pipeline, verify `output()`
  still works for cancelled steps
- `test_output_invalid_name_raises` — verify clear error for nonexistent
  step names
- `test_output_invalid_role_raises` — verify clear error for wrong role
- `test_output_before_expand_completes` — call `output()` for a
  submitted-but-not-completed composite step, verify `OutputReference`
  is returned
