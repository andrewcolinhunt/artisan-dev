# Design: `post_step` Pipeline Sugar

**Date:** 2026-04-04
**Status:** Draft

---

## Problem

When a pipeline step runs N parallel workers that each produce an external
file (or any set of artifacts that need consolidation), users must manually
wire a consolidation curator as a separate step:

```python
main = pipeline.run(RunRosetta, inputs={"reference": prev.output("data")})
consolidated = pipeline.run(
    ConsolidateSilentFiles,
    inputs={"structures": main.output("structures")},
)
next_step = pipeline.run(
    ScoreStructures,
    inputs={"structures": consolidated.output("structures")},
)
```

This is boilerplate: the consolidation step always consumes the main step's
outputs, has no user-facing configuration, and exists only as plumbing.
The downstream step must reference the consolidation result, not the main
step -- an easy mistake.

More generally, any "run this after that, present as one logical unit"
pattern requires manual step wiring today.

---

## Prior Art Survey

### `submit()` and `StepFuture` (`orchestration/pipeline_manager.py`, `orchestration/step_future.py`)

`submit()` assigns step numbers eagerly, validates, dispatches, returns
`StepFuture`. `StepFuture.output(role)` creates `OutputReference` with
`source_step=self.step_number`.

**Key insight:** If `submit()` returns the post_step's `StepFuture`,
downstream `OutputReference` objects automatically point to the post_step.
No reference rewriting needed.

### Composite Expanded Mode (`composites/base/`)

`ExpandedCompositeContext` calls `pipeline.submit()` to create real pipeline
steps and returns a handle whose `.output()` points to the right internal
step. This is a proven precedent for inserting hidden steps and returning
a handle to the last one.

**Reuse:** The `post_step` mechanism follows the same pattern -- insert an
extra step, return its future.

### Validation (`_validate_operation_overrides`)

Called in `submit()` before dispatch. Validates params, resources, execution,
environment, tool keys, and input role/type compatibility. A recursive
`submit()` call for the post_step gets this validation for free.

### Summary

| Existing code | Disposition |
|---|---|
| `submit()` / `StepFuture` | Extend with `post_step` parameter and recursive call |
| Composite expanded mode | Precedent for hidden step insertion |
| `_validate_operation_overrides` | Reused via recursive `submit()` |

---

## Design

### API

A new parameter on `submit()` and `run()` that auto-inserts a step after
the main step, returning the post-step's `StepFuture`:

```python
step = pipeline.run(
    RunRosetta,
    inputs={"reference": prev.output("data")},
    post_step=ConsolidateSilentFiles,
)

# step.output("structures") -> consolidated artifacts
next_step = pipeline.run(
    ScoreStructures,
    inputs={"structures": step.output("structures")},
)
```

### Implementation

Add `post_step` parameter to `submit()` and `run()`. The post_step logic
is a single exit point at the bottom of `submit()`, after all existing
paths (dispatch, cache hit, early exit) have assigned `main_future`.

`submit()` currently has multiple early returns that must be refactored to
assign `main_future` instead of returning directly:

- **Composite routing** — deferred (returns before post_step logic; see
  Composites section)
- **Early exit (pipeline stopped/cancelled)** — post_step is skipped
  (no point consolidating a skipped step). Returns directly.
- **Cache hit** — assigns `main_future`, falls through to post_step logic.
  The post_step `submit()` call gets the cached step's `StepFuture`, so
  the consolidator runs on cached artifacts. If the consolidator is also
  cached, both skip.
- **File path promotion failure** — post_step is skipped. Returns directly.
- **Dispatch** — assigns `main_future`, falls through to post_step logic.

```python
def submit(
    self,
    operation,
    inputs=None,
    ...,
    post_step: type[OperationDefinition] | None = None,
) -> StepFuture:
    # ... composite routing (returns directly, deferred) ...
    # ... validation ...

    early = self._check_early_exit(...)
    if early is not None:
        return early  # post_step skipped for stopped/cancelled

    # ... step spec, cache check, file path promotion ...
    # All paths below assign main_future instead of returning

    if cached is not None:
        main_future = cached
    elif file_path_failure:
        return file_result  # post_step skipped for promotion failure
    else:
        main_future = self._dispatch_step(...)

    if post_step is not None:
        post_inputs = {
            role: main_future.output(role)
            for role in operation.outputs
        }
        return self.submit(
            post_step,
            inputs=post_inputs,
            backend=backend,
            compact=compact,
            skip_cache=skip_cache,
            failure_policy=failure_policy,
            name=f"{step_name}.post",
        )

    return main_future
```

**`post_step` forwards behavioral flags** (`skip_cache`, `failure_policy`,
`backend`, `compact`) but **does not forward overrides** (`params`,
`resources`, `execution`, `environment`, `tool`). The post_step runs with
its own operation defaults. For the current use case (consolidation
curators with minimal config), this is sufficient. A
`post_step_overrides` parameter can be added later if needed.

**Chaining:** If the post_step operation itself is passed a `post_step`,
the recursive call chains naturally — each consumes an additional step
number. This is supported but not a primary use case.

### Step Numbering

```
pipeline.run(RunRosetta, post_step=ConsolidateSilentFiles)
# -> step 0: RunRosetta (parallel, N files)
# -> step 1: ConsolidateSilentFiles (name: "run_rosetta.post")
# -> returned StepFuture has step_number=1
```

Two step numbers consumed. The user interacts with the second one.

### Role Matching

Post_step input roles must match the main step's output roles. Validated
by the existing `_validate_operation_overrides()` in the recursive
`submit()` call. No new validation code needed.

### Caching and Early-Return Behavior

Both steps cache independently:

- If main step is fully cached, the consolidator runs on cached artifacts
- If the consolidator is also cached, both skip
- Partial cache hits on the main step work correctly -- the consolidator
  always receives all artifacts (cached + fresh)

**Post_step behavior by `submit()` path:**

| Path | Post_step fires? | Rationale |
|------|-----------------|-----------|
| Cache hit | Yes | Consolidator runs on cached artifacts |
| Dispatch | Yes | Normal execution |
| Pipeline stopped/cancelled | No | Nothing to consolidate |
| File path promotion failure | No | Main step failed to start |
| Composite routing | No (deferred) | Composites handle internal steps |

**Error recovery:** If the post_step fails, the main step's results are
already committed. Re-running the pipeline re-runs only the post_step
(main step is cached). This is correct and consistent with existing
step-level caching semantics.

### Naming

- Main step uses user-provided `name` (or `operation.name`)
- Post_step gets `f"{step_name}.post"` (`.` separator matches composite
  naming in `ExpandedCompositeContext`)
- Both visible in pipeline results

### Composites

Deferred. If the main step is a composite, `submit()` routes composites
before reaching `post_step` logic (line 1116-1129 of `pipeline_manager.py`).
Composites that need consolidation can use explicit steps in `compose()`.

### Generality

`post_step` accepts any `OperationDefinition` subclass, not just
consolidation curators. It's a general mechanism for "run this after that,
present as one logical step."

**Role matching constraint:** The post_step's input roles must match the
main step's output roles. This is enforced by existing validation in the
recursive `submit()` call. Post_step operations must either declare input
roles that match the main step's output roles, or use
`runtime_defined_inputs = True` to accept arbitrary roles. Generic
curators like `Filter` (which expects `"passthrough"`) cannot be used as
post_steps without matching role names.

---

## Scope

| File | Change |
|------|--------|
| `src/artisan/orchestration/pipeline_manager.py` | Add `post_step` param to `submit()` and `run()`, recursive dispatch logic |

---

## Testing

| Test file | Coverage |
|-----------|----------|
| `tests/artisan/orchestration/test_post_step.py` | StepFuture points to post_step, step numbering (two consumed), downstream OutputReference resolution, caching (both cached, partial cache), role mismatch error, post_step=None is no-op |

---

## Open Questions

- **User mental model with hidden step numbers.** `post_step` consumes two
  step numbers but presents as one logical step. If the user inspects
  `step.step_number`, they get the post_step number (1), not the main step
  (0). Is this surprising? Should the returned `StepFuture` expose both
  (e.g., `step.main_step_number`)?

- **Pipeline results display.** Does the user see both steps in pipeline
  results, or just the consolidated one? Both are real steps with real
  `StepResult` objects. Hiding the main step would require new machinery.
  Showing both is simpler but may confuse users who think they submitted
  one step.

- **Composites interaction.** The current design defers composite support.
  This means composites that produce external files use a different API
  (explicit consolidation step in `compose()`) than non-composite steps
  (`post_step` sugar). Is this asymmetry acceptable long-term?

---

## Related Docs

- `2-external-content-artifacts.md` -- `files_root` infrastructure that
  consolidation curators write to
- `4-silent-file-pipeline.md` -- Domain-specific consolidation curator
  (protein design repo) that motivates this mechanism
