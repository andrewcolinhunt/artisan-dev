# Design: Pipeline Replay (Re-run from Step N)

**Status:** Draft
**Scope:** `PipelineManager`, `StepTracker`, `resolve_output_reference`, `table_schemas`

---

## Problem

Once a pipeline completes, there is no way to re-run a subset of steps. The
two-level cache (step-level via `step_spec_id`, execution-level via
`execution_spec_id`) automatically skips any step whose configuration matches a
previous successful run. There is no `force` flag, no invalidation API, and no
way to say "keep steps 0-1, redo from step 2."

### Real-world scenarios

1. **Parameter tuning** — step 2 used a bad threshold. You want to re-run step 2
   with `threshold=0.5` and propagate through steps 3-4, keeping step 0-1
   outputs as-is.

2. **Bug fix in an operation** — the code for step 3 had a bug. After fixing it,
   you want to re-execute step 3+ without re-running the expensive SLURM steps
   before it.

3. **Exploratory branching** — you want to try a different filter at step 2 and
   compare both runs side-by-side.

### Current workarounds (all bad)

| Workaround | Problem |
|---|---|
| Change params to bust the cache | Only works if the new params are actually different |
| Use a fresh `delta_root` | Loses all previous artifacts, must re-run everything |
| Manually edit Delta tables | Fragile, error-prone, no API |

---

## Goals

1. Let users re-run a pipeline from an arbitrary step, keeping earlier steps intact
2. Preserve full history — old and new runs coexist in the same `delta_root`
3. Prevent output mixing — downstream steps only see outputs from the current run
4. Minimal schema changes, no breaking changes to existing data

### Non-goals (for now)

- Automatic cascade invalidation (too complex for initial implementation)
- Artifact deletion / garbage collection
- Declarative pipeline DAG (the imperative model is fine)

---

## Current Architecture

### How caching works today

```
submit() called
    |
    v
Compute step_spec_id = hash(op_name, step_number, params, input_spec, command)
    |
    v
Step cache check: steps table WHERE step_spec_id = X AND status = "completed"
    |
    +-- HIT  --> return cached StepResult immediately
    +-- MISS --> dispatch to workers
                    |
                    v
              Per-batch: compute execution_spec_id = hash(op_name, artifact_ids, params, command)
                    |
                    +-- HIT  --> skip batch
                    +-- MISS --> execute batch
```

### How output resolution works today

`resolve_output_reference()` in `inputs.py` resolves `OutputReference(source_step=N, role=R)` by:

1. Query `executions` WHERE `origin_step_number = N AND success = True` → get `execution_run_id`s
2. Query `execution_edges` WHERE `execution_run_id IN (...) AND direction = "output" AND role = R`
3. Return deduplicated, sorted artifact IDs

**Critical problem:** This query is **not scoped to a pipeline run**. It returns
outputs from ALL successful executions at step N across ALL runs. If step N has
been run twice (original + replay), downstream steps would receive a union of
both runs' outputs — double the artifacts, mixed provenance.

### Key ID relationships

```
pipeline_run_id     (per pipeline session, stored in steps table only)
  |
  +-- step_spec_id      (deterministic per step config — step-level cache key)
  |     +-- step_run_id  (unique per step attempt)
  |
  +-- execution_spec_id  (deterministic per batch config — execution-level cache key)
        +-- execution_run_id  (unique per execution attempt — FK everywhere)
              +-- execution_edges  (input/output artifact links)
              +-- artifact_edges   (source→target derivation links)
```

Note: `pipeline_run_id` exists in the `steps` table but NOT in `executions` or
`execution_edges`. This is the root cause of the scoping gap.

---

## Design

### Approach: "New run, replay from step N" + scoped output resolution

A replay creates a **new pipeline run** that:
- Inherits completed steps 0..N-1 from a source run (read-only references)
- Re-executes steps N+ with a `force` flag that bypasses caching
- Scopes output resolution to the current run's executions only

Both runs coexist in Delta Lake. Users can compare, discard, or build on either.

### User-facing API

```python
# Original run
pipeline = PipelineManager.create("my_pipeline", delta_root=..., staging_root=...)
s0 = pipeline.run(IngestStructure, inputs=files)
s1 = pipeline.run(ScoreOp, inputs={"structure": s0.output("structure")})
s2 = pipeline.run(FilterOp, inputs={"structure": s1.output("structure")})
pipeline.finalize()

# Later: replay from step 1 with different params
pipeline2 = PipelineManager.replay(
    delta_root=...,
    staging_root=...,
    pipeline_run_id="my_pipeline_20260226_...",
    from_step=1,
)
# step(0) returns the inherited StepResult from the original run
s1 = pipeline2.run(
    ScoreOp,
    inputs={"structure": pipeline2.step(0).output("structure")},
    params={"threshold": 0.5},
    force=True,
)
s2 = pipeline2.run(
    FilterOp,
    inputs={"structure": s1.output("structure")},
    force=True,
)
pipeline2.finalize()
```

### Component changes

#### 1. Schema: Add `pipeline_run_id` to `executions` table

**File:** `src/artisan/storage/core/table_schemas.py`

```python
EXECUTIONS_SCHEMA = {
    "execution_run_id": pl.String,
    "execution_spec_id": pl.String,
    "pipeline_run_id": pl.String,     # NEW — links execution to pipeline run
    "origin_step_number": pl.Int32,
    ...
}
```

This is the key enabler for scoped output resolution. The column is nullable for
backwards compatibility — old data will have `null` values, which means
pre-migration executions are visible to all runs (preserving current behavior
for existing pipelines).

**Migration:** No destructive migration needed. Adding a nullable column to an
append-only Delta table is safe. Old rows will read as `null` for the new column.

#### 2. `PipelineManager.run()` / `submit()`: Add `force` parameter

**File:** `src/artisan/orchestration/pipeline_manager.py`

```python
def run(
    self,
    operation: type[OperationDefinition],
    ...
    force: bool = False,      # NEW
) -> StepResult:
```

When `force=True`:
- Skip the step-level cache check (`_step_tracker.check_cache()`)
- Pass `force=True` down to `execute_step()` which skips per-batch cache checks

When `force=False` (default): current behavior, no change.

#### 3. `execute_step()`: Accept and propagate `force`

**File:** `src/artisan/orchestration/engine/step_executor.py`

```python
def execute_step(
    ...
    force: bool = False,      # NEW
) -> StepResult:
```

When `force=True`, skip the `check_cache_for_batch()` call inside
`generate_execution_unit_batches()`. All batches are dispatched to workers
regardless of cache state.

#### 4. `execute_step()`: Pass `pipeline_run_id` for execution records

The `pipeline_run_id` from `PipelineConfig` must be propagated into the
`ExecutionRecord` so it gets written to the `executions` Delta table.

**File:** `src/artisan/schemas/execution/execution_record.py`

```python
@dataclass(frozen=True)
class ExecutionRecord:
    execution_run_id: str
    execution_spec_id: str
    pipeline_run_id: str          # NEW
    origin_step_number: int
    ...
```

This flows from `PipelineConfig.pipeline_run_id` → `execute_step()` →
`ExecutionRecord` → staged Parquet → committed to `executions` Delta table.

#### 5. Scoped output resolution

**File:** `src/artisan/orchestration/engine/inputs.py`

```python
def resolve_output_reference(
    ref: OutputReference,
    delta_root: Path,
    pipeline_run_id: str | None = None,   # NEW
) -> list[str]:
```

When `pipeline_run_id` is provided:
- Query `executions` WHERE `origin_step_number = N AND success = True
  AND pipeline_run_id = <run_id>`
- Falls back to unscoped query if `pipeline_run_id` column doesn't exist yet
  (backwards compat with pre-migration data)

When `pipeline_run_id` is `None`: current behavior (unscoped).

The `pipeline_run_id` is threaded from `PipelineConfig` → `execute_step()` →
`resolve_inputs()` → `resolve_output_reference()`.

#### 6. `PipelineManager.replay()` factory method

**File:** `src/artisan/orchestration/pipeline_manager.py`

```python
@classmethod
def replay(
    cls,
    delta_root: Path | str,
    staging_root: Path | str,
    pipeline_run_id: str,
    from_step: int,
    name: str | None = None,
    working_root: Path | str | None = None,
    prefect_server: str | None = None,
    **kwargs: Any,
) -> PipelineManager:
    """Replay a pipeline from a specific step.

    Creates a new pipeline run that inherits completed steps 0..from_step-1
    from the source run. Steps from from_step onward must be re-executed
    by the caller (typically with force=True).

    Args:
        delta_root: Root path for Delta Lake tables.
        staging_root: Root path for worker staging files.
        pipeline_run_id: Source run to replay from.
        from_step: First step to re-execute (0-indexed).
        name: Pipeline name override.
        working_root: Root path for worker sandboxes.
        prefect_server: Prefect server URL.
        **kwargs: Additional PipelineConfig options.

    Returns:
        PipelineManager positioned at from_step with inherited history.

    Raises:
        ValueError: If from_step > number of completed steps.
    """
```

Implementation:
1. Load completed steps for `pipeline_run_id` from `steps` table
2. Validate `from_step` is within range
3. Create a **new** `pipeline_run_id` (format: `{name}_replay_{timestamp}_{uuid}`)
4. Create `PipelineConfig` with the new run ID
5. Populate `_step_results` with steps 0..`from_step-1` from the source run
6. Copy the inherited steps' execution records to the new `pipeline_run_id`
   (so output resolution scoped to the new run can find them)
7. Set `_current_step = from_step`
8. Return the instance, ready for `run()` calls from `from_step` onward

Step 6 is important: when step `from_step` references outputs from step
`from_step - 1`, the scoped output resolution needs to find those executions
under the new `pipeline_run_id`. We insert lightweight "alias" rows into the
`executions` table pointing to the inherited execution records.

#### 7. `PipelineManager.step()` accessor

**File:** `src/artisan/orchestration/pipeline_manager.py`

```python
def step(self, step_number: int) -> StepResult:
    """Get a completed step result by number.

    Args:
        step_number: 0-indexed step number.

    Returns:
        StepResult for the completed step.

    Raises:
        IndexError: If step_number is out of range.
    """
    return self._step_results[step_number]
```

This is a convenience accessor that already implicitly works via list indexing
but should be a proper public API for replay ergonomics.

---

## Execution alias strategy (step 6 detail)

When replaying, inherited steps (0..N-1) have executions under the **source**
`pipeline_run_id`. The new run needs output resolution to find these.

**Option A: Copy rows with new `pipeline_run_id`**

Insert new rows into `executions` that duplicate the inherited execution records
but with the new `pipeline_run_id`. Pros: simple queries. Cons: data
duplication.

**Option B: Query with `IN (new_run_id, source_run_id)` for inherited steps**

Keep track of which steps are inherited and their source run. When resolving
outputs, use the source run's `pipeline_run_id` for inherited steps and the
new run's for replayed steps. Pros: no duplication. Cons: more complex query
logic.

**Recommended: Option A** — the `executions` table is small (one row per batch
per step), so duplication is negligible. The simplicity of "always query by
current `pipeline_run_id`" is worth the minor storage cost.

---

## Data flow: replay from step 2

```
Source run: my_pipeline_20260226_abc123
  step 0: IngestStructure  → exec_run_ids: [e0a, e0b, ...]
  step 1: ScoreOp          → exec_run_ids: [e1a, e1b, ...]
  step 2: FilterOp         → exec_run_ids: [e2a, e2b, ...]

replay(from_step=2) → new run: my_pipeline_replay_20260226_def456

  1. Load steps 0,1 from source run
  2. Copy execution records for steps 0,1 with new pipeline_run_id
     (e0a', e0b', e1a', e1b' — same execution_spec_id, new pipeline_run_id)
  3. Set _current_step = 2
  4. User calls: pipeline.run(FilterOp, ..., force=True)
     → dispatches fresh, commits with pipeline_run_id = new run
  5. Output resolution for step 2 scoped to new run only
     → sees e0a', e0b' (inherited) and e2_new (fresh) — NOT e2a, e2b from old run
```

---

## Backwards compatibility

| Concern | Mitigation |
|---|---|
| Old `executions` data lacks `pipeline_run_id` | Column is nullable. Old rows read as `null`. Unscoped resolution (current behavior) used when `pipeline_run_id` is `None`. |
| Old pipelines created without `pipeline_run_id` in executions | `create()` and `resume()` continue to work as before. Scoped resolution is only activated when `pipeline_run_id` is explicitly provided (i.e., replay scenarios). |
| Existing `force=False` default | No behavior change for existing code. |
| `step_spec_id` still works for normal caching | Unchanged. `force=True` is opt-in. |

---

## Implementation plan

### Phase 1: Foundation (required for any replay)

1. **Add `pipeline_run_id` to `EXECUTIONS_SCHEMA`** — nullable String column
2. **Propagate `pipeline_run_id` through execution path** — `PipelineConfig` →
   `execute_step()` → `ExecutionRecord` → staged Parquet
3. **Write `pipeline_run_id` on new executions** — existing `create()` runs
   start populating the field immediately

### Phase 2: Force execution

4. **Add `force` param to `run()` / `submit()`** — skip step-level cache
5. **Add `force` param to `execute_step()`** — skip execution-level cache
6. **Tests** — verify force bypasses both cache levels

### Phase 3: Scoped output resolution

7. **Update `resolve_output_reference()`** — accept optional `pipeline_run_id`,
   filter executions by it
8. **Thread `pipeline_run_id` through resolution path** — `execute_step()` →
   `resolve_inputs()` → `resolve_output_reference()`
9. **Tests** — verify scoped resolution returns only current-run outputs

### Phase 4: Replay API

10. **Add `PipelineManager.replay()` factory** — load source steps, create new
    run, copy inherited execution records
11. **Add `PipelineManager.step()` accessor** — public API for accessing
    completed step results
12. **Tests** — end-to-end replay scenarios
13. **Update `list_runs()`** — show replay lineage (source run, from_step)

---

## Risks and mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Execution alias rows inflate `executions` table | Low — one row per batch per inherited step, small relative to artifact tables | Acceptable trade-off for query simplicity |
| `force=True` used accidentally, re-running expensive SLURM steps | Medium — wasted compute | Default is `force=False`. Could add confirmation log warning. |
| Replayed steps produce different artifact IDs (content changed) | Expected — content-addressed IDs change when content changes | No issue: downstream steps resolve via execution_edges, not artifact IDs directly |
| Old Delta tables without `pipeline_run_id` column | Query would fail if schema mismatch | Nullable column + fallback to unscoped query |

---

## Open questions

1. **Should `force` be per-step or pipeline-wide?** Current design is per-step.
   A pipeline-wide `PipelineManager.create(..., force_all=True)` could be added
   later but isn't needed for replay (where the user controls each `run()` call).

2. **Should `replay()` automatically set `force=True` for all subsequent steps?**
   Probably not — let the user decide. If they change params, cache will miss
   naturally. If they don't change params but want to re-run (e.g., bug fix in
   operation code), they need `force=True`.

3. **Should we add `PipelineManager.compare_runs(run_a, run_b)` as a follow-up?**
   Useful for exploratory branching. Could compare artifact counts, metrics
   distributions, etc. Out of scope for this design but a natural next step.
