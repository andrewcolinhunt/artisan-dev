# Design: `skip_cache` — Step and Pipeline Cache Invalidation

**Status:** Draft

---

## Problem

The two-level cache (step-level via `step_spec_id`, execution-level via
`execution_spec_id`) automatically skips any step whose configuration matches a
previous successful run. There is no way to force re-execution when the cache
key hasn't changed — for example, after fixing a bug in an operation's code.

There is also no output isolation between runs. `resolve_output_reference()`
queries all successful executions for a given step number across all runs. If a
step is re-executed (same params, fixed code), downstream steps see outputs from
both the old and new executions — double the artifacts, mixed provenance.

### Use cases

- **Bug fix in operation code** — the code for step 2 had a bug. After fixing
  it, re-execute step 2 without changing params. Downstream steps should only
  see the new outputs.
- **Non-deterministic operations** — an operation produces different results each
  run (e.g., sampling, stochastic optimization). Re-run to get a new sample.
- **Fresh pipeline** — re-run every step from scratch in the same `delta_root`,
  for example after upgrading a dependency or changing an external resource.

---

## Design

Two changes that work together:

- **`skip_cache` parameter** — bypass cache lookup at both the step and
  execution level, forcing re-execution.
- **`step_run_id` on executions** — tag each execution record with the
  `step_run_id` of the step that produced it, enabling scoped output resolution.

### User-facing API

```python
# Re-run a single step (e.g., after fixing a bug in ScoreOp)
s1 = pipeline.run(ScoreOp, inputs={"data": s0.output("data")}, skip_cache=True)

# Re-run every step in the pipeline
pipeline = PipelineManager.create("my_pipeline", delta_root=..., skip_cache=True)
```

`skip_cache` defaults to `False`. Existing code is unaffected.

---

## Current architecture

### Two-level cache

```
submit() called
    |
    v
Compute step_spec_id = hash(op_name, step_number, params, input_spec, config)
    |
    v
Layer 1 — Step cache: steps table WHERE step_spec_id = X AND status = "completed"
    |
    +-- HIT  --> return cached StepResult immediately (execute_step never called)
    +-- MISS --> dispatch to workers
                    |
                    v
              Layer 2 — Per-batch: execution_spec_id = hash(op_name, artifact_ids, params, config)
                    |
                    +-- HIT  --> skip batch
                    +-- MISS --> execute batch
```

### Output resolution (the mixing problem)

`resolve_output_reference()` in `inputs.py` resolves
`OutputReference(source_step=N, role=R)` by:

```python
# inputs.py:57-59 — no run scoping
.filter(pl.col("origin_step_number") == ref.source_step)
.filter(pl.col("success") == True)
```

This returns outputs from ALL successful executions at step N across ALL runs.

### Key ID relationships

```
pipeline_run_id     (per pipeline session, in steps table only)
  |
  +-- step_run_id       (unique per step attempt, in steps table only)
  |     +-- step_spec_id  (deterministic cache key)
  |
  +-- execution_run_id   (unique per batch attempt, in executions table)
        +-- execution_spec_id  (deterministic batch cache key)
        +-- execution_edges    (input/output artifact links)
```

Gap: `step_run_id` exists in the `steps` table but NOT in `executions`. There
is no way to link an execution record to the specific step attempt that produced
it. This is the root cause of the output mixing problem.

---

## Changes

### `skip_cache` parameter

Add `skip_cache: bool = False` to four places:

**`PipelineConfig`** (`schemas/orchestration/pipeline_config.py`) — pipeline-wide
default:

```python
skip_cache: bool = Field(
    default=False,
    description="Skip cache lookups for all steps in this pipeline.",
)
```

**`PipelineManager.create()`** (`orchestration/pipeline_manager.py`) — factory
method parameter, forwarded to `PipelineConfig`:

```python
def create(cls, ..., skip_cache: bool = False) -> PipelineManager:
```

**`run()` / `submit()`** (`orchestration/pipeline_manager.py`) — per-step
override:

```python
def run(self, operation, ..., skip_cache: bool = False) -> StepResult:
def submit(self, operation, ..., skip_cache: bool = False) -> StepFuture:
```

**`execute_step()`** (`orchestration/engine/step_executor.py`) — execution-level
bypass:

```python
def execute_step(..., skip_cache: bool = False) -> StepResult:
```

#### How it flows

In `submit()`, step 5 (cache check):

```python
# Skip step-level cache when skip_cache is set per-step or pipeline-wide
if not skip_cache and not self._config.skip_cache:
    cached = self._try_cached_step(...)
    if cached is not None:
        return cached
```

The `skip_cache` flag is forwarded through `_dispatch_step()` → its inner
`_run()` closure → `execute_step()`, where it bypasses
`check_cache_for_batch()` in both the creator and curator paths.

Results are still written to Delta. Future runs without `skip_cache` will
cache-hit against the new results.

### `step_run_id` on executions (output isolation)

#### Schema change

Add a nullable `step_run_id` column to `EXECUTIONS_SCHEMA`
(`storage/core/table_schemas.py`):

```python
EXECUTIONS_SCHEMA = {
    "execution_run_id": pl.String,
    "execution_spec_id": pl.String,
    "step_run_id": pl.String,          # NEW — links execution to step attempt
    "origin_step_number": pl.Int32,
    ...
}
```

Nullable for backwards compatibility. Old rows read as `null`. Adding a nullable
column to an append-only Delta table requires no migration.

#### `step_run_id` on `StepResult`

Add `step_run_id` to `StepResult` (`schemas/orchestration/step_result.py`):

```python
class StepResult(BaseModel):
    step_run_id: str | None = Field(
        default=None,
        description="Unique identifier for this step attempt.",
    )
    ...
```

Update `_row_to_step_result()` in `step_tracker.py` to populate it from the
`steps` table row (which already has `step_run_id`).

#### Propagation path

`step_run_id` flows through the system like this:

```
PipelineManager._dispatch_step()
  |  generates step_run_id via _generate_step_run_id()
  |  writes to steps table via StepStartRecord
  |  passes to execute_step()
  v
execute_step(step_run_id=...)
  |  passes to _execute_creator_step() / _execute_curator_step()
  v
_stage_execution(step_run_id=...)
  |  writes to executions parquet via _write_execution_record()
  v
DeltaCommitter.commit_all_tables()
  |  commits staged parquet to executions delta table
  v
executions delta table now has step_run_id column populated
```

This requires threading `step_run_id` through:

- `execute_step()` — new parameter
- `_execute_creator_step()` / `_execute_curator_step()` — new parameter
- `_stage_execution()` in `execution/staging/parquet_writer.py` — new parameter
- `_write_execution_record()` in `execution/staging/parquet_writer.py` — new column
- `ExecutionRecord` in `execution_record.py` — new optional field

#### Scoped output resolution

`resolve_output_reference()` gains an optional `step_run_id` parameter
(`orchestration/engine/inputs.py`):

```python
def resolve_output_reference(
    ref: OutputReference,
    delta_root: Path,
    step_run_id: str | None = None,    # NEW
) -> list[str]:
```

`resolve_inputs()` (the wrapper that `execute_step()` actually calls) gains a
mapping of upstream step numbers to their `step_run_id`s:

```python
def resolve_inputs(
    inputs: ...,
    delta_root: Path,
    step_run_ids: dict[int, str] | None = None,  # NEW — step_number → step_run_id
) -> dict[str, list[str]]:
```

When resolving an `OutputReference`, it looks up the upstream step's
`step_run_id` from the mapping and forwards it to `resolve_output_reference()`.

When `step_run_id` is provided:

```python
query = (
    pl.scan_delta(str(executions_path))
    .filter(pl.col("origin_step_number") == ref.source_step)
    .filter(pl.col("success") == True)
)
if step_run_id:
    query = query.filter(pl.col("step_run_id") == step_run_id)
```

Falls back to unscoped query when `step_run_id` is falsy (`None` or `""`),
providing backwards compat with old data where the column is null.

#### How PipelineManager provides the `step_run_id`

PipelineManager maintains a mapping of `step_number → step_run_id`:

- **Cache-hit steps**: The cached `StepResult` carries the original
  `step_run_id` (from the prior run that wrote the step). The original
  executions in the `executions` table already have that same `step_run_id`.
  Resolution using it finds exactly the right records — no alias rows needed.

- **Fresh/re-executed steps**: A new `step_run_id` is generated. New executions
  are tagged with it. Resolution finds only the new executions.

When `execute_step()` resolves inputs, it receives the upstream steps'
`step_run_id`s (looked up from `_step_results`) and passes them to
`resolve_output_reference()`.

This is the key advantage over scoping by `pipeline_run_id`: cached steps don't
need alias rows because their original `step_run_id` already matches their
executions in the table.

---

## Backwards compatibility

| Concern | Mitigation |
|---|---|
| Old `executions` data lacks `step_run_id` | Nullable column. Old rows read as `null`. Resolution falls back to unscoped when `step_run_id` is `None`. |
| Old `StepResult` objects (from cache) lack `step_run_id` | Field defaults to `None`. Truthy check in resolution falls back to unscoped for both `None` and `""`. |
| `skip_cache=False` default | No behavior change for existing code. |

---

## Implementation plan

### Phase 1: `skip_cache`

- Add `skip_cache: bool = False` to `PipelineConfig`
- Add `skip_cache: bool = False` to `PipelineManager.create()`, forwarded to `PipelineConfig`
- Add `skip_cache: bool = False` to `run()`, `submit()`, `_dispatch_step()`
- In `submit()`: gate `_try_cached_step()` on `not skip_cache and not self._config.skip_cache`
- Add `skip_cache: bool = False` to `execute_step()`, `_execute_creator_step()`, `_execute_curator_step()`
- In creator batch loop: skip `check_cache_for_batch()` when `skip_cache`
- In curator cache path: skip cache check when `skip_cache`
- Tests: verify both cache levels are bypassed, results are still written

### Phase 2: Output isolation

- Add `step_run_id` to `EXECUTIONS_SCHEMA` (nullable)
- Add `step_run_id` to `ExecutionRecord` (optional)
- Add `step_run_id` to `StepResult` (default `None`, type `str | None`)
- Update `_row_to_step_result()` to populate `step_run_id` from steps table
- Thread `step_run_id` through: `_dispatch_step()` → `execute_step()` → `_execute_creator_step()` / `_execute_curator_step()` → `_stage_execution()` → `_write_execution_record()`
- Add `step_run_id` parameter to `resolve_output_reference()`
- Add `step_run_ids: dict[int, str] | None` parameter to `resolve_inputs()`, forwarding per-step `step_run_id` to `resolve_output_reference()`
- Build `step_number → step_run_id` mapping in PipelineManager from `_step_results`
- Pass upstream `step_run_ids` mapping when resolving inputs in `execute_step()`
- Tests: verify scoped resolution returns only current step attempt's outputs

---

## Edge cases

**`skip_cache` with unchanged params**: The `step_spec_id` and
`execution_spec_id` are identical to the previous run. New executions are
committed alongside old ones. The new `step_run_id` distinguishes them. On the
next non-`skip_cache` run, the step-level cache hits against the most recent
completed step (which has the new `step_run_id`), so resolution uses the new
executions.

**Mixed pipeline (some steps `skip_cache`, some cached)**: Each step's
`step_run_id` independently scopes its output resolution. Cached steps use
their original `step_run_id`; re-executed steps use a new one. No interaction
between the two.

**Standalone `execute_step()` calls (tests, no PipelineManager)**: No
`step_run_id` is provided. Resolution falls back to unscoped (current
behavior). No regression.

**Old Delta tables without `step_run_id` column**: The nullable column reads as
`null` for old rows. The truthy check in resolution falls back to unscoped for
both `None` and `""`. First new execution populates the column; subsequent runs
benefit from scoping.
