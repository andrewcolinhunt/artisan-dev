# Design: Integration Test Coverage Expansion

**Status:** Draft (v2 — corrected after codebase research)
**Date:** 2026-03-10

---

## Motivation

The existing integration tests (`test_step_persistence.py`, `test_data_flow_patterns.py`)
cover the happy-path data flow patterns well but leave several core framework features
completely untested at the integration level. This doc specifies the tests needed to
close those gaps.

**Scope:** High and medium priority gaps only — features that users rely on and that
exercise complex multi-component interactions not coverable by unit tests alone.

**Out of scope:** SLURM backend, visualization/timing, crash recovery, custom artifact
types, concurrent pipeline runs, level-2 batching (`units_per_worker`). These are either
environment-dependent or lower risk.

---

## New Test File Overview

| File | Features Covered |
|---|---|
| `test_chaining.py` | ChainBuilder, intermediates modes, role mapping, provenance |
| `test_multi_input.py` | GroupByStrategy (ZIP, CROSS_PRODUCT, LINEAGE), with_associated |
| `test_error_handling.py` | FailurePolicy, failed steps, resume from failure |
| `test_step_overrides.py` | Per-step param/execution overrides, isolation |
| `test_filter_advanced.py` | Multi-criterion filter, provenance walks, passthrough_failures, InteractiveFilter |
| `test_cross_pipeline.py` | IngestPipelineStep, ExecutionConfigArtifact |
| `test_cache_policies.py` | CachePolicy modes, artifact ID identity on cache hit |

---

## Test File 1: `test_chaining.py`

Exercises `pipeline.chain().add(...).add(...).run()` — composing multiple creators into
a single dispatched unit with in-memory artifact passing.

### API Reference (verified from source)

```python
# pipeline.chain() signature (pipeline_manager.py:1189)
pipeline.chain(
    inputs: dict[str, Any] | None = None,  # external inputs to chain
    backend: str | BackendBase | None = None,  # MUST be set here, not on run()
    resources: dict[str, Any] | None = None,
    execution: dict[str, Any] | None = None,
    intermediates: str = "discard",  # "discard" | "persist" | "expose"
    name: str | None = None,
) -> ChainBuilder

# .add() signature (chain_builder.py:73)
chain.add(
    operation: type[OperationDefinition],
    params: dict[str, Any] | None = None,
    environment: str | dict[str, Any] | None = None,
    tool: dict[str, Any] | None = None,
    role_mapping: dict[str, str] | None = None,  # {prev_output_role: next_input_role}
) -> ChainBuilder

# .run() — does NOT accept backend
chain.run(failure_policy=None, compact=True) -> StepResult

# .submit()
chain.submit(failure_policy=None, compact=True) -> StepFuture
```

### Key Behavioral Notes

1. **Role mapping is required** for DataGenerator → DataTransformer because output role
   `"datasets"` ≠ input role `"dataset"`. Without `role_mapping={"datasets": "dataset"}`,
   the chain fails at runtime with `ValueError: Required input role 'dataset' has no source`.
   Validation at `.add()` time passes silently (no overlapping names to check).

2. **PERSIST and EXPOSE are currently identical** in the chain executor code. Both modes
   merge all artifacts and write all of them as execution_edge outputs with prefixed role
   names (`_chain_op{i}_{role}` for non-final operations). There is no code-level
   distinction between the two modes. Tests should document this current behavior.

3. **`StepResult.output()` only knows the final operation's roles.** Intermediate roles
   are NOT accessible via `step.output()`. To access intermediate artifacts, query
   execution_edges with the prefixed role name (e.g., `_chain_op0_datasets`).

4. **ChainIntermediates enum** is defined in
   `artisan.execution.models.execution_chain.ChainIntermediates`.

### Test 1.1: Basic Chain Execution

**Pattern:** DataGenerator → DataTransformer in a single chain step.

**Setup:**
```python
pipeline = PipelineManager.create(
    name="test_chain_basic",
    delta_root=delta_root, staging_root=staging_root, working_root=working_root,
)
step = (
    pipeline.chain(backend=Backend.LOCAL)
    .add(DataGenerator, params={"count": 2, "seed": 42})
    .add(
        DataTransformer,
        role_mapping={"datasets": "dataset"},
        params={"scale_factor": 1.5, "noise_amplitude": 0.1, "variants": 1, "seed": 100},
    )
    .run()
)
pipeline.finalize()
```

**Assertions:**
- `step.success is True`
- `count_artifacts_by_step(delta_root, 0) == 2` (only transformer output with DISCARD)
- Only 1 step record in the steps table (chain dispatches as a single step)
- `count_executions_by_step(delta_root, 0) == 1`
- `step.output("dataset")` returns an OutputReference (final op's role)

**Why it matters:** Validates that chain dispatch produces correct artifacts end-to-end
and that in-memory passing works (no intermediate disk round-trip).

### Test 1.2: Chain Intermediates — DISCARD (Default)

**Pattern:** Same chain, explicitly setting `intermediates="discard"`.

**Setup:** Same as 1.1 with `intermediates="discard"`.

**Assertions:**
- Only final outputs (DataTransformer artifacts) appear in artifact index
- Generator artifacts are NOT in the artifact index
- No artifact_edges exist (generative first op with DISCARD produces no edges)
- `count_executions_by_step(delta_root, 0) == 1`

**Why it matters:** Confirms intermediate artifacts are truly discarded, reducing storage
and avoiding polluting downstream queries.

### Test 1.3: Chain Intermediates — PERSIST/EXPOSE

**Note:** PERSIST and EXPOSE are currently identical in behavior. This test documents
the shared behavior. If they diverge in the future, split into two tests.

**Setup:** Same chain with `intermediates="persist"` (or `"expose"`).

**Assertions:**
- Generator artifacts ARE in the artifact index (persisted under step 0)
- Both generator AND transformer artifacts appear in execution_edges as outputs
- Generator artifacts are accessible via role `_chain_op0_datasets`
- Transformer artifacts are accessible via role `dataset`
- Internal artifact_edges exist: generator → transformer (provenance tracked)
- `step.output("dataset")` returns transformer artifacts (final op's role only)

**Why it matters:** Users may want to inspect intermediate results for debugging.
Verifies that intermediate artifacts are correctly persisted with prefixed role names.

### Test 1.4: Chain with Explicit Role Mapping

**Pattern:** Chain where output role name ≠ input role name, requiring explicit mapping.

**Setup:**
```python
step = (
    pipeline.chain(backend=Backend.LOCAL)
    .add(DataGenerator, params={"count": 2, "seed": 42})
    .add(
        DataTransformer,
        role_mapping={"datasets": "dataset"},
        params={"scale_factor": 1.5, "noise_amplitude": 0.1, "variants": 1, "seed": 100},
    )
    .run()
)
```

**Assertions:**
- Pipeline succeeds
- Output artifact count is 2 (1 variant per input)

**Note:** This is effectively the same as Test 1.1 since all DataGenerator→DataTransformer
chains require this mapping. Consider combining with 1.1 or replacing this with a
**negative test** verifying that omitting `role_mapping` raises an error.

### Test 1.5: Three-Stage Chain with Provenance

**Pattern:** DataGenerator → DataTransformer → MetricCalculator in one chain step.

**Setup:**
```python
step = (
    pipeline.chain(backend=Backend.LOCAL, intermediates="expose")
    .add(DataGenerator, params={"count": 2, "seed": 42})
    .add(
        DataTransformer,
        role_mapping={"datasets": "dataset"},
        params={"scale_factor": 1.5, "noise_amplitude": 0.1, "variants": 1, "seed": 100},
    )
    .add(MetricCalculator)  # "dataset" → "dataset" matches by identity
    .run()
)
```

**Assertions:**
- Pipeline succeeds, 1 step in steps table
- `step.output("metrics")` returns metric artifacts (final op's role)
- With EXPOSE: all three sets of artifacts in artifact_index
- Intermediate roles accessible:
  - `get_execution_outputs(delta_root, 0, "_chain_op0_datasets")` → generator artifacts
  - `get_execution_outputs(delta_root, 0, "_chain_op1_dataset")` → transformer artifacts
  - `get_execution_outputs(delta_root, 0, "metrics")` → metric artifacts
- Artifact edges form DAG: generator → transformer → metrics

**Why it matters:** Validates transitive provenance through 3+ chain stages and that
the ancestor map is correctly built.

---

## Test File 2: `test_multi_input.py`

Exercises `GroupByStrategy` (ZIP, CROSS_PRODUCT, LINEAGE) and `with_associated`.

### Key Behavioral Notes

1. **`group_by` is a ClassVar** — cannot be overridden at runtime via params or execution.
   Each grouping strategy needs its own operation class.

2. **ZIP requires equal counts** across all input roles. Raises `ValueError` if mismatched.

3. **LINEAGE requires exactly 2 input roles.** Raises `ValueError` otherwise.

4. **`with_associated` uses single-hop forward lookup** on artifact_edges (not multi-hop).
   Only finds direct children.

5. **Operation names must be unique** across the registry. Each test operation needs a
   distinct `name` string.

### Prerequisites: Test Operations

Need **separate operation classes** per strategy (since `group_by` is a ClassVar):

```python
class DualInputZip(OperationDefinition):
    name = "dual_input_zip"

    class InputRole(StrEnum):
        primary = "primary"
        secondary = "secondary"

    class OutputRole(StrEnum):
        result = "result"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.primary: InputSpec(artifact_type="data"),
        InputRole.secondary: InputSpec(artifact_type="data"),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.result: OutputSpec(
            artifact_type="data",
            infer_lineage_from={"inputs": ["primary", "secondary"]},
        ),
    }
    group_by: ClassVar[GroupByStrategy | None] = GroupByStrategy.ZIP

    # Minimal lifecycle: preprocess extracts paths, execute concatenates CSVs,
    # postprocess builds DataArtifact drafts.


class DualInputCross(OperationDefinition):
    name = "dual_input_cross"
    # Same as DualInputZip but:
    group_by: ClassVar[GroupByStrategy | None] = GroupByStrategy.CROSS_PRODUCT


class DualInputLineage(OperationDefinition):
    name = "dual_input_lineage"
    # Same as DualInputZip but:
    group_by: ClassVar[GroupByStrategy | None] = GroupByStrategy.LINEAGE
```

For `with_associated`, a single-input operation:

```python
class AssociatedMetricConsumer(OperationDefinition):
    name = "associated_metric_consumer"

    class InputRole(StrEnum):
        primary = "primary"

    class OutputRole(StrEnum):
        result = "result"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.primary: InputSpec(
            artifact_type="data",
            with_associated=("metric",),
        ),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.result: OutputSpec(
            artifact_type="data",
            infer_lineage_from={"inputs": ["primary"]},
        ),
    }

    # In preprocess, access associated metrics via:
    # inputs.associated_artifacts(artifact, "metric")
    # Encode count in output filename for assertion.
```

### Test 2.1: ZIP Grouping

**Setup:**
```python
step0 = pipeline.run(DataGenerator, params={"count": 3, "seed": 42}, backend=Backend.LOCAL)
step1 = pipeline.run(DataGenerator, params={"count": 3, "seed": 100}, backend=Backend.LOCAL)
step2 = pipeline.run(
    DualInputZip,
    inputs={"primary": step0.output("datasets"), "secondary": step1.output("datasets")},
    backend=Backend.LOCAL,
)
```

**Assertions:**
- `count_artifacts_by_step(delta_root, 2) == 3` (one per positional pair)
- `count_executions_by_step(delta_root, 2) == 3` (default `artifacts_per_unit=1`)
- Each execution_run_id has exactly 1 primary + 1 secondary input
  (query execution_edges grouped by execution_run_id)

### Test 2.2: CROSS_PRODUCT Grouping

**Setup:** Same pattern, but count=2 and count=3 for different generators. Uses
`DualInputCross`.

**Assertions:**
- `count_artifacts_by_step(delta_root, 2) == 6` (2×3)
- `count_executions_by_step(delta_root, 2) == 6`

### Test 2.3: LINEAGE Grouping

**Setup:**
```python
step0 = pipeline.run(DataGenerator, params={"count": 2, "seed": 42}, backend=Backend.LOCAL)
step1 = pipeline.run(
    DataTransformer,
    inputs={"dataset": step0.output("datasets")},
    params={"scale_factor": 1.5, "noise_amplitude": 0.1, "variants": 1, "seed": 100},
    backend=Backend.LOCAL,
)  # B1, B2 with edges A1→B1, A2→B2
step2 = pipeline.run(
    MetricCalculator,
    inputs={"dataset": step1.output("dataset")},
    backend=Backend.LOCAL,
)  # M1, M2 with edges B1→M1, B2→M2
step3 = pipeline.run(
    DualInputLineage,
    inputs={"primary": step1.output("dataset"), "secondary": step2.output("metrics")},
    backend=Backend.LOCAL,
)
```

**Provenance walk**: LINEAGE determines target (lower step) vs candidate (higher step)
by sampling step numbers. Step 1 (primary) < Step 2 (secondary), so step 1 is target.
BFS backward from each candidate (M1, M2) finds shared ancestor: M1→B1 match, M2→B2 match.

**Assertions:**
- `count_artifacts_by_step(delta_root, 3) == 2` (B1↔M1, B2↔M2)
- `count_executions_by_step(delta_root, 3) == 2`
- Verify ancestry: both inputs in each group trace to same step 0 artifact

### Test 2.4: `with_associated` Specs

**Setup:**
```python
step0 = pipeline.run(DataGenerator, params={"count": 2, "seed": 42}, backend=Backend.LOCAL)
step1 = pipeline.run(
    MetricCalculator,
    inputs={"dataset": step0.output("datasets")},
    backend=Backend.LOCAL,
)
# MetricCalculator creates artifact_edges: D1→M1, D2→M2 (direct edges)
step2 = pipeline.run(
    AssociatedMetricConsumer,
    inputs={"primary": step0.output("datasets")},
    backend=Backend.LOCAL,
)
```

**Assertions:**
- Pipeline succeeds (associations resolved via single-hop forward lookup)
- `count_artifacts_by_step(delta_root, 2) == 2`
- The operation received associated metrics (verify via output filename encoding
  or metadata)

---

## Test File 3: `test_error_handling.py`

Exercises `FailurePolicy`, partial failures, and resume from failed state.

### Key Behavioral Notes

1. **FAIL_FAST raises `RuntimeError`** from `aggregate_results()`. The exception is caught
   by the pipeline manager's `except Exception` handler. The step is recorded as `"failed"`
   in the steps table. **The commit phase is skipped**, so individual failed execution
   records are NOT committed to Delta.

2. **`StepResult.success` is `False`** for any step with `failed_count > 0`, even with
   CONTINUE policy. The steps table `status` is `"completed"` (not `"failed"`) with
   CONTINUE, but `StepResult.success = (failed_count == 0) = False`.

3. **`_stopped` is set by empty inputs**, not by FAIL_FAST directly. When a step gets
   empty inputs, it's skipped with `metadata={"skipped": True}`, and `_stopped = True`.

4. **Resume loads only "completed" and "skipped" steps.** Failed steps are excluded.
   After resume, the user rebuilds the pipeline from the first failed step onward,
   referencing earlier results via `pipeline[step_number]`.

5. **With FAIL_FAST, `StepResult` counts are all 0** (total=0, succeeded=0, failed=0).
   The error is in `metadata={"error": "..."}`.

### Prerequisites: FailingTransformer

```python
class FailingTransformer(OperationDefinition):
    name = "failing_transformer"

    class InputRole(StrEnum):
        DATASET = "dataset"

    class OutputRole(StrEnum):
        DATASET = "dataset"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.DATASET: InputSpec(artifact_type="data"),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.DATASET: OutputSpec(
            artifact_type="data",
            infer_lineage_from={"inputs": ["dataset"]},
        ),
    }

    class Params(BaseModel):
        fail_on_index: int = -1   # parse from filename "dataset_{i}_seed{seed}.csv"
        fail_on_all: bool = False

    params: Params = Params()

    # In execute(): parse index from input filename, raise ValueError if it matches
    # fail_on_index or if fail_on_all is True. Otherwise copy input to output.
```

### Test 3.1: FAIL_FAST Policy

**Setup:**
```python
pipeline = PipelineManager.create(
    name="test_fail_fast",
    delta_root=delta_root, staging_root=staging_root, working_root=working_root,
    failure_policy=FailurePolicy.FAIL_FAST,
)
step0 = pipeline.run(DataGenerator, params={"count": 3, "seed": 42}, backend=Backend.LOCAL)
step1 = pipeline.run(
    FailingTransformer,
    inputs={"dataset": step0.output("datasets")},
    params={"fail_on_index": 1},
    backend=Backend.LOCAL,
)
step2 = pipeline.run(
    MetricCalculator,
    inputs={"dataset": step1.output("dataset")},
    backend=Backend.LOCAL,
)
pipeline.finalize()
```

**Assertions:**
- `step1.success is False`
- `step1.metadata["error"]` contains "fail_fast"
- `step1.total_count == 0` (exception path doesn't populate counts)
- Steps table: step 1 has `status == "failed"`
- **Cannot assert failed execution records** (commit skipped under FAIL_FAST)
- Step 2 gets empty inputs → skipped: `step2.metadata.get("skipped") is True`

### Test 3.2: CONTINUE Policy

**Setup:**
```python
pipeline = PipelineManager.create(
    name="test_continue",
    delta_root=delta_root, staging_root=staging_root, working_root=working_root,
    failure_policy=FailurePolicy.CONTINUE,
)
step0 = pipeline.run(DataGenerator, params={"count": 3, "seed": 42}, backend=Backend.LOCAL)
step1 = pipeline.run(
    FailingTransformer,
    inputs={"dataset": step0.output("datasets")},
    params={"fail_on_index": 1},
    backend=Backend.LOCAL,
)
step2 = pipeline.run(
    MetricCalculator,
    inputs={"dataset": step1.output("dataset")},
    backend=Backend.LOCAL,
)
pipeline.finalize()
```

**Assertions:**
- `step1.success is False` (failed_count > 0 → success = False)
- `step1.succeeded_count == 2`, `step1.failed_count == 1`, `step1.total_count == 3`
- Steps table: step 1 has `status == "completed"` (not "failed") with `failed_count == 1`
- `count_artifacts_by_step(delta_root, 1) == 2` (only successful executions)
- Executions table: 1 record with `success == False`, 2 with `success == True`
- Step 2 executes on 2 artifacts (only successful outputs in execution_edges)
- `count_artifacts_by_step(delta_root, 2) == 2` (metrics for 2 data artifacts)

### Test 3.3: All Executions Fail (CONTINUE)

**Setup:** Same as 3.2 but `params={"fail_on_all": True}`.

**Assertions:**
- `step1.succeeded_count == 0`, `step1.failed_count == 3`
- `count_artifacts_by_step(delta_root, 1) == 0`
- Step 2: empty inputs → skipped (`step2.metadata.get("skipped") is True`)
- `pipeline.finalize()` completes without error (Delta Lake in consistent state)

### Test 3.4: Resume from Failed Pipeline

**Setup:**
```python
# Run 1: pipeline with FAIL_FAST
p1 = PipelineManager.create(
    name="test_resume_failure",
    delta_root=delta_root, staging_root=staging_root, working_root=working_root,
    failure_policy=FailurePolicy.FAIL_FAST,
)
s0 = p1.run(DataGenerator, params={"count": 2, "seed": 42}, backend=Backend.LOCAL)
s1 = p1.run(
    FailingTransformer,
    inputs={"dataset": s0.output("datasets")},
    params={"fail_on_all": True},
    backend=Backend.LOCAL,
)
p1.finalize()
# Steps table: step 0 = "completed", step 1 = "failed"

# Run 2: resume
p2 = PipelineManager.resume(
    delta_root=delta_root, staging_root=staging_root, working_root=working_root,
)
# p2 has step 0 loaded as completed, _current_step = 1
# Access step 0 output via p2[0] or p2.output(0, "datasets")

s1b = p2.run(
    DataTransformer,  # "fixed" operation (no longer fails)
    inputs={"dataset": p2[0].output("datasets")},
    params={"scale_factor": 1.5, "noise_amplitude": 0.0, "variants": 1, "seed": 100},
    backend=Backend.LOCAL,
)
s2 = p2.run(
    MetricCalculator,
    inputs={"dataset": s1b.output("dataset")},
    backend=Backend.LOCAL,
)
p2.finalize()
```

**Assertions:**
- Step 0 was NOT re-executed (loaded from state by `resume()`)
- `count_executions_by_step(delta_root, 0)` unchanged from run 1
- Step 1 re-executes successfully: `s1b.success is True`
- Step 2 executes: `s2.success is True`
- `count_artifacts_by_step(delta_root, 1) == 2` (transformer output)
- `count_artifacts_by_step(delta_root, 2) == 2` (metrics)

---

## Test File 4: `test_step_overrides.py`

Exercises per-step overrides of params and execution config.

### Key Behavioral Notes

1. **Override merge uses `model_copy(update=...)`** — shallow merge. Unspecified params
   get their defaults from the Params class.

2. **Execution overrides are NOT part of step_spec_id.** Only `operation_name`,
   `step_number`, `params`, `input_spec`, and config overrides (environment + tool)
   affect the hash. Different `artifacts_per_unit` does not change step_spec_id
   (but step_number differences do).

3. **DataTransformer defaults:** `artifacts_per_unit=1` (from `ExecutionConfig`),
   `scale_factor=1.5`, `noise_amplitude=0.1`, `variants=1`, `seed=None`.

### Test 4.1: Param Override

**Setup:**
```python
step0 = pipeline.run(DataGenerator, params={"count": 2, "seed": 42}, backend=Backend.LOCAL)
shared_ref = step0.output("datasets")

step1 = pipeline.run(
    DataTransformer,
    inputs={"dataset": shared_ref},
    params={"scale_factor": 2.0, "noise_amplitude": 0.0, "variants": 1, "seed": 100},
    backend=Backend.LOCAL,
)
step2 = pipeline.run(
    DataTransformer,
    inputs={"dataset": shared_ref},
    params={"scale_factor": 5.0, "noise_amplitude": 0.0, "variants": 1, "seed": 100},
    backend=Backend.LOCAL,
)
pipeline.finalize()
```

**Assertions:**
- Both steps succeed with 2 artifacts each
- Different `step_spec_id` values (different params + different step_number)
- Different artifact IDs (different content from different scale factors)
- Verify transformed data: read artifact content, check numeric values are scaled
  by 2.0 and 5.0 respectively (use `noise_amplitude=0.0` for deterministic verification)

### Test 4.2: Execution Override (Batching)

**Setup:**
```python
step0 = pipeline.run(DataGenerator, params={"count": 6, "seed": 42}, backend=Backend.LOCAL)
shared_ref = step0.output("datasets")

step1 = pipeline.run(
    DataTransformer,
    inputs={"dataset": shared_ref},
    params={"scale_factor": 1.5, "noise_amplitude": 0.0, "variants": 1, "seed": 100},
    execution={"artifacts_per_unit": 3},
    backend=Backend.LOCAL,
)
step2 = pipeline.run(
    DataTransformer,
    inputs={"dataset": shared_ref},
    params={"scale_factor": 1.5, "noise_amplitude": 0.0, "variants": 1, "seed": 100},
    execution={"artifacts_per_unit": 2},
    backend=Backend.LOCAL,
)
pipeline.finalize()
```

**Assertions:**
- `count_executions_by_step(delta_root, 1) == 2` (ceil(6/3))
- `count_executions_by_step(delta_root, 2) == 3` (ceil(6/2))
- Both produce 6 output artifacts (no data loss)

### Test 4.3: Override Does Not Mutate Operation Defaults

**Setup:**
```python
step0 = pipeline.run(DataGenerator, params={"count": 4, "seed": 42}, backend=Backend.LOCAL)
shared_ref = step0.output("datasets")

step1 = pipeline.run(
    DataTransformer,
    inputs={"dataset": shared_ref},
    params={"scale_factor": 1.5, "noise_amplitude": 0.0, "variants": 1, "seed": 100},
    execution={"artifacts_per_unit": 4},  # batch all together
    backend=Backend.LOCAL,
)
step2 = pipeline.run(
    DataTransformer,
    inputs={"dataset": shared_ref},
    params={"scale_factor": 1.5, "noise_amplitude": 0.0, "variants": 1, "seed": 100},
    # NO execution override — should use default artifacts_per_unit=1
    backend=Backend.LOCAL,
)
pipeline.finalize()
```

**Assertions:**
- `count_executions_by_step(delta_root, 1) == 1` (all 4 batched)
- `count_executions_by_step(delta_root, 2) == 4` (default artifacts_per_unit=1)
- Override from step 1 did not leak into step 2

---

## Test File 5: `test_filter_advanced.py`

Exercises advanced Filter capabilities beyond the basic passthrough test.

### Key Behavioral Notes

1. **Filter criteria use raw flattened metric names**: `distribution.min`,
   `distribution.median`, `summary.row_count`, etc. No step_name prefix.

2. **InteractiveFilter criteria use qualified names**: `metric_calculator.distribution.min`
   (prefixed with step_name from the steps table).

3. **Multi-criterion AND**: `pl.all_horizontal(bool_exprs)` — all criteria must pass.

4. **`passthrough_failures`**: ALL artifacts pass through regardless, but diagnostics
   are still computed. The `total_passed` in diagnostics reflects normal filtering,
   not the actual passthrough count.

5. **Step-targeted criterion**: Use `step_number` (int) field on Criterion to target
   metrics from a specific step. Triggers backward provenance walk
   (`_discover_step_metrics`).

### Test 5.1: Multi-Criterion Filter

**Setup:**
```python
step0 = pipeline.run(DataGenerator, params={"count": 5, "seed": 42}, backend=Backend.LOCAL)
step1 = pipeline.run(
    MetricCalculator,
    inputs={"dataset": step0.output("datasets")},
    backend=Backend.LOCAL,
)
step2 = pipeline.run(
    Filter,
    inputs={"passthrough": step0.output("datasets")},
    params={
        "criteria": [
            {"metric": "distribution.min", "operator": "ge", "value": 0},
            {"metric": "distribution.median", "operator": "gt", "value": 0.5},
            {"metric": "summary.row_count", "operator": "eq", "value": 10},
        ],
    },
    backend=Backend.LOCAL,
)
pipeline.finalize()
```

**Assertions:**
- Pipeline succeeds
- `0 <= len(get_execution_outputs(delta_root, 2, "passthrough")) <= 5`
- Verify passed artifacts satisfy all criteria by loading metrics from ArtifactStore

### Test 5.2: Filter with `passthrough_failures` Mode

**Setup:** Same pipeline but with `passthrough_failures=True` and a strict criterion.

```python
params={
    "criteria": [
        {"metric": "distribution.median", "operator": "gt", "value": 0.99},
    ],
    "passthrough_failures": True,
}
```

**Assertions:**
- ALL artifacts pass through: `len(get_execution_outputs(delta_root, 2, "passthrough")) == N`
  (where N = count of step 0 artifacts)
- Downstream step receives all artifacts

### Test 5.3: Step-Targeted Criterion

**Setup:**
```python
step0 = pipeline.run(DataGenerator, params={"count": 2, "seed": 42}, backend=Backend.LOCAL)
step1 = pipeline.run(
    DataTransformer,
    inputs={"dataset": step0.output("datasets")},
    params={"scale_factor": 1.5, "noise_amplitude": 0.1, "variants": 1, "seed": 100},
    backend=Backend.LOCAL,
)
step2 = pipeline.run(
    MetricCalculator,
    inputs={"dataset": step0.output("datasets")},  # metrics on ORIGINAL data
    backend=Backend.LOCAL,
)
step3 = pipeline.run(
    MetricCalculator,
    inputs={"dataset": step1.output("dataset")},   # metrics on TRANSFORMED data
    backend=Backend.LOCAL,
)
step4 = pipeline.run(
    Filter,
    inputs={"passthrough": step1.output("dataset")},
    params={
        "criteria": [
            {"metric": "distribution.median", "operator": "gt", "value": 0.3, "step_number": 3},
        ],
    },
    backend=Backend.LOCAL,
)
pipeline.finalize()
```

**Assertions:**
- Pipeline succeeds
- Filter uses backward provenance walk from passthrough (step 1) to step 3 metrics
- Passthrough count reflects filtering based on transformed data metrics (step 3),
  not original data metrics (step 2)

### Test 5.4: InteractiveFilter End-to-End

**Setup:**
```python
from artisan.operations.curator.interactive_filter import InteractiveFilter

# First run a pipeline to generate data + metrics
pipeline = PipelineManager.create(...)
step0 = pipeline.run(DataGenerator, params={"count": 4, "seed": 42}, backend=Backend.LOCAL)
step1 = pipeline.run(
    MetricCalculator,
    inputs={"dataset": step0.output("datasets")},
    backend=Backend.LOCAL,
)
pipeline.finalize()

# Then use InteractiveFilter
filt = InteractiveFilter(delta_root)
filt.load()  # discovers data artifacts and their derived metrics
```

**Assertions:**
- `filt.wide_df.height == 4`
- Columns include qualified names: `metric_calculator.distribution.min`, etc.
- `filt.set_criteria([{"metric": "metric_calculator.distribution.median", "operator": "gt", "value": 0.3}])`
  succeeds
- `filt.filtered_ids` returns a subset of artifact IDs
- `filt.summary()` returns `FilterSummary` with `criteria` and `funnel` DataFrames
- `result = filt.commit("interactive_filter")` returns `StepResult`
- Steps table has a new step record with `step_name == "interactive_filter"`
- `get_execution_outputs(delta_root, result.step_number, "passthrough")` matches
  `filt.filtered_ids`

---

## Test File 6: `test_cross_pipeline.py`

Exercises cross-pipeline artifact import and ExecutionConfigArtifact.

### Key Behavioral Notes

1. **IngestPipelineStep** is a generative curator with empty `inputs` and `outputs`
   ClassVars. It declares `source_delta_root`, `source_step`, and `artifact_type` as
   Pydantic Field attributes on the class — these are passed via the `params` dict at
   runtime (e.g., `params={"source_delta_root": str(path), "source_step": 1}`).

2. **Imported artifacts get the SAME IDs** (not new IDs) because `_to_draft()` clears
   `artifact_id`, then `finalize()` recomputes it from unchanged content. Content-addressed
   hashing produces identical IDs for identical content.

3. **Downstream wiring**: IngestPipelineStep returns `ArtifactResult` with artifacts keyed
   by artifact type string (e.g., `{"data": [...], "metric": [...]}`). These type strings
   become the output role names in execution_edges. Use `step.output("data")` to reference
   imported data artifacts. The `outputs` ClassVar is empty, so validation is a no-op —
   any role names from `ArtifactResult` pass through unchecked.

4. **`artifact_type` filter**: Optional field (`str | None`, default `None`). When set,
   only artifacts of that type are imported from the source step. When `None`, all types
   at the source step are imported.

5. **`DataTransformerConfig`** exists at
   `artisan.operations.examples.data_transformer_config` and already produces
   `ExecutionConfigArtifact` with `$artifact` references.

### Test 6.1: IngestPipelineStep — Basic Import

**Setup:**
```python
from artisan.operations.curator import IngestPipelineStep

# Pipeline A
pipeline_a = PipelineManager.create(
    name="source_pipeline",
    delta_root=delta_a, staging_root=staging_a, working_root=working_a,
)
step0 = pipeline_a.run(DataGenerator, params={"count": 3, "seed": 42}, backend=Backend.LOCAL)
step1 = pipeline_a.run(
    DataTransformer,
    inputs={"dataset": step0.output("datasets")},
    params={"scale_factor": 1.5, "noise_amplitude": 0.1, "variants": 1, "seed": 100},
    backend=Backend.LOCAL,
)
pipeline_a.finalize()

# Pipeline B
pipeline_b = PipelineManager.create(
    name="import_pipeline",
    delta_root=delta_b, staging_root=staging_b, working_root=working_b,
)
step_import = pipeline_b.run(
    IngestPipelineStep,
    params={"source_delta_root": str(delta_a), "source_step": 1},
    backend=Backend.LOCAL,
)
step_metrics = pipeline_b.run(
    MetricCalculator,
    inputs={"dataset": step_import.output("data")},
    backend=Backend.LOCAL,
)
pipeline_b.finalize()
```

**Assertions:**
- Pipeline B succeeds
- `count_artifacts_by_step(delta_b, 0) == 3` (imported data artifacts)
- Artifact IDs are identical to pipeline A step 1 (content-addressed)
- Imported artifacts have `imported_from_step` in metadata
- `read_table(delta_b, "provenance/artifact_edges")` has no entries linking to
  pipeline A artifacts (imported as new roots)
- MetricCalculator processes imported artifacts successfully

**Note:** Requires two separate pipeline environments (two `tmp_path` subdirectories).
Add a `dual_pipeline_env` fixture to conftest.

### Test 6.2: IngestPipelineStep — Type Filter

**Setup:**
- Pipeline A: DataGenerator (step 0, data) → MetricCalculator (step 1, metric)
- Pipeline B: IngestPipelineStep with `artifact_type="metric"` targeting step 1
  (imports only metrics)

```python
step_import = pipeline_b.run(
    IngestPipelineStep,
    params={"source_delta_root": str(delta_a), "source_step": 1, "artifact_type": "metric"},
    backend=Backend.LOCAL,
)
```

**Assertions:**
- Pipeline B imports only metric artifacts (no data)
- `count_artifacts_by_type(delta_b, "metric") > 0`
- `count_artifacts_by_type(delta_b, "data") == 0`
- `step_import.output("metric")` returns an OutputReference (role = artifact type)

### Test 6.3: ExecutionConfigArtifact — `$artifact` Reference Resolution

**Setup:** Uses `DataTransformerConfig` (existing example operation) to produce
ExecutionConfigArtifact with `$artifact` references, and a custom `ConfigConsumer`
that verifies resolution.

```python
from artisan.operations.examples.data_transformer_config import DataTransformerConfig
```

**Assertions:**
- Pipeline succeeds
- The config artifact's `$artifact` reference was resolved to a path pointing to
  the actual materialized data artifact
- The resolved path exists on disk during execution

**Note:** This test requires reading `DataTransformerConfig` source to understand
its exact output format before implementation.

---

## Test File 7: `test_cache_policies.py`

Exercises cache policy modes and cache hit artifact identity.

### Key Behavioral Notes

1. **The ONLY difference** between ALL_SUCCEEDED and STEP_COMPLETED is one filter line
   in `StepTracker.check_cache()`:
   ```python
   if cache_policy == CachePolicy.ALL_SUCCEEDED:
       query = query.filter(pl.col("failed_count") == 0)
   ```
   With STEP_COMPLETED, this filter is skipped.

2. **Step-level cache** controls whether `execute_step()` is called at all. On cache hit,
   the step result is reconstructed from the steps table and returned immediately.

3. **Cached StepResult with failures**: `success = (failed_count == 0)`, so a cached
   step with `failed_count=1` has `success=False` even though it's a cache hit.

4. **Must use `FailurePolicy.CONTINUE`** for partial failure tests. FAIL_FAST records
   the step as `"failed"` (not `"completed"`), so it's never a cache hit under either
   policy.

5. **`get_execution_outputs()` returns the same IDs** on cache hit because no new
   execution_edges are written.

### Test 7.1: Cache Hit Returns Identical Artifact IDs

**Setup:**
```python
# Run 1
p1 = PipelineManager.create(
    name="test_cache_identity",
    delta_root=delta_root, staging_root=staging_root, working_root=working_root,
)
step0 = p1.run(DataGenerator, params={"count": 2, "seed": 42}, backend=Backend.LOCAL)
step1 = p1.run(
    DataTransformer,
    inputs={"dataset": step0.output("datasets")},
    params={"scale_factor": 1.5, "noise_amplitude": 0.1, "variants": 1, "seed": 100},
    backend=Backend.LOCAL,
)
p1.finalize()

run1_step0_ids = sorted(get_execution_outputs(delta_root, 0, "datasets"))
run1_step1_ids = sorted(get_execution_outputs(delta_root, 1, "dataset"))
run1_exec_count = count_executions_by_step(delta_root, 0)

# Run 2 (identical pipeline, same delta_root)
p2 = PipelineManager.create(
    name="test_cache_identity",
    delta_root=delta_root, staging_root=staging_root, working_root=working_root,
)
step0b = p2.run(DataGenerator, params={"count": 2, "seed": 42}, backend=Backend.LOCAL)
step1b = p2.run(
    DataTransformer,
    inputs={"dataset": step0b.output("datasets")},
    params={"scale_factor": 1.5, "noise_amplitude": 0.1, "variants": 1, "seed": 100},
    backend=Backend.LOCAL,
)
p2.finalize()
```

**Assertions:**
- `sorted(get_execution_outputs(delta_root, 0, "datasets")) == run1_step0_ids`
- `sorted(get_execution_outputs(delta_root, 1, "dataset")) == run1_step1_ids`
- `count_executions_by_step(delta_root, 0) == run1_exec_count` (no new executions)

### Test 7.2: CachePolicy.ALL_SUCCEEDED — Partial Failure NOT Cached

**Setup:**
```python
p1 = PipelineManager.create(
    name="test_all_succeeded",
    delta_root=delta_root, staging_root=staging_root, working_root=working_root,
    cache_policy=CachePolicy.ALL_SUCCEEDED,
    failure_policy=FailurePolicy.CONTINUE,
)
step0 = p1.run(DataGenerator, params={"count": 3, "seed": 42}, backend=Backend.LOCAL)
step1 = p1.run(
    FailingTransformer,
    inputs={"dataset": step0.output("datasets")},
    params={"fail_on_index": 1},
    backend=Backend.LOCAL,
)
p1.finalize()
run1_exec_count = count_executions_by_step(delta_root, 1)

# Run 2 (identical)
p2 = PipelineManager.create(
    name="test_all_succeeded",
    delta_root=delta_root, staging_root=staging_root, working_root=working_root,
    cache_policy=CachePolicy.ALL_SUCCEEDED,
    failure_policy=FailurePolicy.CONTINUE,
)
step0b = p2.run(DataGenerator, params={"count": 3, "seed": 42}, backend=Backend.LOCAL)
step1b = p2.run(
    FailingTransformer,
    inputs={"dataset": step0b.output("datasets")},
    params={"fail_on_index": 1},
    backend=Backend.LOCAL,
)
p2.finalize()
```

**Assertions:**
- `step1.succeeded_count == 2`, `step1.failed_count == 1`
- Step 1 re-executed: `count_executions_by_step(delta_root, 1) > run1_exec_count`
- Step 0 was cached (no new executions for step 0, zero failures)

### Test 7.3: CachePolicy.STEP_COMPLETED — Partial Failure IS Cached

**Setup:** Same as 7.2 but `cache_policy=CachePolicy.STEP_COMPLETED`.

**Assertions:**
- `step1.succeeded_count == 2`, `step1.failed_count == 1`
- Step 1 IS cached: `count_executions_by_step(delta_root, 1) == run1_exec_count`
- Same artifact IDs returned (no new execution_edges)
- `step1b.success is False` (cached result preserves `failed_count == 1`)

---

## Conftest Additions

New shared helpers needed across multiple test files:

```python
def get_artifact_edges(delta_root: Path, source_id: str) -> list[str]:
    """Get target artifact IDs for a given source artifact ID."""
    df = read_table(delta_root, "provenance/artifact_edges")
    if df.is_empty():
        return []
    return df.filter(
        pl.col("source_artifact_id") == source_id
    )["target_artifact_id"].to_list()


def get_step_status(delta_root: Path, step_number: int) -> str | None:
    """Get the latest status for a step from the steps table."""
    df = read_table(delta_root, "orchestration/steps")
    if df.is_empty():
        return None
    rows = df.filter(pl.col("step_number") == step_number).sort("timestamp", descending=True)
    return rows["status"][0] if rows.height > 0 else None


def get_failed_executions(delta_root: Path, step_number: int) -> int:
    """Count failed execution records for a step."""
    df_exec = read_table(delta_root, "orchestration/executions")
    if df_exec.is_empty():
        return 0
    return df_exec.filter(
        (pl.col("origin_step_number") == step_number)
        & (pl.col("success") == False)  # noqa: E712
    ).height


def get_successful_executions(delta_root: Path, step_number: int) -> int:
    """Count successful execution records for a step."""
    df_exec = read_table(delta_root, "orchestration/executions")
    if df_exec.is_empty():
        return 0
    return df_exec.filter(
        (pl.col("origin_step_number") == step_number)
        & (pl.col("success") == True)  # noqa: E712
    ).height


def dual_pipeline_env(tmp_path: Path) -> dict[str, dict[str, Path]]:
    """Create two isolated pipeline environments for cross-pipeline tests."""
    # Returns {"a": {"delta_root": ..., ...}, "b": {"delta_root": ..., ...}}
```

---

## Implementation Notes

### Test Operation Requirements

Several tests need custom operations not in `artisan.operations.examples`:

1. **`DualInputZip`, `DualInputCross`, `DualInputLineage`** — for multi-input grouping
   tests. Define in `test_multi_input.py`. Three classes needed because `group_by` is a
   ClassVar. Minimal implementation: reads two CSVs, concatenates, writes output.

2. **`AssociatedMetricConsumer`** — for `with_associated` test. Single input with
   `with_associated=("metric",)`. Encodes association count in output for assertion.

3. **`FailingTransformer`** — for error handling and cache tests. Define in conftest
   or a shared test utils module. Raises `ValueError` based on input filename index.

4. **`ConfigConsumer`** (optional) — for ExecutionConfigArtifact test. Reads config,
   validates `$artifact` references resolved to real paths.

### Execution Order

Tests within each file are independent (separate `pipeline_env` fixture). Files can
run in parallel via pytest-xdist.

### Estimated Test Count

| File | Tests |
|---|---|
| `test_chaining.py` | 5 (merged 1.3/1.4, removed redundant 1.4) |
| `test_multi_input.py` | 4 |
| `test_error_handling.py` | 4 |
| `test_step_overrides.py` | 3 |
| `test_filter_advanced.py` | 4 |
| `test_cross_pipeline.py` | 3 |
| `test_cache_policies.py` | 3 |
| **Total** | **26** |

---

## Priority Order for Implementation

1. **`test_chaining.py`** — Major feature with zero coverage. High complexity.
2. **`test_error_handling.py`** — Failure modes are critical for production use.
3. **`test_multi_input.py`** — Complex grouping logic, error-prone.
4. **`test_cache_policies.py`** — Cache correctness is foundational.
5. **`test_step_overrides.py`** — Straightforward, catches isolation bugs.
6. **`test_filter_advanced.py`** — Filter is well-used but only trivially tested.
7. **`test_cross_pipeline.py`** — Specialized but important for correctness.
