# Design: Fix Filter Struct Flattening Collision

**Date:** 2026-03-12  **Status:** Draft  **Author:** Claude + ach94

---

## Problem

Filter silently fails when metrics from multiple steps share field names at
different nesting levels. All artifacts are filtered out and the downstream
step receives empty inputs. There is no clear error message — the user sees
"0/5 artifacts passed" with no explanation of what went wrong.

### Reproduction

The `03-metrics-and-filtering.ipynb` tutorial's multi-metric pipeline
demonstrates this. `DataGeneratorWithMetrics` produces flat metrics with a
top-level `row_count`, and `MetricCalculator` produces nested metrics with
`summary.row_count`. When Filter tries to evaluate criteria from both
sources, it crashes internally and all artifacts are filtered out.

```python
# Step 0: DataGeneratorWithMetrics
#   metrics: {"mean_score": 0.34, "row_count": 10, "std_score": 0.25}
#
# Step 1: MetricCalculator
#   metrics: {"distribution": {"range": 0.91, ...}, "summary": {"cv": 0.65, "row_count": 10}}
#
# Step 2: Filter
#   criteria: mean_score > 0.3 AND distribution.range < 0.9
#   Expected: some artifacts pass
#   Actual: 0/5 pass — DuplicateError swallowed by subprocess error handling
```

---

## Root Cause Analysis

The bug is in `_flatten_struct_columns` (`filter.py:58-84`). There are
**two problems** that chain together to produce the observed behavior.

### Problem 1: Polars `unnest` creates duplicate column names

`_flatten_struct_columns` iterates over struct columns and calls
`result.unnest(col_name)` to extract struct fields into top-level columns.
Polars' `unnest()` extracts fields with their **original** names first,
then the function renames them to dot-separated names afterward.

When a struct field has the same name as an existing top-level column,
`unnest()` fails with `DuplicateError` before the rename can happen.

**Concrete trace:**

After `str.json_decode()` on the mixed metric JSON, the superset struct
schema is:

```
Struct({
    'distribution': Struct({'max': f64, 'range': f64, ...}),
    'summary': Struct({'cv': f64, 'row_count': i64}),
    'mean_score': f64,
    'row_count': i64,       ← top-level (from DataGeneratorWithMetrics)
    'std_score': f64,
})
```

After the first `unnest("_parsed")`, the DataFrame has both `row_count`
(top-level) and `summary` (struct containing `row_count`). When
`_flatten_struct_columns` tries to `unnest("summary")`, Polars extracts
the `row_count` field from the struct — but a `row_count` column already
exists:

```
polars.exceptions.DuplicateError:
  column with name 'row_count' has more than one occurrence
```

### Problem 2: The error is silently swallowed

The curator execution path catches the `DuplicateError`:

```
Filter.execute_curator()
  → _build_metric_namespace()
    → _flatten_struct_columns()      ← DuplicateError raised here
```

In `run_curator_flow` (`curator.py:312`), `except Exception` catches the
error and calls `record_execution_failure()`, which returns a
`StagingResult(success=False)`. Back in the pipeline, the step executor
reports "0/1 succeeded" but the filter-specific log at
`step_executor.py:493` misleadingly says "0/5 artifacts passed (5 filtered
out)" — because it uses `succeeded` from `aggregate_results` rather than
the actual filter diagnostics.

**From the user's perspective:** they see a mysterious "everything was
filtered out" with no error traceback or hint that a schema collision
occurred. The failure log exists in the run directory but is easy to miss.

---

## Proposed Fix

### Fix 1: Rename struct fields before unnesting (core fix)

Use `struct.rename_fields()` to prefix field names **before** calling
`unnest()`, preventing the collision entirely:

```python
def _flatten_struct_columns(frame: pl.DataFrame) -> pl.DataFrame:
    result = frame
    changed = True
    while changed:
        changed = False
        for col_name in result.columns:
            col_dtype = result[col_name].dtype
            if isinstance(col_dtype, pl.Struct):
                fields = col_dtype.fields
                prefixed_names = [f"{col_name}.{f.name}" for f in fields]
                result = result.with_columns(
                    pl.col(col_name).struct.rename_fields(prefixed_names)
                ).unnest(col_name)
                changed = True
                break
    return result
```

This is a drop-in replacement. The rename happens inside the struct before
unnesting, so the extracted columns are already dot-prefixed and cannot
collide with existing top-level columns.

**Before (broken):**

```
unnest("summary")
  → extracts fields: "cv", "row_count"   ← collides with existing "row_count"
  → DuplicateError
```

**After (fixed):**

```
rename_fields(["summary.cv", "summary.row_count"])
unnest("summary")
  → extracts fields: "summary.cv", "summary.row_count"   ← no collision
```

### Fix 2: Surface the error clearly (optional improvement)

The current error path swallows the `DuplicateError` into a generic
failure record. Consider adding a targeted try/except in
`_build_metric_namespace` that re-raises with a clear message when struct
flattening fails due to field name collisions. This would help users
diagnosing similar issues with custom operations.

This is a nice-to-have — Fix 1 eliminates the error entirely for this case.

---

## Fix Priority

| Fix | Impact | Effort | Priority |
|-----|--------|--------|----------|
| Fix 1: `rename_fields` before `unnest` | Eliminates the bug | Trivial | Critical |
| Fix 2: Better error surfacing | Better debugging UX | Small | Low |

---

## Files to Modify

| File | Changes |
|------|---------|
| `operations/curator/filter.py` `_flatten_struct_columns` | Use `struct.rename_fields` before `unnest` |

---

## Testing Plan

### Unit test: `_flatten_struct_columns` collision handling

**File:** `tests/artisan/operations/curator/test_filter.py`

The existing `test_nested_struct_flattened` (line 488) tests basic struct
flattening but doesn't cover the collision case. Add a sibling test:

```python
def test_flatten_struct_field_name_collision(self):
    """Struct field with same name as top-level column doesn't crash."""
    df = pl.DataFrame({
        "artifact_id": ["a", "b"],
        "row_count": [10, None],
        "summary": [None, {"cv": 0.5, "row_count": 10}],
    })
    result = _flatten_struct_columns(df)

    # Both columns exist with distinct names
    assert "row_count" in result.columns
    assert "summary.row_count" in result.columns
    assert "summary.cv" in result.columns

    # Values are preserved correctly
    assert result["row_count"][0] == 10
    assert result["summary.row_count"][1] == 10
```

### Integration test: multi-source filter with colliding field names

**File:** `tests/integration/test_filter_advanced.py`

This is the critical missing test. It exercises the exact pipeline pattern
from the tutorial: `DataGeneratorWithMetrics` (flat metrics with
`row_count`) + `MetricCalculator` (nested metrics with `summary.row_count`)
filtered together.

The existing tests in this file all use `DataGenerator` + `MetricCalculator`
(single metric source). None use `DataGeneratorWithMetrics`, so the
multi-source schema merge path is untested.

```python
def test_multi_source_filter_with_colliding_field_names(
    pipeline_env: dict[str, Path],
):
    """Filter across two metric sources where field names collide at
    different nesting levels (top-level row_count vs summary.row_count).

    This is the exact pattern from the 03-metrics-and-filtering tutorial.
    DataGeneratorWithMetrics produces flat metrics (mean_score, row_count, ...),
    MetricCalculator produces nested metrics (distribution.range,
    summary.row_count, ...). Filter must merge both schemas without crashing
    on the shared 'row_count' field name.
    """
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_multi_source_collision",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    # Step 0: Generate datasets + co-produced flat metrics (mean_score, row_count)
    step0 = pipeline.run(
        DataGeneratorWithMetrics,
        params={"count": 5, "seed": 42},
        backend=Backend.LOCAL,
    )

    # Step 1: Compute nested metrics (distribution.range, summary.row_count)
    step1 = pipeline.run(
        MetricCalculator,
        inputs={"dataset": step0.output("datasets")},
        backend=Backend.LOCAL,
    )

    # Step 2: Filter on criteria from BOTH metric sources
    step2 = pipeline.run(
        Filter,
        inputs={"passthrough": step0.output("datasets")},
        params={
            "criteria": [
                {"metric": "mean_score", "operator": "gt", "value": 0.3},
                {"metric": "distribution.range", "operator": "lt", "value": 0.95},
            ],
        },
        backend=Backend.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    passthrough_ids = get_execution_outputs(delta_root, 2, "passthrough")
    # With seed=42, mean_score ranges 0.34-0.55 (all > 0.3) and
    # distribution.range ranges 0.72-0.91 (all < 0.95), so all 5 should pass.
    assert len(passthrough_ids) == 5
```

**Why this catches the bug:** before the fix, `_flatten_struct_columns`
raises `DuplicateError` when unnesting `summary` (because `row_count`
already exists as a top-level column). The error is caught by
`run_curator_flow`, the step fails, and 0 artifacts pass — failing the
`assert len(passthrough_ids) == 5`.

### Manual verification

Run the tutorial notebook and confirm the multi-metric pipeline reports
3/5 or more artifacts passing (not 0/5).

---

## Verification

```bash
~/.pixi/bin/pixi run -e dev test-unit
~/.pixi/bin/pixi run -e dev test-integration
```
