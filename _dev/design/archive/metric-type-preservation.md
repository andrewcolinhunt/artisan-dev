# Design: Metric Type Preservation

**Date:** 2026-03-10 **Status:** Draft **Author:** Claude + ach94

---

## Goal

Preserve original JSON types (int, float, bool, str) through the metric
tidy/wide DataFrame pipeline. Currently all metric values are coerced to
`Float64`, losing type information silently.

---

## Problem

The metric pivoting pipeline has a lossy conversion at the entry point:

```python
# dataframes.py
def to_float(val: Any) -> float | None:
    if isinstance(val, bool):
        return float(val)
    if isinstance(val, int | float):
        return float(val)
    return None
```

This is called in `interactive_filter.py` when building the tidy DataFrame,
and the schema hardcodes `metric_value: pl.Float64`.

**What breaks:**

| Input type | Current output | Expected output |
|---|---|---|
| `int(5)` | `5.0` | `5` |
| `bool(True)` | `1.0` | `True` |
| `str("high")` | `None` (dropped) | `"high"` |
| `list([1,2,3])` | `None` (dropped) | preserved separately |
| `dict(...)` | flattened by `flatten_dict` | flattened (no change) |

The silent dropping of string and compound values is the worst part — no
warning, no error, just missing data.

---

## Design

### Core idea

Metric values from JSON fall into two categories:

- **Scalars** — int, float, bool, str, None. These are filterable,
  comparable, and can be pivoted into typed columns.
- **Compounds** — list, dict. These aren't meaningfully filterable
  (you can't say "where bins >= 0.5"). They should be preserved but
  kept out of the pivot.

The tidy DataFrame gets two value columns. The wide DataFrame only
contains scalars, with each column cast to its natural type after pivot.

### Tidy schema

| Column | Type | Content |
|---|---|---|
| `artifact_id` | `pl.String` | Row identifier |
| `step_number` | `pl.Int32` | Pipeline step |
| `step_name` | `pl.String` | Step name |
| `metric_name` | `pl.String` | Dot-separated metric key |
| `metric_value` | `pl.String` | JSON-encoded scalar, or null for compounds |
| `metric_compound` | `pl.String` | JSON-encoded list/dict, or null for scalars |

Each row has exactly one non-null value column. Using `pl.String` with
JSON encoding is the only way to hold mixed scalar types (int, float,
bool, str) in a single Polars column without `pl.Object`.

### Encoding examples

| Python value | `metric_value` | `metric_compound` |
|---|---|---|
| `5` | `"5"` | null |
| `3.14` | `"3.14"` | null |
| `True` | `"true"` | null |
| `"high"` | `"\"high\""` | null |
| `None` | `"null"` | null |
| `[1, 2, 3]` | null | `"[1, 2, 3]"` |
| `{"a": 1}` | null | `"{\"a\": 1}"` |

### Wide DataFrame (after pivot)

`pivot_metrics_wide` filters to `metric_value IS NOT NULL`, pivots, then
infers the natural type for each column:

| Check (in order) | Result type |
|---|---|
| All values are `true`/`false` | `pl.Boolean` |
| All values are integer strings | `pl.Int64` |
| All values are numeric strings | `pl.Float64` |
| All values are quoted strings | `pl.String` (quotes stripped) |
| Mixed | `pl.String` (kept as JSON) |

This means a column of `[5, 10, 15]` becomes `Int64`, a column of
`[5, 10.5, 15]` becomes `Float64`, and a column of `[5, "high", 10]`
stays `String`.

---

## API changes

### `dataframes.py`

**Remove:**
- `to_float(val)` — replaced by `encode_metric_value`

**Add:**
- `is_scalar_metric(val) -> bool` — True for int, float, bool, str, None
- `encode_metric_value(val) -> tuple[str | None, str | None]` — returns
  `(scalar_json, compound_json)`, exactly one non-null

**Modify:**
- `pivot_metrics_wide(tidy)` — filters to scalar rows, pivots, then
  calls `_infer_column_types` to cast each column

**Add (private):**
- `_infer_column_types(df, exclude)` — iterates non-index columns
- `_cast_json_column(df, col_name)` — tries Bool → Int64 → Float64 →
  String in order

### `interactive_filter.py`

**Line 237:** Replace `to_float(raw_value)` with `encode_metric_value(raw_value)`
and populate both columns.

**Lines 245-251:** Update tidy schema to use `pl.String` for
`metric_value` and add `metric_compound`.

No other changes needed — `summary()`, `plot()`, and `filtered_wide_df`
all operate on the wide DataFrame, which will have naturally-typed
columns after the pivot + inference step.

### `utils/__init__.py`

Replace `to_float` export with `is_scalar_metric` and
`encode_metric_value`.

---

## Downstream impact

### `interactive_filter.py` — summary stats (line 399)

```python
numeric = col_data.cast(pl.Float64, strict=False).drop_nulls()
```

Already handles non-numeric columns gracefully via `strict=False`.
With naturally-typed columns, Int64 and Float64 columns cast without
loss; Boolean and String columns produce nulls (excluded from stats).
No change needed.

### `interactive_filter.py` — plot (line 489)

```python
values = [v for v in self._wide_df[crit.metric].to_list()
          if isinstance(v, (int, float))]
```

Already filters to numeric. With Int64/Float64 columns this works
as-is. No change needed.

### `interactive_filter.py` — filtering (line 368)

```python
mask = mask & getattr(col, op_name)(crit.value)
```

Polars comparison operators work on all native types. An Int64
column compared with `gt(50)` works correctly. No change needed.

### `inspect.py` — `inspect_metrics` (line 294)

Already handles types correctly — preserves ints, only rounds floats.
Independent of this change (it builds its own DataFrame directly from
JSON, doesn't use `to_float` or the tidy format).

---

## Test plan

### `test_dataframes.py`

- **Replace `TestToFloat`** with tests for `is_scalar_metric` and
  `encode_metric_value`
- **Update `_tidy` helper** — schema changes to `pl.String` for
  `metric_value`, add `metric_compound`
- **Add type inference tests:**
  - All-int column → `pl.Int64`
  - All-float column → `pl.Float64`
  - Mixed int/float → `pl.Float64`
  - All-bool column → `pl.Boolean`
  - All-string column → `pl.String`
  - Mixed types → `pl.String`
  - Column with nulls — nulls preserved, type inferred from non-nulls
- **Existing pivot tests** — update input values to JSON-encoded strings,
  verify output types are correct

### `test_interactive_filter.py`

- **Add fixture with mixed-type metrics** — int counts, float scores,
  bool flags, string labels, list compounds
- **Verify wide DataFrame types** — int columns are Int64, float columns
  are Float64
- **Verify compound exclusion** — list/dict metrics appear in tidy but
  not in wide
- **Existing tests** — should pass without modification (the test fixture
  uses float metrics which will become Float64 columns)

---

## Non-goals

- Flattening lists into indexed keys (`bins.0`, `bins.1`, ...) — this
  explodes column count and the values still aren't meaningfully
  comparable.
- Supporting filtering on compound values — if a user needs to filter
  on a list element, they should compute a scalar metric from it upstream.
- Changing `flatten_dict` behavior — it correctly recurses dicts and
  stops at non-dict leaves. No change needed.
- Changing `inspect_metrics` — it already handles types correctly.
