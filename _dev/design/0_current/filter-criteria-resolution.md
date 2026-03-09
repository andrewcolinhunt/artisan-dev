# Design: Filter Criteria Resolution

**Date:** 2026-03-02 **Status:** Draft **Author:** Claude + ach94

---

## Goal

Redesign Filter's criteria resolution to eliminate the implicit/explicit split,
remove role-prefixed criteria, and provide a single consistent UX where criteria
are always just metric field names. Step name/number disambiguation replaces
input role wiring as the mechanism for targeting specific metric sources.

---

## Problem

Filter currently has two resolution paths with different semantics:

| | Explicit | Implicit |
|---|---|---|
| Criteria format | `"role.field"` | `"field"` |
| Discovery | Backward provenance walk | Forward provenance (1-hop) |
| Namespace | Per-role dicts | Single merged dict |
| Wiring | User wires metric roles | Auto-discover descendants |

A dot-based heuristic (`_partition_criteria`) routes criteria between paths.
This breaks on nested metric field names — `"distribution.min"` is a flattened
metric key, but the heuristic classifies it as explicit with role=`"distribution"`.

Beyond the bug, the dual-path design creates several problems:

1. **Users write different criteria for the same metric** depending on how they
   wire inputs — `"quality.distribution.min"` vs `"distribution.min"`.
2. **Role names are arbitrary** — chosen at wiring time, not intrinsic to the
   data. Renaming a role silently breaks criteria.
3. **Implicit is 1-hop only** — `get_descendant_ids_df` misses metrics
   that are 2+ hops away in the provenance graph.
4. **~200 lines of complexity** — partition logic, dual resolution, synthetic
   `_implicit` role, unification layer, collision detection, hybrid mode.

---

## Design Principles

1. **Criteria are always field names.** Never role-prefixed. The user writes
   `"distribution.min"` regardless of how metrics are discovered.

2. **Metric input roles are removed.** Filter only takes `passthrough`. All
   metric discovery is provenance-based.

3. **Step name/number is the disambiguation mechanism.** When field names
   collide across metric sources, the `step` and `step_number` fields on
   `Criterion` scope to a specific step's metrics. Only required when there
   are actual collisions.

4. **Multi-hop by default.** Forward provenance walk uses `walk_forward_to_targets`
   (iterative DataFrame join) to find all descendant metrics regardless of hop distance.

5. **Step-targeted lookup for non-descendants.** When `step`/`step_number` is
   specified and the metrics aren't found via forward walk, backward provenance
   walk finds metrics from sibling/ancestor branches.

### This is a breaking change. 

This is a breaking change to Filter's input wiring and criteria format.

---

## Criterion Model

```python
class Criterion(BaseModel):
    """A single filter criterion."""

    metric: str
    operator: Literal["gt", "ge", "lt", "le", "eq", "ne"]
    value: float | int | str | bool
    step: str | None = None          # Step name for disambiguation
    step_number: int | None = None   # Step number for disambiguation
```

Resolution priority when both are specified:

- Both `step` + `step_number` → must match both (most precise)
- `step` only → filter by step name; error if multiple steps share that name
  and produce conflicting field names
- `step_number` only → filter by step number
- Neither → auto-resolve; error on collision

---

## Resolution Algorithm

### Phase 1: Discover metrics

**Default path (no `step`/`step_number` on criterion):**

1. Get passthrough step range via `artifact_store.get_step_range(passthrough_ids)`
2. Load scoped edges via `artifact_store.load_provenance_edges_df(step_min, step_max)`
   — only edges within the relevant step window
3. Run `walk_forward_to_targets(passthrough_df, edges, target_type="metric")` —
   iterative forward join (frontier joins on `source_artifact_id`, takes
   `target_artifact_id`; loop until frontier is empty). Filters targets by
   `artifact_type` column on the edge/index data. All-match semantics: every
   passthrough artifact's descendant metrics are discovered in a single batch
4. Result: DataFrame of `(passthrough_id, metric_artifact_id)` pairs

**Step-targeted path (criterion has `step`/`step_number`):**

1. Resolve step identity:
   - Load `{step_number: step_name}` via `artifact_store.load_step_name_map()`
   - If `step` (name) given → find matching step number(s)
   - If `step_number` given → use directly
   - If both → validate they match
2. Load metric artifacts from the target step via `artifact_index` filtered by
   `origin_step_number` and `artifact_type="metric"`
3. Match metrics to passthrough via `walk_provenance_to_targets` (backward walk)
   using scoped provenance edges
4. Result: DataFrame of `(passthrough_id, metric_artifact_id)` pairs

### Phase 2: Hydrate and merge

1. Collect unique metric IDs from Phase 1 result
2. Chunked hydration via `artifact_store.load_metrics_df(metric_ids)` →
   two-column DataFrame of `(artifact_id, json_blob)`
3. Polars JSON decode → `pl.col("json_blob").str.json_decode()` → struct column
4. `unnest` → flatten to wide columns (one column per metric field)
5. Join back to Phase 1 mapping to associate each metric row with its
   passthrough artifact
6. Enrich with step provenance via `artifact_index` join on metric artifact ID
   → adds `origin_step_number` and `step_name` columns

### Phase 3: Collision detection

For criteria without `step`/`step_number`:

1. Group the wide metric DataFrame by `step_number` to find which steps
   produce each field name
2. If a criterion's `metric` field appears from a single step → resolve directly
3. If collision (field from multiple steps) → raise `ValueError` with actionable
   message:

```
Field 'score' found in metrics from multiple steps:
  - step 3 ("metric_calculator"): score = 0.85
  - step 5 ("metric_calculator"): score = 0.72
Add 'step' or 'step_number' to disambiguate:
  {"metric": "score", "step_number": 3, ...}
```

For criteria with `step`/`step_number`:

1. Filter the wide metric DataFrame to rows from the specified step only
2. Resolve field within that scope (no collision possible)

### Phase 4: Evaluate

Chunked evaluation: for each passthrough artifact's metric row, build
`pl.all_horizontal(bool_exprs)` where each `bool_expr` is the Polars
expression for one criterion (e.g., `pl.col("distribution.min") > 0.8`).
All criteria are AND'd. Return `PassthroughResult` with artifact IDs
that passed.

---

## User-Facing Examples

### Simple case (single metric source)

```python
# MetricCalculator computed metrics FROM the passthrough artifacts.
# Forward walk finds them automatically. No disambiguation needed.
pipeline.run(
    operation=Filter,
    inputs={"passthrough": merged.output("datasets")},
    params={"criteria": [
        {"metric": "distribution.min", "operator": "gt", "value": 0.8},
        {"metric": "summary.cv", "operator": "lt", "value": 0.5},
    ]},
)
```

### Multiple metric sources, no collision

```python
# Two metric steps produce different field names.
# Forward walk finds all. No disambiguation needed.
pipeline.run(
    operation=Filter,
    inputs={"passthrough": structures.output("structures")},
    params={"criteria": [
        {"metric": "distribution.min", "operator": "gt", "value": 0.8},
        {"metric": "ligand_score", "operator": "gt", "value": 0.5},
    ]},
)
```

### Disambiguation by step name

```python
# Two metric steps both produce "score". Collision detected.
pipeline.run(
    operation=Filter,
    inputs={"passthrough": structures.output("structures")},
    params={"criteria": [
        {"metric": "score", "operator": "gt", "value": 0.8, "step": "calc_quality"},
        {"metric": "score", "operator": "gt", "value": 0.5, "step": "calc_ligand"},
    ]},
)
```

### Disambiguation by step number (identical step names)

```python
# MetricCalculator run twice → both steps named "metric_calculator".
pipeline.run(
    operation=Filter,
    inputs={"passthrough": structures.output("structures")},
    params={"criteria": [
        {"metric": "score", "operator": "gt", "value": 0.8, "step_number": 3},
        {"metric": "score", "operator": "gt", "value": 0.5, "step_number": 5},
    ]},
)
```

### Non-descendant metrics (sibling branch)

```python
# Pipeline: generate(0) → calc_quality(1, metrics) → transform(2, structures)
# Passthrough = step 2 structures. Quality metrics are on step 1 (sibling).
# Forward walk from step 2 won't find step 1 metrics.
# step parameter triggers backward walk to match them.
pipeline.run(
    operation=Filter,
    inputs={"passthrough": transform.output("structures")},
    params={"criteria": [
        {"metric": "score", "operator": "gt", "value": 0.8, "step": "calc_quality"},
    ]},
)
```

---

## Infrastructure

Most required infrastructure already exists. One new function is needed:

| Need | Method | Location | Status |
|---|---|---|---|
| Multi-hop forward walk | `walk_forward_to_targets()` | `artisan/execution/inputs/lineage_matching.py` | **New** |
| Multi-hop backward walk | `walk_provenance_to_targets()` | `artisan/execution/inputs/lineage_matching.py` | Exists |
| Scoped edge loading | `load_provenance_edges_df()` | `ArtifactStore` | Exists (minor extension: optional `target_artifact_type` column) |
| Step range for scoping | `get_step_range()` | `ArtifactStore` | Exists |
| Artifact → step number | `load_step_number_map()` | `ArtifactStore` | Exists |
| Step number → step name | `load_step_name_map()` | `ArtifactStore` (reads `steps` Delta table) | Exists |
| Metric hydration | `load_metrics_df()` | `ArtifactStore` | Exists |

`walk_forward_to_targets()` is structurally identical to `walk_provenance_to_targets()`
with the join direction reversed: frontier joins on `source_artifact_id` and takes
`target_artifact_id` (forward) instead of joining on `target_artifact_id` and taking
`source_artifact_id` (backward). `trace_derived_artifacts` (dict-based BFS) remains
for `InteractiveFilter` single-artifact exploration.

---

## What Changes

### Deleted

| Item | Reason |
|---|---|
| `_partition_criteria()` | No explicit/implicit split |
| `_build_criteria_lists()` | No dual namespace (explicit vs `_implicit.` prefix) |
| `_match_explicit()` | Replaced by `_discover_descendant_metrics()` (forward) and `_discover_step_metrics()` (backward) |
| `_hydrate_as_wide_df()` | Replaced by `_build_metric_namespace()` |
| `_validate_criteria()` | No role validation (no roles) |
| `_implicit` synthetic role key | No dual namespaces |
| `runtime_defined_inputs = True` | No dynamic metric input roles |
| `independent_input_streams = True` | Only one input stream |
| Inline implicit resolution in `execute_curator` (`get_descendant_ids_df` call) | Replaced by unified forward walk |

### Added

| Item | Purpose |
|---|---|
| `Criterion.step` field | Step name disambiguation |
| `Criterion.step_number` field | Step number disambiguation |
| `_discover_descendant_metrics()` | Forward walk via `walk_forward_to_targets` with scoped edges |
| `_discover_step_metrics()` | Step-targeted backward walk |
| `_build_metric_namespace()` | Unified hydrate + flatten + merge with collision tracking |
| `walk_forward_to_targets()` | New infrastructure in `lineage_matching.py` — batch forward provenance walk |
| Collision detection with actionable errors | Guide user to add `step`/`step_number` |

### Modified

| Item | Change |
|---|---|
| `execute_curator()` | Simplified top-level flow using new helpers, no partition/dual-path logic |
| `load_provenance_edges_df()` | Optional `target_artifact_type` column in output |
| `_DiagnosticsAccumulator.finalize()` | Report step name/number instead of role names |

---

## Diagnostics

The diagnostics dict (version 4) reports step-based provenance instead of roles:

```python
{
    "version": 4,
    "total_input": 100,
    "total_metrics_discovered": 95,
    "total_passed": 42,
    "metric_sources": [
        {"step_number": 3, "step_name": "calc_quality", "metric_count": 95},
        {"step_number": 5, "step_name": "calc_ligand", "metric_count": 90},
    ],
    "criteria": [
        {
            "metric": "distribution.min",
            "operator": "gt",
            "value": 0.8,
            "step": None,
            "step_number": None,
            "resolved_from_step": 3,
            "pass_count": 42,
            "stats": {"min": 0.1, "max": 0.99, "mean": 0.65, "median": 0.68},
        },
    ],
    "funnel": [...],
}
```

---

## Migration

This is a breaking change to Filter's input wiring and criteria format.

### Criteria changes

```python
# Before (explicit, role-prefixed):
{"metric": "quality.distribution.min", "operator": "gt", "value": 0.8}

# After (field name only):
{"metric": "distribution.min", "operator": "gt", "value": 0.8}
```

### Input wiring changes

```python
# Before:
pipeline.run(
    operation=Filter,
    inputs={
        "passthrough": merged.output("datasets"),
        "quality": metrics.output("metrics"),
    },
    params={"criteria": [
        {"metric": "quality.distribution.min", "operator": "gt", "value": 0.8},
    ]},
)

# After:
pipeline.run(
    operation=Filter,
    inputs={"passthrough": merged.output("datasets")},
    params={"criteria": [
        {"metric": "distribution.min", "operator": "gt", "value": 0.8},
    ]},
)
```

### Affected files

Production pipelines (`e385_redesign`), integration tests, tutorials, demos,
and how-to guides all need criteria and input wiring updated. The migration is
mechanical — remove metric input roles, strip role prefixes from criteria,
add `step`/`step_number` where needed for disambiguation.

---

## Performance Considerations

- Scoped edge loading via `load_provenance_edges_df(step_min, step_max)` avoids
  full table reads — only edges within the passthrough's step window are loaded.
- `walk_forward_to_targets` uses iterative DataFrame joins (not per-artifact
  Python loops), processing the entire passthrough batch in each iteration.
- `load_metrics_df` → Polars `str.json_decode()` performs JSON parsing in Polars'
  native layer, avoiding Python-level iteration over metric values.
- Chunked evaluation via `pl.all_horizontal(bool_exprs)` bounds peak memory by
  evaluating criteria in Polars expressions rather than materializing Python dicts.
- Step name/number maps are small (one entry per step) and cached per call.

---

## Test Plan

1. **Unit tests** — new tests for:
   - `walk_forward_to_targets()`: single-hop, multi-hop, no targets reachable, type filtering
   - Single metric source, flat fields (forward walk)
   - Single metric source, nested fields (`distribution.min`)
   - Multiple metric sources, no collision
   - Multiple metric sources, collision without `step` → error
   - Disambiguation by `step` name
   - Disambiguation by `step_number`
   - Disambiguation by both `step` + `step_number`
   - Non-descendant metrics via `step` (backward walk)
   - Identical step names, different step numbers
   - Empty metrics → all pass (existing behavior preserved)
   - `passthrough_failures` mode (existing behavior preserved)

2. **Integration tests** — update existing tests in `test_data_flow_patterns.py`
   to use new criteria format.

3. **Delete** — `TestFilterImplicitMetrics`, `TestFilterHybridMode`,
   `TestFilterMultiRoleMultiCriteria`, `TestFilterValidationErrors` role-based
   tests. Replace with unified tests above.
