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

4. **Multi-hop by default.** Forward provenance walk uses `trace_derived_artifacts`
   (BFS) to find all descendant metrics regardless of hop distance.

5. **Step-targeted lookup for non-descendants.** When `step`/`step_number` is
   specified and the metrics aren't found via forward walk, backward provenance
   walk finds metrics from sibling/ancestor branches.

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

1. Load forward provenance map via `artifact_store.load_forward_provenance_map()`
2. Load all metric artifact IDs via `artifact_store.load_artifact_ids_by_type("metric")`
3. For each passthrough artifact, run `trace_derived_artifacts(pt_id, forward_map, metric_ids)` — multi-hop forward BFS
4. Result: `{passthrough_id: [metric_artifact_ids]}`

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
4. Result: `{passthrough_id: [metric_artifact_ids]}`

### Phase 2: Hydrate and merge

1. Bulk-load all discovered metric artifacts via `artifact_store.get_artifacts_by_type()`
2. For each metric, `flatten_dict(metric.values)` → `{field: value}`
3. Track provenance: `{passthrough_id: {field: [(value, step_number, step_name)]}}`

### Phase 3: Collision detection

For criteria without `step`/`step_number`:

1. Check if the criterion's `metric` field appears in metrics from multiple steps
2. If unique → resolve directly
3. If collision → raise `ValueError` with actionable message:

```
Field 'score' found in metrics from multiple steps:
  - step 3 ("metric_calculator"): score = 0.85
  - step 5 ("metric_calculator"): score = 0.72
Add 'step' or 'step_number' to disambiguate:
  {"metric": "score", "step_number": 3, ...}
```

For criteria with `step`/`step_number`:

1. Scope to metrics from the specified step only
2. Resolve field within that scope (no collision possible)

### Phase 4: Evaluate

For each passthrough artifact, evaluate all criteria against its resolved
metric values. All criteria are AND'd. Return `PassthroughResult` with
artifact IDs that passed.

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

All required infrastructure already exists:

| Need | Method | Location |
|---|---|---|
| Multi-hop forward walk | `trace_derived_artifacts()` | `artisan/storage/provenance_utils.py` |
| Forward provenance map | `load_forward_provenance_map()` | `ArtifactStore` |
| Multi-hop backward walk | `walk_provenance_to_targets()` | `artisan/execution/inputs/lineage_matching.py` |
| Scoped edge loading | `load_provenance_edges_df()` | `ArtifactStore` |
| Artifact → step number | `load_step_number_map()` | `ArtifactStore` |
| Step number → step name | `load_step_name_map()` | `ArtifactStore` (reads `steps` Delta table) |
| Metric artifact loading | `get_artifacts_by_type()` | `ArtifactStore` |
| Metric IDs by type | `load_artifact_ids_by_type()` | `ArtifactStore` |

No new storage tables, provenance utilities, or framework methods required.

---

## What Changes

### Deleted

| Item | Reason |
|---|---|
| `_partition_criteria()` | No explicit/implicit split |
| `_build_criteria_lists()` | No dual namespace (explicit vs `_implicit.` prefix) |
| `_match_explicit()` | Replaced by step-targeted backward walk |
| `_hydrate_as_wide_df()` | Replaced by `_build_metric_namespace()` |
| `_validate_criteria()` | No role validation (no roles) |
| `_implicit` synthetic role key | No dual namespaces |
| `runtime_defined_inputs = True` | No dynamic metric input roles |
| `independent_input_streams = True` | Only one input stream |
| Inline implicit resolution in `execute_curator` (`get_descendant_ids_df` call) | Replaced by unified forward walk |
| Role-prefixed column logic in `_hydrate_as_wide_df` | Criteria are just field names |

### Added

| Item | Purpose |
|---|---|
| `Criterion.step` field | Step name disambiguation |
| `Criterion.step_number` field | Step number disambiguation |
| `_discover_descendant_metrics()` | Forward BFS via `trace_derived_artifacts` |
| `_discover_step_metrics()` | Step-targeted backward walk |
| `_build_metric_namespace()` | Unified hydrate + flatten + merge with collision tracking |
| Collision detection with actionable errors | Guide user to add `step`/`step_number` |

### Modified

| Item | Change |
|---|---|
| `execute_curator()` | Simplified top-level flow using new helpers, no partition/dual-path logic |
| `_DiagnosticsAccumulator.finalize()` | Report step name/number instead of role names |
| `_criterion_to_expr()` | No change needed (already field-based) |

---

## Diagnostics

The diagnostics dict (version 3) reports step-based provenance instead of roles:

```python
{
    "version": 3,
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

- `load_forward_provenance_map()` does a full scan of `artifact_edges` (2
  columns). Already used by InteractiveFilter, export, and structure viewer
  without performance issues.
- `trace_derived_artifacts` BFS per passthrough artifact is O(reachable nodes),
  typically bounded by the pipeline's step range.
- For large pipelines, a scoped forward map (analogous to `load_provenance_edges_df`
  step range filtering) could be added as an optimization. Not needed initially —
  existing consumers work at production scale without scoping.
- Step name/number maps are small (one entry per step) and cached per call.

---

## Test Plan

1. **Unit tests** — new tests for:
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
