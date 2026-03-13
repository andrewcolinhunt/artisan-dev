# Interactive Filter Parity with Filter

**Status:** Draft
**Scope:** Bring `InteractiveFilter` into full parity with `Filter`

---

## Context

`Filter` (pipeline operation) received several upgrades that `InteractiveFilter`
(notebook tool) never got. They now diverge in metric discovery, column naming,
null handling, and diagnostics format.

**Filter is the baseline.** We spent significant effort designing Filter's
semantics — criterion syntax, column naming, null handling, diagnostics format,
collision detection. InteractiveFilter should match all of it so that criteria
are portable between the two tools.

## Current Divergences

| Area | Filter | InteractiveFilter |
|------|--------|-------------------|
| Metric discovery | `walk_forward_to_targets` (batch DataFrame joins) | `trace_derived_artifacts` (per-artifact Python BFS) |
| Wide DataFrame | `_build_metric_namespace` — raw field names (`confidence`) | `pivot_metrics_wide` — qualified names (`calc_metrics.confidence`) |
| Null handling | `.fill_null(False)` — nulls explicitly fail | No null handling — nulls silently drop rows |
| Diagnostics | v4: `total_evaluated`, `metric_sources`, per-criterion `stats`, `resolved_from_step` | v2: `interactive: True`, simpler criteria/funnel dicts |
| Step disambiguation | `Criterion.step`/`step_number` + collision detection | Accepted by Pydantic but ignored |
| Criteria evaluation | `_criterion_to_expr` (match/case) | `getattr(pl.col(...), op_name)` |
| Stats | min/max/mean | min/median/max |
| Passthrough failures | Supported | Not supported |
| Chunked evaluation | `chunk_size` param (default 100K) | Full DataFrame at once |

## Decisions

**Column naming**: InteractiveFilter adopts Filter's raw field names
(`confidence`, not `calc_metrics.confidence`). This means replacing
`pivot_metrics_wide` with Filter's `_build_metric_namespace` approach (JSON
decode -> struct unnest -> flatten). Criteria become portable between the two
tools — same `{"metric": "confidence", "operator": "ge", "value": 0.8}` works
in both.

**Mean vs median**: Filter reports mean. InteractiveFilter matches: report mean.
Median adds no value if it disagrees with what Filter would report — users
need to see the same stats the automated filter will use. `FilterSummary`
switches from median to mean.

**Null handling**: Match Filter — `.fill_null(False)` everywhere criteria are
evaluated.

**Diagnostics**: Emit v4 format. Keep `interactive: True` flag to distinguish
provenance.

**Step disambiguation**: Wire up `Criterion.step`/`step_number` and add
collision detection matching Filter's `_check_collision`.

**Criteria evaluation**: Use `_criterion_to_expr` from Filter (import it)
instead of the separate `getattr` pattern. One code path, not two.

**Metric discovery**: Migrate to `walk_forward_to_targets`. Same code path as
Filter, better performance.

**Passthrough failures**: Not needed. Interactive workflow lets users adjust
thresholds before committing. Omit.

**Chunked evaluation**: Not needed. Data is already in memory from `load()`.
Omit.

## Implementation Plan

### Phase 1: Column naming + criteria evaluation

This is the biggest behavioral change. Replace `pivot_metrics_wide` with
Filter's `_build_metric_namespace` approach so wide_df uses raw field names.

- Replace `load()`'s tidy->wide pipeline with metric hydration matching
  Filter's `_build_metric_namespace` (JSON decode, struct unnest, flatten)
- Import and use `_criterion_to_expr` from `filter.py` in `filtered_wide_df`
  and `summary()` instead of the `_PL_OPERATORS` / `_OPERATORS` mappings
- Add `.fill_null(False)` to all criteria evaluation
- Add collision detection by calling `Filter._check_collision` (already a
  `@staticmethod`, no extraction needed)
- Wire up `Criterion.step`/`step_number` in `set_criteria()`: when raw field
  names collide across steps, `set_criteria()` calls `_check_collision` and
  raises unless the criterion provides `step` or `step_number` to disambiguate.
  This matches Filter's evaluation-time behavior but surfaces the error earlier,
  while the user can still adjust.

Impact on `load()`: The tidy DataFrame can stay (useful for `tidy_df` property),
but the wide DataFrame construction changes completely. The tidy_df is still
built with `encode_metric_value` / qualified names for exploration. The wide_df
switches to raw field names for filtering.

### Phase 2: Diagnostics v4

- Store `_total_metrics_discovered` and `_metric_sources` during `load()`
- Update `FilterSummary` to report mean instead of median
- Update `commit()` to emit v4 diagnostics with `total_evaluated`,
  `total_metrics_discovered`, `metric_sources`, per-criterion
  `resolved_from_step` and `stats` (min/max/mean), funnel with
  `label`/`count`/`eliminated`
- Keep `interactive: True` flag

### Phase 3: Metric discovery migration

- Replace `trace_derived_artifacts` + `load_forward_provenance_map` with
  `walk_forward_to_targets` + `load_provenance_edges_df`
- Remove `_OPERATORS` and `_PL_OPERATORS` dicts (no longer needed)
- Update imports (remove `trace_derived_artifacts`, `encode_metric_value`,
  `pivot_metrics_wide`)

## Cleanup

After all phases, remove from `interactive_filter.py`:
- `_OPERATORS` dict
- `_PL_OPERATORS` dict
- `encode_metric_value` import
- `pivot_metrics_wide` import
- `trace_derived_artifacts` import
- `load_forward_provenance_map` usage
- `flatten_dict` import (if no longer needed)

These utilities remain in the codebase for other consumers (tutorial notebooks,
provenance_utils tests).

## Test Impact

- Column naming: existing tests break — update all `wide_df` column assertions
  from `calc_metrics.confidence` to `confidence`
- Null handling: add tests for null metric values (criteria should fail, not
  silently drop)
- Criteria evaluation: update to use Filter-style criterion dicts
- Diagnostics: update `TestCommit` to validate v4 keys
- Discovery migration: existing test fixtures already write real
  `provenance/artifact_edges` tables, which `load_provenance_edges_df` reads
  from — no fixture data changes needed. The code path switches from
  `load_forward_provenance_map` + `trace_derived_artifacts` to
  `load_provenance_edges_df` + `walk_forward_to_targets`.

## Files Changed

| File | Change |
|------|--------|
| `src/artisan/operations/curator/interactive_filter.py` | All phases |
| `src/artisan/operations/curator/filter.py` | No changes needed — `_criterion_to_expr` is already module-level, `_check_collision` is a `@staticmethod` |
| `tests/artisan/operations/curator/test_interactive_filter.py` | Update all assertions |
