# Design: Provenance Traversal Reorganization

**Date:** 2026-03-10 **Status:** Draft **Author:** Claude + ach94

---

## Problem

Provenance traversal logic is scattered across 4 files in 3 packages. The
naming is inconsistent, two paradigms (dict-based BFS and DataFrame-based
walks) coexist without justification, and `ArtifactStore` has grown into a
God class. A developer looking for "how do I trace ancestors?" has to know to
look in `storage/provenance_utils.py`, `execution/inputs/lineage_matching.py`,
AND `storage/core/artifact_store.py`.

See `_dev/analysis/lineage-tracing-organization.md` for the full inventory,
problem analysis, and paradigm evaluation.

---

## Context

### Where traversal logic lives today

| File | Functions | Paradigm | Used by |
|---|---|---|---|
| `storage/provenance_utils.py` | `trace_derived_artifacts` (forward BFS) | dict | **Nobody** (dead code) |
| `execution/inputs/lineage_matching.py` | `walk_provenance_to_targets` (backward) | DataFrame | `Filter` |
| `execution/inputs/lineage_matching.py` | `walk_forward_to_targets` (forward) | DataFrame | `Filter`, `InteractiveFilter` |
| `execution/inputs/lineage_matching.py` | `match_by_ancestry` + helpers | dict | `grouping.py` |
| `execution/executors/chain.py` | `update_ancestor_map` | edge-list | chain executor |
| `storage/core/artifact_store.py` | 6+ provenance query methods | Delta Lake | everywhere |

### Forward vs backward asymmetry

| Capability | Forward | Backward |
|---|---|---|
| 1-hop IDs | `get_descendant_artifact_ids` | `get_ancestor_artifact_ids` |
| 1-hop hydrated | `get_associated` | **missing** |
| Multi-hop (dict) | `trace_derived_artifacts` (dead code) | **missing** |
| Multi-hop (DataFrame) | `walk_forward_to_targets` | `walk_provenance_to_targets` |
| Multi-hop hydrated | **missing** | **missing** |

---

## Design Decisions

### Single paradigm: DataFrames only

The codebase has two traversal paradigms: dict-based BFS (`match_by_ancestry`)
and DataFrame-based walks (`walk_forward_to_targets`,
`walk_provenance_to_targets`). After evaluating every caller (see analysis
doc), we consolidate to DataFrames only.

The dict approach exists because `match_by_ancestry` was written that way, not
because DataFrames can't do the same operation. `walk_provenance_to_targets`
already does backward candidate-to-target matching — the same operation
`match_by_ancestry` performs with its two-phase dict BFS.

Why DataFrames win:

- **Less memory.** `load_provenance_edges_df(step_min, step_max)` loads only
  edges within the relevant step range. `load_provenance_map()` loads
  everything.
- **Faster at scale.** Polars joins are SIMD-optimized and parallel.
  Python-level `deque` BFS with dict lookups processes candidates one at a
  time.
- **Already the majority.** 3 of 4 callers (Filter forward, Filter backward,
  InteractiveFilter) use DataFrames. Only `match_by_ancestry` uses dicts.

The dict two-phase optimization (pre-computing a target ancestry index) is
clever but unnecessary for pipeline graph sizes (3-5 steps, hundreds to
thousands of artifacts) and the memory cost of loading the full provenance map
works against it.

For `ProvenanceStore` methods like `get_ancestors()`, the implementation may
internally use a dict BFS if it's simpler to write for single-artifact
queries — but that's a private implementation detail, not a paradigm that
leaks into the module structure or naming. Similarly, data-loading methods
like `load_backward_map` return dicts because that's the natural structure
for map lookups — "DataFrames only" applies to multi-hop traversal logic,
not to every return type in the provenance API.

### Package: top-level `provenance/`

Traversal logic gets its own top-level package rather than living inside
`storage/` or `execution/`. Rationale:

- It's not storage (no I/O in the traversal functions)
- It's not execution-specific (Filter ops and user queries both need it)
- It parallels the existing `schemas/provenance/` package which holds the
  data models

### Store extraction: `ProvenanceStore`

Provenance query methods move out of `ArtifactStore` into a dedicated
`ProvenanceStore` in `storage/core/`. This keeps `ArtifactStore` focused on
artifact CRUD and gives provenance queries a coherent home with symmetric
naming (`load_backward_map`/`load_forward_map` instead of
`load_provenance_map`/`load_forward_provenance_map`).

### Rewrite `match_by_ancestry` to use DataFrames

`match_by_ancestry` gets rewritten from dict-based two-phase BFS to
step-filtered DataFrame loading + `walk_backward`. This:

- Eliminates the only dict-based caller
- Reduces memory by loading only the relevant step range
- Removes ~120 lines of dict BFS helpers (`_build_target_ancestry_index`,
  `_find_target`)
- Aligns grouping with the same traversal path as Filter

### What stays where it is

- **`match_by_ancestry`** stays in `execution/inputs/lineage_matching.py` —
  it's input-pairing domain logic, not generic traversal. Gets rewritten
  internally to use `provenance.traversal.walk_backward`.
- **`update_ancestor_map`** stays in `chain.py` — chain-execution bookkeeping
  that builds edges during execution.
- **`Filter._discover_*`** stays in the Filter op — operation-specific
  orchestration calling `provenance.traversal` directly.

---

## Proposed Structure

```
src/artisan/
├── provenance/                        # NEW top-level package
│   ├── __init__.py                    # Re-exports walk_forward, walk_backward
│   └── traversal.py                   # walk_forward, walk_backward
│
├── storage/
│   ├── core/
│   │   ├── artifact_store.py          # SHRINKS: artifact CRUD only
│   │   └── provenance_store.py        # NEW: primary API for provenance queries
│   └── provenance_utils.py            # DELETE (dead code)
│
├── execution/
│   ├── inputs/
│   │   └── lineage_matching.py        # SHRINKS: match_by_ancestry only (rewritten)
│   └── executors/
│       └── chain.py                   # unchanged
```

### `provenance/traversal.py`

One module, two functions, symmetric naming.

```python
def walk_forward(
    sources: pl.DataFrame,
    edges: pl.DataFrame,
    *,
    target_type: str | None = None,
) -> pl.DataFrame:
    """Walk forward from sources through provenance edges.

    Walks forward from all sources simultaneously. Each hop joins the
    frontier against edges to step forward. All-match semantics — one
    source can reach multiple targets.

    Args:
        sources: DataFrame with ``artifact_id`` column.
        edges: DataFrame with ``source_artifact_id``,
            ``target_artifact_id``, and optionally
            ``target_artifact_type``.
        target_type: If set, only collect nodes of this type.

    Returns:
        DataFrame with columns [source_id, target_id].
    """

def walk_backward(
    candidates: pl.DataFrame,
    targets: pl.DataFrame,
    edges: pl.DataFrame,
) -> pl.DataFrame:
    """Walk backward from candidates through provenance edges.

    Walks backward from all candidates simultaneously. Each hop joins
    the frontier against edges to step backward, checking for target
    matches. First-match semantics per candidate.

    Args:
        candidates: DataFrame with ``artifact_id`` column.
        targets: DataFrame with ``artifact_id`` column.
        edges: DataFrame with ``source_artifact_id``,
            ``target_artifact_id``.

    Returns:
        DataFrame with columns [candidate_id, target_id].
    """
```

### `storage/core/provenance_store.py`

Delta Lake I/O extracted from `ArtifactStore`. Primary API for provenance
queries — callers say what they want, the store handles loading, traversal,
and hydration.

```python
class ProvenanceStore:
    """Query provenance edges and artifact metadata from Delta Lake."""

    def __init__(self, base_path: Path | str): ...

    # --- Phase 3: extracted from ArtifactStore (renamed) ---

    # Map loading (symmetric naming)
    def load_backward_map(self) -> dict[str, list[str]]: ...     # was load_provenance_map
    def load_forward_map(self) -> dict[str, list[str]]: ...      # was load_forward_provenance_map
    def load_type_map(self, artifact_ids=None) -> dict[str, str]: ...   # was load_artifact_type_map
    def load_step_map(self, artifact_ids=None) -> dict[str, int]: ...   # was load_step_number_map

    # Direct queries (1-hop)
    def get_direct_ancestors(self, artifact_id) -> list[str]: ...       # was get_ancestor_artifact_ids
    def get_direct_descendants(self, source_ids, type=None) -> dict[str, list[str]]: ...  # was get_descendant_artifact_ids

    # Extracted as-is
    def get_associated(self, artifact_ids, associated_type) -> dict[str, list[Artifact]]: ...
    def get_descendant_ids_df(self, source_ids, target_type=None) -> pl.DataFrame: ...
    def get_artifact_step_number(self, artifact_id) -> int | None: ...
    def get_step_range(self, artifact_ids) -> tuple[int, int] | None: ...
    def load_artifact_ids_by_type(self, step_number, artifact_type) -> list[str]: ...
    def load_step_name_map(self, pipeline_run_id=None) -> dict[int, str]: ...

    # Edge loading (for callers that need DataFrames directly)
    def load_edges_df(self, step_min, step_max, ...) -> pl.DataFrame: ...  # was load_provenance_edges_df

    # --- Phase 4: new functionality ---

    def get_ancestors(self, artifact_id, ancestor_type=None) -> list[Artifact]: ...
    def get_descendants(self, artifact_id, descendant_type=None) -> list[Artifact]: ...
```

### `ArtifactStore` method disposition

Every current `ArtifactStore` method and where it goes after Phase 3:

| Current method | Destination | Notes |
|---|---|---|
| `get_artifact` | stays | Artifact CRUD |
| `get_artifacts_by_type` | stays | Artifact CRUD |
| `artifact_exists` | stays | Artifact CRUD |
| `get_artifact_type` | stays | Artifact CRUD |
| `load_metrics_df` | stays | Artifact CRUD |
| `prepare_artifact_index_entry` | stays | Write preparation |
| `get_ancestor_artifact_ids` | `ProvenanceStore.get_direct_ancestors` | Renamed |
| `get_descendant_artifact_ids` | `ProvenanceStore.get_direct_descendants` | Renamed |
| `get_descendant_ids_df` | `ProvenanceStore` | Moved as-is |
| `get_associated` | `ProvenanceStore` | Moved as-is |
| `load_provenance_map` | `ProvenanceStore.load_backward_map` | Renamed |
| `load_forward_provenance_map` | `ProvenanceStore.load_forward_map` | Renamed |
| `load_provenance_edges_df` | `ProvenanceStore.load_edges_df` | Renamed |
| `load_step_number_map` | `ProvenanceStore.load_step_map` | Renamed |
| `get_step_range` | `ProvenanceStore` | Moved as-is |
| `get_artifact_step_number` | `ProvenanceStore` | Moved as-is |
| `load_artifact_type_map` | `ProvenanceStore.load_type_map` | Renamed |
| `load_artifact_ids_by_type` | `ProvenanceStore` | Moved as-is |
| `load_step_name_map` | `ProvenanceStore` | Moved as-is |

---

## Scope

### In scope

- Create `provenance/` package with `traversal.py`
- Create `storage/core/provenance_store.py`
- Move DataFrame walk functions into `provenance/traversal.py`
- Rewrite `match_by_ancestry` to use DataFrame walks
- Delete dict BFS helpers (`_build_target_ancestry_index`, `_find_target`)
- Delete `storage/provenance_utils.py` and its test file
  `tests/artisan/storage/test_provenance_utils.py`
- Update docs that reference `trace_derived_artifacts`:
  `docs/how-to-guides/inspecting-provenance.md` and
  `docs/tutorials/working-with-results/01-provenance-graphs.ipynb`
- Extract provenance methods from `ArtifactStore` into `ProvenanceStore`
- Update all imports
- Add `ProvenanceStore.get_ancestors()` and `get_descendants()` as thin
  wrappers over `walk_backward`/`walk_forward` (Phase 4)

### Out of scope

- Changing the Delta Lake table schema
- Changing the provenance edge data model in `schemas/provenance/`
- Visualization changes

---

## Migration

Pure refactor, each phase independently shippable.

**Phase 1 — Create `provenance/traversal.py`.** Move
`walk_forward_to_targets` → `walk_forward` and `walk_provenance_to_targets` →
`walk_backward`. Update imports in Filter and InteractiveFilter. Delete
`storage/provenance_utils.py`, its test file
(`tests/artisan/storage/test_provenance_utils.py`), and update docs references
in `docs/how-to-guides/inspecting-provenance.md` and
`docs/tutorials/working-with-results/01-provenance-graphs.ipynb`.

**Phase 2 — Rewrite `match_by_ancestry`.** Replace dict-based two-phase BFS
with step-filtered DataFrame loading + `walk_backward`. Remove
`_build_target_ancestry_index` and `_find_target`. Both callers in
`grouping.py` are affected: `_match_by_ancestry` (line 217) and
`_match_primary_by_ancestry` (line 277) — both call `match_by_ancestry` and
will use the rewritten version. `lineage_matching.py` shrinks to only the
domain-level `match_by_ancestry` function; `grouping.py` stops calling
`load_provenance_map` and instead passes step-filtered edges.

**Phase 3 — Extract `ProvenanceStore`.** Move provenance methods from
`ArtifactStore` into `provenance_store.py`. Rename to symmetric names.
`ArtifactStore` keeps artifact CRUD only.

**Phase 4 — Ancestor tracing.** Add `ProvenanceStore.get_ancestors()` and
`ProvenanceStore.get_descendants()` using `walk_backward`/`walk_forward`.
This is the only phase that adds new functionality.

---

## Testing

Phases 1-3 validate with existing tests — no behavior changes, only import
paths and internal implementation. Run full test suite after each phase:

```bash
~/.pixi/bin/pixi run -e dev fmt
~/.pixi/bin/pixi run -e dev test
```

Phase 2 (rewriting `match_by_ancestry`) needs extra attention: the existing
integration tests for multi-input grouping must still pass with identical
output.

Phase 4 (ancestor tracing) adds new tests per the ancestor-tracing design doc.
