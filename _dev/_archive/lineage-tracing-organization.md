# Analysis: Lineage Tracing Organization

**Date:** 2026-03-10

---

## Summary

Provenance traversal logic is scattered across 4 files in 3 packages with
inconsistent naming, two paradigms (dict-based BFS and DataFrame-based walks),
and unclear boundaries. This analysis maps the current state, identifies the
problems, evaluates whether both paradigms are needed (they aren't), and
proposes a coherent single-paradigm structure.

---

## Current Inventory

### Where traversal logic lives today

| File | Functions | Paradigm | Used by |
|---|---|---|---|
| `storage/provenance_utils.py` | `trace_derived_artifacts` (forward BFS) | dict-based | **Nobody** (dead code) |
| `execution/inputs/lineage_matching.py` | `walk_provenance_to_targets` (backward walk) | DataFrame | `Filter` |
| `execution/inputs/lineage_matching.py` | `walk_forward_to_targets` (forward walk) | DataFrame | `Filter`, `InteractiveFilter` |
| `execution/inputs/lineage_matching.py` | `match_by_ancestry`, `_build_target_ancestry_index`, `_find_target` | dict-based BFS | `grouping.py` |
| `execution/executors/chain.py` | `update_ancestor_map`, `_build_shortcut_edges` | edge-list | chain executor only |
| `storage/core/artifact_store.py` | `get_ancestor_artifact_ids`, `load_provenance_map`, `load_forward_provenance_map`, `get_descendant_artifact_ids`, `get_descendant_ids_df`, `get_associated`, `load_provenance_edges_df`, `load_step_number_map`, `get_step_range`, `load_artifact_type_map`, `load_artifact_ids_by_type` | Delta Lake queries | everywhere |

### Forward vs backward asymmetry

| Capability | Forward | Backward |
|---|---|---|
| 1-hop IDs | `get_descendant_artifact_ids` | `get_ancestor_artifact_ids` |
| 1-hop hydrated | `get_associated` | **missing** |
| Multi-hop BFS (dict) | `trace_derived_artifacts` (dead code) | **missing** |
| Multi-hop walk (DataFrame) | `walk_forward_to_targets` | `walk_provenance_to_targets` |
| Multi-hop hydrated | **missing** | **missing** |

---

## Problems

### Scattered locations

A developer looking for "how do I trace ancestors?" has to know to look in
`storage/provenance_utils.py`, `execution/inputs/lineage_matching.py`, AND
`ArtifactStore`. Nothing about the file paths suggests these are related.

### Naming incoherence

The same concepts have different names depending on where you look:

| Direction | Names used |
|---|---|
| Backward | "ancestor", "source", "parent", "provenance_to_targets" |
| Forward | "derived", "descendant", "target", "forward_to_targets", "associated" |

Methods on `ArtifactStore` use "ancestor"/"descendant". Functions in
`lineage_matching.py` use "to_targets"/"forward_to_targets". The function in
`provenance_utils.py` uses "derived". The provenance map loaders aren't
symmetric: `load_provenance_map` (backward) vs `load_forward_provenance_map`
(forward).

### `lineage_matching.py` conflates generic traversal with domain logic

This file contains:

- **Generic graph walks** (`walk_forward_to_targets`,
  `walk_provenance_to_targets`) — general-purpose forward/backward DataFrame
  traversal
- **Domain-specific matching** (`match_by_ancestry`,
  `_build_target_ancestry_index`, `_find_target`) — multi-input pairing logic
  specific to input grouping

These serve completely different consumers — Filter operations vs input
grouping — but they're tangled in one 370-line module. Adding more traversal
utilities here would make the conflation worse.

### `ArtifactStore` is a God class

735 lines mixing artifact CRUD, provenance map loading, descendant queries,
step number queries, metric loading, and write preparation. The
provenance-related methods alone account for ~250 lines. The class has no
internal organization besides a "Read operations" / "Write preparation"
comment split.

### Dead code

`trace_derived_artifacts` in `provenance_utils.py` is not imported anywhere in
the codebase. It's the only function in the file.

---

## Do We Need Both Paradigms?

There are two traversal implementations: dict-based BFS and DataFrame-based
hop-by-hop joins. Here's what each caller actually does, grounded in source
code.

### Dict-based callers

**`_match_by_ancestry` in `grouping.py:241`** — Pairs multi-input artifacts
from different pipeline branches. Example: step 1 generated structures, step 2
calculated metrics, and now each metric needs to be paired with the structure
it descended from.

How it works:

1. `artifact_store.load_provenance_map()` — loads ALL backward edges into a dict
2. Determines which role is "targets" (earlier step) vs "candidates" (later step)
3. Calls `match_by_ancestry` which does a **two-phase** BFS:
   - Phase 1: BFS backward from EACH target, building an index `{ancestor_id: target_id}`
   - Phase 2: BFS backward from EACH candidate, checking the index to find its target

This is a many-to-many matching operation — not single-artifact traversal.

**`_match_primary_by_ancestry` in `grouping.py:292`** — Same as above but
with an explicit primary/anchor role. Identical pattern: loads full provenance
map, calls `match_by_ancestry`.

### DataFrame-based callers

**`Filter._discover_descendant_metrics` in `filter.py:598`** — Given
passthrough artifacts (e.g. structures), find all descendant metrics. The
Filter needs to know which metrics belong to which structure to evaluate
criteria like "energy < -100".

How it works:

1. `artifact_store.get_step_range(passthrough_ids)` — find the relevant step range
2. `artifact_store.load_provenance_edges_df(step_min, step_max)` — loads edges FILTERED TO STEP RANGE as a DataFrame
3. `walk_forward_to_targets(sources, edges, target_type="metric")` — walks forward from ALL passthrough artifacts simultaneously via DataFrame joins

**`Filter._discover_step_metrics` in `filter.py:646`** — Given passthrough
artifacts and a specific step, find metrics from that step that are
lineage-related to each passthrough. The backward version of the above.

How it works:

1. Loads metric IDs from the target step
2. `artifact_store.load_provenance_edges_df(step_min, step_max)` — step-filtered edges
3. `walk_provenance_to_targets(candidates=metrics, targets=passthroughs, edges=edges)` — walks backward from ALL metrics simultaneously

**`InteractiveFilter.load` in `interactive_filter.py:150`** — Identical to
`Filter._discover_descendant_metrics`. Load step-range edges, walk forward,
collect metrics.

### What's actually different

| | Dict callers (grouping) | DataFrame callers (Filter) |
|---|---|---|
| **Edges loaded** | ALL edges (`load_provenance_map`) | Step-range filtered (`load_provenance_edges_df`) |
| **Starting points** | Many (all targets + all candidates) | Many (all passthrough artifacts) |
| **Algorithm** | Two-phase: build index, then query it | Linear: walk hop by hop |
| **Why this paradigm** | Two-phase index needs random-access BFS | Straightforward sweep; joins are natural |

An earlier draft of this analysis framed the difference as "single artifact
through a full graph" vs "batch of artifacts through filtered edges." That was
wrong:

- **Both paradigms operate on batches.** `match_by_ancestry` loops over many
  targets and many candidates. The DataFrame walkers process many starting
  points. Neither is single-artifact.
- **The loading difference is incidental.** The dict approach loads all edges
  because `match_by_ancestry` needs random-access BFS from arbitrary nodes.
  The DataFrame approach filters by step range because its callers know the
  relevant range. But a dict could be step-filtered too, and edges could be
  loaded unfiltered.

### Can `match_by_ancestry` just use DataFrames?

Yes. `walk_provenance_to_targets` does the same thing: walks backward from
candidates and matches them to targets. The two-phase dict algorithm is an
optimization (pre-computing a target ancestry index so shared ancestors are
only traversed once), but it's not a fundamentally different operation.

`_match_by_ancestry` in `grouping.py` could be rewritten as:

```python
def _match_by_ancestry(inputs, artifact_store):
    # ... existing role determination logic ...

    all_ids = pl.Series(inputs[role_a] + inputs[role_b])
    step_range = artifact_store.get_step_range(all_ids)
    edges = artifact_store.load_provenance_edges_df(*step_range)

    candidates_df = pl.DataFrame({"artifact_id": inputs[candidate_role]})
    targets_df = pl.DataFrame({"artifact_id": sorted(target_ids)})

    result = walk_provenance_to_targets(candidates_df, targets_df, edges)
    # reshape (candidate_id, target_id) pairs into matched_sets...
```

This would:

- Load only step-range-filtered edges (less memory than `load_provenance_map`
  which loads everything)
- Use Polars-optimized joins instead of Python-level BFS
- Eliminate the dict paradigm from the calling code

### Performance comparison

The two-phase dict optimization sounds good in theory, but:

- **Pipeline graphs are small.** 3-5 steps, hundreds to low thousands of
  artifacts. Both approaches are milliseconds.
- **The dict approach loads MORE data.** `load_provenance_map()` loads all
  edges from Delta Lake. `load_provenance_edges_df(step_min, step_max)` loads
  only the relevant step range.
- **Polars joins are heavily optimized.** SIMD, parallel execution, hash
  joins. Python-level `deque` BFS with dict lookups can't compete at scale.
- **The dict approach does N individual BFS traversals in Python.**
  `match_by_ancestry` loops over every candidate calling `_find_target` one
  at a time. The DataFrame approach processes all candidates in one pass per
  hop.

For the proposed single-artifact `get_ancestors()` feature: dict BFS is
slightly more natural to write, but it's a trivial operation on a small graph.
A DataFrame approach works fine too. Either way, it's an internal
implementation detail of `ProvenanceStore`, not a paradigm choice that leaks
to callers.

### Conclusion: consolidate to DataFrames

**We don't need two paradigms.** The DataFrame approach is:

- More memory-efficient (step-range filtering at load time)
- Faster at scale (Polars-optimized batch joins vs Python BFS loops)
- Already used by the majority of callers (Filter, InteractiveFilter)
- Capable of everything the dict approach does

The dict approach exists because `match_by_ancestry` was written that way, not
because DataFrames can't do it. The two-phase optimization is clever but
unnecessary for the graph sizes we work with, and the memory cost of loading
the full provenance map works against it.

For `ProvenanceStore` methods like `get_ancestors()`, the implementation may
internally use a dict BFS if it's cleaner to write — but that's a private
implementation detail, not a separate paradigm that leaks into naming, module
structure, or caller decisions.

---

## Proposed Structure

Single paradigm (DataFrame-based traversal), with `ProvenanceStore` as the
primary API.

```
src/artisan/
├── provenance/                        # NEW top-level package
│   ├── __init__.py                    # Re-exports public API
│   └── traversal.py                   # walk_forward, walk_backward (DataFrame-based)
│
├── storage/
│   ├── core/
│   │   ├── artifact_store.py          # SHRINKS: artifact CRUD only
│   │   └── provenance_store.py        # NEW: primary API for provenance queries
│   └── provenance_utils.py            # DELETE (dead code)
│
├── execution/
│   ├── inputs/
│   │   └── lineage_matching.py        # SHRINKS: only match_by_ancestry (rewired to DataFrames)
│   └── executors/
│       └── chain.py                   # unchanged (chain-specific bookkeeping)
```

### `provenance/traversal.py` — DataFrame-based traversal

One module, two functions, symmetric naming.

```python
def walk_forward(
    sources: pl.DataFrame,
    edges: pl.DataFrame,
    *,
    target_type: str | None = None,
) -> pl.DataFrame:
    """Walk forward from sources through provenance edges.

    All-match semantics — one source can reach multiple targets.

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

    First-match semantics per candidate.

    Args:
        candidates: DataFrame with ``artifact_id`` column.
        targets: DataFrame with ``artifact_id`` column.
        edges: DataFrame with ``source_artifact_id``,
            ``target_artifact_id``.

    Returns:
        DataFrame with columns [candidate_id, target_id].
    """
```

Absorbs `walk_forward_to_targets` and `walk_provenance_to_targets` from
`lineage_matching.py`.

### `ProvenanceStore` — Primary API

Callers say what they want; the store handles loading and traversal.

```python
class ProvenanceStore:
    """Query provenance edges and artifact metadata from Delta Lake."""

    # Map loading (symmetric naming)
    def load_backward_map(self) -> dict[str, list[str]]: ...
    def load_forward_map(self) -> dict[str, list[str]]: ...
    def load_type_map(self, artifact_ids=None) -> dict[str, str]: ...
    def load_step_map(self, artifact_ids=None) -> dict[str, int]: ...

    # Direct queries (1-hop)
    def get_direct_ancestors(self, artifact_id) -> list[str]: ...
    def get_direct_descendants(self, source_ids, type=None) -> dict[str, list[str]]: ...

    # High-level (I/O + traversal + hydration)
    def get_ancestors(self, artifact_id, ancestor_type=None) -> list[Artifact]: ...
    def get_descendants(self, artifact_id, descendant_type=None) -> list[Artifact]: ...
    def get_associated(self, artifact_ids, associated_type) -> dict[str, list[Artifact]]: ...

    # Edge loading (for callers that need DataFrames directly)
    def load_edges_df(self, step_min, step_max, ...) -> pl.DataFrame: ...
```

`ArtifactStore` shrinks to artifact CRUD: `get_artifact`,
`get_artifacts_by_type`, `artifact_exists`, `get_artifact_type`,
`load_metrics_df`, and write preparation.

### What stays where it is

- **`match_by_ancestry`** stays in `execution/inputs/lineage_matching.py` —
  it's input-pairing domain logic, not generic traversal. Gets **rewritten**
  to use `provenance.traversal.walk_backward` instead of dict-based BFS.
- **`update_ancestor_map`** stays in `chain.py` — chain-execution-specific
  bookkeeping that builds edges during execution, not a query utility.
- **`Filter._discover_*`** stays in the Filter op — operation-specific
  orchestration calling `provenance.traversal` directly.

---

## Migration Path

This can happen incrementally without breaking anything:

**Phase 1 — Create `provenance/traversal.py`.** Move `walk_forward_to_targets`
and `walk_provenance_to_targets` from `lineage_matching.py` into
`provenance/traversal.py` as `walk_forward` and `walk_backward`. Update
imports in Filter and InteractiveFilter. Delete `storage/provenance_utils.py`
(dead code).

**Phase 2 — Rewrite `match_by_ancestry`.** Replace the dict-based two-phase
BFS with step-filtered DataFrame loading + `walk_backward`. Remove
`_build_target_ancestry_index`, `_find_target`, and `load_provenance_map()`
calls from grouping. `lineage_matching.py` shrinks to only the domain-level
`match_by_ancestry` function.

**Phase 3 — Extract `ProvenanceStore`.** Move provenance methods out of
`ArtifactStore` into `storage/core/provenance_store.py`. Rename to symmetric
names. `ArtifactStore` keeps artifact CRUD only.

**Phase 4 — Ancestor tracing feature.** Add `ProvenanceStore.get_ancestors()`
and `ProvenanceStore.get_descendants()` using `walk_backward`/`walk_forward`
internally.

---

## Relationship to Design Docs

The **ancestor-tracing** design doc identifies a real gap (no multi-hop
backward traversal, no hydrated ancestor retrieval). Its proposed functionality
is correct. The placement should align with this analysis:

| Design doc proposes | This analysis recommends |
|---|---|
| `trace_ancestor_artifacts` in `provenance_utils.py` | `walk_backward` in `provenance/traversal.py` |
| `get_ancestors` on `ArtifactStore` | `get_ancestors` on `ProvenanceStore` |
