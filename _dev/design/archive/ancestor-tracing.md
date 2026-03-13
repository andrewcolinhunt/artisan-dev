# Design: Ancestor Tracing for Artifacts

**Date:** 2026-03-10 **Status:** Draft **Author:** Claude + ach94

---

## Problem

Given an artifact, there is no easy way to trace back through its full lineage
and retrieve all ancestor artifacts — or a subset filtered by type.

**Use case:** A user has a final structure artifact (e.g. an MPNN output) and
wants to display it alongside all of its ancestor structures in the structure
viewer. Today, this requires manual multi-hop traversal using low-level
primitives.

---

## Context

### What exists today

The provenance system stores directed edges (`source → target`) in a Delta Lake
table (`provenance/artifact_edges`). The `ArtifactStore` provides:

| Method | Direction | Depth | Returns |
|--------|-----------|-------|---------|
| `get_ancestor_artifact_ids(id)` | Backward | 1 hop | `list[str]` (IDs only) |
| `get_descendant_artifact_ids(ids, type)` | Forward | 1 hop | `dict[str, list[str]]` |
| `get_associated(ids, type)` | Forward | 1 hop | Hydrated artifacts |
| `load_provenance_map()` | Backward | Full graph | `dict[str, list[str]]` (raw map) |
| `load_forward_provenance_map()` | Forward | Full graph | `dict[str, list[str]]` (raw map) |

In `provenance_utils.py`:

| Function | Direction | Depth | Returns |
|----------|-----------|-------|---------|
| `trace_derived_artifacts(source, fwd_map, targets)` | Forward | Multi-hop BFS | `list[str]` (IDs matching target set) |

### The gap

- **No multi-hop backward traversal.** `get_ancestor_artifact_ids` only goes
  one hop. To get the full ancestor chain, callers must manually load the
  provenance map and write their own BFS.
- **No type-filtered ancestor query.** There's no way to say "give me all
  `structure` ancestors" without building custom traversal logic.
- **No hydrated ancestor retrieval.** Even after collecting ancestor IDs, the
  caller must separately bulk-load artifacts by type.

The forward direction has `trace_derived_artifacts` (multi-hop BFS) and
`get_associated` (1-hop hydrated), but the backward direction has no equivalent.

---

## Proposed Solution

Two additions, mirroring the existing forward-direction patterns:

### 1. `trace_ancestor_artifacts()` in `provenance_utils.py`

Pure BFS function walking backward through the provenance graph. Mirrors
`trace_derived_artifacts` but in the opposite direction, with optional type
filtering.

```python
def trace_ancestor_artifacts(
    artifact_id: str,
    backward_map: dict[str, list[str]],
    *,
    type_map: dict[str, str] | None = None,
    ancestor_type: str | None = None,
) -> list[str]:
    """Collect all ancestor IDs via BFS backward through provenance.

    Args:
        artifact_id: Starting artifact ID to trace backward from.
        backward_map: Adjacency list mapping each artifact ID to its
            direct parents. From ``ArtifactStore.load_provenance_map()``.
        type_map: Mapping of artifact ID to type string. Required when
            ``ancestor_type`` is set.
        ancestor_type: If given, only collect ancestors of this type.

    Returns:
        Ancestor artifact IDs in BFS visit order (nearest first).
        The starting artifact_id is never included.
    """
```

**Design notes:**

- Always traverses the full ancestor graph regardless of `ancestor_type` — the
  filter only controls which IDs are _collected_, not which nodes are _visited_.
  This ensures ancestors behind a non-matching intermediate node are still found.
- BFS order means nearest ancestors come first (useful for display ordering).
- Raises `ValueError` if `ancestor_type` is set but `type_map` is `None`.

### 2. `get_ancestors()` on `ArtifactStore`

High-level method that combines provenance traversal with hydration. Mirrors the
pattern of `get_associated()` (which does forward 1-hop + hydrate).

```python
def get_ancestors(
    self,
    artifact_id: str,
    ancestor_type: str | None = None,
) -> list[Artifact]:
    """Retrieve all ancestor artifacts via multi-hop backward traversal.

    Args:
        artifact_id: Starting artifact to trace backward from.
        ancestor_type: If given, only return ancestors of this type.

    Returns:
        Hydrated ancestor artifacts in BFS order (nearest first).
        Empty list if no ancestors exist.
    """
```

**Implementation:**

1. Load backward provenance map (`load_provenance_map()`)
2. If `ancestor_type` is set, load type map (`load_artifact_type_map()`)
3. Call `trace_ancestor_artifacts()` for BFS traversal
4. If `ancestor_type` is set, bulk-load with `get_artifacts_by_type()`
5. Otherwise, group by type and bulk-load each type separately
6. Return artifacts in BFS order (preserving the traversal ordering)

---

## Scope

### In scope

- `trace_ancestor_artifacts()` pure function
- `get_ancestors()` on `ArtifactStore`
- Unit tests for both

### Out of scope

- Depth-limiting (not needed for current use cases; graphs are shallow)
- DataFrame-native variant (can add later if performance requires it)
- Visualization integration (caller's responsibility — structure viewer already
  accepts a list of `StructureArtifact`)

---

## Testing

### `trace_ancestor_artifacts` (unit, no Delta Lake)

| Test | Graph | Expected |
|------|-------|----------|
| Direct parent | `A → B` | `trace(B)` → `[A]` |
| Multi-hop chain | `A → B → C` | `trace(C)` → `[B, A]` |
| Diamond | `A → C, B → C, R → A, R → B` | `trace(C)` → `[A, B, R]` (or `[B, A, R]`) |
| Type filter | `A(struct) → B(metric) → C(struct)` | `trace(C, type=struct)` → `[A]` |
| Type filter traverses through non-matching | `A(struct) → B(metric) → C(struct)` | Must still find `A` despite `B` not matching |
| Root node | `A` (no parents) | `trace(A)` → `[]` |
| Cycle handling | `A → B → A` | No infinite loop |
| Missing type_map with ancestor_type | — | Raises `ValueError` |

### `get_ancestors` (integration, Delta Lake fixtures)

| Test | Setup | Expected |
|------|-------|----------|
| Full chain hydrated | `A → B → C` | `get_ancestors(C)` → `[B, A]` hydrated |
| Type-filtered | `struct → metric → struct` | `get_ancestors(last, "structure")` → only structures |
| No ancestors | Root artifact | `[]` |
| Empty provenance table | No edges | `[]` |

---

## Alternatives Considered

### Load edges lazily instead of full provenance map

Could do iterative single-hop queries instead of loading the full backward map.
Rejected because:

- The full map is already used elsewhere (`load_provenance_map()` exists)
- Iterative Delta scans would be slower for multi-hop traversal
- Provenance graphs are small relative to memory

### Add `depth` parameter

Could limit traversal depth. Deferred because current pipelines are 3-5 steps
deep and the full ancestor set is always small. Easy to add later if needed.
