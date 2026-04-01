# Analysis: Artifact Sort Order at Dispatch

**Date:** 2026-03-24
**Context:** Exploring how users could control artifact ordering before
dispatch — e.g., sorting by a property like sequence length — and how this
integrates with the existing step execution pipeline.

---

## Current Behavior

After `resolve_inputs()`, artifact IDs are sorted **alphabetically by ID**
(`inputs.py:104`). This gives determinism but no semantic meaning. Batching
then slices in that order:

```
resolve_inputs → [group_by] → batch_and_cache → execute → commit
```

The alphabetical sort is arbitrary — artifact IDs are 32-character hex
strings with no relationship to content.

---

## Motivation

Control over artifact ordering before dispatch enables:

- **Load balancing** — group items of similar expected cost into the same
  batch so batches finish at similar times, reducing straggler effects
- **Priority** — process high-value or time-sensitive items first
- **Locality** — group similar items together for cache-friendly or
  algorithm-friendly processing
- **Progressive results** — e.g., process sequences shortest-to-longest so
  early results are available sooner

---

## Integration Point

The natural insertion point is a new `sort_artifacts` phase between input
resolution (including grouping) and batching:

```
resolve_inputs → [group_by] → sort_artifacts → batch_and_cache → execute
```

This is clean because:

- Sorting operates on the fully resolved, optionally paired artifact ID lists
- Batching simply slices whatever order it receives
- No changes needed downstream — `ExecutionUnit` doesn't care about ordering

---

## API Options

### Option A: Step-level parameter (recommended)

Specified per-step on `pipeline.run()` / `submit()`:

```python
pipeline.run(
    MyOp,
    inputs={"data": step1.output("data")},
    sort_by=SortBy(role="data", key="sequence_length"),
)

# With direction
sort_by=SortBy(role="data", key="sequence_length", descending=True)
```

**Pros:**

- Same operation can have different sort orders in different pipeline contexts
- Sorting is a dispatch concern, not an operation concern
- Consistent with how `resources`, `execution`, and `failure_policy` are
  already step-level overrides

**Cons:**

- One more parameter on `submit()`

### Option B: Operation-level config

Declared on the operation class:

```python
class MyOp(OperationDefinition):
    sort_by = SortConfig(key="sequence_length")
```

**Pros:**

- Encodes "this operation cares about input ordering" as part of its contract

**Cons:**

- Couples a dispatch concern to the operation definition
- Can't vary per-step without override mechanisms
- Operations shouldn't need to know how their inputs arrive

### Recommendation

**Option A.** Sorting is about how you dispatch work, not what work you do.
It belongs alongside the other dispatch-level knobs (`execution`,
`resources`, `failure_policy`).

---

## How the Sort Key Is Resolved

Sorting by an artifact property requires reading artifact data. Two
sub-options:

### Sub-option 1: Delta Lake field query (recommended)

Sort by a named field on the artifact's Delta Lake table:

```python
sort_by=SortBy(role="data", key="sequence_length")
```

Implementation queries the artifact table:

```python
artifact_ids = resolved_inputs[sort_spec.role]
sort_df = (
    pl.scan_delta(str(delta_root / artifact_table_path))
    .filter(pl.col("artifact_id").is_in(artifact_ids))
    .select("artifact_id", sort_spec.key)
    .collect()
)
# Argsort and reorder all roles + group_ids
```

**Pros:**

- Clean, declarative, serializable
- Works with the existing storage layer
- Sort key is part of the cache identity (reproducible)

**Cons:**

- Limited to fields that exist as columns in the artifact table
- Requires a Delta Lake read (adds latency proportional to table size)

### Sub-option 2: Callable sort key

```python
sort_by=SortBy(role="data", key=lambda artifact: len(artifact.sequence))
```

**Pros:**

- Arbitrary sort logic, maximum flexibility

**Cons:**

- Not serializable → breaks caching determinism
- Requires loading full artifact objects (expensive)
- Harder to reason about reproducibility

### Recommendation

**Sub-option 1** for the initial implementation. The Delta Lake query
approach fits the existing architecture. A callable could be added later if
field-based sorting proves insufficient.

---

## Interaction with Grouping

When `group_by` is active, artifacts are paired across roles before batching
(ZIP, CROSS_PRODUCT, or LINEAGE strategies). Sorting should happen **after**
grouping:

```
resolve_inputs → group_by → sort_artifacts → batch_and_cache
```

The sort key references one role. All other roles and `group_ids` follow
the same reordering — the pairing is preserved, just reordered.

If no `group_by` is active and there are multiple roles, the sort key
specifies which role drives the ordering. Other roles follow in lockstep
(they must be the same length per the `ExecutionUnit` contract).

---

## Interaction with Caching

The `execution_spec_id` is a deterministic hash of (operation name + inputs
+ params + config overrides). The inputs component includes the specific
artifact IDs.

Sorting changes **which artifacts land in which batch**. For example, with
artifacts `[A, B, C, D]` and `artifacts_per_unit=2`:

| Sort order     | Batch 1  | Batch 2  |
|----------------|----------|----------|
| Alphabetical   | [A, B]   | [C, D]   |
| By length desc | [C, A]   | [D, B]   |

Different batches → different `execution_spec_id` per batch → cache miss if
the sort order changes. This is correct behavior: a different dispatch
strategy should not hit caches from a previous strategy.

The sort specification should be included in the `execution_spec_id` hash
to make this explicit rather than relying on the side effect of different
batch composition.

**Step-level caching** (`step_spec_id`) hashes all inputs regardless of
batch boundaries. Changing sort order alone would not change the step-level
cache. This is also correct — the overall step produces the same outputs
regardless of dispatch order.

However: if step-level caching is a hit, sorting is irrelevant (the step
is skipped entirely). Sorting only matters for the batch-level cache path.

---

## Edge Cases

- **Single-artifact batches** (`artifacts_per_unit=1`): sorting controls
  execution order but not batch composition — each batch has one item
  regardless. Still useful for priority ordering if the backend preserves
  dispatch order.
- **Generative operations** (no inputs): sorting is a no-op. No validation
  error needed — just skip the phase.
- **Empty inputs**: sorting is a no-op (step is skipped anyway).
- **Sort key missing on some artifacts**: raise a clear error during the
  sort phase rather than silently dropping items. Could offer a
  `null_position="first"|"last"` option.
- **Non-numeric sort keys**: support any orderable type (string, int, float,
  datetime). Polars handles this natively.

---

## Implementation Sketch

### Schema

```python
@dataclass
class SortBy:
    """Artifact sort specification for dispatch ordering."""
    role: str                    # Which input role to sort by
    key: str                     # Field name on the artifact table
    descending: bool = False     # Sort direction
    null_position: str = "last"  # Where nulls go: "first" or "last"
```

### Sort function

```python
def sort_artifacts(
    inputs: dict[str, list[str]],
    group_ids: list[str] | None,
    sort_by: SortBy,
    delta_root: Path,
) -> tuple[dict[str, list[str]], list[str] | None]:
    """Reorder artifact ID lists by a sort key."""
    artifact_ids = inputs[sort_by.role]
    if not artifact_ids:
        return inputs, group_ids

    # Query sort key values from artifact table
    sort_df = _query_sort_key(artifact_ids, sort_by, delta_root)

    # Argsort
    ordered = sort_df.sort(
        sort_by.key,
        descending=sort_by.descending,
        nulls_last=(sort_by.null_position == "last"),
    )
    ordered_ids = ordered["artifact_id"].to_list()
    id_to_index = {aid: i for i, aid in enumerate(ordered_ids)}
    order = [id_to_index[aid] for aid in artifact_ids]

    # Apply ordering to all roles and group_ids
    sorted_inputs = {
        role: [ids[i] for i in order] for role, ids in inputs.items()
    }
    sorted_group_ids = (
        [group_ids[i] for i in order] if group_ids is not None else None
    )
    return sorted_inputs, sorted_group_ids
```

### Step executor integration

Inserted between grouping and batching in `_execute_curator_step` and
`_execute_creator_step`:

```python
# After group_by, before batch_and_cache
if sort_by is not None:
    with phase_timer("sort_artifacts", timings):
        paired_inputs, group_ids = sort_artifacts(
            paired_inputs, group_ids, sort_by, config.delta_root
        )
```

---

## Complexity Assessment

**Small-medium.** The core is a single function (~30 lines) plus:

- `SortBy` schema (trivial)
- Plumbing `sort_by` through `submit()` → `execute_step()` (mechanical)
- One Delta Lake query per sort (no new table, no new write path)
- Tests: happy path, multi-role, with grouping, null handling, edge cases

No architectural changes. No new storage. No changes to the backend
interface or `ExecutionUnit` contract.

---

## Open Questions

- **Multiple sort keys?** `SortBy` could accept a list of keys for
  tie-breaking. Not needed initially but the schema should be forward-
  compatible (easy: make `key` accept `str | list[str]`).
- **Sort across roles?** Current design sorts by one role. Sorting by a
  derived value across multiple roles (e.g., combined length of two inputs)
  is a different problem — punt until there's a use case.
- **Backend execution order guarantees?** Sorting controls batch
  composition, but whether batch 1 runs before batch 2 depends on the
  backend. Local ProcessPool and SLURM job arrays don't guarantee
  submission order = execution order. If execution-order priority matters,
  that's a separate concern (see streaming pipeline design).
