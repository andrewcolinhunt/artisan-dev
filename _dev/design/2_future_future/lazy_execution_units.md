# Design: Lazy Execution Units

**Date:** 2026-03-03  **Status:** Draft  **Author:** Claude + ach94

---

## Overview

Execution units currently carry artifact IDs by value — `dict[str, list[str]]`
with every 32-char hex ID materialized as a Python string. At 10M+ artifacts
this creates multi-GB memory pressure at the orchestrator during dispatch, even
though the IDs originated from Delta Lake and will be read back from Delta Lake
on the worker side.

This document proposes making execution units carry **query descriptors** instead
of materialized IDs, so that artifact IDs are resolved only where and when they
are needed. It covers both creator and curator paths, identifies which parts of
the dispatch pipeline can be made lazy and which fundamentally cannot, and lays
out an incremental adoption path.

**Companion docs:**

- `design_curator_input_scalability.md` — detailed materialization chain for
  curators (12 materialization points, memory analysis)
- `design_filter_chunked_dispatch.md` — chunked subprocess dispatch for Filter
- `design_bulk_cache_lookup.md` — batching N cache lookups into one scan
- `design_data_plane_separation.md` — the broader "stay columnar inside the
  framework" principle

---

## The problem

### Current flow (both creator and curator)

```
Delta Lake (on disk, columnar, compressed)
│
│  resolve_output_reference()
│  ├─ pl.scan_delta("execution_edges")       ← lazy, ~0 bytes
│  ├─ .filter(...).select("artifact_id")     ← lazy
│  ├─ .collect()                             ← Arrow DataFrame (~400 MB at 10M)
│  ├─ ["artifact_id"].to_list()              ← Python list[str] (~800 MB)
│  └─ sorted(set(...))                       ← set (~640 MB) + sorted list (~800 MB)
│                                               Peak: ~2.2 GB, settles to ~800 MB
│
│  group_inputs()  [if group_by is set]
│  ├─ ZIP: list[dict] intermediate           ← ~2.4 GB at 10M
│  ├─ CROSS_PRODUCT: cartesian expansion     ← O(N*M), catastrophic at scale
│  └─ LINEAGE: loads provenance map + BFS    ← unbounded
│
│  generate_execution_unit_batches()
│  ├─ eagerly builds ALL batches             ← list of N tuples
│  └─ Python list slicing copies each chunk  ← ~3.7 GB at 10M with per-artifact batches
│
│  for each batch:
│    compute_execution_spec_id()             ← set + sort + join per batch
│    cache_lookup()                          ← one Delta scan per batch
│    ExecutionUnit(inputs=dict[str,list[str]])← validates all IDs
│
│  Creator: _save_units() pickle all         ← ~500 MB+ byte blob
│  Curator: pickle to subprocess             ← ~930 MB byte blob at 22.2M
```

### Memory at 10M artifacts (creator, no group_by, artifacts_per_unit=100)

| Phase | Peak | Settles to |
|-------|------|------------|
| `resolve_inputs` | ~2.2 GB | ~800 MB |
| `generate_execution_unit_batches` (100K batches) | ~1.6 GB | ~1.6 GB |
| `compute_spec_id` × 100K | ~40 KB each | negligible |
| `cache_lookup` × 100K | negligible memory | **20+ min I/O** |
| `_save_units` pickle | ~1.3 GB peak | 0 (freed) |
| **Orchestrator total peak** | **~4–5 GB** | |

With `artifacts_per_unit=1` (the default for heavyweight ops like AF3), the
batch count equals the artifact count. 10M single-artifact batches produce
~3.7 GB of pure container overhead (10M list objects + 10M dict wrappers +
10M tuple wrappers).

### Memory at 22.2M artifacts (curator, Filter step)

| Phase | Peak |
|-------|------|
| `resolve_inputs` | ~2.2 GB |
| Pickle to subprocess | ~930 MB |
| Unpickle in child | ~1.4 GB |
| Python list → Arrow | ~710 MB (coexists with list) |
| Operation logic (matching) | ~10–15 GB |
| `build_execution_edges` | ~710 MB |
| jemalloc inflation | ~60 GB RSS observed |

See `design_curator_input_scalability.md` for the full 12-point materialization
chain.

---

## The lazy reference concept

### `InputRef`: a query descriptor

Instead of carrying artifact IDs by value, an `ExecutionUnit` carries a
descriptor that can reconstruct the Delta Lake query:

```python
@dataclass(frozen=True)
class InputRef:
    """Lazy reference to artifacts in Delta Lake.

    Carries the query parameters needed to resolve artifact IDs from the
    execution_edges table. Serializes to ~200 bytes instead of ~800 MB.
    """

    delta_root: str
    source_step: int
    role: str
    execution_run_ids: tuple[str, ...]  # typically 1–100 IDs

    def scan(self) -> pl.LazyFrame:
        """Reconstruct the lazy scan — zero materialization."""
        edges_path = Path(self.delta_root) / "provenance" / "execution_edges"
        return (
            pl.scan_delta(str(edges_path))
            .filter(pl.col("execution_run_id").is_in(self.execution_run_ids))
            .filter(pl.col("direction") == "output")
            .filter(pl.col("role") == self.role)
            .select("artifact_id")
            .unique()
            .sort("artifact_id")
        )

    def count(self) -> int:
        """Count without materializing IDs."""
        return self.scan().select(pl.len()).collect().item()

    def collect_ids(self) -> list[str]:
        """Materialize to Python list (the escape hatch)."""
        return self.scan().collect()["artifact_id"].to_list()

    def collect_series(self) -> pl.Series:
        """Materialize to Arrow Series (columnar, ~2.5x cheaper than list)."""
        return self.scan().collect()["artifact_id"]
```

### Relationship to `OutputReference`

`OutputReference` is the user-facing lazy pointer: `(source_step, role,
artifact_type)`. It is resolved at the boundary between pipeline wiring and
dispatch.

`InputRef` is the executor-facing lazy pointer. It adds the information needed
to reconstruct the query without re-scanning the executions table:
`execution_run_ids` (which successful runs produced these outputs) and
`delta_root` (where to find the tables).

```
Pipeline wiring:   OutputReference(step=2, role="structure")
                         │
                         │  resolve: query executions table for successful runs
                         ▼
Dispatch:          InputRef(step=2, role="structure",
                           execution_run_ids=("abc...", "def..."),
                           delta_root="/path/to/delta")
                         │
                         │  worker resolves: query execution_edges
                         ▼
Worker:            list[str] of artifact IDs (materialized on demand)
```

The resolution from `OutputReference` to `InputRef` is cheap — it queries the
executions table for a handful of `execution_run_id` values (typically 1–100
rows, not millions). The expensive step (collecting millions of artifact IDs) is
deferred to the worker.

---

## What can be lazy, what cannot

### The constraint matrix

| Dispatch phase | Needs concrete IDs? | Why | Can be lazy? |
|----------------|---------------------|-----|--------------|
| `resolve_inputs` | No | Just constructs the query | **Yes** — return `InputRef` |
| `group_inputs` (ZIP) | Yes | Positional alignment needs lengths + IDs | No |
| `group_inputs` (CROSS_PRODUCT) | Yes | Cartesian expansion | No |
| `group_inputs` (LINEAGE) | Yes | Provenance graph traversal | No |
| `generate_execution_unit_batches` | Partially | Needs total count for batch boundaries | **Yes** — use count + offset/limit or hash partitioning |
| `compute_execution_spec_id` | Yes (current) | Hashes sorted IDs | **Can be redesigned** (see below) |
| `cache_lookup` | No (needs spec_id) | Looks up by spec_id, not by artifact IDs | **Yes** — if spec_id doesn't require IDs |
| `ExecutionUnit` validation | Yes (current) | Checks 32-char hex format | **Can be deferred** — Delta schema enforces this |
| `_save_units` / pickle | N/A | Serializes whatever the unit contains | **Yes** — lazy refs are ~200 bytes |
| Worker `instantiate_inputs` | Yes | Hydrates artifacts from Delta Lake | Already queries Delta — natural resolution point |

**Key insight:** Framework pairing (`group_by`) is the only phase that
fundamentally requires concrete artifact IDs. Everything else can work with
query descriptors, counts, or redesigned spec-id computation.

### When group_by is not set (the common case at scale)

Most large-scale operations (Filter, Merge, single-input creators like AF3,
MPNN) don't use framework pairing. For these, the entire dispatch path can be
lazy:

```
OutputReference
  → InputRef (cheap: query executions table for run_ids)
  → count (cheap: single aggregation scan)
  → batch boundaries (arithmetic: offset/limit per batch)
  → LazyBatchRef per ExecutionUnit
  → worker resolves its slice from Delta Lake
```

### When group_by IS set

Pairing requires materializing IDs. But pairing is rare at 10M+ scale — it's
typically used for multi-role operations at 1K–100K scale. The eager path
remains for these cases. The optimization is to **not pay the lazy→eager
penalty for the common case**.

---

## Batching strategies for lazy references

The core challenge: how does the orchestrator assign non-overlapping,
deterministic batches without materializing all IDs?

### Option A: Offset/limit on deterministic sort

```python
@dataclass(frozen=True)
class LazyBatchRef(InputRef):
    offset: int
    limit: int

    def scan_batch(self) -> pl.LazyFrame:
        return self.scan().slice(self.offset, self.limit)
```

The `InputRef.scan()` produces a deterministic order (`unique().sort(
"artifact_id")`). Since `artifact_id` is a content hash (xxh3_128), the sort
order is stable as long as the underlying data doesn't change — which it
doesn't within a pipeline run (upstream steps have committed before downstream
steps dispatch).

**Pros:** Simple, intuitive, deterministic.

**Cons:** Polars `slice()` on a lazy scan may need to materialize the full
sorted result to skip to the offset. For late batches (high offset), this could
be expensive. Needs benchmarking.

### Option B: Hash-based partitioning

```python
@dataclass(frozen=True)
class LazyBatchRef(InputRef):
    partition_index: int
    n_partitions: int

    def scan_batch(self) -> pl.LazyFrame:
        return self.scan().filter(
            pl.col("artifact_id").hash(seed=0) % self.n_partitions
            == self.partition_index
        )
```

Each worker independently scans the full ID set but filters to its partition.
The hash function distributes IDs uniformly without requiring a global sort.

**Pros:**

- Each partition is independent — no offset dependency, trivially parallel.
- Deterministic without global ordering.
- Predicate can be pushed down (Polars evaluates the hash in Rust on the Arrow
  buffer, never creating Python strings).
- Works with any Delta table layout — no Z-ORDER dependency.

**Cons:**

- Each worker scans the full column (filtered, but still reads all Parquet
  files). At 10M IDs this is ~400 MB of I/O per worker (though columnar reads
  are fast and cached by the OS page cache after the first worker).
- Partition sizes aren't exactly equal (hash distribution variance). For
  `artifacts_per_unit=100` and 10M artifacts, variance is negligible (~0.1%).
- Partition count must be known at dispatch time — need a count scan first.

### Option C: Materialize as Arrow, slice in parent

```python
# Parent: one materialization, stays in Arrow format
all_ids: pl.Series = input_ref.collect_series()  # ~400 MB Arrow, not ~800 MB Python

# Slice to batches (Arrow slicing is zero-copy)
for i in range(0, len(all_ids), batch_size):
    batch_ids = all_ids.slice(i, batch_size).to_list()  # small Python list per batch
    unit = ExecutionUnit(inputs={role: batch_ids}, ...)
```

**Pros:**

- Simple, compatible with current `ExecutionUnit` interface.
- Arrow slicing is zero-copy — the parent holds ~400 MB total, not ~800 MB.
- Each batch converts only its slice to Python strings (~8 KB at batch_size=100).

**Cons:**

- Parent still holds ~400 MB (vs. ~0 for fully lazy). Fine for the orchestrator
  head node, but not "fully lazy."
- Still requires all IDs to pass through the parent process.

### Recommendation

**Option C for near-term** (Level 2 in the scalability doc's terminology):
simple, low-risk, halves parent memory. Combine with the bulk cache lookup to
eliminate the per-batch I/O bottleneck.

**Option B for long-term** (Level 3): fully lazy, each worker resolves its own
partition. Requires the spec-id redesign below.

Option A has the offset-scan performance concern and doesn't offer advantages
over B.

---

## Redesigning execution spec IDs for lazy inputs

### The current approach

```python
def compute_execution_spec_id(operation_name, inputs, params, command_overrides):
    all_ids = set()
    for role_ids in inputs.values():
        all_ids.update(role_ids)
    sorted_ids = ",".join(sorted(all_ids))
    hash_input = f"{operation_name}|{sorted_ids}|{params_json}|{command_json}"
    return xxhash.xxh3_128(hash_input.encode()).hexdigest()
```

This requires materializing all artifact IDs in the batch to compute a
deterministic hash. At `artifacts_per_unit=100`, the per-batch cost is ~40 KB —
fine. But it forces IDs to exist as Python strings in the parent process.

### The lazy alternative: query-derived spec IDs

The key observation: **the same query always produces the same IDs.** If the
query descriptor is deterministic, hashing the descriptor produces a
deterministic spec ID without materializing the IDs.

```python
def compute_lazy_spec_id(
    operation_name: str,
    input_refs: dict[str, LazyBatchRef],
    params: dict,
    command_overrides: dict | None,
) -> str:
    """Compute spec ID from query descriptors instead of artifact IDs.

    Deterministic because: same source_step + role + execution_run_ids +
    partition parameters always resolves to the same artifact IDs.
    """
    ref_parts = []
    for role, ref in sorted(input_refs.items()):
        ref_parts.append(
            f"{role}:{ref.source_step}:{ref.role}"
            f":{','.join(sorted(ref.execution_run_ids))}"
            f":{ref.partition_index}:{ref.n_partitions}"
        )
    refs_str = "|".join(ref_parts)
    hash_input = f"{operation_name}|{refs_str}|{params_json}|{command_json}"
    return xxhash.xxh3_128(hash_input.encode()).hexdigest()
```

**Semantic equivalence:** Two execution units with the same query descriptor
will resolve to the same artifact IDs, so they represent the same computation.
The spec ID is a hash of "what computation to perform," not "which specific
bytes are the inputs." The query descriptor captures the former.

**Caveat — cache compatibility:** Lazy spec IDs are NOT the same strings as
eager spec IDs for the same artifacts. This means:

- A pipeline run using lazy dispatch will not find cache hits from a previous
  run that used eager dispatch (and vice versa).
- Within a consistent dispatch mode, caching works correctly.
- This is acceptable: the transition is one-directional (once lazy, always
  lazy), and a one-time cache miss on the transition run is a minor cost.

### Curator path: already solved

The pipeline-manager path already provides `step_spec_id` for curators, which
is derived from the pipeline DAG structure (not artifact IDs). This skips
`compute_execution_spec_id` entirely.

---

## Impact on `ExecutionUnit`

### Current contract

```python
class ExecutionUnit(BaseModel):
    operation: OperationDefinition
    inputs: dict[str, list[str]]  # role → concrete artifact IDs
    execution_spec_id: str
    step_number: int
    group_ids: list[str] | None
    user_overrides: dict[str, Any] | None
```

The docstring states: "The orchestrator guarantees: all inputs are concrete
artifact IDs. All referenced artifacts exist in Delta Lake."

### Options for lazy inputs

**Option 1: Union type**

```python
inputs: dict[str, list[str] | LazyBatchRef]
```

Minimal change. Workers check `isinstance` and resolve lazy refs before use.
Messy — every consumer needs the branch.

**Option 2: Separate model**

```python
class LazyExecutionUnit(BaseModel):
    operation: OperationDefinition
    input_refs: dict[str, LazyBatchRef]
    execution_spec_id: str
    step_number: int
    # no group_ids — lazy units don't support framework pairing
```

Clean separation. The executor dispatches on unit type. No impact on existing
`ExecutionUnit` consumers.

**Option 3: Resolution at the boundary**

Keep `ExecutionUnit` unchanged. The dispatch layer constructs `LazyBatchRef`
objects internally, but resolves them to `list[str]` just before creating each
`ExecutionUnit`. The unit itself always carries concrete IDs, but the parent
process never holds ALL IDs at once — only one batch at a time.

```python
# Parent holds ~0 IDs in memory at any given time
for partition_idx in range(n_partitions):
    ref = LazyBatchRef(..., partition_index=partition_idx, ...)
    batch_ids = ref.collect_ids()  # small: one batch worth
    unit = ExecutionUnit(inputs={role: batch_ids}, ...)
    # process unit (cache check, save, etc.)
    del batch_ids  # freed immediately
```

**Recommendation:** Option 3 for the near-term (no interface changes, works
with existing workers), Option 2 for the long-term (cleanest architecture,
enables workers to resolve their own refs).

---

## Incremental adoption path

### Phase 1: Low-hanging fruit (no interface changes)

**Target:** Halve orchestrator memory, eliminate the cache I/O bottleneck.

Changes:

1. **`resolve_output_reference` returns Arrow Series, not Python list.**
   Replace `.to_list()` + `sorted(set(...))` with `.unique().sort().collect()`
   returning a `pl.Series`. Keep the Python list conversion at the point of
   use (batch slicing). Saves ~400 MB per 10M artifacts.

2. **`generate_execution_unit_batches` becomes a generator.**
   `yield` instead of `append`. The caller already iterates one-at-a-time. No
   API change needed. Eliminates ~3.7 GB of container overhead at worst case.

3. **Bulk cache lookup.**
   Compute all spec IDs first, then one `is_in()` scan. See
   `design_bulk_cache_lookup.md`. Eliminates 100K+ individual Delta scans.

4. **Skip `CacheHit` provenance query in dispatch context.**
   The caller only checks hit/miss — the edge data is unused. Add
   `load_edges=False` to `cache_lookup()`. Eliminates one wasted Delta scan
   per cache hit.

**Estimated impact:** Orchestrator peak memory drops from ~4–5 GB to ~1.5–2 GB
at 10M scale. Cache lookup drops from 20+ minutes to seconds.

### Phase 2: Lazy dispatch for unpaired operations

**Target:** Near-zero orchestrator memory for the common case.

Changes:

1. **Introduce `InputRef` and `LazyBatchRef`** as first-class schema types.

2. **Split `_execute_creator_step` into paired and unpaired paths.**
   - Unpaired (no `group_by`): construct `InputRef`, get count, generate batch
     refs, compute lazy spec IDs, bulk cache check, create units with resolved
     batch slices (Option 3 — one batch at a time).
   - Paired: existing eager path (unchanged).

3. **Curator path: pass `InputRef` through to executor.**
   The curator executor already wraps IDs as `pl.DataFrame` — it can construct
   the DataFrame directly from `InputRef.scan()` without ever creating Python
   strings.

4. **Query-derived spec IDs** for the lazy path.

**Estimated impact:** Orchestrator holds ~0 artifact IDs for unpaired
operations. Curators skip the pickle serialization of millions of IDs entirely.

### Phase 3: Worker-resolved lazy units (future)

**Target:** Full end-to-end laziness — workers resolve their own partition from
Delta Lake.

Changes:

1. **Introduce `LazyExecutionUnit`** (Option 2) carrying `LazyBatchRef` instead
   of concrete IDs.

2. **Workers call `ref.scan_batch().collect()`** to resolve their partition.
   This moves the Delta Lake read from the parent to the worker, eliminating
   the pickle serialization boundary entirely for artifact IDs.

3. **Hash-based partitioning** (Option B) for deterministic, independent
   partition resolution.

**Estimated impact:** Zero artifact ID data crosses process boundaries. Each
worker reads only its partition from Delta Lake. Total I/O is the same (every
ID is read once), but memory is bounded per-worker.

---

## What this does NOT address

- **Operation logic memory** (Filter's provenance walk at ~10–15 GB) — addressed
  by `design_filter_chunked_dispatch.md` and the chunked filter design.
- **jemalloc fragmentation** — addressed by per-chunk subprocesses (subprocess
  isolation work).
- **Framework pairing at scale** — fundamentally requires concrete IDs. If
  pairing is needed at 10M+ scale, the eager path is unavoidable. In practice,
  pairing at that scale is rare — most 10M+ operations are single-input or
  curator ops.

---

## Interaction with existing work

| Work item | Interaction |
|-----------|-------------|
| Subprocess isolation (`feat/subprocess-isolation`) | Foundation for Phase 2 curator path — lazy refs replace the 930 MB pickle |
| Filter chunked dispatch | Phase 2 enables it naturally — each chunk is a `LazyBatchRef` partition |
| Bulk cache lookup | Phase 1 prerequisite — lazy dispatch depends on batched cache checks |
| Data plane separation | Philosophical alignment — "stay columnar inside the framework" |
| Backend abstraction | Compatible — `_save_units` / `_load_units` work with any unit type |

---

## Summary

| Dimension | Current | Phase 1 | Phase 2 | Phase 3 |
|-----------|---------|---------|---------|---------|
| Parent memory (10M) | ~4–5 GB | ~1.5–2 GB | ~0 | ~0 |
| Cache lookup I/O | N scans (20+ min) | 1 scan (seconds) | 1 scan | 1 scan |
| Pickle size (curator, 22M) | ~930 MB | ~930 MB | ~200 bytes | N/A (no pickle) |
| Interface changes | — | None | `InputRef` type, split dispatch path | `LazyExecutionUnit` |
| Framework pairing | Eager | Eager | Eager (paired) / Lazy (unpaired) | Same |
| Risk | — | Very low | Medium | High |
