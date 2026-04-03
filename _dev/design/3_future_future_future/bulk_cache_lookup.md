# Design: Bulk Cache Lookup for Creator Dispatch

**Status:** Draft
**Date:** 2026-03-02

---

## Problem

The creator dispatch loop in `_execute_creator_step()` calls `cache_lookup()`
**once per ExecutionUnit** inside a serial Python loop. Each call performs a
full `pl.scan_delta()` of the executions table — parsing the Delta log, reading
Parquet metadata, and scanning for a single `execution_spec_id` match.

At 11M artifacts with `artifacts_per_unit=1` (the default), this is 11M
individual Delta scans. On a first run (all cache misses), each miss triggers a
**second** scan to distinguish "no previous execution" from "previous execution
failed." That is 22M Delta scans — observed as >20 minutes of wall time with
the pipeline still not dispatched.

Even with a reasonable `artifacts_per_unit=10000` (1,100 units), the loop
performs 1,100 independent scans. Each scan re-parses the same Delta log and
re-reads the same Parquet file metadata. This is redundant I/O that should be a
single scan.

---

## Current Flow

`step_executor.py:602-631` — the dispatch loop:

```python
for execution_unit_inputs, batch_group_ids in execution_unit_batches:
    # 1. Compute spec_id (Python hash per batch)
    spec_id = compute_execution_spec_id(
        operation_name=operation.name,
        inputs=execution_unit_inputs,
        params=merged_params,
        command_overrides=command_overrides,
    )

    # 2. Cache lookup (Delta scan per batch — THE BOTTLENECK)
    cache_result = check_cache_for_batch(spec_id, config.delta_root)

    if cache_result is not None:
        cached_count += ...
        cached_units += 1
        continue

    # 3. Create ExecutionUnit (Pydantic construction per batch)
    unit = ExecutionUnit(...)
    units_to_dispatch.append(unit)
```

`cache_lookup()` (`cache_lookup.py:106-128`) performs up to two Delta scans per
call:

```python
# Scan 1: successful execution?
result = (
    pl.scan_delta(str(records_path))
    .filter(pl.col("execution_spec_id") == execution_spec_id)
    .filter(pl.col("success") == True)
    .sort("timestamp_start", descending=True)
    .limit(1)
    .collect()
)

if result.is_empty():
    # Scan 2: any execution at all? (for error classification)
    any_exec = (
        pl.scan_delta(str(records_path))
        .filter(pl.col("execution_spec_id") == execution_spec_id)
        .limit(1)
        .collect()
    )
```

On a cache hit, it performs a **third** scan of the `execution_edges` table
(lines 138-142) to fetch input/output artifact associations.

### Key observation: CacheHit contents are unused

The caller at `step_executor.py:614-620` treats the result as a boolean:

```python
if cache_result is not None:
    cached_count += ...
    cached_units += 1
    continue  # CacheHit.inputs/outputs never read
```

The execution_edges scan is wasted work in this context. The dispatch loop only
needs to know **which spec_ids have a successful cached execution** — a set
membership check.

---

## Proposed Change

Split the dispatch loop into three phases: compute spec_ids, bulk cache lookup,
then create units for misses.

### Phase 1: Compute all spec_ids

Still a Python loop, but no I/O. This is O(N) hash computations. At 1,100
batches (10K per unit) this is negligible. At 11M batches (1 per unit) this is
~30-55 seconds — tolerable, and a natural follow-up optimization target.

```python
batch_specs: list[tuple[dict[str, list[str]], list[str] | None, str]] = []
for execution_unit_inputs, batch_group_ids in execution_unit_batches:
    spec_id = compute_execution_spec_id(
        operation_name=operation.name,
        inputs=execution_unit_inputs,
        params=merged_params,
        command_overrides=command_overrides,
    )
    batch_specs.append((execution_unit_inputs, batch_group_ids, spec_id))
```

### Phase 2: Bulk cache lookup — single Delta scan

One scan, one `is_in()` predicate, returns the set of cached spec_ids:

```python
all_spec_ids = [spec_id for _, _, spec_id in batch_specs]
cached_spec_ids = bulk_cache_lookup(executions_path, all_spec_ids)
```

### Phase 3: Create ExecutionUnits for cache misses only

```python
for execution_unit_inputs, batch_group_ids, spec_id in batch_specs:
    if spec_id in cached_spec_ids:
        cached_count += (
            sum(len(ids) for ids in execution_unit_inputs.values()) or 1
        )
        cached_units += 1
        continue

    unit = ExecutionUnit(
        operation=operation,
        inputs=execution_unit_inputs,
        execution_spec_id=spec_id,
        step_number=step_number,
        group_ids=batch_group_ids,
        user_overrides=user_overrides,
    )
    units_to_dispatch.append(unit)
```

---

## `bulk_cache_lookup`

```python
def bulk_cache_lookup(
    executions_path: Path | str,
    spec_ids: list[str],
) -> set[str]:
    """Return the set of spec_ids that have a successful cached execution.

    Single Delta scan with an is_in() predicate. Replaces N individual
    cache_lookup() calls with one bulk query.

    Args:
        executions_path: Path to the executions Delta table.
        spec_ids: All execution_spec_ids to check.

    Returns:
        Set of spec_ids that have at least one successful execution.
    """
    records_path = Path(executions_path)

    if not records_path.exists() or not spec_ids:
        return set()

    cached = (
        pl.scan_delta(str(records_path))
        .filter(pl.col("execution_spec_id").is_in(spec_ids))
        .filter(pl.col("success") == True)
        .select("execution_spec_id")
        .unique()
        .collect()
    )
    return set(cached["execution_spec_id"].to_list())
```

Properties:
- **One scan replaces N scans.** Polars builds a hash set from `spec_ids` in
  Rust, then makes a single pass through the executions Parquet files.
- **No execution_edges scan.** The dispatch loop only needs hit/miss — it does
  not use `CacheHit.inputs` or `CacheHit.outputs`.
- **No failed-execution classification.** The dispatch loop does not
  distinguish "no previous execution" from "failed execution." Both are cache
  misses that result in dispatching a new unit.
- **Predicate pushdown.** `is_in()` on a string column can be pushed down to
  the Parquet reader when the Delta table has row-group statistics (min/max on
  `execution_spec_id`). Even without pushdown, one full scan is vastly cheaper
  than N individual scans.

### Scaling

| `artifacts_per_unit` | Units (N) | Current: N scans | Proposed: 1 scan |
|---|---|---|---|
| 1 | 11M | >20 min | ~seconds (11M-element hash set, 1 table pass) |
| 1,000 | 11K | ~30-60 sec | <1 sec |
| 10,000 | 1,100 | ~5-10 sec | <1 sec |

The `is_in()` hash set construction for 11M strings is ~1-2 seconds. The table
scan depends on the size of the executions table, not the number of spec_ids
being checked.

---

## What Stays

- **`cache_lookup()`** — the per-unit function remains. It returns rich
  `CacheHit` / `CacheMiss` objects and is useful outside the dispatch loop
  (e.g., a future "explain cache" feature, or any caller that needs the cached
  execution's inputs/outputs).
- **`check_cache_for_batch()`** — the thin wrapper stays as well, for any
  single-unit cache check use cases.
- **`compute_execution_spec_id()`** — unchanged. The hash computation is the
  same; it is just called in a separate loop before the bulk lookup.
- **`ExecutionUnit`** — unchanged. Units are still constructed the same way,
  just only for cache misses.
- **Curator path** — `_execute_curator_step()` calls `check_cache_for_batch()`
  once (single unit for all inputs), so it is not affected.

---

## Scope

| # | File | Change |
|---|------|--------|
| 1 | `storage/cache/cache_lookup.py` | Add `bulk_cache_lookup()` alongside existing `cache_lookup()` |
| 2 | `orchestration/engine/step_executor.py` | Refactor `_execute_creator_step()` dispatch loop into three phases: compute spec_ids, bulk lookup, create units |

All paths prefixed with `src/artisan/`.

Tests:

| # | File | Change |
|---|------|--------|
| 3 | `tests/artisan/storage/cache/test_cache_lookup.py` | Add tests for `bulk_cache_lookup()`: empty table, all hits, all misses, partial hits, failed-only executions |
| 4 | `tests/artisan/orchestration/engine/test_step_executor.py` | Verify dispatch loop uses bulk lookup; verify cached_count/cached_units are correct with mixed hits/misses |

---

## Future Optimizations (Out of Scope)

These build on the same direction but are separate changes:

1. **Vectorize spec_id computation.** For `artifacts_per_unit=1` with a single
   input role, the spec_id payload is
   `f"{op_name}|{artifact_id}|{params}|{cmd}"` — a Polars string concat +
   hash. Would eliminate the Python loop in Phase 1. Requires either
   `map_elements` with xxhash or accepting a different hash function (cache
   compatibility tradeoff).

2. **Default `artifacts_per_unit` for lightweight ops.** The default of 1 is
   correct for heavyweight operations (AF3, MPNN) but creates unnecessary
   overhead for lightweight ones (MetricCalculator). Could be tuned per
   operation class or auto-detected.

3. **Step-level caching.** `compute_step_spec_id()` already exists in
   `hashing.py` — it computes a deterministic ID from upstream step specs
   rather than resolved artifact IDs. A step-level cache check before input
   resolution would skip the entire dispatch phase in O(1) for fully cached
   steps. This is orthogonal to bulk cache lookup (which optimizes the
   per-unit path when step-level caching misses).
