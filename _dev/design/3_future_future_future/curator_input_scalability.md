# Design: Curator Input Scalability — Problem Analysis

**Date:** 2026-03-03  **Status:** Reference  **Author:** Claude + ach94

---

## Overview

This document describes the structural scalability problems in the curator
execution path. It traces how artifact IDs flow from Delta Lake through the
orchestration layer to curator operations, identifies every materialization
point, and frames the design space for making the path lazier and more
memory-efficient.

This is a reference document for ongoing and future work, not a specific
implementation proposal.

---

## The materialization chain

When a curator step (Filter, Merge) executes, artifact IDs pass through nine
distinct materialization points. Each creates a new in-memory representation of
the same data.

### Annotated flow

```
Delta Lake (on disk, columnar, compressed)
│
│  resolve_output_reference (inputs.py)
│  ├─ pl.scan_delta("execution_edges")     ← lazy scan, ~0 bytes
│  ├─ .filter(...).select("artifact_id")   ← still lazy
│  ├─ .collect()                           ← MATERIALIZE #1: Arrow DataFrame
│  ├─ ["artifact_id"].to_list()            ← MATERIALIZE #2: Python list[str]
│  └─ sorted(set(...))                     ← MATERIALIZE #3: deduplicated sorted list
│
│  → dict[str, list[str]]  (~710 MB per 11.1M IDs, ~1.4 GB for 22.2M)
│
│  compute_execution_spec_id (hashing.py)  — skipped when step_spec_id provided
│  ├─ set().update(all role IDs)           ← MATERIALIZE #4: hash set of all IDs
│  ├─ sorted(set)                          ← MATERIALIZE #5: sorted list
│  └─ ",".join(sorted)                     ← MATERIALIZE #6: single giant string
│  → ~1.4 GB temporary (22.2M × 33 bytes) — freed after hash computed
│
│  ExecutionUnit (execution_unit.py)
│  ├─ inputs = dict[str, list[str]]        ← references existing lists (no copy)
│  └─ validator iterates all IDs           ← MATERIALIZE #7: touches every string
│                                             (validates len==32, isinstance str)
│
│  pool.submit(run_curator_flow, unit)     — pickle serialization
│  ├─ pickle.dumps(unit)                   ← MATERIALIZE #8: ~930 MB byte blob
│  └─ pipe.send(blob)                      ← sent to child process
│
│  ┌─ CHILD PROCESS ────────────────────────────────────────────────────┐
│  │                                                                    │
│  │  pickle.loads(blob)                   ← MATERIALIZE #9: 22.2M new  │
│  │                                          Python string objects     │
│  │                                                                    │
│  │  input_dfs = {role: pl.DataFrame(ids)}                             │
│  │  ├─ Python list → Arrow buffer        ← MATERIALIZE #10: Arrow     │
│  │  │                                       copy of all IDs           │
│  │  └─ Python list still alive           ← two copies coexist         │
│  │                                                                    │
│  │  execute_curator(input_dfs, ...)      — operation processes IDs    │
│  │                                                                    │
│  │  build_execution_edges(inputs, outputs)                            │
│  │  ├─ pl.DataFrame({"artifact_id": ids}) ← MATERIALIZE #11: another  │
│  │  │                                        Arrow copy per role      │
│  │  └─ pl.concat(parts)                  ← final execution edge DF     │
│  │                                                                    │
│  │  StagingResult.artifact_ids           ← MATERIALIZE #12: output    │
│  │  pickle back to parent                   IDs as Python list        │
│  │                                                                    │
│  └────────────────────────────────────────────────────────────────────┘
```

### Size at 22.2M artifacts (filter step)

| Materialization | Data | Size |
|-----------------|------|------|
| #1–3 resolve_inputs | 22.2M Python strings | ~1.4 GB |
| #4–6 compute_spec_id | set + sorted + joined string | ~1.4 GB (transient, skipped in pipeline path) |
| #7 ExecutionUnit validation | iteration only | ~0 (no copy) |
| #8 pickle serialize | byte blob | ~930 MB |
| #9 pickle deserialize (child) | 22.2M new strings | ~1.4 GB |
| #10 Python list → Arrow | Arrow buffers | ~710 MB (both copies alive) |
| #11 build_execution_edges | Arrow DataFrame | ~710 MB |
| **Child peak (before operation logic)** | | **~2.8 GB** |
| Operation logic (Phase 1 matching) | edges + BFS walk | **~10–15 GB** |
| **Total child peak** | | **~15–20 GB** |
| **Observed RSS (jemalloc inflation)** | | **~60 GB** |

The operation logic (matching) is the largest single cost center, but the
serialization overhead alone is ~2.8 GB before any operation code runs.

---

## Three independent cost centers

The memory problem has three independent causes. Fixing one doesn't solve the
others.

### 1. Serialization boundary (~2.8 GB)

The `ExecutionUnit` carries `dict[str, list[str]]` — 22.2M Python strings that
get pickled to the subprocess. Pickle produces a ~930 MB byte blob. The child
unpickles it into 22.2M fresh string objects. Then `run_curator_flow` converts
them to Arrow DataFrames, creating a third copy.

**Root cause:** IDs are eagerly materialized as Python strings and carried across
the process boundary by value.

### 2. Operation logic (~10–15 GB)

For the filter operation specifically: `_match_explicit` loads all provenance
edges via `load_provenance_edges_df` (two 22.2M-row joins) and walks them via
`walk_provenance_to_targets` (BFS with frontier/visited/stepped sets).

**Root cause:** Phase 1 matching operates on the full dataset regardless of the
Phase 2 chunk size.

### 3. Recording (~2.5 GB)

`build_execution_edges` creates a DataFrame with one row per input/output
artifact ID per role. At 22.2M inputs, this is a 22.2M+ row DataFrame with
four string columns.

**Root cause:** Execution edge recording is a single-shot materialization.

### jemalloc compounding

Polars uses jemalloc, which retains freed virtual memory pages rather than
returning them to the OS. In a long-lived subprocess that allocates and frees
large DataFrames, RSS only grows. The 15–20 GB logical peak inflates to ~60 GB
observed RSS.

Per-chunk subprocesses (one subprocess per chunk, exit after processing)
eliminate this entirely — the OS reclaims the full address space on exit.

---

## The lazy reference design space

The fundamental question: **can the orchestration layer avoid materializing all
IDs?**

### What `resolve_inputs` actually needs

`resolve_output_reference` constructs a lazy scan:

```python
result = (
    pl.scan_delta(str(execution_edges_path))
    .filter(pl.col("execution_run_id").is_in(run_ids))
    .filter(pl.col("direction") == "output")
    .filter(pl.col("role") == role)
    .select("artifact_id")
)
```

Then immediately materializes it:

```python
artifact_ids = result.collect()["artifact_id"].to_list()
return sorted(set(artifact_ids))
```

A lazy reference would carry the scan parameters instead of the result:

```python
@dataclass(frozen=True)
class InputRef:
    delta_root: str
    source_step: int
    role: str
    execution_run_ids: tuple[str, ...]  # typically 1–5 IDs

    def scan(self) -> pl.LazyFrame:
        """Reconstruct the lazy scan — zero materialization."""
        ...

    def count(self) -> int:
        """Count without materializing IDs."""
        return self.scan().select(pl.len()).collect().item()
```

This is ~200 bytes to pickle instead of ~930 MB.

### Challenges with fully lazy references

**1. Chunking a lazy scan deterministically**

Delta Lake doesn't expose stable row ordering. A `pl.LazyFrame` has no native
offset/limit that guarantees deterministic, non-overlapping chunks across
multiple scans. Options:

- **Hash-based partitioning**: `xxh3(artifact_id) % n_chunks == chunk_i`.
  Deterministic, but requires scanning all IDs per chunk to filter (though
  Polars can push the predicate down).
- **Row-index based**: `with_row_index().filter(row_nr.is_between(start, end))`.
  Forces a full scan to assign row numbers. Not truly lazy.
- **Materialize once, chunk in parent**: resolve all IDs as a Polars Series (not
  Python list), slice into chunks, send each chunk as a small Python list. The
  parent holds ~710 MB of Arrow-backed IDs but never converts to Python strings
  until chunking.

**2. `compute_execution_spec_id` needs all IDs**

The fallback path (when `step_spec_id` is not provided) hashes all input IDs:

```python
all_ids: set[str] = set()
for role_ids in inputs.values():
    all_ids.update(role_ids)
sorted_ids = ",".join(sorted(all_ids))
hash_input = f"{operation_name}|{sorted_ids}|..."
```

This requires materializing all IDs. However, the pipeline-manager path always
provides `step_spec_id` and skips this computation entirely. A lazy input
system would need to either:
- Require `step_spec_id` for large inputs (enforce via validation)
- Compute the hash lazily (e.g., hash the Delta scan parameters instead of the
  results — semantically different but functionally equivalent for cache keys)

**3. `ExecutionUnit` validation**

The Pydantic validator iterates all IDs to check `isinstance(str)` and
`len == 32`. With lazy references, this validation would need to be deferred
to the point of materialization (inside the subprocess), or replaced with a
schema-level check on the Delta table.

**4. `build_execution_edges` assumes all IDs available**

The recorder builds a single DataFrame from all input/output IDs. With chunked
dispatch, each chunk writes its own execution edges. The commit layer would need
to handle multiple parquet files per execution or merge them.

**5. Backwards compatibility**

`ExecutionUnit.inputs` is typed `dict[str, list[str]]` and used throughout the
execution layer. Worker-dispatched operations (non-curator) also use this
interface. Changing the type to accept lazy references requires either:
- A union type (`dict[str, list[str] | InputRef]`)
- A separate `CuratorExecutionUnit` model
- A protocol/interface that both concrete and lazy inputs satisfy

### Levels of laziness

| Level | Parent memory | Subprocess memory | Complexity |
|-------|--------------|-------------------|------------|
| **Current** | 1.4 GB (Python lists) | 15–20 GB | Low |
| **Level 1**: materialize in parent, chunk to subprocesses | 1.4 GB (Python lists) | ~500 MB per chunk | Low |
| **Level 2**: Arrow in parent, chunk to subprocesses | 710 MB (Arrow Series) | ~500 MB per chunk | Medium |
| **Level 3**: lazy ref, materialize in subprocess | ~0 | ~500 MB per chunk | High |

Level 1 solves the OOM. The parent holding 1.4 GB is fine — it's the head node
with plenty of memory and the curator step is the only thing running. The
subprocess OOM at 60 GB is the critical problem.

Level 2 saves ~700 MB in the parent by keeping IDs in Arrow format until
chunking. Minor improvement, worth doing if already refactoring.

Level 3 is the cleanest architecture but requires changes to `ExecutionUnit`,
validation, spec-id computation, and the recording path. Worth pursuing as a
separate effort once Level 1 proves the chunked dispatch pattern.

---

## Scope boundaries

### What Level 1 (chunked dispatch) addresses

- Subprocess memory: bounded at ~500 MB per chunk
- jemalloc fragmentation: eliminated (subprocess exits after each chunk)
- Serialization: ~16 MB pickle per chunk instead of ~930 MB
- Recording: per-chunk execution edge writes instead of 22.2M-row DataFrame

### What Level 1 does NOT address

- Parent memory: still materializes all IDs (~1.4 GB)
- `resolve_inputs` eagerness: still calls `.to_list()` immediately
- `ExecutionUnit` type: still `dict[str, list[str]]`
- Other curator operations: only the filter path is chunked initially

### What Level 3 (fully lazy) would address

- Parent memory: near-zero (just query descriptors)
- End-to-end laziness: IDs resolved from Delta only where needed
- Generic chunked dispatch: any curator operation can be chunked
- But: significantly more complex, changes execution model interfaces

---

## Implications for future design

1. **`InputRef` as a first-class concept**: if we introduce lazy references, they
   should be a proper schema type, not a special case. The execution layer
   should be able to handle both concrete IDs and lazy references uniformly.

2. **Delta Lake as the serialization medium**: instead of pickling IDs across the
   process boundary, both parent and child can read from the same Delta tables.
   The "message" between them is a query, not data.

3. **Streaming execution edges**: the recording path should support appending
   chunks rather than building one large DataFrame. PyArrow's `ParquetWriter`
   or writing multiple parquet files per staging directory both work.

4. **Spec-id computation**: for large-scale operations, the spec ID should be
   computed from the query descriptor (source step + role + execution run IDs),
   not from the artifact IDs themselves. This is semantically equivalent (same
   query produces same IDs) and avoids materializing IDs just for hashing.

5. **Validation deferral**: ID format validation (32-char hex) can be deferred to
   the point of use. Delta tables already enforce schema. Validating 22.2M
   strings in the constructor adds latency without catching real errors.
