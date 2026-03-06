# Design: Chunked Filter Dispatch (Option A)

**Date:** 2026-03-03  **Status:** Draft  **Author:** Claude + ach94

---

## Context

The filter subprocess OOMs at 22.2M artifacts (~60 GB RSS). The design doc
[design_curator_input_scalability.md](design_curator_input_scalability.md)
traces the full materialization chain and identifies three cost centers:
serialization (~2.8 GB), matching (~10–15 GB), and recording (~2.5 GB), with
jemalloc fragmentation inflating RSS 2–3×.

This doc proposes **Level 1 chunked dispatch**: materialize IDs in the parent,
chunk them, and dispatch each chunk to a separate subprocess. This is the
simplest fix that solves the OOM.

---

## Approach

Materialize the full ID lists in the parent process (as today), then iterate
over chunks. Each chunk gets its own subprocess call. The subprocess processes a
bounded slice and exits, fully reclaiming memory.

### Why materialize in the parent?

The parent is the SLURM head node. Holding ~1.4 GB of IDs is fine — no other
step runs concurrently. The problem is the subprocess, which peaks at 15–20 GB
and inflates to 60 GB via jemalloc. Chunked dispatch caps the subprocess at
~500 MB and eliminates jemalloc accumulation.

Fully lazy references (Level 3) would reduce parent memory to near-zero but
require changes to `ExecutionUnit`, validation, spec-id computation, and the
recording path. That's a separate effort.

---

## New flow

```
_execute_curator_step (step_executor.py)
│
│  1. resolve_inputs()
│     → resolved_inputs = {"passthrough": [11.1M], "metrics": [11.1M]}
│     → ~1.4 GB in parent (same as today)
│
│  2. Determine if chunked dispatch applies
│     → only for filter operations (has passthrough role)
│     → only when total_artifacts > chunk_threshold (e.g., 500K)
│     → otherwise, fall through to existing single-dispatch path
│
│  3. For each chunk of passthrough IDs (500K per chunk):
│     │
│     │  a. Slice passthrough IDs: chunk_pt = passthrough[i:i+500K]
│     │
│     │  b. Resolve metric IDs for this chunk (see Decision 1):
│     │     → single-hop get_descendant_ids_df on artifact_edges
│     │     → returns [source_artifact_id, target_artifact_id]
│     │     → inner join with full metric ID set → chunk_metrics
│     │     (may happen here in step executor or inside the filter)
│     │
│     │  c. Build chunk inputs:
│     │     chunk_inputs = {"passthrough": chunk_pt, "metrics": chunk_metrics}
│     │     → ~32 MB (500K + ~500K IDs)
│     │
│     │  d. Create chunk ExecutionUnit (small)
│     │     → Filter already sets independent_input_streams=True,
│     │       so different-length roles are accepted
│     │
│     │  e. _run_curator_in_subprocess(chunk_unit, runtime_env)
│     │     → pickle: ~32 MB (not 930 MB)
│     │     → single-hop metric resolution (not backward walk)
│     │     → child builds execution edges for ~1M IDs (not 22.2M)
│     │     → child peak: ~300–500 MB
│     │     → child exits → OS reclaims everything
│     │
│     │  f. Collect chunk result:
│     │     → passed IDs from StagingResult.artifact_ids
│     │     → staging path (execution edges already written)
│     │     → diagnostics handling: see Decision 3
│     │
│  4. Merge across chunks:
│     → combine passed IDs from all chunks
│     → execution_run_id handling: see Decision 2
│     → diagnostics handling: see Decision 3
│
│  5. Commit (unchanged — commit layer handles multiple staging dirs)
```

### Memory comparison

| Component | Current | Chunked (per subprocess) |
|-----------|---------|--------------------------|
| Pickle payload | 930 MB | ~32 MB |
| Child: Python lists + Arrow | 2.1 GB | ~48 MB |
| Child: matching (edges + walk) | 10–15 GB | eliminated (single-hop `get_descendant_ids_df`, ~50 MB) |
| Child: execution edge DF | 2.5 GB | ~80 MB |
| **Child peak** | **15–20 GB** | **~300–500 MB** |
| **Child RSS (with jemalloc)** | **~60 GB** | **~500 MB** (exits before fragmentation) |
| Parent | ~1.4 GB | ~1.4 GB (unchanged) |

---

## Detailed design

### Step executor changes (`step_executor.py`)

#### New function: `_execute_curator_step_chunked`

Called from `_execute_curator_step` when the operation is a filter and the input
count exceeds the chunk threshold.

```python
def _execute_curator_step_chunked(
    operation: OperationDefinition,
    resolved_inputs: dict[str, list[str]],
    config: PipelineConfig | None,
    runtime_env: RuntimeEnvironment,
    step_number: int,
    spec_id: str,
    chunk_size: int = 500_000,
) -> StepResult:
```

> **Note:** Whether `execution_run_id` is a parameter here depends on
> [Decision 2](#decision-2-how-is-execution_run_id-unified-across-chunks).

Responsibilities:
1. Identify the passthrough and metric roles from `resolved_inputs`
2. Chunk the passthrough IDs into slices of `chunk_size`
3. For each chunk, resolve corresponding metrics via forward edge scan
4. Build a small `ExecutionUnit` per chunk
5. Dispatch to subprocess via `_run_curator_in_subprocess`
6. Collect and merge results

#### Chunking logic

> **Note:** This pseudocode assumes [Decision 1](#decision-1-where-does-metric-resolution-happen)
> Option A (step executor resolves metrics). If Option B, the step executor only
> chunks passthrough IDs and the metric resolution moves into the filter.

```python
passthrough_ids = resolved_inputs["passthrough"]
metric_ids_set = set(resolved_inputs["metrics"])

for chunk_start in range(0, len(passthrough_ids), chunk_size):
    chunk_pt = passthrough_ids[chunk_start : chunk_start + chunk_size]

    # Single-hop scan: find direct child metrics of this chunk's passthrough IDs
    descendants_df = artifact_store.get_descendant_ids_df(
        pl.Series(chunk_pt), target_artifact_type="metric"
    )
    chunk_metrics = (
        descendants_df
        .filter(pl.col("target_artifact_id").is_in(metric_ids_set))
        ["target_artifact_id"]
        .unique()
        .to_list()
    )

    chunk_inputs = {"passthrough": chunk_pt, "metrics": chunk_metrics}
    # ... build unit, dispatch, collect result
```

#### When to use chunked dispatch

```python
_CHUNK_THRESHOLD = 500_000

def _should_use_chunked_dispatch(
    operation: OperationDefinition,
    resolved_inputs: dict[str, list[str]],
) -> bool:
    """Chunked dispatch for filter operations with large inputs."""
    if not isinstance(operation, Filter):
        return False
    if operation.group_by is not None:
        return False
    total = sum(len(ids) for ids in resolved_inputs.values())
    return total > _CHUNK_THRESHOLD
```

Filter-specific for now. Generalizing to other curator operations is future
work (see scalability doc).

### Filter changes (`filter.py`)

#### Remove `_match_explicit`

The entire backward-walk matching phase (`_match_explicit`) is eliminated.
Metric resolution uses single-hop `get_descendant_ids_df` (direct
parent→metric lookups on `artifact_edges`) instead of the multi-hop BFS walk
through `provenance_edges`. Multi-hop provenance is not a current use case
and is explicitly out of scope.

Depending on [Decision 1](#decision-1-where-does-metric-resolution-happen),
the filter either receives pre-matched inputs from the step executor or resolves
its own metrics via the simpler single-hop call. Either way, it only needs to:
1. Build the role lookup (which metrics belong to which role)
2. Hydrate metrics as a wide DataFrame
3. Evaluate criteria
4. Return passed IDs

#### Simplified `execute_curator`

> **Depends on [Decision 1](#decision-1-where-does-metric-resolution-happen).**
> The filter's `execute_curator` implementation changes based on whether the
> step executor pre-resolves metrics (Option A) or the filter resolves them
> internally (Option B). Pseudocode will be finalized after this decision.

The filter no longer needs internal chunking because its inputs are already
bounded. The `_DiagnosticsAccumulator` still works — it accumulates within the
single (bounded) call.

### Recording changes

#### Per-chunk execution edges

Each subprocess call writes its own execution edges to its staging directory.
`build_execution_edges` is called with ~1M IDs per chunk instead of 22.2M.

The commit layer already handles multiple staging directories from batched
worker dispatches. The same mechanism works for chunked curator dispatch.

#### Execution record

> **Note:** How `execution_run_id` is handled depends on
> [Decision 2](#decision-2-how-is-execution_run_id-unified-across-chunks).
> Currently `run_curator_flow` generates its own run ID per invocation.

### Result merging

```python
all_passed_ids: list[str] = []

for chunk_result in chunk_results:
    # StagingResult.artifact_ids contains the chunk's passed IDs
    all_passed_ids.extend(chunk_result.artifact_ids)
```

> **Note:** Diagnostics merging depends on [Decision 3](#decision-3-how-are-diagnostics-surfaced-from-subprocesses).
> `StagingResult` does not currently carry diagnostics.

---

## What changes, what doesn't

### Changes

| File | Change |
|------|--------|
| `step_executor.py` | Add `_execute_curator_step_chunked`, `_should_use_chunked_dispatch`, chunk loop with forward edge resolution, result merging |
| `filter.py` | Remove `_match_explicit`, simplify `execute_curator` to use single-hop `get_descendant_ids_df` on bounded inputs (scope depends on [Decision 1](#decision-1-where-does-metric-resolution-happen)) |
| `test_step_executor.py` | Tests for chunked dispatch: correct chunking, metric resolution, result merging, threshold behavior |
| `test_filter.py` | Simplify mocks (remove edge loading + walk mocks), test bounded-input behavior |

### Unchanged

| Component | Why |
|-----------|-----|
| `resolve_inputs` | Still returns `dict[str, list[str]]` — parent can hold 1.4 GB |
| `ExecutionUnit` | Still takes `dict[str, list[str]]` — chunks are small |
| `run_curator_flow` | Still receives `ExecutionUnit`, converts to DataFrames, records. **May change** depending on [Decision 2](#decision-2-how-is-execution_run_id-unified-across-chunks). |
| `build_execution_edges` | Still builds a DataFrame — but from ~1M IDs, not 22.2M |
| `_stage_execution_edges` | Still writes parquet — but ~80 MB, not 2.5 GB |
| Commit layer | Already handles multiple staging directories |
| `_DiagnosticsAccumulator` | Still accumulates within each (bounded) call |

---

## Chunk size tuning

| Chunk size | Chunks at 11.1M | Pickle per chunk | Subprocess peak | Delta scans |
|------------|-----------------|-------------------|-----------------|-------------|
| 100K | 111 | ~6 MB | ~200 MB | 111 |
| 500K | 22 | ~32 MB | ~500 MB | 22 |
| 1M | 11 | ~64 MB | ~800 MB | 11 |

Default: **500K**. Balances subprocess memory (~500 MB) against scan count (22
scans, all from page cache after the first). Users can override via operation
params or environment variable for tuning.

At 500K chunks, the per-subprocess overhead (spawn + pickle + Delta scan) is
~2–3 seconds. Total overhead for 22 chunks: ~45–60 seconds, comparable to the
current single-process execution time.

---

## Edge cases

### Filter with no criteria (passthrough all)

If the filter has no criteria, all passthrough IDs pass through. The chunked
path still works — each chunk's `execute_curator` returns all its passthrough
IDs. Result merging produces the same output as the unchunked path.

### Filter with `group_by`

**Not supported in v1.** If chunked dispatch is triggered and the operation has
`group_by` set, fall through to the existing single-dispatch path (no chunking).
Group-aware chunk boundaries are future work if the combination becomes needed.

### Small inputs (below threshold)

When `total_artifacts <= 500K`, the existing single-dispatch path is used. No
chunking overhead. This covers the common case (most filter steps process <100K
artifacts).

### Implicit metrics

The filter supports implicit metrics (discovered via `get_descendant_ids_df`
at runtime, not provided as explicit inputs). With single-hop resolution, the
per-chunk `get_descendant_ids_df` call discovers all direct child metrics of
each chunk's passthrough IDs — this covers both explicit and implicit metrics
regardless of where the call happens (see
[Decision 1](#decision-1-where-does-metric-resolution-happen)). No special
handling needed.

### Cache lookup

The `execution_spec_id` is computed once for the full step (not per chunk). The
cache lookup checks if the step has already been executed. If cached, the entire
step is skipped — no chunked dispatch needed.

---

## Risks and mitigations

| Risk | Mitigation |
|------|------------|
| Forward scan misses multi-hop metrics | Only single-hop (direct parent→metric) is supported. Multi-hop provenance is not a current use case and is explicitly out of scope. |
| Chunk overhead (spawn + scan) is too high | Page cache makes subsequent scans fast. 22 chunks × 3s = ~66s overhead. |
| Diagnostics merging loses precision | Depends on [Decision 3](#decision-3-how-are-diagnostics-surfaced-from-subprocesses). If merging: sum counts/sums/pass_counts, min mins, max maxes, recompute mean. |
| `group_by` splits groups across chunks | v1: fall through to single-dispatch when `group_by` is set. Group-aware chunking is future work. |
| Metric deduplication across chunks | Metrics are children of specific passthrough IDs. A metric can appear in multiple chunks if it has multiple parents. Each chunk processes it independently — filter criteria are deterministic, so the result is correct. |

---

## Open decisions

These must be resolved before implementation begins.

### Decision 1: Where does metric resolution happen?

**Option A — Step executor resolves metrics per chunk before dispatch.**
The filter receives pre-matched inputs (`{"passthrough": [...], "role_x": [...]}`).
`_match_explicit` is fully removed. The filter's `execute_curator` only hydrates
and evaluates — no `artifact_store.get_descendant_ids_df` call inside the filter.

**Option B — Filter resolves its own metrics (bounded by chunk size).**
The step executor only chunks passthrough IDs. The filter still calls
`get_descendant_ids_df` internally, but on a bounded chunk (~500K IDs) instead
of the full 11.1M. `_match_explicit` is replaced with the simpler single-hop
call, but the call stays inside the filter.

Implications: Option A changes the Filter's contract (it no longer self-resolves
metrics). Option B keeps the Filter self-contained but requires `artifact_store`
access in the subprocess.

### Decision 2: How is `execution_run_id` unified across chunks?

Currently `run_curator_flow` generates its own `execution_run_id` via
`generate_execution_run_id()` per invocation. With chunked dispatch, each
subprocess call creates a separate run ID.

**Option A — Pass `execution_run_id` into `run_curator_flow`.**
Generate once in the step executor, pass to each chunk. Requires changing
`run_curator_flow`'s signature (and removing it from the "Unchanged" table).

**Option B — Let each chunk have its own `execution_run_id`.**
Accept N execution records per step. Simpler, but changes the 1:1 relationship
between steps and execution records.

### Decision 3: How are diagnostics surfaced from subprocesses?

`StagingResult` (the subprocess return type) has fields: `success`, `error`,
`staging_path`, `execution_run_id`, `artifact_ids`. It does **not** carry
diagnostics. The filter's diagnostics are currently consumed inside
`run_curator_flow` and stored in the execution record metadata — they don't
reach the step executor.

**Option A — Add a `metadata` field to `StagingResult`.**
The subprocess returns diagnostics alongside artifact IDs. The step executor
merges diagnostics across chunks.

**Option B — Skip cross-chunk diagnostics merging in v1.**
Each chunk's diagnostics are written to its own execution record. No merged
summary. Simpler but loses the single-step diagnostic view.

**If merging (Option A):** The merge algorithm must be specified. The
`_DiagnosticsAccumulator.finalize()` output includes per-criterion count, sum,
min, max, and pass counts. Merging requires: sum counts, sum sums, min mins,
max maxes, sum pass counts. Mean is recomputed as merged_sum/merged_count.

---

## Verification plan

1. `~/.pixi/bin/pixi run -e dev fmt`
2. `~/.pixi/bin/pixi run -e dev test-unit`
3. Scale test: `~/.pixi/bin/pixi run python _dev/demos/demo_scale_test.py --slurm`
   — verify Step 6 (filter) completes without OOM
4. Compare filter output (passed IDs) with a known-good run at smaller scale
   to verify correctness
