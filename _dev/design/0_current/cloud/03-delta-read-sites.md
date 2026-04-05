# Design: Delta Read Sites

**Date:** 2026-04-04
**Status:** Draft

---

## Problem

After doc 02 migrates the core storage classes (`ArtifactStore`,
`ProvenanceStore`, `DeltaCommitter`, `StagingArea`, `StagingManager`),
there are still 25 `pl.scan_delta()` and 2 `df.write_delta()` call
sites scattered across orchestration, visualization, operations, and
cache code that don't pass `storage_options`. These calls work locally
but fail on S3/GCS URIs because delta-rs needs credentials.

This PR is a mechanical update: thread `storage_options: dict | None`
through each function signature and pass it to every `scan_delta` and
`write_delta` call. The pattern is the same everywhere:

```python
# Before
pl.scan_delta(str(path))

# After
pl.scan_delta(str(path), storage_options=storage_options)
```

For local runs, `storage_options` is `None` (from
`StorageConfig.delta_storage_options()`), which is the same as not
passing it — zero behavior change.

Depends on doc 01 (StorageConfig provides `delta_storage_options()`).
Independent of doc 02 (no overlap in files changed).

---

## Prior Art Survey

### `StepTracker` (`orchestration/engine/step_tracker.py`)

Constructor: `__init__(self, delta_root: Path, pipeline_run_id: str)`.
Stores `self._steps_path = delta_root / TablePath.STEPS`. Has 4
`scan_delta` calls and 2 `write_delta` calls for reading/writing step
state. All use `pl.scan_delta(str(self._steps_path))`.

Created by `PipelineManager.__init__` with `config.delta_root`.

### `resolve_output_reference` / `resolve_inputs` (`orchestration/engine/inputs.py`)

Free functions that accept `delta_root: Path`. Construct
`executions_path` and `execution_edges_path` from `delta_root`. Have 2
`scan_delta` calls. Also use `path.exists()` as guards.

Called by `step_executor.py` with `config.delta_root`.

### `cache_lookup` (`storage/cache/cache_lookup.py`)

Free function that accepts `executions_path: Path | str` and optional
`execution_edges_path`. Has 3 `scan_delta` calls. Derives
`delta_root` from `records_path.parent.parent` to find the provenance
table.

Called by `step_executor.py` with paths derived from `config.delta_root`.

### `interactive_filter.py` (`operations/curator/interactive_filter.py`)

Has 3 `scan_delta` calls inside methods that receive `delta_root` from
the operation's execution context. Reads artifact index and step tables
for the interactive filter UI.

### Visualization modules (`visualization/`)

Four files with a combined 13 `scan_delta` calls:

- `inspect.py` (7): `inspect_pipeline`, `inspect_step`,
  `inspect_metrics`, `inspect_data`, `inspect_provenance`,
  `inspect_content`. Accept `delta_root: Path | str`.
- `timing.py` (2): `load_timing_data`. Accepts `delta_root: Path | str`.
- `graph/macro.py` (1): `macro_provenance_graph`. Accepts
  `delta_root: Path | str`.
- `graph/micro.py` (3): `_load_delta`, `micro_provenance_graph`.
  Accept `delta_root: Path | str`.

All visualization functions are user-facing analysis tools that take
`delta_root` directly.

---

## Design

### Threading pattern

Each function or class gains `storage_options: dict[str, str] | None = None`
as a parameter. The parameter defaults to `None` (local behavior unchanged).
Callers that have access to `StorageConfig` pass
`config.storage.delta_storage_options()`.

### StepTracker

```python
class StepTracker:
    def __init__(
        self,
        delta_root: Path,
        pipeline_run_id: str = "",
        storage_options: dict[str, str] | None = None,
    ) -> None:
        self._steps_path = delta_root / TablePath.STEPS
        self._pipeline_run_id = pipeline_run_id
        self._storage_options = storage_options
```

All 4 `scan_delta` calls become:
```python
pl.scan_delta(str(self._steps_path), storage_options=self._storage_options)
```

Both `write_delta` calls become:
```python
df.write_delta(
    str(self._steps_path),
    storage_options=self._storage_options,
    delta_write_options={...},
)
```

`PipelineManager` passes `storage_options=config.storage.delta_storage_options()`
when constructing `StepTracker`.

### resolve_output_reference / resolve_inputs

```python
def resolve_output_reference(
    ref: OutputReference,
    delta_root: Path,
    step_run_id: str | None = None,
    storage_options: dict[str, str] | None = None,
) -> list[str]:
```

Both `scan_delta` calls become:
```python
pl.scan_delta(str(executions_path), storage_options=storage_options)
```

`step_executor.py` passes `storage_options` from `config.storage`.

### cache_lookup

```python
def cache_lookup(
    executions_path: Path | str,
    execution_spec_id: str,
    ...,
    storage_options: dict[str, str] | None = None,
) -> CacheLookupResult:
```

All 3 `scan_delta` calls get `storage_options=storage_options`.

### interactive_filter

The `InteractiveFilter` class methods that call `scan_delta` gain
`storage_options` either through the constructor or through the
`ExecutionContext` (which carries `RuntimeEnvironment`).

### Visualization functions

All user-facing functions gain `storage_options`:

```python
def inspect_pipeline(
    delta_root: Path | str,
    *,
    pipeline_run_id: str | None = None,
    storage_options: dict[str, str] | None = None,
) -> pl.DataFrame:
```

Pattern is identical across all visualization functions. For user
convenience, these stay as optional kwargs — users only pass them
for cloud analysis.

### `.exists()` guards

Several of these functions guard `scan_delta` calls with
`path.exists()`:

```python
if not steps_path.exists():
    return ...
```

These guards continue to use `Path.exists()` during this PR because
the path fields are still `Path` objects. After the field rename
(doc 04), they'll switch to `fs.exists()`. For this PR, behavior is
unchanged: local paths use pathlib, and cloud paths aren't possible
yet (the rename hasn't happened).

---

## Scope

| File | Change |
|------|--------|
| `orchestration/engine/step_tracker.py` | `__init__` gains `storage_options`. 4 `scan_delta` + 2 `write_delta` calls updated. |
| `orchestration/engine/inputs.py` | `resolve_output_reference` and `resolve_inputs` gain `storage_options`. 2 `scan_delta` calls updated. |
| `storage/cache/cache_lookup.py` | `cache_lookup` gains `storage_options`. 3 `scan_delta` calls updated. |
| `operations/curator/interactive_filter.py` | 3 `scan_delta` calls updated. `storage_options` threaded through class. |
| `visualization/inspect.py` | 7 `scan_delta` calls updated across 6 functions. |
| `visualization/timing.py` | 2 `scan_delta` calls updated. |
| `visualization/graph/macro.py` | 1 `scan_delta` call updated. |
| `visualization/graph/micro.py` | 3 `scan_delta` calls updated. |
| `orchestration/pipeline_manager.py` | Pass `storage_options` when constructing `StepTracker`. |
| `orchestration/engine/step_executor.py` | Pass `storage_options` to `resolve_inputs`, `cache_lookup`. |

---

## Testing

| Test file | Coverage |
|-----------|----------|
| `tests/artisan/orchestration/engine/test_step_tracker.py` | Existing tests pass unchanged (`storage_options=None` is default). New test: verify `storage_options` is forwarded to `scan_delta` (mock). |
| `tests/artisan/orchestration/engine/test_dispatch.py` | Unchanged — dispatch doesn't touch delta reads. |
| `tests/artisan/storage/test_cache_lookup.py` | Existing tests pass unchanged. New test: `storage_options` forwarded. |
| `tests/artisan/visualization/` | Visualization tests use local `tmp_path` — unchanged. New test for one representative function verifying `storage_options` passthrough. |

This PR is low risk — all changes are additive (`storage_options`
defaults to `None`), so existing test suites validate no regression
without modification.

---

## Related Docs

- `cloud-storage-design.md` — full storage design (this is Phase 3)
- `01-storage-config.md` — provides `StorageConfig.delta_storage_options()`
- `02-storage-layer.md` — migrates the storage classes (independent, no file overlap)
- `04-runtime-env-rename.md` — the rename that makes cloud URIs actually flow through
