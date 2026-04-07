# Design: RuntimeEnvironment Field Rename

**Date:** 2026-04-04
**Status:** Draft

---

## Problem

`RuntimeEnvironment` carries five path fields as `pathlib.Path`:

- `delta_root_path: Path`
- `staging_root_path: Path`
- `working_root_path: Path | None`
- `failure_logs_root: Path | None`
- `files_root_path: Path | None`

Cloud backends need S3/GCS URIs (`s3://bucket/delta`), which aren't
representable as `pathlib.Path`. After docs 01–03 add `StorageConfig`,
migrate storage classes to fsspec, and thread `storage_options` through
delta-rs calls, this final PR renames the fields to `str` and drops the
`_path` suffix — the last piece needed before cloud URIs can flow end
to end.

This is a cross-cutting rename that touches every consumer of
`RuntimeEnvironment` and `PipelineConfig`. It's done last so that
docs 01–03 can land independently without a flag-day migration.

Depends on docs 02 and 03 (storage classes and delta read sites must
already accept `str` URIs and `storage_options`).

---

## Prior Art Survey

### `RuntimeEnvironment` (`schemas/execution/runtime_environment.py`)

Frozen Pydantic `BaseModel`. Five path fields: `delta_root_path: Path`,
`staging_root_path: Path`, `working_root_path: Path | None`,
`failure_logs_root: Path | None`, `files_root_path: Path | None`. Not
persisted to disk, stored in a database, or cached across runs —
constructed fresh per pipeline run by
`step_executor._create_runtime_environment()`. The field rename is
therefore a code-only change with no data migration required.

### `PipelineConfig` (`schemas/orchestration/pipeline_config.py`)

Frozen Pydantic `BaseModel`. Four path fields: `delta_root: Path`,
`staging_root: Path`, `working_root: Path`,
`files_root: Path | None`. Note: already uses the target names (no
`_path` suffix), but types are still `Path`. Has a
`_default_files_root` model validator that derives `files_root` from
`self.delta_root.parent / "files"` — this uses `Path.parent` and
must migrate to `uri_parent()`/`uri_join()`.

### Consumers of `RuntimeEnvironment` path fields (7 source files)

| File | Fields accessed |
|------|----------------|
| `schemas/execution/runtime_environment.py` | Definition |
| `orchestration/engine/step_executor.py` | `delta_root_path`, `staging_root_path`, `working_root_path`, `files_root_path` |
| `execution/executors/creator.py` | `delta_root_path`, `staging_root_path`, `working_root_path`, `files_root_path` |
| `execution/executors/curator.py` | `delta_root_path`, `staging_root_path`, `files_root_path` |
| `execution/executors/composite.py` | `delta_root_path`, `staging_root_path`, `working_root_path`, `files_root_path` |
| `execution/context/builder.py` | `delta_root_path`, `staging_root_path` (as parameters) |
| `composites/base/composite_context.py` | `staging_root_path`, `delta_root_path` |

### Consumers of `PipelineConfig` path fields (2 source files)

| File | Fields accessed |
|------|----------------|
| `orchestration/engine/step_executor.py` | `config.delta_root`, `config.staging_root`, `config.working_root`, `config.files_root` |
| `orchestration/pipeline_manager.py` | `config.delta_root`, `config.staging_root`, `config.delta_root.parent` |

### `failure_logs_root` consumers

| File | Usage |
|------|-------|
| `orchestration/engine/dispatch.py` | `.iterdir()`, `.is_dir()`, `/`, `.exists()`, `.open("a")` |
| `execution/staging/recorder.py` | `Path /`, `.mkdir()`, `.write_text()` |
| `orchestration/backends/base.py` | Type signature only |
| `orchestration/backends/local.py` | Type signature only |
| `orchestration/backends/slurm.py` | Type signature, pass-through |
| `execution/executors/creator.py` | Pass-through |
| `execution/executors/curator.py` | Pass-through |

`failure_logs_root` stays local — it's written by workers on the same
machine as the orchestrator (local) or on a shared NFS (SLURM). The
type changes to `str | None` but all operations use `Path(failure_logs_root)`
internally since this path is always local.

### `working_root` stays local

`working_root` is always a local path — it's the sandbox directory
(`/tmp`). Operations write files there; it's never on cloud storage.
The type changes to `str | None` but `Path(working_root)` is used
internally by sandbox creation.

### Test files that construct `RuntimeEnvironment` or `PipelineConfig`

| File | What it constructs |
|------|-------------------|
| `tests/artisan/execution/test_executor_creator.py` | `RuntimeEnvironment` |
| `tests/artisan/execution/test_creator_lifecycle.py` | `RuntimeEnvironment` |
| `tests/artisan/execution/test_files_dir_threading.py` | `RuntimeEnvironment` (uses `files_root_path=`) |
| `tests/artisan/composites/test_composite_context.py` | `RuntimeEnvironment` |
| `tests/artisan/orchestration/test_step_executor.py` | `PipelineConfig` |
| `tests/artisan/orchestration/test_pipeline_manager.py` | `PipelineConfig` |
| `tests/artisan/orchestration/test_signal_handling.py` | `PipelineConfig` |
| `tests/artisan/orchestration/test_orchestration_api.py` | `PipelineConfig` |
| `tests/artisan/orchestration/test_pipeline_helpers.py` | `PipelineConfig` |
| `tests/artisan/schemas/test_runtime_environment.py` | `RuntimeEnvironment` (uses `files_root_path=`) |
| `tests/artisan/schemas/test_pipeline_config_files_root.py` | `PipelineConfig` (tests `files_root` derivation) |
| `tests/integration/` | Multiple files construct `PipelineConfig` |

---

## Design

### Path `/` operator replacement

The codebase uses the `Path /` operator and `Path.parent` on config
fields. With `str` fields, these break — and `Path()` wrapping is
wrong for S3/GCS URIs. Doc 01 introduces `uri_join()` and
`uri_parent()` in `utils/path.py` (backed by `posixpath`) for this
purpose.

Migration pattern:

```python
# Before (Path fields)
config.delta_root / TablePath.EXECUTIONS
config.delta_root.parent / "logs" / "failures"
config.staging_root / "_dispatch"

# After (str fields)
uri_join(config.delta_root, TablePath.EXECUTIONS)
uri_join(uri_parent(config.delta_root), "logs", "failures")
uri_join(config.staging_root, "_dispatch")
```

Sites in `step_executor.py` that need this migration:
- `config.delta_root / TablePath.EXECUTIONS` — `check_cache_for_batch`
- `config.delta_root.parent / "logs" / "failures"` — `_create_runtime_environment`
- `config.delta_root.parent / "logs" / "slurm"` — SLURM log path derivation
- `config.staging_root / "_dispatch"` — dispatch directory

Sites in `pipeline_manager.py`:
- `config.delta_root.parent / "logs"` — `__init__` logging configuration

### Staging scope: cloud-capable vs always-local

`delta_root` and `staging_root` are cloud-capable — after this PR,
they accept S3/GCS URIs. `working_root` and `failure_logs_root` are
always local (sandbox and log directories on the machine running the
executor). Consumers of always-local fields wrap with `Path()`:

```python
# Cloud-capable — use uri_join, fs.*, storage_options
uri_join(runtime_env.delta_root, TablePath.EXECUTIONS)

# Always-local — wrap with Path() for filesystem operations
sandbox_base = Path(runtime_env.working_root)
log_dir = Path(runtime_env.failure_logs_root)
```

### RuntimeEnvironment renames

```python
class RuntimeEnvironment(BaseModel):
    delta_root: str = Field(
        ...,
        description="Root URI for Delta Lake tables. "
        "Local path or s3://bucket/delta.",
    )

    staging_root: str = Field(
        ...,
        description="Root URI for staged Parquet files. "
        "Local path or s3://bucket/staging.",
    )

    working_root: str | None = Field(
        None,
        description="Base directory for execution sandboxes. "
        "Always a local path.",
    )

    failure_logs_root: str | None = Field(
        None,
        description="Where to write human-readable failure log files. "
        "Always a local path.",
    )

    files_root: str | None = Field(
        None,
        description="Root directory for Artisan-managed external files. "
        "Always a local path.",
    )

    storage: StorageConfig = Field(
        default_factory=StorageConfig,
        description="Storage backend configuration.",
    )

    # ... other fields unchanged ...
```

### PipelineConfig type changes

```python
class PipelineConfig(BaseModel):
    delta_root: str = Field(...)     # was Path
    staging_root: str = Field(...)   # was Path
    working_root: str = Field(       # was Path
        default_factory=lambda: tempfile.gettempdir(),
    )
    files_root: str | None = Field(  # was Path | None
        default=None,
    )
    # ... other fields unchanged ...

    @model_validator(mode="after")
    def _default_files_root(self) -> PipelineConfig:
        """Derive files_root from delta_root when not explicitly set."""
        if self.files_root is None:
            # Was: self.delta_root.parent / "files" (Path semantics)
            object.__setattr__(
                self, "files_root", uri_join(uri_parent(self.delta_root), "files")
            )
        return self
```

`PipelineConfig` already uses the target field names (`delta_root`,
not `delta_root_path`), so only the types change from `Path` to `str`.

### Consumer update pattern

Every consumer that currently writes:

```python
ArtifactStore(runtime_env.delta_root_path)
```

becomes:

```python
ArtifactStore(runtime_env.delta_root, fs, storage_options)
```

(The `fs` and `storage_options` parameters were already added in doc 02.)

Every consumer that uses `runtime_env.staging_root_path` in string
contexts (passing to `StagingManager`, path arithmetic) switches to
`runtime_env.staging_root`.

### `working_root`, `failure_logs_root`, and `files_root` — local path handling

These paths are always local. `files_root` stores Artisan-managed
external files on the local filesystem — it's derived from
`delta_root` via the `_default_files_root` validator but only used
locally by executors. Consumers that need `Path` operations wrap them:

```python
# Sandbox creation (always local)
sandbox_base = Path(runtime_env.working_root)
sandbox_base.mkdir(parents=True, exist_ok=True)

# Failure log writes (always local)
log_dir = Path(runtime_env.failure_logs_root) / f"{step_number}_{op_name}"
log_dir.mkdir(parents=True, exist_ok=True)
```

### `_create_runtime_environment` changes

```python
def _create_runtime_environment(
    config: PipelineConfig, backend: BackendBase, is_curator: bool
) -> RuntimeEnvironment:
    return RuntimeEnvironment(
        delta_root=config.delta_root,        # was delta_root_path=
        staging_root=config.staging_root,    # was staging_root_path=
        working_root=(                       # was working_root_path=
            None if is_curator else config.working_root
        ),
        failure_logs_root=uri_join(          # was config.delta_root.parent / "logs" / "failures"
            uri_parent(config.delta_root), "logs", "failures"
        ),
        files_root=config.files_root,       # was files_root_path=config.files_root
        storage=config.storage,
        # ... other fields ...
    )
```

### `_build_execution_context` changes

```python
def _build_execution_context(
    *,
    delta_root: str,           # was delta_root_path: Path
    staging_root: str,         # was staging_root_path: Path
    ...
) -> ExecutionContext:
    fs = storage.filesystem()       # from StorageConfig
    storage_options = storage.delta_storage_options()
    artifact_store = ArtifactStore(delta_root, fs, storage_options)
    ...
```

### `.exists()` guards in inputs.py, step_tracker, etc.

Functions that guard `scan_delta` with `path.exists()` switch to
`fs.exists()`:

```python
# Before (path is Path)
if not executions_path.exists():
    return []

# After (path is str, fs available)
if not fs.exists(executions_path):
    return []
```

These functions gain `fs: AbstractFileSystem` as a parameter alongside
the existing `storage_options`.

---

## Scope

### Source files

| File | Change |
|------|--------|
| `schemas/execution/runtime_environment.py` | Rename `delta_root_path` → `delta_root`, `staging_root_path` → `staging_root`, `working_root_path` → `working_root`, `files_root_path` → `files_root`. Change `failure_logs_root` type from `Path \| None` to `str \| None` (name stays). Change all types from `Path` to `str`. |
| `schemas/orchestration/pipeline_config.py` | Change `delta_root`, `staging_root`, `working_root`, `files_root` types from `Path` to `str`. Migrate `_default_files_root` validator from `self.delta_root.parent / "files"` to `uri_join(uri_parent(self.delta_root), "files")`. |
| `schemas/execution/execution_context.py` | Owned by doc 02 — no changes in this doc. |
| `orchestration/engine/step_executor.py` | Update all field references (including `files_root_path` → `files_root`). Replace `config.delta_root / ...` with `uri_join(config.delta_root, ...)`. Replace `config.delta_root.parent / ...` with `uri_join(uri_parent(config.delta_root), ...)`. Update `check_cache_for_batch` and `_compact_step_tables` (`delta_root: str`, `staging_root: str`). |
| `orchestration/pipeline_manager.py` | Update `config.delta_root` usage (already correct name, now `str`). Replace `config.delta_root.parent / "logs"` with `uri_join(uri_parent(config.delta_root), "logs")`. Remove `Path()` wrapping in `create()` and `resume()` (lines 894-899) — `PipelineConfig` fields are now `str`, so pass through directly. |
| `execution/executors/creator.py` | `runtime_env.delta_root` replaces `.delta_root_path`. `runtime_env.files_root` replaces `.files_root_path`. `Path(runtime_env.working_root)` for sandbox (always-local). |
| `execution/executors/curator.py` | Same pattern. `files_root_path` → `files_root`. |
| `execution/executors/composite.py` | Same pattern. `files_root_path` → `files_root`. |
| `execution/context/builder.py` | `delta_root: str` and `staging_root: str` parameters. |
| `composites/base/composite_context.py` | Update field references. |
| `orchestration/engine/inputs.py` | `delta_root: str`, add `fs` parameter for `.exists()` guards. |
| `orchestration/engine/step_tracker.py` | `delta_root: str`, `fs` for `.exists()`. (Already has `storage_options` from doc 03.) |
| `orchestration/engine/dispatch.py` | `failure_logs_root` handling uses `Path()` wrapper for local ops. `staging_root: str` parameters on `_save_units`, `_patch_worker_logs`, `_find_staging_dir`. |
| `storage/cache/cache_lookup.py` | Path parameters → `str`. Add `fs` for `.exists()`. |
| `storage/io/staging_verification.py` | `staging_root: str` parameter. |
| `execution/staging/parquet_writer.py` | `_create_staging_path` receives `staging_root: str` (from `ExecutionContext`). |
| `execution/staging/recorder.py` | `failure_logs_root` handling uses `Path()` wrapper. |
| `orchestration/backends/base.py` | `capture_logs` signature: `staging_root: str`, `failure_logs_root: str \| None`. `create_dispatch_handle` signature: `log_folder: str \| None`, `staging_root: str \| None` (was `Path \| None`). |
| `orchestration/backends/local.py` | Update `capture_logs` and `create_dispatch_handle` signatures. |
| `orchestration/backends/slurm.py` | Update `capture_logs` and `create_dispatch_handle` signatures. |
| `orchestration/backends/slurm_intra.py` | Update `capture_logs` and `create_dispatch_handle` signatures. |
| `visualization/inspect.py` | `delta_root` parameter accepts `str` (already does `Path \| str`). Remove `Path()` coercion, add `fs` for `.exists()`. |
| `visualization/timing.py` | Same pattern. |
| `visualization/graph/macro.py` | Same pattern. |
| `visualization/graph/micro.py` | Same pattern. |
| `visualization/graph/stepper.py` | `delta_root: str`. Replace `delta_root.parent / "images"` with `uri_join(uri_parent(delta_root), "images")`. |
| `operations/curator/interactive_filter.py` | Update delta_root references. |
| `operations/curator/ingest_pipeline_step.py` | `source_delta_root` field type `Path` → `str`. |
| `utils/tutorial.py` | `TutorialEnv` fields `delta_root`, `staging_root`, `working_root` type `Path` → `str`. |

### Test files

| File | Change |
|------|--------|
| `tests/artisan/execution/test_executor_creator.py` | Update `RuntimeEnvironment` construction: `delta_root=str(path)`, `files_root=str(path)`, etc. |
| `tests/artisan/execution/test_creator_lifecycle.py` | Update `RuntimeEnvironment` construction: `delta_root_path=` → `delta_root=`, etc. |
| `tests/artisan/execution/test_files_dir_threading.py` | Update `RuntimeEnvironment` construction: `files_root_path=` → `files_root=`. |
| `tests/artisan/composites/test_composite_context.py` | Update `RuntimeEnvironment` mock attribute names (`working_root_path` → `working_root`, `files_root_path` → `files_root`, etc.). |
| `tests/artisan/orchestration/test_step_executor.py` | Update `PipelineConfig` construction: `delta_root=str(path)`. |
| `tests/artisan/orchestration/test_pipeline_manager.py` | Same. |
| `tests/artisan/orchestration/test_signal_handling.py` | Same. |
| `tests/artisan/orchestration/test_orchestration_api.py` | Same. |
| `tests/artisan/orchestration/test_pipeline_helpers.py` | Same. |
| `tests/artisan/schemas/test_runtime_environment.py` | Update `files_root_path=` → `files_root=`. Verify `str` fields accept URIs. |
| `tests/artisan/schemas/test_pipeline_config_files_root.py` | Update `delta_root=` to `str`, verify `_default_files_root` validator uses `uri_parent`/`uri_join`. |
| `tests/artisan/orchestration/backends/test_base.py` | Update `capture_logs` and `create_dispatch_handle` signatures in test implementations. |
| `tests/artisan/execution/test_failure_logs.py` | Update `failure_logs_root=str(tmp_path)`. |
| `tests/integration/conftest.py` | Update `pipeline_env` and `dual_pipeline_env` fixtures — `dict[str, Path]` values become `str`. This is the single point of update for ~18 integration test files. |

---

## Testing

| Test file | Coverage |
|-----------|----------|
| All existing unit tests | Pass after mechanical rename. `str(tmp_path)` replaces `tmp_path` in `PipelineConfig` and `RuntimeEnvironment` construction. |
| All existing integration tests | Same — `str()` wrapping of path fixtures. |
| `tests/artisan/schemas/test_runtime_environment.py` | Verify `str` fields accept both local paths and URIs. Verify `working_root` and `failure_logs_root` work with `Path()` wrapping. |

No new test files — the rename is validated by the existing test suite
running green.

---

## Open Questions

None — the `uri_join`/`uri_parent` utilities (doc 01) resolve the
`Path /` operator concern, and visualization functions already accept
`Path | str` so the type change is transparent to users.

---

## Related Docs

- `cloud-deployment.md` — high-level cloud deployment plan; after this PR the codebase is cloud-ready
- `01-storage-config.md` — provides `StorageConfig`
- `02-storage-layer.md` — storage classes already accept `str` + `fs`
- `03-delta-read-sites.md` — delta calls already have `storage_options`
