# Design: Storage Layer Migration

**Date:** 2026-04-04
**Status:** Draft

---

## Problem

The five core storage classes — `StagingArea`, `StagingManager`,
`DeltaCommitter`, `ArtifactStore`, `ProvenanceStore` — hardcode
`pathlib.Path` operations: `Path.mkdir()`, `Path.exists()`,
`Path.rglob()`, `shutil.rmtree()`, `df.write_parquet(path)`. These
don't work with S3 or GCS.

This PR migrates these classes to accept an `fsspec.AbstractFileSystem`
instance. Local runs pass `LocalFileSystem` (same behavior as today).
Cloud runs pass `S3FileSystem` or `GCSFileSystem`. The API surface of
each class is unchanged — only the I/O backend is parameterized.

Depends on doc 01 (StorageConfig) for the `StorageConfig.filesystem()`
and `StorageConfig.delta_storage_options()` methods.

---

## Prior Art Survey

### `StagingArea` (`storage/io/staging.py:32`)

Constructor accepts `staging_dir: Path | str`, coerces to `Path`.
Uses `Path.mkdir()`, `Path.exists()`, `shutil.rmtree()`,
`df.write_parquet(path)`, `pl.read_parquet(path)`. Returns `Path`
from `stage_dataframe()`.

Changes: accept `fs: AbstractFileSystem`, replace `Path` ops with
`fs.*` calls, return `str` URIs instead of `Path`.

### `StagingManager` (`storage/io/staging.py:162`)

Constructor accepts `staging_dir: Path | str`, coerces to `Path`.
Uses `Path.exists()`, `Path.iterdir()`, `Path.rglob()`,
`shutil.rmtree()`, `pl.read_parquet(path)`. Has
`_invalidate_nfs_cache()` using `os.listdir()`.

Changes: accept `fs: AbstractFileSystem`, replace path ops with
`fs.exists()`, `fs.ls()`, `fs.glob()`, `fs.rm()`. NFS cache
invalidation becomes a no-op for non-local filesystems.

### `DeltaCommitter` (`storage/io/commit.py:58`)

Constructor accepts `delta_base_path: Path | str` and
`staging_dir: Path | str`, internally creates its own
`StagingManager`. Uses `Path.exists()` for table guards,
`pl.scan_delta(str(path))`, `df.write_delta(str(path))`,
`DeltaTable(str(path))`.

Changes: accept `fs: AbstractFileSystem`,
`storage_options: dict | None`, and receive a `StagingManager`
instead of creating one internally. Pass `storage_options` to all
delta-rs calls. Use `fs.exists()` for table guards.

### `ArtifactStore` (`storage/core/artifact_store.py:22`)

Constructor accepts `base_path: Path | str`, coerces to `Path`.
Uses `Path.exists()` as table guard, `pl.scan_delta(str(path))` for
all reads. Creates `ProvenanceStore(self.base_path)` lazily.

Changes: accept `fs: AbstractFileSystem`,
`storage_options: dict | None`. Pass to `pl.scan_delta()`. Pass both
to `ProvenanceStore`.

### `ProvenanceStore` (`storage/core/provenance_store.py:17`)

Same pattern as `ArtifactStore`. Constructor accepts
`base_path: Path | str`. Uses `Path.exists()` and
`pl.scan_delta(str(path))`.

Changes: same as `ArtifactStore`.

### `shard_path()` (`utils/path.py:133`)

Pure path arithmetic: `root: Path` → `Path`. Uses `/` operator.
No I/O. Becomes `shard_uri()`: `root: str` → `str`, using string
concatenation.

### `_create_staging_path()` (`execution/staging/parquet_writer.py:44`)

Calls `shard_path()` then `Path.mkdir()`. Returns `Path`.

Changes: call `shard_uri()` then `fs.makedirs()`. Return `str`.

### `_sync_staging_to_nfs()` (`execution/staging/parquet_writer.py:27`)

NFS-specific `os.fsync()` calls. Gated on `shared_filesystem` trait.

Changes: gate on `StorageConfig.is_local` instead — cloud
filesystems don't have NFS cache coherency issues.

### Executor callers (`execution/executors/creator.py`, `curator.py`, `composite.py`)

These construct `ArtifactStore(runtime_env.delta_root_path)` and pass
`staging_root_path` to the parquet writer. They're the plumbing that
creates `fs` from `StorageConfig` and threads it through.

### `_build_execution_context` (`execution/context/builder.py`)

Accepts `delta_root_path: Path` and `staging_root_path: Path`, creates
`ArtifactStore(delta_root_path)`. Needs to pass `fs` and
`storage_options` through.

---

## Design

### Pattern: fs-parameterized constructors

Every storage class gains an `fs` parameter. The pattern is consistent:

```python
class StagingArea:
    def __init__(
        self,
        staging_dir: str,
        fs: AbstractFileSystem,
        batch_id: str | None = None,
        worker_id: int = 0,
    ):
        self._fs = fs
        self.staging_dir = staging_dir
        # ...
```

Local callers pass `LocalFileSystem()`. Cloud callers pass
`S3FileSystem()` or `GCSFileSystem()`. Test callers pass
`MemoryFileSystem()`.

### StagingArea changes

| Current | Proposed |
|---------|----------|
| `self.staging_dir = Path(staging_dir)` | `self.staging_dir = staging_dir` (str) |
| `self._batch_dir = self.staging_dir / self.batch_id` | `self._batch_dir = f"{self.staging_dir}/{self.batch_id}"` |
| `self._batch_dir.mkdir(parents=True, exist_ok=True)` | `self._fs.makedirs(self._batch_dir, exist_ok=True)` |
| `parquet_path = self._batch_dir / f"{table_name}.parquet"` | `parquet_uri = f"{self._batch_dir}/{table_name}.parquet"` |
| `df.write_parquet(parquet_path, compression="zstd")` | `with self._fs.open(parquet_uri, "wb") as f: df.write_parquet(f, compression="zstd")` |
| `pl.read_parquet(parquet_path)` | `with self._fs.open(parquet_uri, "rb") as f: pl.read_parquet(f)` |
| `parquet_path.exists()` | `self._fs.exists(parquet_uri)` |
| `shutil.rmtree(self._batch_dir)` | `self._fs.rm(self._batch_dir, recursive=True)` |
| Returns `Path` | Returns `str` |

### StagingManager changes

| Current | Proposed |
|---------|----------|
| `self.staging_dir = Path(staging_dir)` | `self.staging_dir = staging_dir` (str) |
| `self.staging_dir.exists()` | `self._fs.exists(self.staging_dir)` |
| `self.staging_dir.iterdir()` | `self._fs.ls(self.staging_dir, detail=False)` |
| `step_dir.rglob(f"{table_name}.parquet")` | `self._fs.glob(f"{step_dir}/**/{table_name}.parquet")` |
| `pl.read_parquet(f)` | `with self._fs.open(uri, "rb") as f: pl.read_parquet(f)` |
| `shutil.rmtree(step_dir)` | `self._fs.rm(step_dir, recursive=True)` |
| `_invalidate_nfs_cache(directory)` | No-op for non-local; `os.listdir()` for local |
| Returns `list[Path]` | Returns `list[str]` |

### DeltaCommitter changes

```python
class DeltaCommitter:
    def __init__(
        self,
        delta_base_path: str,
        staging_manager: StagingManager,
        fs: AbstractFileSystem,
        storage_options: dict[str, str] | None = None,
    ):
        self.delta_base_path = delta_base_path
        self.staging_manager = staging_manager
        self._fs = fs
        self._storage_options = storage_options or {}
```

Key changes:
- Receives `StagingManager` instead of creating one internally — the
  orchestrator shares a single instance.
- `table_path.exists()` → `self._fs.exists(table_uri)`.
- `df.write_delta(str(table_path))` →
  `df.write_delta(table_uri, storage_options=self._storage_options)`.
- `pl.scan_delta(str(table_path))` →
  `pl.scan_delta(table_uri, storage_options=self._storage_options)`.
- `DeltaTable(str(table_path))` →
  `DeltaTable(table_uri, storage_options=self._storage_options)`.

### ArtifactStore / ProvenanceStore changes

```python
class ArtifactStore:
    def __init__(
        self,
        base_path: str,
        fs: AbstractFileSystem,
        storage_options: dict[str, str] | None = None,
    ):
        self.base_path = base_path
        self._fs = fs
        self._storage_options = storage_options or {}
```

- `_table_path()` returns `str` (URI), not `Path`.
- `table_path.exists()` → `self._fs.exists(table_uri)`.
- `pl.scan_delta(str(table_path))` →
  `pl.scan_delta(table_uri, storage_options=self._storage_options)`.
- `ProvenanceStore` receives `fs` and `storage_options` from
  `ArtifactStore`.

### `shard_path()` → `shard_uri()`

```python
def shard_uri(
    root: str,
    execution_run_id: str,
    step_number: int | None = None,
    operation_name: str | None = None,
) -> str:
    """Create sharded URI from execution_run_id."""
    if step_number is not None:
        step_segment = (
            step_dir_name(step_number, operation_name)
            if operation_name
            else str(step_number)
        )
        return f"{root}/{step_segment}/{execution_run_id[:2]}/{execution_run_id[2:4]}/{execution_run_id}"
    return f"{root}/{execution_run_id[:2]}/{execution_run_id[2:4]}/{execution_run_id}"
```

Pure string concatenation. No I/O. `shard_path()` is kept as a
deprecated alias during this PR to avoid touching downstream call sites
that are renamed in doc 04.

### parquet_writer changes

`_create_staging_path()` gains an `fs` parameter:

```python
def _create_staging_path(
    staging_root: str,
    execution_run_id: str,
    step_number: int,
    operation_name: str | None,
    fs: AbstractFileSystem,
) -> str:
    staging_uri = shard_uri(staging_root, execution_run_id, step_number, operation_name)
    fs.makedirs(staging_uri, exist_ok=True)
    return staging_uri
```

`_sync_staging_to_nfs()` is gated on `StorageConfig.is_local`:

```python
def _sync_staging_if_needed(staging_uri: str, storage: StorageConfig) -> None:
    if not storage.is_local:
        return
    _sync_staging_to_nfs(Path(staging_uri))
```

### How components get the fs instance

```
step_executor._create_runtime_environment()
    RuntimeEnvironment carries StorageConfig

Worker (execute_unit_task → run_creator_flow / run_curator_flow):
    fs = runtime_env.storage.filesystem()
    storage_options = runtime_env.storage.delta_storage_options()
    ArtifactStore(str(runtime_env.delta_root_path), fs, storage_options)
    StagingArea(staging_dir_str, fs)

Orchestrator (step_executor._commit_and_compact):
    fs = config.storage.filesystem()
    storage_options = config.storage.delta_storage_options()
    StagingManager(str(config.staging_root), fs)
    DeltaCommitter(str(config.delta_root), staging_manager, fs, storage_options)
```

The `fs` instance is short-lived — created per step on the
orchestrator, per execution on the worker.

---

## Scope

| File | Change |
|------|--------|
| `storage/io/staging.py` | `StagingArea` and `StagingManager` accept `fs` parameter, replace all `Path` ops with `fs.*` calls. Return `str` instead of `Path`. |
| `storage/io/commit.py` | `DeltaCommitter` accepts `fs`, `storage_options`, and `StagingManager`. Pass `storage_options` to all delta-rs calls. Use `fs.exists()`. |
| `storage/core/artifact_store.py` | `ArtifactStore` accepts `fs`, `storage_options`. Pass `storage_options` to `pl.scan_delta()`. Use `fs.exists()`. Thread to `ProvenanceStore`. |
| `storage/core/provenance_store.py` | `ProvenanceStore` accepts `fs`, `storage_options`. Same pattern as `ArtifactStore`. |
| `utils/path.py` | Add `shard_uri()` (string-based). Keep `shard_path()` as deprecated alias. |
| `execution/staging/parquet_writer.py` | `_create_staging_path` accepts `fs`, uses `shard_uri`. `_sync_staging_to_nfs` gated on `StorageConfig.is_local`. All staging writes use `fs.open()`. |
| `execution/staging/recorder.py` | Pass `fs` through to parquet_writer calls. Failure log writes stay local (`Path`). |
| `execution/context/builder.py` | `_build_execution_context` accepts `fs`, `storage_options`, passes to `ArtifactStore`. |
| `execution/executors/creator.py` | Create `fs` from `runtime_env.storage`, pass to builder and parquet_writer. |
| `execution/executors/curator.py` | Same — create `fs`, pass through. |
| `execution/executors/composite.py` | Same — create `fs`, pass through. |
| `composites/base/composite_context.py` | Create `fs` from `runtime_env.storage`, pass to storage classes. |
| `orchestration/engine/step_executor.py` | Create `fs` and `StagingManager` for `DeltaCommitter`. Pass `storage_options`. |

---

## Testing

| Test file | Coverage |
|-----------|----------|
| `tests/artisan/storage/test_staging.py` | `StagingArea` and `StagingManager` with `MemoryFileSystem`: write/read round-trip, glob discovery, cleanup, append-by-concat, empty DataFrame no-op. Existing `tmp_path` tests updated to pass `LocalFileSystem`. |
| `tests/artisan/storage/test_commit.py` | `DeltaCommitter` with `LocalFileSystem` (Delta Lake needs real files). Verify `storage_options` passed to `write_delta`. Receives `StagingManager` instead of creating one. |
| `tests/artisan/storage/test_artifact_store.py` | `ArtifactStore` with `LocalFileSystem`. Verify `storage_options` passed to `scan_delta`. |
| `tests/artisan/storage/test_provenance_store.py` | Same pattern — `fs` and `storage_options` threaded through. |
| `tests/artisan/utils/test_path.py` | `shard_uri` produces correct string paths. Same test cases as existing `shard_path` tests. |
| `tests/artisan/execution/test_parquet_writer.py` | `_create_staging_path` with `MemoryFileSystem`. `_sync_staging_if_needed` is no-op when `is_local` is False. |

New `memory_fs` fixture:

```python
@pytest.fixture
def memory_fs():
    fs = fsspec.filesystem("memory")
    yield fs
    fs.store.clear()
```

---

## Open Questions

- **fsspec glob prefix behavior.** `S3FileSystem.glob()` returns paths
  without the protocol prefix (`bucket/path` not `s3://bucket/path`),
  while `LocalFileSystem.glob()` returns absolute paths. Since glob
  results are passed to `fs.open()` on the same filesystem instance,
  this should be fine. Verify during implementation.

---

## Related Docs

- `cloud-storage-design.md` — full storage abstraction design (this is Phase 2)
- `01-storage-config.md` — prerequisite: `StorageConfig` provides `filesystem()` and `delta_storage_options()`
- `03-delta-read-sites.md` — the remaining `scan_delta`/`write_delta` call sites outside the storage layer
- `04-runtime-env-rename.md` — the `Path` → `str` field rename that follows
