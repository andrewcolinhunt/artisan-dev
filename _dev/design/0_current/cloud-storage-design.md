# Cloud Storage Design

The storage abstraction for cloud deployment. Uses fsspec as the unified
storage layer so that staging, commit, and store access code work
identically across local, S3, and GCS backends.

---

## Design Principles

- **One mental model.** All storage operations go through `fsspec`. Local
  runs use `LocalFileSystem`. Cloud runs use `S3FileSystem` or
  `GCSFileSystem`. The staging code, commit code, and store access code
  are the same regardless of backend.

- **Libraries do the heavy lifting.** Polars and delta-rs already support
  S3/GCS URIs natively. The framework passes URI strings and
  `storage_options` dicts. No custom I/O code for cloud.

- **Worker sandbox stays local.** Workers always use local `/tmp` for
  their sandbox (materialized inputs, execute dir, postprocess dir).
  Operations never see the storage abstraction.

---

## StorageConfig

**File:** `src/artisan/schemas/execution/storage_config.py`

A serializable config object that creates fsspec filesystem instances
and provides `storage_options` dicts for Polars/delta-rs.

```python
class StorageConfig(BaseModel):
    """Storage backend configuration.

    Credentials are NOT stored here — they come from the execution
    environment (IAM roles, env vars, service accounts). This config
    carries only the protocol and non-sensitive options (region,
    endpoint, bucket).
    """

    model_config = {"frozen": True}

    protocol: str = "file"
    options: dict[str, str] = Field(default_factory=dict)

    @property
    def is_local(self) -> bool:
        """Whether this config targets a local filesystem."""
        return self.protocol == "file"

    def filesystem(self) -> AbstractFileSystem:
        """Create an fsspec filesystem instance."""
        return fsspec.filesystem(self.protocol, **self.options)

    def delta_storage_options(self) -> dict[str, str] | None:
        """Storage options for deltalake-rs / Polars.

        Returns None for local filesystem (no options needed).
        """
        if self.is_local:
            return None
        # delta-rs uses AWS_* env vars by default for S3
        # only non-default options need to be passed explicitly
        return dict(self.options)
```

**Why `BaseModel`?** `StorageConfig` is embedded inside `RuntimeEnvironment`
(a Pydantic model). Using `BaseModel` gives native Pydantic
serialization/validation and matches every other config class in the project.

**Why not pass the fsspec instance directly?** Because the config must be
serializable (it travels in RuntimeEnvironment to workers). The `fs`
instance is created on-demand by each component.

**Credentials:** Cloud backends use environment-based credentials. AWS
containers get IAM roles. GCP pods get service accounts. The StorageConfig
carries the protocol and region — credentials are resolved from the
environment by fsspec and delta-rs automatically.

---

## RuntimeEnvironment

Path fields become URI strings. A StorageConfig is added.

```python
class RuntimeEnvironment(BaseModel):
    # Paths are now URI strings
    delta_root: str             # "/data/delta" or "s3://bucket/delta"
    staging_root: str           # "/data/staging" or "s3://bucket/staging"
    working_root: str | None    # always local: "/tmp/artisan"

    failure_logs_root: str | None

    # Storage configuration
    storage: StorageConfig

    # Flags
    preserve_staging: bool = False
    preserve_working: bool = False

    # Backend traits (unchanged)
    worker_id_env_var: str | None = None
    shared_filesystem: bool = False
    compute_backend_name: str = "local"
```

**Migration:** Four fields are renamed (dropping the `_path` suffix) and
change from `Path` to `str`:

- `delta_root_path: Path` → `delta_root: str`
- `staging_root_path: Path` → `staging_root: str`
- `working_root_path: Path | None` → `working_root: str | None`
- `failure_logs_root: Path | None` → `failure_logs_root: str | None`

All consumers that currently do `Path(runtime_env.delta_root_path)` (or
the staging/working equivalents) switch to `runtime_env.delta_root` (a
string). `working_root` stays as a local path string — it's always on
the worker's local filesystem. `failure_logs_root` stays local — it's
written by workers on the same machine as the orchestrator (local) or
on a shared NFS (SLURM).

---

## StagingArea (worker-side writes)

Parameterized by an fsspec filesystem. Same API, different I/O backend.

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
        self.batch_id = batch_id or f"w{worker_id}_{uuid.uuid4().hex[:12]}"
        self._batch_dir = f"{staging_dir}/{self.batch_id}"
        self._fs.makedirs(self._batch_dir, exist_ok=True)
        self._staged_tables: set[str] = set()

    def stage_dataframe(self, df: pl.DataFrame, table_name: str) -> str:
        """Write DataFrame to staged Parquet. Returns URI."""
        if df.is_empty():
            return f"{self._batch_dir}/{table_name}.parquet"

        parquet_uri = f"{self._batch_dir}/{table_name}.parquet"

        if table_name in self._staged_tables and self._fs.exists(parquet_uri):
            with self._fs.open(parquet_uri, "rb") as f:
                existing = pl.read_parquet(f)
            df = pl.concat([existing, df], rechunk=True)

        with self._fs.open(parquet_uri, "wb") as f:
            df.write_parquet(f, compression="zstd")

        self._staged_tables.add(table_name)
        return parquet_uri

    def stage_artifacts(self, artifacts_by_table: dict[str, pl.DataFrame]) -> None:
        """Stage multiple artifact DataFrames by table name."""
        for table_name, df in artifacts_by_table.items():
            self.stage_dataframe(df, table_name)

    def get_staged_file(self, table_name: str) -> str | None:
        """Return URI of a staged parquet file, or None."""
        uri = f"{self._batch_dir}/{table_name}.parquet"
        return uri if self._fs.exists(uri) else None

    def list_staged_tables(self) -> list[str]:
        """Return table names that have been staged."""
        return sorted(self._staged_tables)

    def cleanup(self) -> None:
        if self._fs.exists(self._batch_dir):
            self._fs.rm(self._batch_dir, recursive=True)
        self._staged_tables.clear()

    def __enter__(self) -> StagingArea:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the staging context, preserving files for debugging on error."""
        if exc_type is None:
            # No exception — cleanup is handled by orchestrator after commit
            pass
        # On exception, leave staging files for debugging
```

**What changes:** `Path` → `str`, `path.mkdir()` → `fs.makedirs()`,
`df.write_parquet(path)` → `df.write_parquet(fs.open(uri, "wb"))`,
`path.exists()` → `fs.exists()`, `shutil.rmtree()` → `fs.rm(recursive=True)`.

**What stays the same:** The staging area API. The Parquet compression.
The append-by-concat logic. The batch_id scheme. The table tracking.
The `__exit__` behavior (preserves files for debugging on error,
defers cleanup to orchestrator on success).

---

## StagingManager (orchestrator-side reads)

Same pattern — parameterized by fsspec.

```python
class StagingManager:
    def __init__(self, staging_dir: str, fs: AbstractFileSystem):
        self._fs = fs
        self.staging_dir = staging_dir

    def get_staged_files_for_table(
        self,
        table_name: str,
        step_number: int | None = None,
        operation_name: str | None = None,
    ) -> list[str]:
        if not self._fs.exists(self.staging_dir):
            return []

        if step_number is not None:
            step_dir = f"{self.staging_dir}/{step_dir_name(step_number, operation_name)}"
            if not self._fs.exists(step_dir):
                return []
            return self._fs.glob(f"{step_dir}/**/{table_name}.parquet")

        return self._fs.glob(f"{self.staging_dir}/**/{table_name}.parquet")

    def read_all_staged_for_table(self, table_name: str, **kwargs) -> pl.DataFrame | None:
        files = self.get_staged_files_for_table(table_name, **kwargs)
        if not files:
            return None

        dfs = []
        for uri in files:
            try:
                with self._fs.open(uri, "rb") as f:
                    dfs.append(pl.read_parquet(f))
            except Exception as exc:
                logger.warning("Skipping corrupted staging file %s: %s", uri, exc)

        if not dfs:
            return None
        return pl.concat(dfs, rechunk=True)

    def cleanup_step(self, step_number: int, operation_name: str | None = None) -> None:
        step_dir = f"{self.staging_dir}/{step_dir_name(step_number, operation_name)}"
        if self._fs.exists(step_dir):
            self._fs.rm(step_dir, recursive=True)
```

**NFS cache invalidation drops out naturally.** The current code has
`_invalidate_nfs_cache()` calls scattered through the manager. With
fsspec, these are only relevant for `LocalFileSystem` on NFS. The staging
verification layer (separate from the manager) handles NFS consistency.
The manager itself doesn't need NFS-specific code — `fs.glob()` and
`fs.exists()` behave correctly for each filesystem.

---

## ArtifactStore / ProvenanceStore

These use `pl.scan_delta()` and `pl.read_parquet()`, which already
support URI strings. The change is minimal: stop converting to `Path`,
stop calling `.exists()` via `pathlib`.

```python
class ArtifactStore:
    def __init__(self, base_path: str, fs: AbstractFileSystem, storage_options: dict | None = None):
        self._base = base_path
        self._fs = fs
        self._storage_options = storage_options or {}

    def _table_uri(self, table: str) -> str:
        return f"{self._base}/{table}"

    def get_artifact(self, artifact_id: str, artifact_type: str) -> Artifact | None:
        table_path = self._table_uri(ArtifactTypeDef.get(artifact_type).table_path)
        if not self._fs.exists(table_path):
            return None
        # pl.scan_delta already accepts URI strings + storage_options
        lf = pl.scan_delta(table_path, storage_options=self._storage_options)
        df = lf.filter(pl.col("artifact_id") == artifact_id).collect()
        ...
```

**Key insight:** `pl.scan_delta("s3://bucket/delta/artifacts/data",
storage_options={...})` works today. We just need to pass the URI and
options instead of `str(Path(...))`. Both `scan_delta` and `write_delta`
accept `storage_options` as a top-level keyword argument.

The `fs` instance is used only for `.exists()` guards. Polars and
delta-rs handle the actual Parquet/Delta I/O through their own S3
clients.

---

## DeltaCommitter

Reads staged Parquet via StagingManager (which uses fsspec). Writes to
Delta Lake via URI strings (delta-rs native).

```python
class DeltaCommitter:
    def __init__(
        self,
        delta_base_path: str,
        staging_manager: StagingManager,
        fs: AbstractFileSystem,
        storage_options: dict | None = None,
    ):
        self._delta_base = delta_base_path
        self._staging = staging_manager
        self._fs = fs
        self._storage_options = storage_options or {}

    def _table_uri(self, table: str) -> str:
        return f"{self._delta_base}/{table}"

    def commit_table(self, table: str, step_number: int, ...) -> None:
        staged_df = self._staging.read_all_staged_for_table(table, step_number=step_number)
        if staged_df is None:
            return

        table_uri = self._table_uri(table)
        if self._fs.exists(table_uri):
            staged_df.write_delta(
                table_uri, mode="append",
                storage_options=self._storage_options,
                delta_write_options={
                    "writer_properties": DEFAULT_WRITER_PROPERTIES,
                    "schema_mode": "merge",
                },
            )
        else:
            staged_df.write_delta(
                table_uri, mode="overwrite",
                storage_options=self._storage_options,
                delta_write_options={
                    "writer_properties": DEFAULT_WRITER_PROPERTIES,
                },
            )
```

**What changes:** `Path` → `str`, `str(table_path)` → `table_uri`,
`.exists()` → `fs.exists()`. The constructor now receives a
`StagingManager` instead of a `staging_dir` string (currently
`DeltaCommitter` creates the `StagingManager` internally). This allows
the orchestrator to share a single `StagingManager` instance with other
components. The core commit logic (read staged, write delta, dedup,
compact) is unchanged.

---

## parquet_writer (worker-side staging writes)

The recorder and parquet_writer create staging directories and write
Parquet files. They receive an `fs` instance.

```python
def _create_staging_path(
    staging_root: str,
    execution_run_id: str,
    step_number: int,
    operation_name: str | None,
    fs: AbstractFileSystem,
) -> str:
    """Create and return the sharded staging directory."""
    staging_uri = shard_uri(staging_root, execution_run_id, step_number, operation_name)
    fs.makedirs(staging_uri, exist_ok=True)
    return staging_uri
```

`shard_path()` in `utils/path.py` becomes `shard_uri()` (same file) —
pure string concatenation (it's already pure path arithmetic, no I/O).
Takes `root: str` instead of `root: Path`, returns `str` instead of
`Path`.

The NFS fsync function becomes conditional on `StorageConfig.is_local`:

```python
def _sync_staging_if_needed(staging_uri: str, storage: StorageConfig) -> None:
    """Flush staged files to NFS. No-op for non-local filesystems."""
    if not storage.is_local:
        return
    _sync_staging_to_nfs(Path(staging_uri))
```

---

## Staging Verification

NFS staging verification is inherently local-filesystem-specific. It
stays as-is but is gated on the filesystem type:

```python
def await_staging_files(
    staging_root: str,
    execution_run_ids: list[str],
    storage: StorageConfig,
    timeout_seconds: float,
    ...
) -> None:
    """Wait for staging files to appear. Only runs for local FS on NFS."""
    if not storage.is_local:
        return  # S3/GCS don't have cache coherency issues
    # ... existing NFS verification code, using Path(staging_root) ...
```

---

## Unit Transport (out of scope)

How ExecutionUnits get to workers (pickle files, `.map()` arguments,
object stores) is handled by the DispatchHandle abstraction, designed
separately in `dispatch-handle.md`. This design covers storage only —
the filesystem layer that workers write staged artifacts to and the
orchestrator reads them from. The two designs are independent and can
land in either order.

---

## Worker Sandbox

Unchanged. Workers always use local `/tmp`. Operations never see
the storage abstraction.

```python
def create_sandbox(working_root: str, execution_run_id: str, ...) -> Path:
    """Create local sandbox directory. Always on local filesystem."""
    sandbox_path = Path(working_root) / ...
    sandbox_path.mkdir(parents=True, exist_ok=True)
    return sandbox_path
```

`working_root` is always a local path string. The sandbox uses
`pathlib.Path` internally — no fsspec here.

---

## How Components Get the fs Instance

Every component needs an `AbstractFileSystem` instance. Where does it
come from?

```
PipelineConfig
    └── StorageConfig (protocol + options)

PipelineManager._dispatch_step()
    ├── Creates RuntimeEnvironment with StorageConfig
    ├── Creates fs = storage_config.filesystem()
    ├── Creates StagingManager(staging_root, fs)
    └── Creates DeltaCommitter(delta_root, staging_manager, fs, storage_options)

Worker (inside execute_unit_task):
    ├── Creates fs = runtime_env.storage.filesystem()
    ├── Creates StagingArea(staging_root, fs)
    └── Creates ArtifactStore(delta_root, fs, storage_options)
        (for input hydration)
```

The fs instance is short-lived — created per step on the orchestrator,
per execution on the worker. No long-lived connections to manage.

---

## Future Optimization: Return-Value Staging

For serverless backends (Modal, Lambda), the fsspec path writes to S3
and reads back from S3. This adds ~100ms per execution unit in S3 API
latency. For most workloads this is fine. If it becomes a bottleneck,
workers could accumulate DataFrames in memory and return them through
the dispatch handle instead of writing to the filesystem.

This is deferred. The fsspec path is correct, uniform, and testable.
See `dispatch-handle.md` for the handle-side of this optimization.

---

## What Changes vs What Stays the Same

### Changes

| Component | Current | Proposed |
|-----------|---------|----------|
| `RuntimeEnvironment` fields | `delta_root_path`, `staging_root_path`, `working_root_path` | `delta_root`, `staging_root`, `working_root` (drop `_path` suffix) |
| `RuntimeEnvironment` path types | `Path` | `str` (URI) |
| `RuntimeEnvironment` `failure_logs_root` | `Path \| None` | `str \| None` |
| `RuntimeEnvironment` | no storage config | `storage: StorageConfig` |
| `StagingArea.__init__` | `Path(staging_dir)` | `str` + `fs: AbstractFileSystem` |
| `StagingArea` I/O | `path.mkdir()`, `df.write_parquet(path)` | `fs.makedirs()`, `df.write_parquet(fs.open())` |
| `StagingManager.__init__` | `Path(staging_dir)` | `str` + `fs: AbstractFileSystem` |
| `StagingManager` I/O | `rglob()`, `pl.read_parquet(path)` | `fs.glob()`, `pl.read_parquet(fs.open())` |
| `DeltaCommitter.__init__` | `Path(delta_base_path)`, `staging_dir` (creates `StagingManager` internally) | `str` + `fs` + `storage_options` + receives `StagingManager` |
| `DeltaCommitter` writes | `df.write_delta(str(path))` | `df.write_delta(uri, storage_options=..., delta_write_options=...)` |
| `ArtifactStore.__init__` | `Path(base_path)` | `str` + `fs` + `storage_options` |
| `ArtifactStore` reads | `pl.scan_delta(str(path))` | `pl.scan_delta(uri, storage_options=...)` |
| `ProvenanceStore` | same as ArtifactStore | same pattern |
| All `pl.scan_delta` callers | `pl.scan_delta(str(path))` with no storage options | `pl.scan_delta(uri, storage_options=...)` — see migration inventory |
| All `write_delta` callers | `df.write_delta(str(path))` with no storage options | `df.write_delta(uri, storage_options=..., delta_write_options=...)` |
| `.exists()` guards | `table_path.exists()` | `fs.exists(uri)` |
| `parquet_writer` | `Path.mkdir()`, `df.write_parquet(path)` | `fs.makedirs()`, `df.write_parquet(fs.open())` |
| `recorder` failure log writes | `Path /`, `.mkdir()`, `.open("w")` | `fs.makedirs()`, `fs.open(uri, "w")` |
| NFS fsync (`_sync_staging_to_nfs`) | always runs for shared FS | gated on `StorageConfig.is_local` |
| Staging verification | NFS-specific polling | gated on `StorageConfig.is_local` |
| `shard_path()` | `root: Path` → `Path` | `shard_uri()`: `root: str` → `str` |

### Unchanged

| Component | Why |
|-----------|-----|
| Worker sandbox (`sandbox.py`) | Always local /tmp. Uses `pathlib.Path`. |
| Input materialization | Writes to sandbox (local). |
| Operation code (preprocess/execute/postprocess) | Never sees storage abstraction. |
| Content-addressed hashing | Hash by content, not location. |
| Cache keys | Based on artifact IDs + params, not paths. |
| Failure policies | Orthogonal to storage. |
| Provenance capture | Produces DataFrames, consumed by staging. |
| Composite execution | Runs operations, uses same staging. |
| PipelineManager API | `pipeline.run()`, `pipeline.submit()` — unchanged. |

---

## Dependency Impact

**fsspec:** Already a transitive dependency of `pyarrow` and `deltalake`.
Adding it as a direct dependency has zero install impact. It's a pure
Python package, ~300KB.

**s3fs / gcsfs:** Optional extras for S3/GCS support. `pip install
artisan[s3]` or `artisan[gcs]`. Not required for local/SLURM.

**Prefect:** Unchanged by this design. Prefect remains the dispatch
mechanism. Decoupling Prefect is covered by the DispatchHandle design
(`dispatch-handle.md`).

---

## Testing

fsspec provides `fsspec.filesystem("memory")` — a fully functional
in-memory filesystem. All storage tests can run against it:

```python
@pytest.fixture
def memory_fs():
    fs = fsspec.filesystem("memory")
    yield fs
    fs.store.clear()

def test_staging_area_writes(memory_fs):
    area = StagingArea("mem://staging", fs=memory_fs)
    area.stage_dataframe(df, "data")
    assert memory_fs.exists("mem://staging/w0_.../data.parquet")

def test_staging_manager_reads(memory_fs):
    # ... write via area, read via manager, same memory_fs
```

No mocks. No temp directories. No S3 credentials in CI. The memory
filesystem behaves like a real filesystem (supports glob, makedirs,
exists, open).

Existing tests that use `tmp_path` (pytest fixture) continue to work —
`LocalFileSystem` handles local paths. New storage tests should use
`memory_fs` to validate the abstraction. Test files follow the project
convention: `tests/artisan/storage/test_staging.py`,
`tests/artisan/storage/test_commit.py`, etc.

---

## Migration Inventory

Every file that needs changes, grouped by subsystem.

### `pl.scan_delta` call sites (44 total)

| Subsystem | File | Calls |
|-----------|------|-------|
| storage | `storage/core/artifact_store.py` | 4 |
| storage | `storage/core/provenance_store.py` | 14 |
| storage | `storage/cache/cache_lookup.py` | 3 |
| storage | `storage/io/commit.py` | 1 |
| orchestration | `orchestration/engine/step_tracker.py` | 4 |
| orchestration | `orchestration/engine/inputs.py` | 2 |
| visualization | `visualization/inspect.py` | 7 |
| visualization | `visualization/timing.py` | 2 |
| visualization | `visualization/graph/micro.py` | 3 |
| visualization | `visualization/graph/macro.py` | 1 |
| operations | `operations/curator/interactive_filter.py` | 3 |

All follow the same pattern: `pl.scan_delta(str(path))` → `pl.scan_delta(uri, storage_options=storage_options)`.

### `write_delta` call sites (8 total)

| Subsystem | File | Calls |
|-----------|------|-------|
| storage | `storage/io/commit.py` | 6 |
| orchestration | `orchestration/engine/step_tracker.py` | 2 |

All follow the same pattern: add `storage_options=...` alongside existing `delta_write_options`.

### `RuntimeEnvironment` path field consumers

| Subsystem | File | Fields accessed |
|-----------|------|----------------|
| schemas | `schemas/execution/runtime_environment.py` | definition (all four fields) |
| orchestration | `orchestration/engine/step_executor.py` | `delta_root_path`, `staging_root_path` |
| execution | `execution/executors/creator.py` | `working_root_path`, `delta_root_path`, `staging_root_path` |
| execution | `execution/executors/curator.py` | `delta_root_path`, `staging_root_path` |
| execution | `execution/executors/composite.py` | `delta_root_path`, `working_root_path`, `staging_root_path` |
| composites | `composites/base/composite_context.py` | `staging_root_path`, `delta_root_path` |

### `failure_logs_root` consumers (heavy Path API usage)

| Subsystem | File | Path operations |
|-----------|------|-----------------|
| orchestration | `orchestration/engine/dispatch.py` | `.iterdir()`, `.is_dir()`, `/`, `.exists()`, `.open("a")` |
| execution | `execution/staging/recorder.py` | `Path /`, `.mkdir()`, `.open("w")` |
| orchestration | `orchestration/backends/base.py` | type signature only |
| orchestration | `orchestration/backends/local.py` | type signature only |
| orchestration | `orchestration/backends/slurm.py` | type signature, pass-through |
| execution | `execution/executors/creator.py` | pass-through |
| execution | `execution/executors/curator.py` | pass-through |

### Summary

| Subsystem | Files affected |
|-----------|---------------|
| storage/ | 4 |
| orchestration/ | 6 |
| execution/ | 4 |
| composites/ | 1 |
| visualization/ | 4 |
| operations/ | 1 |
| utils/ | 1 (`path.py`) |
| **Total** | **21** |

---

## Rollout Strategy

The migration can be done incrementally. Each phase is independently
shippable and testable.

### Phase 1: Foundation

Add `StorageConfig` and wire it into `RuntimeEnvironment` with a default
that preserves current behavior (`protocol="file"`). Add `fsspec` as a
direct dependency. No consumers change yet — this is purely additive.

**Files:** `schemas/execution/storage_config.py` (new),
`schemas/execution/runtime_environment.py`, `pyproject.toml`

### Phase 2: Storage layer

Migrate the core storage classes that are constructed with explicit `fs`
parameters: `StagingArea`, `StagingManager`, `DeltaCommitter`,
`ArtifactStore`, `ProvenanceStore`. Update `shard_path` → `shard_uri`.
Update the parquet_writer and recorder. Update their callers in
`step_executor.py` and the executors to create `fs` from
`StorageConfig` and pass it through.

**Files:** `storage/` (4 files), `execution/` (4 files),
`orchestration/engine/step_executor.py`, `composites/base/composite_context.py`,
`utils/path.py`

### Phase 3: Delta read sites

Mechanically update all `pl.scan_delta(str(path))` calls to pass
`storage_options`. These are read-only consumers — low risk. Can be
done file-by-file.

**Files:** `storage/cache/cache_lookup.py`,
`orchestration/engine/step_tracker.py`, `orchestration/engine/inputs.py`,
`visualization/` (4 files), `operations/curator/interactive_filter.py`

### Phase 4: RuntimeEnvironment field rename

Rename `delta_root_path` → `delta_root`, `staging_root_path` →
`staging_root`, `working_root_path` → `working_root`. Change types
from `Path` to `str`. This is a cross-cutting rename that touches all
consumers — do it last so the earlier phases can land without a flag day.

**Files:** All 21 files in the migration inventory.

### RuntimeEnvironment is not persisted

`RuntimeEnvironment` is constructed fresh per pipeline run by
`step_executor.py` from `PipelineConfig` fields. It is not serialized
to disk, stored in a database, or cached across runs. The field rename
is a code-only change with no data migration required.
