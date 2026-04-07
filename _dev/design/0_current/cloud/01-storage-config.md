# Design: StorageConfig Foundation

**Date:** 2026-04-04
**Status:** Draft

---

## Problem

Every storage component in artisan operates on `pathlib.Path` objects.
`RuntimeEnvironment` carries five `Path` fields (`delta_root_path`,
`staging_root_path`, `working_root_path`, `failure_logs_root`,
`files_root_path`) and `PipelineConfig` mirrors them. This works for local and SLURM (shared NFS), but cloud backends
need S3/GCS URIs. Before any storage component can be made
cloud-capable, there must be a configuration object that tells the
framework which filesystem protocol to use and how to connect to it.

This PR introduces `StorageConfig` — a serializable config that creates
fsspec filesystem instances and provides `storage_options` dicts for
Polars/delta-rs. It wires into `RuntimeEnvironment` with a sensible
default so that existing local/SLURM behavior is unchanged.

This is purely additive — no consumers change yet. It lays the
foundation for the storage layer migration (doc 02), delta read site
updates (doc 03), and the field rename (doc 04).

---

## Prior Art Survey

### `RuntimeEnvironment` (`schemas/execution/runtime_environment.py`)

Frozen Pydantic `BaseModel` with `delta_root_path: Path`,
`staging_root_path: Path`, `working_root_path: Path | None`,
`failure_logs_root: Path | None`, `files_root_path: Path | None`,
plus flattened backend traits.
Created per step by `_create_runtime_environment()` in
`step_executor.py` from `PipelineConfig` fields. Serialized to
workers via pickle (Prefect). Not persisted to disk or database.

Note: `failure_logs_root` is derived from
`config.delta_root.parent / "logs" / "failures"` — this uses `Path`
semantics that won't work with cloud URIs. Doc 04 addresses this
using the `uri_parent`/`uri_join` utilities introduced below.

`StorageConfig` will be added as a new field with a default that
preserves current behavior.

### `PipelineConfig` (`schemas/orchestration/pipeline_config.py`)

Frozen Pydantic `BaseModel` with `delta_root: Path`,
`staging_root: Path`, `working_root: Path`,
`files_root: Path | None`. These are the user-facing entry point —
`_create_runtime_environment` copies them into `RuntimeEnvironment`.
`files_root` has a model validator that derives it from
`self.delta_root.parent / "files"` when not set (doc 04 migrates this
to `uri_parent`/`uri_join`). `PipelineConfig` will gain a `storage`
field that flows through to `RuntimeEnvironment`.

### fsspec (transitive dependency)

Already a transitive dependency of `prefect` and `dask`. Adding it as
a direct dependency has zero install-time impact. Provides
`AbstractFileSystem` interface with `LocalFileSystem` (default),
`S3FileSystem` (via `s3fs`), `GCSFileSystem` (via `gcsfs`), and
`MemoryFileSystem` (for testing).

### Polars / delta-rs `storage_options`

`pl.scan_delta()`, `df.write_delta()`, and `DeltaTable()` all accept a
`storage_options: dict` keyword argument for cloud credentials and
config. Currently no call site passes this argument — all use
`pl.scan_delta(str(path))` with no options. `StorageConfig` will
produce these dicts.

---

## Design

### StorageConfig

```python
class StorageConfig(BaseModel):
    """Storage backend configuration.

    Credentials are NOT stored here — they come from the execution
    environment (IAM roles, env vars, service accounts). This config
    carries only the protocol and non-sensitive options (region,
    endpoint, bucket).

    ``options`` uses fsspec-style keys natively (``endpoint_url``,
    ``region``, ``project``). The ``delta_storage_options()`` method
    translates these to the environment-variable-style keys that
    delta-rs expects (``AWS_ENDPOINT_URL``, ``AWS_REGION``, etc.).

    Args:
        protocol: fsspec protocol identifier. ``"file"`` for local
            filesystem, ``"s3"`` for S3, ``"gcs"`` for Google Cloud
            Storage.
        options: Non-sensitive fsspec constructor arguments. Values
            may be any type fsspec accepts (str, bool, int).
            Credentials come from the environment.
    """

    model_config = {"frozen": True}

    protocol: str = "file"
    options: dict[str, Any] = Field(default_factory=dict)

    @property
    def is_local(self) -> bool:
        """Whether this config targets a local filesystem."""
        return self.protocol == "file"

    def filesystem(self) -> AbstractFileSystem:
        """Create an fsspec filesystem instance.

        Returns:
            Configured filesystem for the protocol.
        """
        return fsspec.filesystem(self.protocol, **self.options)

    def delta_storage_options(self) -> dict[str, str] | None:
        """Storage options dict for Polars/delta-rs.

        Translates fsspec-style option keys to the
        environment-variable-style keys that delta-rs expects, and
        stringifies all values.

        Returns:
            Options dict for cloud protocols, None for local (no
            options needed — delta-rs handles local paths natively).
        """
        if self.is_local:
            return None

        # NOTE: Verify GCS key mappings against delta-rs docs during
        # implementation. The correct delta-rs key for GCP project
        # may differ from the fsspec kwarg name.
        _DELTA_KEY_MAP: dict[str, str] = {
            "endpoint_url": "AWS_ENDPOINT_URL",
            "region": "AWS_REGION",
            "project": "GOOGLE_CLOUD_PROJECT",
        }
        return {
            _DELTA_KEY_MAP.get(k, k): str(v)
            for k, v in self.options.items()
        }
```

**Why `BaseModel`?** `StorageConfig` is embedded inside
`RuntimeEnvironment` (a Pydantic model). Using `BaseModel` gives native
serialization and matches every other config class in the project.

**Why not pass the `fs` instance directly?** Because the config must be
serializable — it travels inside `RuntimeEnvironment` to workers via
pickle. The `fs` instance is created on-demand by each component.

**Credentials:** Cloud backends use environment-based credentials. AWS
containers get IAM roles. GCP pods get service accounts. `StorageConfig`
carries protocol and region — credentials are resolved from the
environment by fsspec and delta-rs automatically.

### RuntimeEnvironment changes

```python
class RuntimeEnvironment(BaseModel):
    # ... existing fields unchanged ...

    storage: StorageConfig = Field(
        default_factory=StorageConfig,
        description="Storage backend configuration for fsspec and delta-rs.",
    )
```

The default `StorageConfig()` produces `protocol="file"` with no
options — identical to current local behavior.

### PipelineConfig changes

```python
class PipelineConfig(BaseModel):
    # ... existing fields unchanged ...

    storage: StorageConfig = Field(
        default_factory=StorageConfig,
        description="Storage backend configuration.",
    )
```

### `_create_runtime_environment` changes

The helper in `step_executor.py` passes the storage config through:

```python
def _create_runtime_environment(config: PipelineConfig, ...) -> RuntimeEnvironment:
    return RuntimeEnvironment(
        delta_root_path=config.delta_root,
        staging_root_path=config.staging_root,
        working_root_path=None if is_curator else config.working_root,
        # ... existing fields ...
        storage=config.storage,
    )
```

### URI path utilities

Docs 02–04 need to replace `Path /` and `Path.parent` operations
with equivalents that work for both local paths and cloud URIs
(`s3://bucket/path`). This PR introduces two thin utilities in
`utils/path.py`, backed by `posixpath`:

```python
import posixpath

def uri_join(base: str, *parts: str) -> str:
    """Join URI/path segments. Works for local paths and cloud URIs."""
    return posixpath.join(base, *parts)

def uri_parent(uri: str) -> str:
    """Parent directory of a URI/path. Works for local paths and cloud URIs."""
    return posixpath.dirname(uri)
```

Both work because `posixpath` operates on string structure without
assuming OS conventions, and both local POSIX paths and cloud URIs
use `/` as the separator:

```python
uri_join("s3://bucket/delta", "executions")      # → s3://bucket/delta/executions
uri_join("/data/delta", "executions")             # → /data/delta/executions
uri_parent("s3://bucket/project/delta")           # → s3://bucket/project
uri_parent("/data/pipelines/delta")               # → /data/pipelines
```

Introduced in this PR (not docs 02–04) so they're available from
the start. Zero new dependencies — `posixpath` is stdlib.

### fsspec dependency

Add `fsspec` as a direct dependency in `[tool.pixi.pypi-dependencies]`.
It's already a transitive dependency (via prefect and dask), so
install behavior is unchanged.

Cloud filesystem packages are optional extras. This requires creating
a `[project.optional-dependencies]` section (none exists today):

```toml
[project.optional-dependencies]
s3 = ["s3fs"]
gcs = ["gcsfs"]
```

---

## Scope

| File | Change |
|------|--------|
| `schemas/execution/storage_config.py` | **New file.** `StorageConfig` Pydantic model. |
| `schemas/execution/__init__.py` | Re-export `StorageConfig`. |
| `schemas/__init__.py` | Re-export `StorageConfig`. |
| `schemas/execution/runtime_environment.py` | Add `storage: StorageConfig` field with default. |
| `schemas/orchestration/pipeline_config.py` | Add `storage: StorageConfig` field with default. |
| `orchestration/engine/step_executor.py` | Pass `storage=config.storage` in `_create_runtime_environment`. |
| `utils/path.py` | Add `uri_join()` and `uri_parent()` functions. |
| `pyproject.toml` | Add `fsspec` to `[tool.pixi.pypi-dependencies]`. Create `[project.optional-dependencies]` for `s3`/`gcs` extras. |

---

## Testing

| Test file | Coverage |
|-----------|----------|
| `tests/artisan/schemas/test_storage_config.py` | **New file.** `StorageConfig` defaults: `is_local` is True, `filesystem()` returns `LocalFileSystem`, `delta_storage_options()` returns None. S3 config: `is_local` is False, `filesystem()` returns `S3FileSystem` (mocked), `delta_storage_options()` returns translated+stringified options dict. Frozen model: mutation raises. Serialization round-trip via Pydantic. |
| `tests/artisan/execution/test_executor_creator.py` | Existing `RuntimeEnvironment` tests pass unchanged (default `StorageConfig` is transparent). New test: `storage` field serializes correctly. |
| `tests/artisan/orchestration/test_orchestration_api.py` | Existing `PipelineConfig` tests pass unchanged. New test: `storage` field is propagated. |
| `tests/artisan/utils/test_path.py` | `uri_join` and `uri_parent` with local paths and cloud URIs. Edge cases: trailing slashes, protocol prefixes. |

---

## Related Docs

- `cloud-deployment.md` — high-level cloud deployment plan
- `02-storage-layer.md` — next: migrate storage classes to fsspec
- `03-delta-read-sites.md` — thread `storage_options` through delta read sites
- `04-runtime-env-rename.md` — final: rename `Path` fields to `str`
