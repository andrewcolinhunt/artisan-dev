# Configure S3-Compatible Storage

How to point an Artisan pipeline at S3, MinIO, LocalStack, or any
S3-compatible backend for Delta Lake tables, staging, and external files.

**Prerequisites:** [Configuring Execution](configuring-execution.md),
the `artisan[s3]` install extra (`pip install 'dexterity-artisan[s3]'`).

**Related:** [Pipeline Configuration](../concepts/pipeline-configuration.md)
explains how storage and execution settings fit together.

**Key types:** `StorageConfig`, `PipelineConfig`.

---

## Minimal working example — production AWS S3

When running on EC2 (or anywhere with an IAM role / `~/.aws/credentials` /
`AWS_*` env vars set), pass `protocol="s3"` and let `delta-rs` and `s3fs`
read credentials from the environment:

```python
from artisan.orchestration import PipelineConfig, PipelineManager
from artisan.schemas import StorageConfig

pipeline = PipelineManager(
    PipelineConfig(
        name="prod-ingest",
        delta_root="s3://my-bucket/delta",
        staging_root="s3://my-bucket/staging",
        files_root="s3://my-bucket/files",
        working_root="/var/run/artisan",  # always local — sandboxes + failure logs
        storage=StorageConfig(protocol="s3"),
    )
)
```

`working_root` and `failure_logs_root` (derived from `working_root` when
`storage` is cloud) **must** stay local — they're used for sandbox dirs
and human-readable failure logs. For cloud stores, failure files live under
`<working_root>/logs/failures/YYYYMMDD/YYYYMMDDTHHMMSSffffffZ_executionID.log`,
using each source execution's UTC start time.

Pipeline session file logging is disabled for cloud stores, so
`pipeline.log_path` is `None`; console logging remains available. Local stores
create a separate session file under `<runs_dir>/logs/runs/` for every manager,
including resumes. MCP reads failure files available on its own host and does
not download cloud logs.

---

## MinIO / LocalStack / on-prem S3

For a non-default endpoint, configure both the fsspec client and delta-rs.
`StorageConfig` carries their settings separately:

```python
storage = StorageConfig(
    protocol="s3",
    # fsspec-facing — used for staging Parquet and FileRef/Appendable bytes.
    options={
        "key": "minioadmin",
        "secret": "minioadmin",
        "client_kwargs": {"endpoint_url": "http://minio.local:9000"},
        "use_ssl": False,
    },
    # delta-rs-facing — used by polars.read_delta / write_delta.
    # Keys follow the delta-rs / object_store schema, not fsspec's.
    delta_options={
        "AWS_ENDPOINT_URL": "http://minio.local:9000",
        "AWS_ACCESS_KEY_ID": "minioadmin",
        "AWS_SECRET_ACCESS_KEY": "minioadmin",
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
    },
)
```

fsspec and delta-rs use different option schemas, so `StorageConfig`
carries both side by side — see
[Pipeline Configuration](../concepts/pipeline-configuration.md) for why.
Leave `delta_options={}` (the default) on AWS to fall back to env-var
discovery.

---

## Cloud-URI inputs

Pipelines can ingest files directly from cloud URIs — no local download
step needed:

```python
from artisan.operations.curator import IngestData

# `pipeline` continues from the minimal working example above.
pipeline.run(
    IngestData,
    inputs=[
        "s3://my-bucket/raw/dataset_a.csv",
        "s3://my-bucket/raw/dataset_b.csv",
        "/local/cached/dataset_c.csv",  # mixed lists work too
    ],
)
```

For each path, Artisan resolves the filesystem via a two-step rule
(`src/artisan/schemas/execution/fs.py`):

1. If the path's protocol matches `config.storage.protocol`, use the
   pipeline's configured `StorageConfig.filesystem()` — credentials and
   endpoint already wired.
2. Otherwise fall back to fsspec's standard ambient credential discovery
   (env vars, IAM roles, `~/.aws/credentials`).

A local pipeline can therefore ingest from S3 using ambient S3 credentials.
MinIO and public AWS S3 both use `s3://`, so a MinIO-backed pipeline routes both
to its configured MinIO endpoint and credentials. There is no automatic routing
between S3-compatible endpoints. To ingest from a different endpoint, download
those files with the source's client first, then pass their local paths.

---

## External file outputs (`files_root`) on cloud

`files_root` holds `LargeFileArtifact` and `AppendableArtifact` content —
bytes too large to embed in Delta. Point it at a cloud URI and existing
creators keep working with no code change: they still write to the local
`inputs.files_dir` sandbox, and the framework uploads each finalized file
to `files_root` for you.

If an upload fails, the unit fails with a postprocess-style error and the
local sandbox is preserved so the bytes are recoverable.

See [Storage and Delta Lake](../concepts/storage-and-delta-lake.md) for the
external-file storage model in depth.

---

## Verification

Inspect the configured endpoint without printing credentials:

```python
print(pipeline.config.storage.protocol)
print(pipeline.config.storage.delta_storage_options().get("AWS_ENDPOINT_URL"))
```

This confirms configuration only; a small ingest verifies access and writes.
If you're testing against MinIO locally, the project provides a
`testcontainers`-based fixture (`tests/conftest.py`'s `s3_fs`) that
boots a MinIO container per pytest session. See
`tests/artisan/storage/test_smoke_s3.py` for the smallest end-to-end
example.

---

## Cross-references

- [Pipeline Configuration](../concepts/pipeline-configuration.md) — the
  `PipelineConfig` and `StorageConfig` schemas
- [Storage and Delta Lake](../concepts/storage-and-delta-lake.md) — how
  tables, staging, and external files are laid out
- [External File Storage tutorial](../tutorials/06-storage/02-external-file-storage.ipynb) —
  `files_root` and external content in action
- [Configure Execution](configuring-execution.md) — runners, batching,
  and compute routing
