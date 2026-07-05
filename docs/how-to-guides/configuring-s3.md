# Configure S3-Compatible Storage

How to point an Artisan pipeline at S3, MinIO, LocalStack, or any
S3-compatible backend for Delta Lake tables, staging, and external files.

**Prerequisites:** [Configuring Execution](configuring-execution.md),
the `artisan[s3]` install extra (`pip install 'dexterity-artisan[s3]'`).

**Related:** [Pipeline Configuration](../concepts/pipeline-configuration.md)
explains the `PipelineConfig` schema in depth.

**Key types:** `StorageConfig`, `PipelineConfig`.

---

## Minimal working example — production AWS S3

When running on EC2 (or anywhere with an IAM role / `~/.aws/credentials` /
`AWS_*` env vars set), pass `protocol="s3"` and let `delta-rs` and `s3fs`
read credentials from the environment:

```python
from artisan.orchestration import PipelineConfig, PipelineManager
from artisan.schemas.execution.storage_config import StorageConfig

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
and human-readable failure logs that the orchestrator writes with
`os.*` calls.

---

## MinIO / LocalStack / on-prem S3

Non-default endpoints can't be reached via env-var discovery alone — you
need to pass an explicit `endpoint_url` to fsspec **and** to delta-rs.
`StorageConfig` carries both:

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

This lets a local pipeline ingest from S3, an on-prem MinIO pipeline
ingest from public S3 in a different account, or any other cross-protocol
combination — without forcing users to think about which credentials apply.

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

Construct the manager and submit a no-op:

```python
pipeline = PipelineManager(config)
print(pipeline.config.storage.delta_storage_options())  # confirms delta-rs sees the endpoint
```

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
