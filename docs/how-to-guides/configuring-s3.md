# Configure S3-Compatible Storage

Store Delta Lake tables, staged results, and external files on S3 or an
S3-compatible service such as MinIO.

**Prerequisites:** [Configuring Execution](configuring-execution.md),
the `dexterity-artisan[s3]` install extra (`pip install 'dexterity-artisan[s3]'`).

**Related:** [Pipeline Configuration](../concepts/pipeline-configuration.md)
explains how storage and execution settings fit together.

## Configure AWS S3

Pass `protocol="s3"` to use credentials from an IAM role,
`~/.aws/credentials`, or `AWS_*` environment variables:

```python
from pathlib import Path

from artisan.orchestration import PipelineManager
from artisan.schemas import StorageConfig

pipeline = PipelineManager.create(
    name="prod-ingest",
    delta_root="s3://my-bucket/delta",
    staging_root="s3://my-bucket/staging",
    files_root="s3://my-bucket/files",
    working_root=str(Path("runs/working").resolve()),
    storage=StorageConfig(protocol="s3"),
)
```

`create()` assigns a fresh run ID while keeping this run's history in the
selected store. Call `pipeline.finalize()` when you finish submitting work.

Set `working_root` to an absolute local path for worker sandboxes. Artisan
publishes failure logs under `<parent of delta_root>/logs/failures/`, using each
source execution's UTC start time in the filename. If `delta_root` is a bare
bucket URI, the logs directory stays inside that bucket.

Pipeline session file logging is disabled for cloud stores, so
`pipeline.log_path` is `None`; console logging remains available. Local stores
create a separate session file under `<runs_dir>/logs/runs/` for every manager,
including resumes. MCP reads failure files available on its own host and does
not download cloud logs.

---

## Configure an S3-compatible endpoint

For a non-default endpoint, configure both the fsspec client and delta-rs.
`StorageConfig` carries their settings separately. This example assumes a local
test service at `http://minio.local:9000` and credentials in your environment;
use your service's HTTPS endpoint for a remote store:

```python
import os

storage = StorageConfig(
    protocol="s3",
    # fsspec reads staging and external file content.
    options={
        "key": os.environ["AWS_ACCESS_KEY_ID"],
        "secret": os.environ["AWS_SECRET_ACCESS_KEY"],
        "client_kwargs": {"endpoint_url": "http://minio.local:9000"},
        "use_ssl": False,
    },
    # delta-rs-facing — used by polars.read_delta / write_delta.
    # Keys follow the delta-rs / object_store schema, not fsspec's.
    delta_options={
        "AWS_ENDPOINT_URL": "http://minio.local:9000",
        "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
    },
)
```

Pass this object to `PipelineManager.create(..., storage=storage)`. On AWS, leave
`delta_options={}` (the default) to use environment credential discovery.

---

## Cloud-URI inputs

Pass cloud URIs directly as inputs; you do not need to download them before
calling the pipeline:

```python
from artisan.operations.curator import IngestCSV

# `pipeline` continues from the AWS S3 example above.
pipeline.run(
    IngestCSV,
    inputs=[
        "s3://my-bucket/raw/dataset_a.csv",
        "s3://my-bucket/raw/dataset_b.csv",
        "/local/cached/dataset_c.csv",  # mixed lists work too
    ],
)
```

For each path, Artisan chooses a filesystem as follows:

1. If the path's protocol matches `config.storage.protocol`, use the
   pipeline's configured filesystem, credentials, and endpoint.
2. Otherwise fall back to fsspec's standard ambient credential discovery
   (env vars, IAM roles, `~/.aws/credentials`).

A local pipeline can therefore ingest from S3 using ambient S3 credentials.
MinIO and public AWS S3 both use `s3://`, so a MinIO-backed pipeline routes both
to its configured MinIO endpoint and credentials. There is no automatic routing
between S3-compatible endpoints. To ingest from a different endpoint, download
those files with the source's client first, then pass their local paths.

---

## External file outputs (`files_root`) on cloud

Point `files_root` at a cloud URI to store generated `FileRefArtifact` and
`AppendableArtifact` content there. Creators still write to local
`inputs.files_dir`. Artisan captures and verifies each complete external carrier
when staging the result, then publishes a managed copy under `files_root` before
acceptance. This includes retained references to files outside `files_dir`; the
accepted result no longer depends on those original locations.

Function creators can write files during execute; command creators copy selected
outputs into `files_dir` during postprocess. See
[Retain command-generated files](writing-creator-operations.md#retain-command-files).

Upload or verification failure prevents acceptance of that result. Set
`preserve_working=True` when creating the pipeline to keep local sandbox files
for debugging. Otherwise, workers remove temporary working files after confirmed
closure; unresolved ownership retains its evidence.

See [Storage and Delta Lake](../concepts/storage-and-delta-lake.md) for external
file storage.

---

## Verification

Inspect the configured endpoint without printing credentials:

```python
print(pipeline.config.storage.protocol)
options = pipeline.config.storage.delta_storage_options() or {}
print(options.get("AWS_ENDPOINT_URL", "default AWS endpoint"))
```

This confirms configuration only. Run a small ingest and inspect its status to
verify access and writes:

```python
from artisan.orchestration import StepStatus

step = pipeline.run(IngestCSV, inputs=["s3://my-bucket/raw/dataset_a.csv"])
assert step.status is StepStatus.SUCCEEDED, step.error
pipeline.finalize()
```

Replace the URI with a small CSV that exists on your selected endpoint. For
repository qualification, see the [S3 test task](../getting-started/using-pixi.md#dev-environment).

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
