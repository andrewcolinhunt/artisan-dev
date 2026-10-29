"""Smoke test: DeltaCommitter round-trip against MinIO.

Exercise MinIO fixtures and `StorageConfig.delta_storage_options` together.
Observe actual metadata requests instead of inferring them from storage latency.
"""

from __future__ import annotations

import os
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import polars as pl
import pytest

from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import ARTIFACT_INDEX_SCHEMA
from artisan.storage.io.commit import DeltaCommitter
from artisan.storage.io.commit_plan import build_commit_plan
from artisan.storage.io.staging import StagingManager


@pytest.fixture(autouse=True)
def metadata_requests(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Iterator[tuple[str, list[tuple[str, str]]]]:
    """Observe Python and Rust metadata I/O before the S3 fixture creates clients."""
    requests: list[tuple[str, str]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            requests.append((self.command, self.path))
            self.send_response(403)
            self.send_header("Content-Length", "0")
            self.end_headers()

        do_PUT = do_GET

        def log_message(self, _format: str, *_args: object) -> None:
            pass

    # Ambient credentials must not hide missing explicit MinIO credentials or
    # redirect a positive control to a developer's STS/container credential URL.
    preserved = {
        "AWS_EC2_METADATA_DISABLED",
        "AWS_REQUEST_CHECKSUM_CALCULATION",
        "AWS_RESPONSE_CHECKSUM_VALIDATION",
    }
    for key in tuple(os.environ):
        if key.startswith("AWS_") and key not in preserved:
            monkeypatch.delenv(key)
    empty_config = tmp_path / "empty-aws-config"
    empty_config.write_text("")
    for key in ("AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE", "BOTO_CONFIG"):
        monkeypatch.setenv(key, str(empty_config))
    no_proxy = ",".join(
        (os.environ.get("NO_PROXY", ""), os.environ.get("no_proxy", ""), "127.0.0.1")
    )
    for key in ("NO_PROXY", "no_proxy"):
        monkeypatch.setenv(key, no_proxy)

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        endpoint = f"http://127.0.0.1:{server.server_port}"
        monkeypatch.setenv("AWS_EC2_METADATA_SERVICE_ENDPOINT", endpoint)
        monkeypatch.setenv("AWS_METADATA_ENDPOINT", endpoint)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield endpoint, requests
        finally:
            server.shutdown()
            thread.join()


@pytest.mark.parametrize("backend", ["s3fs", "delta"])
def test_metadata_sentinel_observes_missing_credentials(
    backend: str,
    metadata_requests: tuple[str, list[tuple[str, str]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prove that each real credential client reaches the guarded endpoint."""
    endpoint, requests = metadata_requests
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "false")
    if backend == "s3fs":
        import s3fs
        from botocore.exceptions import NoCredentialsError

        fs = s3fs.S3FileSystem(
            client_kwargs={"endpoint_url": endpoint}, skip_instance_cache=True
        )
        with pytest.raises(NoCredentialsError):
            fs.ls("metadata-oracle-bucket")
    else:
        from deltalake import DeltaTable

        with pytest.raises(OSError, match="latest/api/token"):
            DeltaTable(
                "s3://metadata-oracle-bucket/missing",
                storage_options={
                    "AWS_ENDPOINT_URL": endpoint,
                    "AWS_REGION": "us-east-1",
                    "AWS_ALLOW_HTTP": "true",
                },
            )
    assert ("PUT", "/latest/api/token") in requests


def test_delta_commit_roundtrip_on_minio(
    s3_fs, metadata_requests: tuple[str, list[tuple[str, str]]]
) -> None:
    """End-to-end DeltaCommitter round-trip against per-test MinIO bucket."""
    import s3fs

    _, storage, uri_prefix = s3_fs
    # Other S3 tests may already have initialized the fixture's cached client.
    fs = s3fs.S3FileSystem(**storage.options, skip_instance_cache=True)

    delta_root = f"{uri_prefix}/delta"
    staging_root = f"{uri_prefix}/staging"

    staging_manager = StagingManager(staging_root, fs)
    committer = DeltaCommitter(
        delta_root,
        staging_manager,
        fs=fs,
        storage_options=storage.delta_storage_options(),
    )
    committer.initialize_tables()

    df = pl.DataFrame(
        {
            "artifact_id": ["a" * 32, "b" * 32],
            "artifact_type": ["csv", "metric"],
            "origin_step_number": [0, 0],
            "metadata": ["{}", "{}"],
        },
        schema=ARTIFACT_INDEX_SCHEMA,
    )

    step_run_id = "c" * 32
    staging_manager.stage_orchestrator_dataframe(
        df,
        TablePath.ARTIFACT_INDEX.value,
        commit_kind="input_registration",
        step_run_id=step_run_id,
        step_number=0,
        operation_name="smoke_input_registration",
    )
    plan = build_commit_plan(
        delta_root=delta_root,
        staging_root=staging_root,
        fs=fs,
        commit_kind="input_registration",
        step_run_id=step_run_id,
        step_number=0,
        operation_name="smoke_input_registration",
    )
    rows = committer.commit_logical(plan)

    assert rows == {"index": 2}

    table_uri = f"{delta_root}/{TablePath.ARTIFACT_INDEX.value}"
    read_back = pl.read_delta(
        table_uri, storage_options=storage.delta_storage_options()
    )
    assert read_back.height == 2
    assert set(read_back["artifact_id"].to_list()) == {"a" * 32, "b" * 32}
    assert metadata_requests[1] == []
