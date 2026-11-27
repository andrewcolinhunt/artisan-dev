"""Smoke test: retained acceptance round-trip against MinIO.

Exercise MinIO fixtures and `StorageConfig.delta_storage_options` together.
Observe actual metadata requests instead of inferring them from storage latency.
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from fixtures.retained_inspection import commit_inspection_attempt

from artisan.schemas import MetricArtifact
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.io.retained_store import create_retained_store

_DELTA_METADATA_CONTROL = """
import sys

import pytest
from deltalake import DeltaTable

with pytest.raises(OSError, match="latest/api/token"):
    DeltaTable(
        "s3://metadata-oracle-bucket/missing",
        storage_options={
            "AWS_ENDPOINT_URL": sys.argv[1],
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "true",
        },
    )
"""


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
        result = subprocess.run(
            [sys.executable, "-c", _DELTA_METADATA_CONTROL, endpoint],
            env=os.environ.copy(),
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        assert result.returncode == 0, result.stderr
    assert ("PUT", "/latest/api/token") in requests


def test_retained_acceptance_roundtrip_on_minio(
    s3_fs, metadata_requests: tuple[str, list[tuple[str, str]]]
) -> None:
    """Actual acceptance and hydration use only the explicit MinIO credentials."""
    import s3fs

    _, storage, uri_prefix = s3_fs
    fs = s3fs.S3FileSystem(**storage.options, skip_instance_cache=True)
    committer = create_retained_store(
        f"{uri_prefix}/delta",
        f"{uri_prefix}/files",
        fs=fs,
        storage_options=storage.delta_storage_options(),
    )
    metric = MetricArtifact.draft({"score": 4}, "score.json", 0).finalize()
    commit_inspection_attempt(
        committer.delta_root, committer=committer, outputs={"metric": [metric]}
    )
    store = ArtifactStore(
        committer.delta_root,
        fs=fs,
        files_root=committer.files_root,
        storage_options=storage.delta_storage_options(),
    )
    assert store.get_artifact(metric.artifact_id).to_row() == metric.to_row()
    assert metadata_requests[1] == []
