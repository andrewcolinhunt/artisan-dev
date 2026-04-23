"""Pytest configuration and shared fixtures."""

from __future__ import annotations

import contextlib
import logging
import os
import uuid
from collections.abc import Iterator

import pytest
import rootutils

# Setup project root once for all tests
rootutils.setup_root(
    search_from=__file__,
    indicator=[".project-root", "pyproject.toml"],
    pythonpath=True,
    dotenv=True,
    cwd=False,
)


# =============================================================================
# S3 / MinIO fixtures (single canonical pin location)
# =============================================================================
#
# The MinIO image tag is pinned for CI reproducibility. Bump this tag during
# periodic dependency review — track upstream releases at
# https://github.com/minio/minio/releases. When bumping, run
# `pytest -m integration` to confirm the new image still passes the smoke
# latency assertion (catches IMDS regressions and behavior drift) and the
# parametrized [local, s3] tests (catches checksum-handling regressions
# in DeleteObjects — older MinIO releases require Content-MD5 while
# modern boto3 sends only CRC32, breaking recursive bucket cleanup).
_MINIO_IMAGE = "minio/minio:RELEASE.2025-04-22T22-12-26Z"


@pytest.fixture(scope="session")
def minio_endpoint() -> Iterator[dict[str, str] | None]:
    """Endpoint + credentials for the session-scoped MinIO backend.

    Honors ``ARTISAN_S3_ENDPOINT`` for developer-owned instances; otherwise
    boots a MinIO container via testcontainers. Yields ``None`` when boot
    fails so dependent fixtures can ``pytest.skip`` cleanly without
    breaking unit tests that don't need S3.
    """
    # Disable EC2 IMDS probing in this test process so s3fs/boto3 don't
    # spend ~3 s timing out against 169.254.169.254 on every fixture
    # init when running off-EC2. Set on os.environ here (not via env=)
    # because s3fs runs inside this same process — there is no subprocess
    # to inherit a separate env.
    os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")

    # Recent boto3 (used by s3fs) defaults to CRC32 integrity checksums on
    # write/delete, but the pinned MinIO image only accepts the legacy
    # MD5/optional path. Force the request checksum to MD5-on-required
    # so DeleteObjects against MinIO doesn't 400 with `MissingContentMD5`.
    # Real S3 ignores this header so production behavior is unaffected.
    os.environ.setdefault("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
    os.environ.setdefault("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")

    existing = os.environ.get("ARTISAN_S3_ENDPOINT")
    if existing:
        yield {
            "endpoint_url": existing,
            "access_key": os.environ.get("ARTISAN_S3_ACCESS_KEY", "minioadmin"),
            "secret_key": os.environ.get("ARTISAN_S3_SECRET_KEY", "minioadmin"),
        }
        return

    try:
        from testcontainers.minio import MinioContainer
    except ImportError:
        yield None
        return

    try:
        with MinioContainer(image=_MINIO_IMAGE) as minio:
            host_ip = minio.get_container_host_ip()
            port = minio.get_exposed_port(9000)
            yield {
                "endpoint_url": f"http://{host_ip}:{port}",
                "access_key": minio.access_key,
                "secret_key": minio.secret_key,
            }
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "MinIO container failed to start: %s. S3-parametrized tests "
            "will skip. Set ARTISAN_S3_ENDPOINT to use a long-running "
            "MinIO instance instead.",
            exc,
        )
        yield None


@pytest.fixture
def s3_fs(minio_endpoint):
    """Per-test S3 filesystem with an isolated bucket on the session MinIO.

    Returns a ``(fs, storage_config, uri_prefix)`` tuple. Bucket is created
    in setup and torn down at test end. Skips cleanly when the session
    MinIO fixture is unavailable.
    """
    if minio_endpoint is None:
        pytest.skip("MinIO backend unavailable")

    import s3fs

    from artisan.schemas.execution.storage_config import StorageConfig

    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "gw-main")
    bucket = f"artisan-test-{worker_id}-{uuid.uuid4().hex[:8]}"

    fs = s3fs.S3FileSystem(
        key=minio_endpoint["access_key"],
        secret=minio_endpoint["secret_key"],
        client_kwargs={"endpoint_url": minio_endpoint["endpoint_url"]},
        use_ssl=False,
    )
    fs.mkdir(bucket)

    storage = StorageConfig(
        protocol="s3",
        options={
            "key": minio_endpoint["access_key"],
            "secret": minio_endpoint["secret_key"],
            "client_kwargs": {"endpoint_url": minio_endpoint["endpoint_url"]},
            "use_ssl": False,
        },
        delta_options={
            "AWS_ENDPOINT_URL": minio_endpoint["endpoint_url"],
            "AWS_ACCESS_KEY_ID": minio_endpoint["access_key"],
            "AWS_SECRET_ACCESS_KEY": minio_endpoint["secret_key"],
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "true",
        },
    )

    yield fs, storage, f"s3://{bucket}"

    # Best-effort cleanup; a dead bucket on a containerized MinIO is
    # acceptable since the container itself is torn down at session end.
    with contextlib.suppress(Exception):
        fs.rm(bucket, recursive=True)
