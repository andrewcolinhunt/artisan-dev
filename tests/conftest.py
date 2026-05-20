"""Pytest configuration and shared fixtures."""

from __future__ import annotations

import contextlib
import logging
import os
import socket
import sys
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
# Suppress ResourceWarning noise from s3fs/asyncio socket teardown
# =============================================================================
#
# s3fs / aiobotocore spawn asyncio self-pipe socketpair wakeup sockets
# internally and don't always close them cleanly when the filesystem
# instances are garbage-collected during interpreter shutdown. Python's
# gc then prints ``Exception ignored in: <socket.socket ...>`` +
# ``ResourceWarning: unclosed <socket.socket ...>`` via
# ``sys.unraisablehook`` — pairs of adjacent loopback TCP ports the OS
# already closes at process exit, so the warning is cosmetic.
#
# pyproject.toml's ``filterwarnings = ["error"]`` doesn't catch these
# because they fire after pytest has finished, via the unraisable hook
# rather than ``warnings.warn``. Narrow the hook to swallow ONLY
# ``ResourceWarning`` on ``socket.socket`` objects — anything else still
# surfaces.
_original_unraisablehook = sys.unraisablehook


def _quiet_socket_resourcewarning_hook(args):
    if args.exc_type is ResourceWarning and isinstance(args.object, socket.socket):
        return
    _original_unraisablehook(args)


sys.unraisablehook = _quiet_socket_resourcewarning_hook


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

# Common Docker UNIX socket locations, in probe order. Used by
# ``_detect_minio_unavailable`` to tell the user which runtime is missing.
_DOCKER_SOCKET_CANDIDATES: tuple[str, ...] = (
    os.path.expanduser("~/.docker/run/docker.sock"),  # Docker Desktop (macOS)
    "/var/run/docker.sock",  # Linux default / Docker Desktop classic
    os.path.expanduser("~/.colima/default/docker.sock"),  # Colima
    os.path.expanduser("~/.orbstack/run/docker.sock"),  # OrbStack
)

# Set by ``minio_endpoint`` when it yields None, read by ``s3_fs`` to
# produce a specific skip reason. Module-level so it survives across the
# session-scoped fixture's yield without re-running setup.
_MINIO_SKIP_REASON: dict[str, str] = {}

# Boilerplate appended to every skip reason so the message is actionable
# at a glance.
_MINIO_HOWTO = (
    "Start Docker Desktop / OrbStack / Colima, or "
    "`brew install minio/stable/minio && minio server ~/minio-data &` "
    "and `export ARTISAN_S3_ENDPOINT=http://127.0.0.1:9000` to bypass."
)


def _probe_docker_socket() -> str | None:
    """Return the path of a reachable Docker UNIX socket, or None."""
    docker_host = os.environ.get("DOCKER_HOST", "")
    candidates: list[str] = []
    if docker_host.startswith("unix://"):
        candidates.append(docker_host.removeprefix("unix://"))
    candidates.extend(_DOCKER_SOCKET_CANDIDATES)

    for path in candidates:
        if not os.path.exists(path):
            continue
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(1.0)
            sock.connect(path)
            sock.close()
            return path
        except OSError:
            continue
    return None


def _detect_minio_unavailable() -> str | None:
    """Return a skip reason if MinIO can't be brought up locally, else None.

    Classifies the failure mode so the user knows what to fix:
    testcontainers missing, Docker daemon unreachable, or the MinIO
    container itself failing to come up.
    """
    try:
        import testcontainers.minio  # noqa: F401
    except ImportError as exc:
        return (
            f"testcontainers package not installed ({exc.name}). "
            f"Install dev deps (`pixi install -e dev`), or set "
            f"ARTISAN_S3_ENDPOINT to a long-lived MinIO/S3 instance."
        )

    docker_host = os.environ.get("DOCKER_HOST", "")
    if docker_host and not docker_host.startswith("unix://"):
        # TCP-based Docker context — can't easily probe without docker-py;
        # let MinioContainer try and bubble up its error if it fails.
        return None

    if _probe_docker_socket() is None:
        tried = ", ".join(_DOCKER_SOCKET_CANDIDATES)
        return f"No Docker daemon detected (probed {tried}). {_MINIO_HOWTO}"
    return None


@pytest.fixture(scope="session")
def minio_endpoint() -> Iterator[dict[str, str] | None]:
    """Endpoint + credentials for the session-scoped MinIO step_runner.

    Honors ``ARTISAN_S3_ENDPOINT`` for developer-owned instances; otherwise
    boots a MinIO container via testcontainers. Yields ``None`` when boot
    fails — and stashes a specific skip reason in ``_MINIO_SKIP_REASON``
    so dependent fixtures surface *why* MinIO is unavailable (no Docker,
    no testcontainers, image pull failed) instead of just "unavailable".
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

    skip_reason = _detect_minio_unavailable()
    if skip_reason is not None:
        _MINIO_SKIP_REASON["reason"] = skip_reason
        logging.getLogger(__name__).warning(skip_reason)
        yield None
        return

    from testcontainers.minio import MinioContainer

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
        reason = (
            f"MinIO container failed to start ({type(exc).__name__}: {exc}). "
            f"{_MINIO_HOWTO}"
        )
        _MINIO_SKIP_REASON["reason"] = reason
        logging.getLogger(__name__).warning(reason)
        yield None


@pytest.fixture
def s3_fs(minio_endpoint):
    """Per-test S3 filesystem with an isolated bucket on the session MinIO.

    Returns a ``(fs, storage_config, uri_prefix)`` tuple. Bucket is created
    in setup and torn down at test end. Skips with a specific reason
    (no Docker, no testcontainers, image pull failed, etc.) read from
    ``_MINIO_SKIP_REASON`` so the user knows what to fix.
    """
    if minio_endpoint is None:
        pytest.skip(
            _MINIO_SKIP_REASON.get("reason", "MinIO unavailable (reason unknown)")
        )

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
