"""Tests for shared test-infrastructure helpers in tests/conftest.py."""

from __future__ import annotations

import importlib.util
import shutil
import socket
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest


def _load_root_conftest():
    """Load tests/conftest.py by explicit path.

    The bare module name ``conftest`` is claimed by whichever conftest
    pytest happens to import first (e.g. tests/artisan/conftest.py in a
    full run), so a name-based import is unreliable.
    """
    path = Path(__file__).with_name("conftest.py")
    spec = importlib.util.spec_from_file_location("_tests_root_conftest", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


root_conftest = _load_root_conftest()


@pytest.fixture
def sock_dir():
    """Short-lived dir under /tmp — AF_UNIX paths are capped (~104 chars
    on macOS) and pytest's tmp_path is too deep to bind a socket in."""
    path = Path(tempfile.mkdtemp(prefix="artisan-sock-", dir="/tmp"))
    yield path
    shutil.rmtree(path, ignore_errors=True)


class TestProbeDockerSocket:
    def test_probe_closes_socket_when_daemon_is_dead(self, sock_dir, monkeypatch):
        """A stale socket file (no listener) must not leak the probe socket.

        A leaked socket is garbage-collected later and emits a
        ResourceWarning, which pytest's unraisable-exception hook turns
        into an ERROR on whatever unrelated test happens to be running —
        phantom failures that shift identity between runs.
        """
        stale = sock_dir / "dead-docker.sock"
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(stale))
        listener.close()  # the file survives; connect() now raises

        created: list[socket.socket] = []
        real_socket_cls = socket.socket

        class _TrackingSocket(real_socket_cls):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                created.append(self)

        monkeypatch.setattr(socket, "socket", _TrackingSocket)
        monkeypatch.setenv("DOCKER_HOST", f"unix://{stale}")

        root_conftest._probe_docker_socket()

        assert created, "probe never attempted the stale socket"
        leaked = [s for s in created if s.fileno() != -1]
        assert not leaked, f"{len(leaked)} probe socket(s) left unclosed"

    def test_probe_returns_none_for_missing_socket_file(self, monkeypatch):
        """Nonexistent candidate paths are skipped without opening sockets."""
        monkeypatch.setenv("DOCKER_HOST", "unix:///nonexistent/docker.sock")
        monkeypatch.setattr(
            root_conftest,
            "_DOCKER_SOCKET_CANDIDATES",
            ("/nonexistent/a.sock", "/nonexistent/b.sock"),
        )
        assert root_conftest._probe_docker_socket() is None

    def test_probe_returns_path_for_live_listener(self, sock_dir, monkeypatch):
        """A socket file with a live listener is reported reachable."""
        live = sock_dir / "live-docker.sock"
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(live))
        listener.listen(1)
        try:
            monkeypatch.setenv("DOCKER_HOST", f"unix://{live}")
            assert root_conftest._probe_docker_socket() == str(live)
        finally:
            listener.close()
            Path(live).unlink(missing_ok=True)


class TestMinioAvailability:
    @pytest.fixture(autouse=True)
    def isolate_service_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("ARTISAN_S3_ENDPOINT", raising=False)
        monkeypatch.setattr(root_conftest, "_MINIO_SKIP_REASON", {})

    @pytest.mark.parametrize("required", [False, True])
    @pytest.mark.parametrize("failure", ["no Docker", "image pull failed"])
    def test_startup_failure_respects_required_s3(
        self, monkeypatch: pytest.MonkeyPatch, required: bool, failure: str
    ) -> None:
        config = MagicMock(spec=pytest.Config)
        config.getoption.return_value = required
        monkeypatch.setattr(
            root_conftest,
            "_detect_minio_unavailable",
            lambda: failure if failure == "no Docker" else None,
        )
        if failure == "image pull failed":
            service = MagicMock()
            service.__enter__.side_effect = RuntimeError(failure)
            factory = MagicMock(return_value=service)
            monkeypatch.setattr("testcontainers.minio.MinioContainer", factory)

        fixture = root_conftest.minio_endpoint.__wrapped__(config)
        if required:
            with pytest.raises(
                pytest.fail.Exception, match=f"S3 tests are required:.*{failure}"
            ):
                next(fixture)
        else:
            assert next(fixture) is None
            assert failure in root_conftest._MINIO_SKIP_REASON["reason"]
            with pytest.raises(StopIteration):
                next(fixture)
        config.getoption.assert_called_once_with("--require-s3")

    def test_existing_endpoint_bypasses_docker(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ARTISAN_S3_ENDPOINT", "http://existing:9000")
        monkeypatch.setenv("ARTISAN_S3_ACCESS_KEY", "test-access")
        monkeypatch.setenv("ARTISAN_S3_SECRET_KEY", "test-secret")
        probe = MagicMock(side_effect=AssertionError("must not probe Docker"))
        monkeypatch.setattr(root_conftest, "_detect_minio_unavailable", probe)
        config = MagicMock(spec=pytest.Config)
        config.getoption.return_value = True

        fixture = root_conftest.minio_endpoint.__wrapped__(config)
        assert next(fixture) == {
            "endpoint_url": "http://existing:9000",
            "access_key": "test-access",
            "secret_key": "test-secret",
        }
        with pytest.raises(StopIteration):
            next(fixture)
        probe.assert_not_called()

    @pytest.mark.parametrize("teardown_fails", [False, True])
    def test_started_service_is_closed_without_hiding_teardown_errors(
        self, monkeypatch: pytest.MonkeyPatch, teardown_fails: bool
    ) -> None:
        monkeypatch.setattr(root_conftest, "_detect_minio_unavailable", lambda: None)
        service = MagicMock()
        service.__enter__.return_value = service
        service.get_container_host_ip.return_value = "127.0.0.1"
        service.get_exposed_port.return_value = "9000"
        service.access_key = "test-access"
        service.secret_key = "test-secret"
        if teardown_fails:
            service.__exit__.side_effect = RuntimeError("container cleanup failed")
        factory = MagicMock(return_value=service)
        monkeypatch.setattr("testcontainers.minio.MinioContainer", factory)
        config = MagicMock(spec=pytest.Config)
        config.getoption.return_value = False

        fixture = root_conftest.minio_endpoint.__wrapped__(config)
        assert next(fixture)["endpoint_url"] == "http://127.0.0.1:9000"
        with pytest.raises(RuntimeError if teardown_fails else StopIteration):
            next(fixture)
        factory.assert_called_once_with(image=root_conftest._MINIO_IMAGE)
        service.__exit__.assert_called_once()
        assert not root_conftest._MINIO_SKIP_REASON
