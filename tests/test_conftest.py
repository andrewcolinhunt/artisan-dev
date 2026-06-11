"""Tests for shared test-infrastructure helpers in tests/conftest.py."""

from __future__ import annotations

import importlib.util
import shutil
import socket
import tempfile
from pathlib import Path

import pytest


def _load_root_conftest():
    """Load tests/conftest.py by explicit path.

    The bare module name ``conftest`` is claimed by whichever conftest
    pytest happens to import first (e.g. tests/artisan/conftest.py in a
    full run), so a name-based import is unreliable.
    """
    path = Path(__file__).with_name("conftest.py")
    spec = importlib.util.spec_from_file_location("_tests_root_conftest", path)
    assert spec is not None and spec.loader is not None
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
