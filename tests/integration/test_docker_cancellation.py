"""Qualify exact-container closure against an explicitly selected Docker engine."""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from fixtures.docker_commands import DockerCommandProbe

from artisan.utils.command_supervisor import (
    CommandOwnershipError,
    CommandSupervisor,
    ManagedCommand,
)
from artisan.utils.external_tools import ExternalToolError, run_command

pytestmark = pytest.mark.integration
_TREE = """
trap '' TERM
(trap '' TERM; while :; do echo child >> /evidence/writes; sleep .05; done) &
hostname > /evidence/identity
while :; do sleep 1; done
"""


def _docker_environment() -> dict[str, str]:
    return {
        key: value
        for key, value in os.environ.items()
        if key in {"PATH", "HOME"} or key.startswith("DOCKER_")
    }


def _docker(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
        env=_docker_environment(),
    )


class DockerWork:
    def __init__(self, image: str, root: Path) -> None:
        self.root = root
        root.chmod(0o777)
        self.owner = CommandSupervisor.start()
        self.command: ManagedCommand | None = None
        self.identity: str | None = None
        self.environment = DockerCommandProbe(
            image=image, binds=[(str(root), "/evidence")]
        )

    def launch(self) -> ManagedCommand:
        wrapped = self.environment.wrap_command(["sh", "-c", _TREE])
        self.command = ManagedCommand(
            self.owner.attachment,
            wrapped,
            _docker_environment(),
            None,
            merged=False,
            external=True,
            docker_create=self.environment._managed_container_command(wrapped),
        )
        deadline = time.monotonic() + 10
        marker = self.root / "identity"
        while (
            not marker.exists() or not marker.read_text().strip()
        ) and time.monotonic() < deadline:
            time.sleep(0.02)
        assert marker.exists(), "Container never reached its readiness barrier"
        inspected = _docker("inspect", marker.read_text().strip())
        assert inspected.returncode == 0, inspected.stderr
        self.identity = json.loads(inspected.stdout)[0]["Id"]
        assert len(self.identity) == 64
        return self.command

    def assert_removed(self) -> None:
        assert self.identity is not None
        inspected = _docker("inspect", self.identity)
        assert inspected.returncode != 0
        assert f"no such object: {self.identity}" in inspected.stderr.lower()
        writes = (self.root / "writes").read_bytes()
        time.sleep(0.2)
        assert (self.root / "writes").read_bytes() == writes

    def close(self) -> None:
        if self.command is not None:
            self.command.dispose()
        self.owner.request_close(time.monotonic() + 5, immediate=True)
        self.owner.finish(time.monotonic() + 5)
        if self.identity is not None:
            _docker("rm", "--force", self.identity)


@pytest.fixture
def docker_work(docker_image: str, tmp_path: Path) -> Iterator[DockerWork]:
    work = DockerWork(docker_image, tmp_path)
    try:
        yield work
    finally:
        work.close()


def test_docker_cancel_closes_term_resistant_tree(docker_work: DockerWork) -> None:
    docker_work.launch()
    started = time.monotonic()
    deadline = started + 8
    docker_work.owner.request_close(deadline)
    docker_work.owner.request_close(deadline + 10)
    outcome = docker_work.owner.finish(deadline)
    assert outcome.state == "closed"
    assert not outcome.external
    assert 2.5 < time.monotonic() - started < 8
    docker_work.assert_removed()


def test_docker_attach_client_loss_still_removes_container(
    docker_work: DockerWork,
) -> None:
    import sys

    command = docker_work.launch()
    status = command.connection.request("command", id=command.identity)
    assert status["state"] == "running"
    client_pid = status["pid"]
    if sys.platform == "linux":
        tree = Path(f"/proc/{client_pid}")
        assert tree.stat().st_uid == os.getuid()
        assert (tree / "cmdline").read_bytes().split(b"\0")[1:3] == [
            b"-m",
            b"artisan.utils._command_tree",
        ]
        initial = (tree / "stat").read_text().rsplit(")", 1)[1].split()
        assert int(initial[1]) == docker_work.owner.process.pid
        try:
            # Stop the reaper so its direct client's PID stays reserved until signaled.
            os.kill(client_pid, signal.SIGSTOP)
            deadline = time.monotonic() + 1
            while True:
                current = (tree / "stat").read_text().rsplit(")", 1)[1].split()
                assert current[19] == initial[19]
                if current[0] == "T":
                    break
                assert time.monotonic() < deadline, "Command reaper did not stop"
                time.sleep(0.01)
            (child,) = (tree / f"task/{client_pid}/children").read_text().split()
            child_argv = (
                (Path("/proc") / child / "cmdline").read_bytes().split(b"\0")[:-1]
            )
            assert Path(os.fsdecode(child_argv[0])).name == "docker"
            assert [os.fsdecode(value) for value in child_argv[-3:]] == [
                "start",
                "--attach",
                docker_work.identity,
            ]
            os.kill(int(child), signal.SIGKILL)
        finally:
            os.kill(client_pid, signal.SIGCONT)
    else:
        os.kill(client_pid, signal.SIGKILL)
    command.communicate()
    assert command.returncode == -signal.SIGKILL
    assert docker_work.owner.finish(time.monotonic() + 5).state == "closed"
    docker_work.assert_removed()


def test_docker_worker_connection_loss_removes_container(
    docker_work: DockerWork,
) -> None:
    command = docker_work.launch()
    command.dispose()
    deadline = time.monotonic() + 8
    docker_work.owner.worker_lost(os.getpid(), deadline)
    assert docker_work.owner.finish(deadline).state == "closed"
    docker_work.assert_removed()


def test_docker_parent_loss_removes_container(docker_work: DockerWork) -> None:
    docker_work.launch()
    docker_work.owner.connection.close()
    docker_work.owner.process.wait(timeout=12)
    docker_work.assert_removed()


def test_docker_destroyed_owner_never_claims_container_stopped(
    docker_work: DockerWork,
) -> None:
    command = docker_work.launch()
    docker_work.owner.process.kill()
    docker_work.owner.process.wait(timeout=3)
    with pytest.raises(CommandOwnershipError):
        command.communicate()
    assert docker_work.owner.poll().state == "unknown"
    assert docker_work.identity is not None
    state = json.loads(_docker("inspect", docker_work.identity).stdout)[0]["State"]
    assert state["Running"] is True


def test_docker_removed_identity_rejects_delayed_start(
    docker_image: str, tmp_path: Path
) -> None:
    import shlex

    executable = shutil.which("docker")
    assert executable is not None
    wrapper = tmp_path / "bin"
    wrapper.mkdir()
    identity_file = tmp_path / "created-id"
    waiting_file = tmp_path / "start-pending"
    script = wrapper / "docker"
    script.write_text(
        "#!/bin/sh\n"
        f'if [ "$3" = create ]; then\n  identity=$({shlex.quote(executable)} "$@") || exit $?\n'
        f"  printf '%s' \"$identity\" > {shlex.quote(str(identity_file))}\n"
        "  printf '%s\\n' \"$identity\"\n"
        f'elif [ "$3" = start ]; then\n  echo ready > {shlex.quote(str(waiting_file))}\n'
        "  while :; do sleep 1; done\n"
        f'else\n  exec {shlex.quote(executable)} "$@"\nfi\n'
    )
    script.chmod(0o755)
    owner = CommandSupervisor.start()
    command = None
    identity = None
    try:
        environment = DockerCommandProbe(image=docker_image)
        wrapped = environment.wrap_command(["sh", "-c", "echo should-not-start"])
        env = _docker_environment()
        env["PATH"] = f"{wrapper}{os.pathsep}{env['PATH']}"
        command = ManagedCommand(
            owner.attachment,
            wrapped,
            env,
            None,
            merged=False,
            external=True,
            docker_create=environment._managed_container_command(wrapped),
        )
        deadline = time.monotonic() + 10
        while not waiting_file.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert waiting_file.exists()
        identity = identity_file.read_text()
        assert len(identity) == 64
        deadline = time.monotonic() + 5
        owner.request_close(deadline, immediate=True)
        assert owner.finish(deadline).state == "closed"
        late = _docker("start", identity)
        assert late.returncode != 0
        assert "No such container" in late.stderr
    finally:
        if command is not None:
            command.dispose()
        owner.request_close(time.monotonic() + 3, immediate=True)
        owner.finish(time.monotonic() + 3)
        if identity is not None:
            _docker("rm", "--force", identity)


def test_docker_cancel_does_not_stop_independent_sibling(
    docker_work: DockerWork,
    docker_image: str,
    tmp_path: Path,
) -> None:
    other_root = tmp_path / "sibling"
    other_root.mkdir()
    sibling = DockerWork(docker_image, other_root)
    try:
        docker_work.launch()
        sibling.launch()
        deadline = time.monotonic() + 8
        docker_work.owner.request_close(deadline, immediate=True)
        assert docker_work.owner.finish(deadline).state == "closed"
        docker_work.assert_removed()
        assert sibling.identity is not None
        state = json.loads(_docker("inspect", sibling.identity).stdout)[0]["State"]
        assert state["Running"] is True
        before = (other_root / "writes").stat().st_size
        time.sleep(0.2)
        assert (other_root / "writes").stat().st_size > before
    finally:
        sibling.close()


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("exit_code", [0, 7])
def test_docker_natural_result_preserves_output_and_exit(
    docker_image: str,
    streaming: bool,
    exit_code: int,
) -> None:
    environment = DockerCommandProbe(image=docker_image)
    cmd = ["sh", "-c", f"echo out; echo err >&2; exit {exit_code}"]
    if exit_code:
        with pytest.raises(ExternalToolError) as failure:
            run_command(environment, cmd, stream_output=streaming)
        assert failure.value.return_code == exit_code
        stdout, stderr = failure.value.stdout, failure.value.stderr
    else:
        result = run_command(environment, cmd, stream_output=streaming)
        assert result.args == environment.wrap_command(cmd)
        stdout, stderr = result.stdout, result.stderr
    assert "out\n" in stdout
    assert "err\n" in (stdout if streaming else stderr)


def test_docker_large_output_env_and_cwd(docker_image: str, tmp_path: Path) -> None:
    environment = DockerCommandProbe(
        image=docker_image, env={"PUBLIC_MESSAGE": "hello world"}
    )
    result = run_command(
        environment,
        [
            "sh",
            "-c",
            'printf "%s\\n" "$PUBLIC_MESSAGE" > value; head -c 200000 /dev/zero | tr "\\000" x',
        ],
        cwd=str(tmp_path),
    )
    assert result.stdout == "x" * 200000
    assert (tmp_path / "value").read_text() == "hello world\n"
