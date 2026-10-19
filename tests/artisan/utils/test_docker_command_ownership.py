"""Exercise container protocol failures with real, independently owned CLI children."""

from __future__ import annotations

import json
import os
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from artisan.schemas.operation_config.environment_spec import DockerEnvironmentSpec
from artisan.utils.command_supervisor import (
    CommandOwnershipError,
    CommandSupervisor,
    ManagedCommand,
)
from artisan.utils.configuration_secrets import COMMAND_ATTACHMENT_ENV
from artisan.utils.external_tools import run_command

_FAKE = r"""
import json, os, signal, sys, time
from pathlib import Path
root = Path(ROOT)
mode = MODE
args = sys.argv[1:]
endpoint = ''
if args[:1] == ['--host']:
    endpoint = args[1]
    args = args[2:]
kind = args[0]
identity = 'a' * 64
(root / (kind + '-args')).write_text(json.dumps(args))
(root / (kind + '-environment')).write_text(json.dumps(sorted(os.environ)))
(root / (kind + '-host')).write_text(endpoint)
(root / (kind + '-platform')).write_text(os.environ.get('DOCKER_DEFAULT_PLATFORM', ''))
if kind == 'context':
    print(json.dumps({'Host': (root / 'active-context-host').read_text() if (root / 'active-context-host').exists() else 'unix:///fake-engine'}))
elif kind == 'create':
    (root / 'active-context-host').write_text('unix:///different-engine')
    if mode == 'pending-create':
        (root / 'create-pending').write_text('waiting')
        while not (root / 'release-create').exists():
            time.sleep(.01)
    if mode == 'failed-create':
        sys.exit(1)
    if mode == 'lost-id':
        sys.exit(0)
    if mode == 'stalled-create':
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        (root / 'stalled-pid').write_text(str(os.getpid()))
        time.sleep(30)
    print(identity)
elif kind == 'start':
    if mode in {'natural', 'bad-inspect', 'missing-container'}:
        print('payload')
        sys.exit(0)
    (root / 'start-accepted').write_text('ready')
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    while not (root / 'removed').exists():
        time.sleep(.01)
elif kind == 'inspect':
    if mode == 'missing-container':
        sys.exit(1)
    print(json.dumps({'Status': 'running' if mode == 'bad-inspect' else 'exited', 'Running': mode == 'bad-inspect', 'Restarting': False, 'ExitCode': 0}))
elif kind == 'kill':
    pass
elif kind == 'rm':
    if mode == 'missing-container':
        sys.exit(1)
    if mode == 'stalled-remove':
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        (root / 'stalled-pid').write_text(str(os.getpid()))
        time.sleep(30)
    (root / 'removed').write_text('closed')
    print(identity)
elif kind == 'run':
    print('external')
"""


@pytest.fixture
def fake_docker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    def install(mode: str) -> Path:
        executable = tmp_path / "docker"
        executable.write_text(
            f"#!{sys.executable}\n"
            + _FAKE.replace("ROOT", repr(str(tmp_path))).replace("MODE", repr(mode))
        )
        executable.chmod(0o755)
        monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
        monkeypatch.setenv("DOCKER_HOST", "unix:///fake-engine")
        monkeypatch.delenv("DOCKER_CONTEXT", raising=False)
        return tmp_path

    return install


@pytest.fixture
def owner() -> Iterator[CommandSupervisor]:
    controller = CommandSupervisor.start()
    try:
        yield controller
    finally:
        controller.request_close(time.monotonic() + 2, immediate=True)
        controller.finish(time.monotonic() + 2)


def _launch(owner: CommandSupervisor) -> ManagedCommand:
    environment = DockerEnvironmentSpec(image="test-image")
    wrapped = environment.wrap_command(["synthetic-payload"])
    return ManagedCommand(
        owner.attachment,
        wrapped,
        dict(os.environ),
        None,
        merged=False,
        external=True,
        docker_create=environment._managed_container_command(wrapped),
    )


def _ready(path: Path) -> None:
    deadline = time.monotonic() + 5
    while (not path.exists() or not path.read_text()) and time.monotonic() < deadline:
        time.sleep(0.01)
    assert path.exists()
    assert path.read_text()


@pytest.mark.parametrize("mode", ["failed-create", "lost-id"])
def test_missing_create_identity_never_starts_or_claims_closure(
    fake_docker, owner, mode
):
    root = fake_docker(mode)
    command = _launch(owner)
    try:
        with pytest.raises(CommandOwnershipError):
            command.communicate()
        assert not (root / "start-args").exists()
        assert not (root / "rm-args").exists()
        assert owner.finish(time.monotonic() + 2).state == "unknown"
    finally:
        command.dispose()


def test_cancellation_fences_delayed_start_with_exact_forced_removal(
    fake_docker, owner
):
    root = fake_docker("delayed-start")
    command = _launch(owner)
    try:
        _ready(root / "start-accepted")
        deadline = time.monotonic() + 3
        owner.request_close(deadline, immediate=True)
        assert owner.finish(deadline).state == "closed"
        assert json.loads((root / "rm-args").read_text()) == [
            "rm",
            "--force",
            "--volumes",
            "a" * 64,
        ]
        assert not (root / "inspect-args").exists()
    finally:
        command.dispose()


@pytest.mark.parametrize("mode", ["stalled-create", "stalled-remove"])
def test_stalled_control_cli_uses_original_deadline_and_remains_unknown(
    fake_docker, owner, mode
):
    root = fake_docker(mode)
    command = _launch(owner)
    try:
        _ready(root / ("stalled-pid" if mode == "stalled-create" else "start-accepted"))
        started = time.monotonic()
        deadline = started + 0.5
        owner.request_close(deadline, immediate=True)
        assert owner.finish(deadline).state == "unknown"
        assert time.monotonic() - started < 1
        _ready(root / "stalled-pid")
        pid = int((root / "stalled-pid").read_text())
        time.sleep(0.2)
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
    finally:
        command.dispose()


def test_attach_success_without_terminal_container_evidence_is_not_success(fake_docker):
    root = fake_docker("bad-inspect")
    with pytest.raises(CommandOwnershipError):
        run_command(DockerEnvironmentSpec(image="test-image"), ["payload"])
    assert (root / "removed").exists()
    assert "--force" in json.loads((root / "rm-args").read_text())


def test_missing_container_before_positive_removal_proof_is_unknown(fake_docker):
    fake_docker("missing-container")
    with pytest.raises(CommandOwnershipError):
        run_command(DockerEnvironmentSpec(image="test-image"), ["payload"])


def test_remote_docker_endpoint_retains_external_obligation(
    fake_docker, owner, monkeypatch
):
    root = fake_docker("natural")
    monkeypatch.setenv("DOCKER_HOST", "tcp://remote.example:2376")
    command = _launch(owner)
    try:
        assert command.communicate()[0] == "external\n"
        deadline = time.monotonic() + 2
        owner.request_close(deadline)
        assert owner.finish(deadline).state == "unknown"
        assert not (root / "create-args").exists()
    finally:
        command.dispose()


def test_control_children_keep_helper_env_without_forwarding_host_secrets_to_container(
    fake_docker, monkeypatch
):
    root = fake_docker("natural")
    monkeypatch.setenv("UNRELATED_HOST_SECRET", "dummy-sensitive-value")
    assert (
        run_command(DockerEnvironmentSpec(image="test-image"), ["payload"]).stdout
        == "payload\n"
    )
    for stage in ("create", "start", "inspect", "rm"):
        keys = set(json.loads((root / f"{stage}-environment").read_text()))
        assert not keys.intersection(COMMAND_ATTACHMENT_ENV)
        assert "UNRELATED_HOST_SECRET" in keys
    create = json.loads((root / "create-args").read_text())
    assert "--env" not in create
    assert "dummy-sensitive-value" not in repr(create)


def test_private_create_plan_reuses_exact_wrapped_configuration(monkeypatch):
    from artisan.schemas.operation_config import environment_spec

    calls = []
    monkeypatch.setattr(
        environment_spec, "_find_free_port", lambda: calls.append(1) or 12345
    )
    environment = DockerEnvironmentSpec(image="test-image", gpu=True)
    wrapped = environment.wrap_command(["payload"], "/execution")
    assert environment._managed_container_command(wrapped) == [
        "docker",
        "create",
        *wrapped[3:],
    ]
    assert len(calls) == 1


def test_control_launch_immediately_reserves_identity_before_later_group_probe(
    monkeypatch,
):
    from types import SimpleNamespace

    from artisan.utils import _command_supervisor as native
    from artisan.utils._docker_supervisor import DockerCommand

    reserved = {424242: "earlier"}
    with open(os.devnull, "r+b") as null:
        command = DockerCommand(
            native.Command("container", 1), [], [], {}, None, [null.fileno()] * 3
        )
    command._reserved = reserved

    def launch(child, argv, env, cwd, fds):
        child.process = SimpleNamespace(pid=424242)
        child.state = "running"

    monkeypatch.setattr(native.Command, "launch", launch)

    def forbidden_wait(*args):
        pytest.fail("Old group probe must not reap a newly reserved CLI child")

    monkeypatch.setattr(native.os, "waitid", forbidden_wait)
    try:
        command._job("inspect", ["docker", "inspect"])
        earlier = native.Command(
            "earlier", 1, process=SimpleNamespace(pid=424242), reaped=True
        )
        earlier._observe_closure(reserved)
        assert earlier.state == "closed"
        assert reserved[424242] == "container:inspect"
    finally:
        command._release_fds()
        for descriptor in command.readers.values():
            os.close(descriptor)


def test_start_helper_launch_failure_removes_container_without_invented_result(
    fake_docker, monkeypatch
):
    from artisan.utils import _command_supervisor as native
    from artisan.utils._docker_supervisor import DockerCommand

    root = fake_docker("natural")
    launch = native.Command.launch

    def fail_start(child, argv, env, cwd, fds):
        if "start" in argv:
            child.state = "launch_failed"
            child.launch_error = "missing"
        else:
            launch(child, argv, env, cwd, fds)

    monkeypatch.setattr(native.Command, "launch", fail_start)
    with open(os.devnull, "r+b") as null:
        command = DockerCommand(
            native.Command("container", 1),
            ["docker", "run", "--rm", "image"],
            ["docker", "create", "image"],
            dict(os.environ),
            None,
            [null.fileno()] * 3,
        )
    command.begin()
    deadline = time.monotonic() + 2
    while command.state not in {"closed", "unknown"} and time.monotonic() < deadline:
        reserved = {
            child.pid: child.request_id
            for child in command.owned_commands()
            if child.process is not None and not child.reaped
        }
        command.tick(reserved)
        time.sleep(0.01)
    assert command.state == "unknown"
    assert command.snapshot()["returncode"] is None
    assert (root / "removed").exists()


def test_cancellation_during_create_removes_late_identity_without_start(
    fake_docker, owner
):
    root = fake_docker("pending-create")
    command = _launch(owner)
    try:
        _ready(root / "create-pending")
        deadline = time.monotonic() + 3
        owner.request_close(deadline, immediate=True)
        (root / "release-create").write_text("return identity")
        assert owner.finish(deadline).state == "closed"
        assert (root / "removed").exists()
        assert not (root / "start-args").exists()
    finally:
        command.dispose()


def test_resolved_context_is_frozen_and_default_platform_is_preserved(
    fake_docker, monkeypatch
):
    root = fake_docker("natural")
    monkeypatch.delenv("DOCKER_HOST")
    monkeypatch.setenv("DOCKER_CONTEXT", "chosen-context")
    monkeypatch.setenv("DOCKER_DEFAULT_PLATFORM", "linux/amd64")
    assert (
        run_command(DockerEnvironmentSpec(image="test-image"), ["payload"]).stdout
        == "payload\n"
    )
    assert "chosen-context" in json.loads((root / "context-args").read_text())
    for stage in ("create", "start", "inspect", "rm"):
        assert (root / f"{stage}-host").read_text() == "unix:///fake-engine"
        assert (root / f"{stage}-platform").read_text() == "linux/amd64"
