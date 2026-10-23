"""Production ownership regressions with native processes and exact identities."""

from __future__ import annotations

import concurrent.futures
import os
import sys
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from artisan.schemas.operation_config.environment_spec import (
    EnvironmentSpec,
    LocalEnvironmentSpec,
    PixiEnvironmentSpec,
)
from artisan.utils._command_supervisor import ATTACHMENT_ENV
from artisan.utils.command_supervisor import (
    CommandOwnershipError,
    CommandSupervisor,
    ManagedCommand,
    attachment_sensitive_values,
    close_command_scope,
    managed_invocation,
    reject_nested_native_scope,
)
from artisan.utils.external_tools import ExternalToolError, run_command


def _wait_absent(pid: int) -> None:
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.01)
    pytest.fail("Managed child group member remains present")


def _wait_path(path: Path) -> None:
    deadline = time.monotonic() + 5
    while not path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert path.exists()


@pytest.mark.parametrize("streaming", [False, True])
def test_natural_exit_closes_term_resistant_descendant_and_output_pipe(
    tmp_path, streaming
):
    marker = tmp_path / "child"
    code = """
import os, signal, time, pathlib
child = os.fork()
if not child:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.alarm(15)
    pathlib.Path(__import__('sys').argv[1]).write_text(str(os.getpid()))
    while True: time.sleep(.1)
while not pathlib.Path(__import__('sys').argv[1]).exists(): time.sleep(.01)
print('leader finished', flush=True)
os._exit(0)
"""
    started = time.monotonic()
    result = run_command(
        LocalEnvironmentSpec(),
        [sys.executable, "-c", code, str(marker)],
        stream_output=streaming,
    )
    assert result.returncode == 0
    assert result.stdout == "leader finished\n"
    assert time.monotonic() - started < 5
    _wait_absent(int(marker.read_text()))


def test_nested_managed_group_closes_when_foreground_parent_exits(tmp_path):
    marker = tmp_path / "nested"
    nested = "import os,signal,time,pathlib; signal.signal(signal.SIGTERM,signal.SIG_IGN); signal.alarm(15); pathlib.Path(__import__('sys').argv[1]).write_text(str(os.getpid())); time.sleep(12)"
    code = """
import os, sys, pathlib, time
from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
from artisan.utils.external_tools import run_command
child = os.fork()
if not child:
    run_command(LocalEnvironmentSpec(), [sys.executable, '-c', sys.argv[2], sys.argv[1]])
    os._exit(0)
while not pathlib.Path(sys.argv[1]).exists(): time.sleep(.01)
os._exit(0)
"""
    with pytest.raises(CommandOwnershipError):
        run_command(
            LocalEnvironmentSpec(), [sys.executable, "-c", code, str(marker), nested]
        )
    _wait_absent(int(marker.read_text()))


def test_joined_command_threads_share_one_invocation_owner():
    with managed_invocation():
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            futures = [
                pool.submit(
                    run_command,
                    LocalEnvironmentSpec(),
                    [sys.executable, "-c", "import os;print(os.getppid())"],
                )
                for _ in range(2)
            ]
            parents = [future.result().stdout for future in futures]
        close_command_scope()
    assert len(set(parents)) == 1


def test_independent_standalone_threads_have_independent_owners():
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        results = list(
            pool.map(
                lambda _: run_command(
                    LocalEnvironmentSpec(),
                    [
                        sys.executable,
                        "-c",
                        "import os,time; time.sleep(.1);print(os.getppid())",
                    ],
                ),
                range(2),
            )
        )
    assert results[0].stdout != results[1].stdout
    assert attachment_sensitive_values() == ()


@pytest.mark.parametrize("mode", ["partial", "stale", "malformed"])
def test_supplied_invalid_attachment_never_creates_independent_owner(monkeypatch, mode):
    values = ("/tmp/missing-artisan-owner.sock", "not-a-real-secret", "root", "parent")
    for key, value in zip(ATTACHMENT_ENV, values, strict=True):
        monkeypatch.setenv(key, value)
    if mode == "partial":
        monkeypatch.delenv(ATTACHMENT_ENV[2])
    elif mode == "malformed":
        monkeypatch.setenv(ATTACHMENT_ENV[0], "")
    with (
        patch.object(CommandSupervisor, "start") as start,
        pytest.raises(CommandOwnershipError),
    ):
        run_command(LocalEnvironmentSpec(), [sys.executable, "-c", "pass"])
    start.assert_not_called()


def test_native_pool_rejection_precedes_any_owner_creation():
    with (
        managed_invocation(),
        pytest.raises(CommandOwnershipError, match="Nested native"),
    ):
        reject_nested_native_scope()


def test_private_capability_is_redacted_without_command_recorder():
    command = [
        sys.executable,
        "-c",
        f"import os,sys;print(os.environ[{ATTACHMENT_ENV[1]!r}]);sys.exit(1)",
    ]
    with managed_invocation():
        secret = attachment_sensitive_values()[0]
        with pytest.raises(ExternalToolError) as failure:
            run_command(LocalEnvironmentSpec(), command)
        assert secret not in str(failure.value)
        assert "<redacted>" in failure.value.stdout


def test_nested_scope_uses_same_secret_and_raw_output_stays_raw():
    code = f"import os; print(os.environ[{ATTACHMENT_ENV[1]!r}])"
    outer = """
import sys
from artisan.utils.external_tools import run_command
from artisan.utils.command_supervisor import managed_invocation
from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
with managed_invocation():
    print(run_command(LocalEnvironmentSpec(), [sys.executable, '-c', sys.argv[1]]).stdout, end='')
"""
    with managed_invocation():
        secret = attachment_sensitive_values()[0]
        result = run_command(
            LocalEnvironmentSpec(), [sys.executable, "-c", outer, code]
        )
        assert result.stdout.strip() == secret


def test_external_obligation_survives_completed_command_until_cancel():
    owner = CommandSupervisor.start()
    command = ManagedCommand(
        owner.attachment,
        [sys.executable, "-c", "pass"],
        dict(os.environ),
        None,
        merged=False,
        external=True,
    )
    try:
        command.communicate()
        command.dispose()
        deadline = time.monotonic() + 2
        owner.request_close(deadline)
        result = owner.finish(deadline)
        assert result.state == "unknown"
        assert result.external is True
    finally:
        command.dispose()
        owner.finish(time.monotonic() + 2)


def test_ordinary_external_completion_can_close_host_scope():
    result = run_command(EnvironmentSpec(), [sys.executable, "-c", "print('done')"])
    assert result.stdout == "done\n"


def test_owner_uses_one_shortened_deadline_for_concurrent_groups(tmp_path):
    owner = CommandSupervisor.start()
    commands = []
    try:
        for index in range(2):
            marker = tmp_path / str(index)
            code = "import os,signal,time,pathlib,sys;signal.signal(signal.SIGTERM,signal.SIG_IGN);signal.alarm(15);pathlib.Path(sys.argv[1]).write_text(str(os.getpid()));time.sleep(12)"
            commands.append(
                ManagedCommand(
                    owner.attachment,
                    [sys.executable, "-c", code, str(marker)],
                    dict(os.environ),
                    None,
                    merged=False,
                    external=False,
                )
            )
            _wait_path(marker)
        started = time.monotonic()
        owner.request_close(started + 5)
        owner.request_close(started + 0.5)
        owner.request_close(started + 10)
        result = owner.finish(started + 10)
        assert time.monotonic() - started < 1.5
        assert owner.deadline == started + 0.5
        # An exactly exhausted deadline can be unknown, but both groups received
        # the same final signal before any leader identity was released.
        assert result.state in {"closed", "unknown"}
        for index in range(2):
            _wait_absent(int((tmp_path / str(index)).read_text()))
    finally:
        for command in commands:
            command.dispose()
        owner.finish(time.monotonic() + 2)


def test_capability_is_not_a_serialized_or_hashed_environment_field():
    assert LocalEnvironmentSpec.cancellation_scope == "process_group"
    assert EnvironmentSpec.cancellation_scope == "external"
    assert PixiEnvironmentSpec.cancellation_scope == "external"
    assert "cancellation_scope" not in LocalEnvironmentSpec().model_dump()
    assert (
        "cancellation_scope"
        not in LocalEnvironmentSpec.model_json_schema()["properties"]
    )


@pytest.mark.parametrize("provider", [False, True])
def test_copied_runtime_attachment_is_excluded_from_authored_environment(provider):
    from artisan.schemas.operation_config.compute import ModalComputeConfig

    model = ModalComputeConfig if provider else LocalEnvironmentSpec
    values = dict.fromkeys(ATTACHMENT_ENV, "runtime-only-value")
    environment = model(env={**values, "FORMAT": "json"}, public_env=("FORMAT",))
    assert environment.env == {"FORMAT": "json"}
    assert "runtime-only-value" not in environment.model_dump_json()
    assert not set(ATTACHMENT_ENV) & set(environment.model_dump()["env"])
