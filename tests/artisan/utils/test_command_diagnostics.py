"""Verify attachment rejection and diagnostic attribution at caller boundaries."""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from collections import UserDict
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest

from artisan import cli
from artisan.execution.recording.commands import capture_commands, invocation_scope
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.operation_config.compute import ModalComputeConfig
from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
from artisan.utils import command_supervisor
from artisan.utils.command_supervisor import (
    CommandOwnershipError,
    CommandSupervisor,
    close_command_scope,
    managed_invocation,
)
from artisan.utils.configuration_secrets import COMMAND_ATTACHMENT_ENV
from artisan.utils.external_tools import run_command
from artisan.utils.process_call import execute_process_call, serialize_process_call


@pytest.mark.parametrize("model", [LocalEnvironmentSpec, ModalComputeConfig])
@pytest.mark.parametrize("mapping_type", [dict, UserDict, MappingProxyType])
def test_authored_environment_mapping_never_keeps_runtime_attachment(
    model: Any, mapping_type: Any
) -> None:
    values = dict.fromkeys(COMMAND_ATTACHMENT_ENV, "synthetic-runtime-capability")
    values["FORMAT"] = "json"

    environment = model(env=mapping_type(values), public_env=("FORMAT",))

    assert environment.env == {"FORMAT": "json"}
    assert (
        environment.model_dump_json()
        == model(env={"FORMAT": "json"}, public_env=("FORMAT",)).model_dump_json()
    )


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("invalid", ["partial", "stale", "malformed"])
def test_invalid_attachment_rejected_before_environment_hooks_or_log_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    streaming: bool,
    invalid: str,
) -> None:
    values = (str(tmp_path / "missing.sock"), "synthetic-secret", "root", "")
    for name, value in zip(COMMAND_ATTACHMENT_ENV, values, strict=True):
        monkeypatch.setenv(name, value)
    if invalid == "partial":
        monkeypatch.delenv(COMMAND_ATTACHMENT_ENV[2])
    elif invalid == "malformed":
        monkeypatch.setenv(COMMAND_ATTACHMENT_ENV[0], "")
    environment = Mock(env={}, public_env=())
    environment.wrap_command.return_value = ["not-launched"]
    environment.prepare_env.return_value = None
    log = tmp_path / "retained.log"
    log.write_text("existing evidence")

    with pytest.raises(CommandOwnershipError):
        run_command(
            environment, ["not-launched"], stream_output=streaming, log_path=str(log)
        )

    environment.wrap_command.assert_not_called()
    environment.prepare_env.assert_not_called()
    assert log.read_text() == "existing evidence"


def test_revoked_invocation_rejected_before_environment_hooks() -> None:
    environment = Mock(env={}, public_env=())
    with managed_invocation():
        close_command_scope()
        with pytest.raises(CommandOwnershipError):
            run_command(environment, ["not-launched"])
    environment.wrap_command.assert_not_called()


def test_unknown_parent_command_rejected_before_environment_hooks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner = CommandSupervisor.start()
    try:
        attachment = {**owner.attachment, "parent": "missing-command"}
        for name, key in zip(
            COMMAND_ATTACHMENT_ENV, ("socket", "secret", "scope", "parent"), strict=True
        ):
            monkeypatch.setenv(name, attachment[key])
        environment = Mock(env={}, public_env=())
        with pytest.raises(CommandOwnershipError):
            run_command(environment, ["not-launched"])
        environment.wrap_command.assert_not_called()
    finally:
        owner.finish(time.monotonic() + 5)


def test_cli_invalid_target_redacts_attachment_value(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    secret = "synthetic-cli-capability"
    monkeypatch.setattr(
        command_supervisor, "attachment_sensitive_values", lambda: (secret,)
    )

    assert cli._op_run_managed(argparse.Namespace(target=secret)) == 1

    assert secret not in capsys.readouterr().err


@pytest.mark.parametrize("inherited", [False, True])
@pytest.mark.parametrize("phase", ["bootstrap", "execute"])
def test_cli_runtime_failure_redacts_active_capability(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    inherited: bool,
    phase: str,
) -> None:
    sensitive: set[str] = set()
    original_values = command_supervisor.attachment_sensitive_values

    def capture_values() -> tuple[str, ...]:
        values = original_values()
        sensitive.update(values)
        return values

    monkeypatch.setattr(
        command_supervisor, "attachment_sensitive_values", capture_values
    )
    monkeypatch.setattr(OperationDefinition, "_registry", {})
    monkeypatch.setattr(OperationDefinition, "_name_collisions", [])
    module_name = "cli_capability_failure"
    failure = (
        f'raise RuntimeError("{phase} failed " '
        '+ " ".join(attachment_sensitive_values()))'
    )
    source = (
        "from artisan.utils.command_supervisor import attachment_sensitive_values\n"
    )
    if phase == "bootstrap":
        source += failure + "\n"
    else:
        source += (
            "from artisan.operations.examples.data_generator import DataGenerator\n"
            "class FailingOperation(DataGenerator):\n"
            "    def execute_function(self, inputs):\n"
            f"        {failure}\n"
        )
    (tmp_path / f"{module_name}.py").write_text(source)
    monkeypatch.syspath_prepend(str(tmp_path))
    args = argparse.Namespace(
        target=f"{module_name}:FailingOperation",
        inputs="{}",
        params="{}",
        execute_dir=str(tmp_path),
    )
    try:
        with ExitStack() as stack:
            if inherited:
                stack.enter_context(managed_invocation())
            assert cli._op_run(args) == 1
    finally:
        sys.modules.pop(module_name, None)

    stderr = capsys.readouterr().err
    assert sensitive
    assert all(value not in stderr for value in sensitive)
    assert f"RuntimeError: {phase} failed" in stderr
    assert "<redacted>" in stderr


def test_invalid_worker_attachment_prevents_payload_deserialization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    marker = tmp_path / "deserialized"

    class Payload:
        def __reduce__(self) -> tuple[Any, tuple[str]]:
            return marker.write_text, ("user code ran",)

    call = serialize_process_call(lambda value: value, Payload())
    monkeypatch.setenv(COMMAND_ATTACHMENT_ENV[1], "synthetic-worker-capability")
    with pytest.raises(CommandOwnershipError):
        execute_process_call(call)
    assert not marker.exists()


@pytest.mark.parametrize("capture_first", [False, True])
def test_joined_commands_keep_enclosing_dispatch_slot_and_declared_tool(
    capture_first: bool,
) -> None:
    operation = SimpleNamespace(
        tool=SimpleNamespace(
            executable="declared-tool", interpreter=None, subcommand=None
        )
    )
    with ExitStack() as stack:
        if capture_first:
            recorder = stack.enter_context(capture_commands())
            stack.enter_context(invocation_scope(operation, 7))
        stack.enter_context(managed_invocation())
        if not capture_first:
            recorder = stack.enter_context(capture_commands())
            stack.enter_context(invocation_scope(operation, 7))
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(
                    run_command,
                    LocalEnvironmentSpec(),
                    [sys.executable, "-c", "print('joined')"],
                )
                for _ in range(2)
            ]
            assert [future.result(timeout=10).stdout for future in futures] == [
                "joined\n",
                "joined\n",
            ]

    commands = recorder.snapshot().commands
    assert [(command.invocation, command.sequence) for command in commands] == [
        (7, 0),
        (7, 1),
    ]
    assert all(
        command.tool and command.tool.executable == "declared-tool"
        for command in commands
    )


@pytest.mark.parametrize("streaming", [False, True])
def test_managed_command_inherits_callers_redirected_stdin(streaming: bool) -> None:
    code = f"""
import sys
from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
from artisan.utils.external_tools import run_command
result = run_command(LocalEnvironmentSpec(),
    [sys.executable, '-c', 'import sys;sys.stdout.write(sys.stdin.read())'],
    stream_output={streaming!r})
if not {streaming!r}:
    sys.stdout.write(result.stdout)
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        input="inherited stdin\n",
        capture_output=True,
        text=True,
        timeout=15,
        check=True,
    )
    assert result.stdout == "inherited stdin\n"


@pytest.mark.parametrize("streaming", [False, True])
def test_standalone_ctrl_c_preserves_interruption_after_command_cleanup(
    tmp_path: Path, streaming: bool
) -> None:
    marker = tmp_path / "command.pid"
    command_code = """
import os, pathlib, signal, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
signal.alarm(20)
pathlib.Path(sys.argv[1]).write_text(str(os.getpid()))
time.sleep(15)
"""
    caller_code = f"""
import sys
from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
from artisan.utils.external_tools import run_command
try:
    run_command(LocalEnvironmentSpec(), [sys.executable, '-c', sys.argv[1], sys.argv[2]],
        stream_output={streaming!r})
except KeyboardInterrupt:
    print('original interruption preserved')
else:
    raise AssertionError('command was not interrupted')
"""
    caller = subprocess.Popen(
        [sys.executable, "-c", caller_code, command_code, str(marker)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 10
        while not marker.exists() and time.monotonic() < deadline:
            assert caller.poll() is None, "caller exited before command startup"
            time.sleep(0.01)
        assert marker.exists()
        caller.send_signal(signal.SIGINT)
        stdout, _stderr = caller.communicate(timeout=12)
        assert caller.returncode == 0
        assert stdout == "original interruption preserved\n"
        with pytest.raises(ProcessLookupError):
            os.kill(int(marker.read_text()), 0)
    finally:
        if caller.poll() is None:
            caller.kill()
        caller.communicate(timeout=5)
