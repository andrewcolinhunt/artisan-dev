"""Real exec failure, tool status and read-only internal helper coverage."""

from __future__ import annotations

import errno
import json
import platform
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest

from artisan.execution.compute.prepared import PreparedExecuteRouter
from artisan.execution.environments.command_witness import (
    BUILD_MANIFEST,
    WitnessInvocation,
    lookup_command_witness,
    prepare_command_witness,
    verify_command_witness,
)
from artisan.execution.exceptions import AssignmentExecutionError
from artisan.execution.scientific_failure import assignment_science
from artisan.schemas.execution.command_witness import PreparedCommandWitness
from artisan.schemas.specs.input_models import ExecuteInput

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def prepared(
    tmp_path_factory: pytest.TempPathFactory,
) -> tuple[Path, PreparedCommandWitness]:
    if (platform.system(), platform.machine()) != ("Linux", "x86_64"):
        pytest.skip("The static command helper is qualified for Linux x86_64")
    root = tmp_path_factory.mktemp("witness with space")
    trap = root / "host-bin"
    trap.mkdir()
    for name in ("gcc", "cc", "as", "ld"):
        executable = trap / name
        executable.write_text("#!/bin/sh\nexit 99\n")
        executable.chmod(0o755)
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("PATH", f"{trap}:/usr/bin:/bin")
        patch.setenv("CC", str(trap / "cc"))
        prepared = prepare_command_witness(root)
    assert prepared.compiler_version.startswith("x86_64-conda-linux-gnu-cc")
    return root, prepared


def test_changed_build_profile_requires_new_explicit_preparation(
    prepared: tuple[Path, PreparedCommandWitness],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = tmp_path / "pixi.toml"
    manifest.write_bytes(BUILD_MANIFEST.read_bytes() + b"\n# Changed build source\n")
    shutil.copyfile(BUILD_MANIFEST.with_name("pixi.lock"), tmp_path / "pixi.lock")
    monkeypatch.setattr(
        "artisan.execution.environments.command_witness.BUILD_MANIFEST", manifest
    )

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Read-only helper validation launched a process")

    monkeypatch.setattr(subprocess, "run", forbidden)
    monkeypatch.setattr(subprocess, "check_output", forbidden)
    with pytest.raises(ValueError, match="not prepared"):
        lookup_command_witness(prepared[0])
    with pytest.raises(ValueError, match="identity changed"):
        verify_command_witness(prepared[1])


def test_prepared_helper_does_not_need_build_environment_at_runtime(
    prepared: tuple[Path, PreparedCommandWitness], tmp_path: Path
) -> None:
    root, target = prepared
    builders = root / "native-v2"
    hidden = root / "unavailable-builders"
    builders.rename(hidden)
    try:
        assert lookup_command_witness(root) == target
        invocation = WitnessInvocation(target, tmp_path)
        result = subprocess.run(invocation.wrap(["/bin/true"]), check=False)
        assert result.returncode == 0
        assert invocation.verify_exit(0)
    finally:
        hidden.rename(builders)


@pytest.mark.parametrize("code", [0, 1, 17, 125, 126, 127, 255])
def test_actual_tool_exit_including_exec_reserved_codes(
    prepared: tuple[Path, PreparedCommandWitness], tmp_path: Path, code: int
) -> None:
    invocation = WitnessInvocation(prepared[1], tmp_path)
    result = subprocess.run(
        invocation.wrap(
            [
                "/bin/sh",
                "-c",
                'test "$1" = \'literal $value\' || exit 99; test "$TOKEN" = selected || exit 98; exit "$2"',
                "probe",
                "literal $value",
                str(code),
            ]
        ),
        env={"TOKEN": "selected"},
        capture_output=True,
        check=False,
    )
    assert result.returncode == code
    assert invocation.verify_exit(code)
    assert result.stdout == result.stderr == b""


@pytest.mark.parametrize("missing_interpreter", [False, True])
def test_exec_failure_is_not_scientific(
    prepared: tuple[Path, PreparedCommandWitness],
    tmp_path: Path,
    missing_interpreter: bool,
) -> None:
    executable = tmp_path / "unavailable"
    if missing_interpreter:
        executable.write_text("#!/missing/interpreter\n")
        executable.chmod(0o755)
    invocation = WitnessInvocation(prepared[1], tmp_path)
    result = subprocess.run(invocation.wrap([str(executable)]), check=False)
    assert result.returncode == 125
    assert json.loads(invocation.receipt.read_text())["kind"] == "exec_error"
    with pytest.raises(RuntimeError, match="did not confirm"):
        invocation.verify_exit(result.returncode)


@pytest.mark.parametrize("symlink", [False, True])
def test_prefix_check_preserves_existing_tools_and_literal_arguments(
    prepared: tuple[Path, PreparedCommandWitness], tmp_path: Path, symlink: bool
) -> None:
    prefix = tmp_path / "tool prefix"
    (prefix / "bin").mkdir(parents=True)
    executable = prefix / "bin/tool"
    shutil.copyfile("/bin/echo", executable)
    executable.chmod(0o755)
    if symlink:
        (prefix / "bin/alias").symlink_to("tool")
        executable = prefix / "bin/alias"
    literal = 'literal $value, spaces and "quotes"\nsecond line'
    invocation = WitnessInvocation(prepared[1], tmp_path / "receipts")
    result = subprocess.run(
        invocation.wrap([str(executable), literal], prefix=prefix),
        check=False,
        capture_output=True,
    )
    assert result.stdout == (literal + "\n").encode()
    assert result.stderr == b""
    assert result.returncode == 0
    assert invocation.verify_exit(0)


@pytest.mark.parametrize("symlink", ["executable", "parent", "prefix"])
def test_prefix_check_rejects_symlink_escape_before_tool_exec(
    prepared: tuple[Path, PreparedCommandWitness], tmp_path: Path, symlink: str
) -> None:
    prefix = tmp_path / "prefix"
    outside = tmp_path / "prefix-sibling"
    (outside / "bin").mkdir(parents=True)
    executable = outside / "bin/tool"
    executable.write_text("#!/bin/sh\n: > escaped\n")
    executable.chmod(0o755)
    if symlink == "prefix":
        prefix.symlink_to(outside, target_is_directory=True)
    elif symlink == "parent":
        prefix.mkdir()
        (prefix / "bin").symlink_to(outside / "bin", target_is_directory=True)
    else:
        (prefix / "bin").mkdir(parents=True)
        (prefix / "bin/tool").symlink_to(executable)
    invocation = WitnessInvocation(prepared[1], tmp_path / "receipts")
    result = subprocess.run(
        invocation.wrap([str(prefix / "bin/tool")], prefix=prefix),
        cwd=tmp_path,
        check=False,
    )
    assert result.returncode == 125
    assert not (tmp_path / "escaped").exists()
    receipt = json.loads(invocation.receipt.read_text())
    assert (receipt["kind"], receipt["code"]) == ("exec_error", errno.EACCES)
    with pytest.raises(RuntimeError, match="did not confirm"):
        invocation.verify_exit(result.returncode)


def test_prefix_check_reports_missing_tool_as_exec_failure(
    prepared: tuple[Path, PreparedCommandWitness], tmp_path: Path
) -> None:
    prefix = tmp_path / "prefix"
    prefix.mkdir()
    invocation = WitnessInvocation(prepared[1], tmp_path / "receipts")
    result = subprocess.run(
        invocation.wrap([str(prefix / "bin/missing")], prefix=prefix), check=False
    )
    assert result.returncode == 125
    receipt = json.loads(invocation.receipt.read_text())
    assert (receipt["kind"], receipt["code"]) == ("exec_error", errno.ENOENT)
    with pytest.raises(RuntimeError, match="did not confirm"):
        invocation.verify_exit(result.returncode)


def test_signal_status_does_not_claim_normal_tool_exit(
    prepared: tuple[Path, PreparedCommandWitness], tmp_path: Path
) -> None:
    invocation = WitnessInvocation(prepared[1], tmp_path)
    result = subprocess.run(
        invocation.wrap(["/bin/sh", "-c", "kill -TERM $$"]), check=False
    )
    assert result.returncode == 143
    assert not invocation.verify_exit(result.returncode)


def test_lookup_never_builds_and_missing_changed_or_wrong_receipts_reject(
    prepared: tuple[Path, PreparedCommandWitness],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Lookup launched a process")

    monkeypatch.setattr(subprocess, "run", forbidden)
    monkeypatch.setattr(subprocess, "check_output", forbidden)
    assert lookup_command_witness(prepared[0]) == prepared[1]
    with pytest.raises(ValueError, match="not prepared"):
        lookup_command_witness(tmp_path)
    invocation = WitnessInvocation(prepared[1], tmp_path)
    with pytest.raises(FileNotFoundError):
        invocation.verify_exit(17)
    invocation.receipt.write_text(
        json.dumps({"nonce": "0" * 32, "kind": "exit", "code": 17})
    )
    with pytest.raises(RuntimeError, match="did not confirm"):
        invocation.verify_exit(17)
    invocation.receipt.write_text(
        json.dumps({"nonce": invocation.nonce, "kind": "exit", "code": 17})
    )
    with pytest.raises(RuntimeError, match="did not confirm"):
        invocation.verify_exit(1)


@dataclass
class _TestCommandEnvironment:
    witness: WitnessInvocation
    mode: str

    def wrap_command(self, command: list[str], cwd: str | None = None) -> list[str]:
        if self.mode == "launcher_failure":
            return ["/bin/sh", "-c", "exit 17"]
        if self.mode == "exec_failure":
            return self.witness.wrap(["/missing/executable"])
        return self.witness.wrap(command)

    def prepare_env(self) -> dict[str, str]:
        return {"PATH": "/usr/bin:/bin"}


@pytest.mark.parametrize("code", [17, 126, 127])
def test_router_marks_only_confirmed_normal_tool_failure(
    prepared: tuple[Path, PreparedCommandWitness], tmp_path: Path, code: int
) -> None:
    router = PreparedExecuteRouter(
        None,
        pixi=Path("/unused"),
        system_env=Path("/usr/bin/env"),
        runtime_env={},
        witness=prepared[1],
        command_environment=lambda sandbox, witness: _TestCommandEnvironment(
            witness, "tool"
        ),
    )
    operation = SimpleNamespace(
        is_command_op=lambda: True,
        execute_as_tool=True,
        execute_command=lambda inputs: ["/bin/sh", "-c", f"exit {code}"],
    )
    with assignment_science(), pytest.raises(AssignmentExecutionError) as caught:
        router.route_execute(operation, [ExecuteInput(str(tmp_path))], str(tmp_path))
    assert caught.value.phase == "execute"
    assert f"exit code {code}" in str(caught.value)


@pytest.mark.parametrize("mode", ["launcher_failure", "exec_failure"])
def test_router_keeps_launcher_or_exec_failure_infrastructure(
    prepared: tuple[Path, PreparedCommandWitness], tmp_path: Path, mode: str
) -> None:
    router = PreparedExecuteRouter(
        None,
        pixi=Path("/unused"),
        system_env=Path("/usr/bin/env"),
        runtime_env={},
        witness=prepared[1],
        command_environment=lambda sandbox, witness: _TestCommandEnvironment(
            witness, mode
        ),
    )
    operation = SimpleNamespace(
        is_command_op=lambda: True,
        execute_as_tool=True,
        execute_command=lambda inputs: ["/bin/true"],
    )
    with (
        assignment_science(),
        pytest.raises((FileNotFoundError, RuntimeError)) as caught,
    ):
        router.route_execute(operation, [ExecuteInput(str(tmp_path))], str(tmp_path))
    assert not isinstance(caught.value, AssignmentExecutionError)


def test_changed_helper_payload_rejects_before_execution(
    prepared: tuple[Path, PreparedCommandWitness], tmp_path: Path
) -> None:
    executable = tmp_path / "witness"
    executable.write_bytes(prepared[1].executable.read_bytes() + b"changed")
    executable.chmod(0o555)
    target = prepared[1].model_copy(update={"executable": executable})
    with pytest.raises(ValueError, match="identity changed"):
        verify_command_witness(target)
