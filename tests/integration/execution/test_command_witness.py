"""Real exec failure, tool status and read-only internal helper coverage."""

from __future__ import annotations

import errno
import json
import os
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
    COMPILE_FLAGS,
    COMPILER,
    SOURCE,
    WitnessInvocation,
    lookup_command_witness,
    prepare_command_witness,
    verify_command_witness,
)
from artisan.execution.environments.external_preflight import (
    encode_external_preflight,
    write_external_preflight,
)
from artisan.execution.environments.native import lookup_native_environment
from artisan.execution.environments.native_target import native_executable
from artisan.execution.exceptions import AssignmentExecutionError
from artisan.execution.scientific_failure import assignment_science
from artisan.schemas.artifact.external_descriptor import (
    ExternalFile,
    ExternalFilesDescriptor,
    PreparedExternalFiles,
)
from artisan.schemas.execution.command_witness import PreparedCommandWitness
from artisan.schemas.operation_config.environment import Environment
from artisan.schemas.specs.input_models import ExecuteInput
from artisan.storage.external_access import prepare_external_files
from artisan.utils.hashing import compute_content_digest

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
    executable.write_text("#!/bin/sh\nprintf '%s\\n' \"$@\"\n")
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
    assert result.returncode == 0, result.stderr
    assert result.stdout == (literal + "\n").encode()
    assert result.stderr == b""
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


@pytest.fixture(scope="module")
def readonly_witness(
    prepared: tuple[Path, PreparedCommandWitness],
    tmp_path_factory: pytest.TempPathFactory,
) -> PreparedCommandWitness:
    """Mock only readonly mount observation using the same locked C toolchain."""
    root = tmp_path_factory.mktemp("external-witness")
    shim = root / "readonly.c"
    shim.write_text(
        "#include <sys/statvfs.h>\n"
        "int __real_fstatvfs(int fd, struct statvfs *value);\n"
        "int __wrap_fstatvfs(int fd, struct statvfs *value) {\n"
        "  int status = __real_fstatvfs(fd, value);\n"
        "  if (status == 0) value->f_flag |= ST_RDONLY;\n"
        "  return status;\n}\n"
    )
    builder = lookup_native_environment(
        Environment.from_pixi(BUILD_MANIFEST), cache_root=prepared[0]
    )
    compiler = native_executable(builder, COMPILER)
    executable = root / "witness"
    subprocess.run(
        [
            str(compiler),
            *COMPILE_FLAGS,
            "-Wl,--wrap=fstatvfs",
            str(SOURCE),
            str(shim),
            "-o",
            str(executable),
        ],
        env={"PATH": str(builder.prefix / "bin"), "LANG": "C", "TMPDIR": str(root)},
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return prepared[1].model_copy(update={"executable": executable})


def _external_installation(root: Path) -> PreparedExternalFiles:
    content = {"config.json": b"{}", "nested/content.bin": b"external-data"}
    for name, value in content.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(value)
    return prepare_external_files(
        root,
        ExternalFilesDescriptor(
            files=tuple(
                ExternalFile(
                    path=name,
                    content_hash=compute_content_digest(value),
                    size_bytes=len(value),
                )
                for name, value in content.items()
            )
        ),
    )


def test_external_preflight_checks_metadata_then_preserves_actual_tool_exit(
    readonly_witness: PreparedCommandWitness,
    tmp_path: Path,
) -> None:
    installation = _external_installation(tmp_path / "installation")
    payload = installation.root / "nested/content.bin"
    payload.chmod(0o000)
    record = write_external_preflight((installation,), tmp_path / "control/access.bin")
    invocation = WitnessInvocation(readonly_witness, tmp_path / "exits")
    try:
        result = subprocess.run(
            invocation.wrap(["/bin/sh", "-c", "exit 17"], external_access=record),
            check=False,
            capture_output=True,
        )
        assert result.returncode == 17, result.stderr
        assert invocation.verify_exit(17)
    finally:
        payload.chmod(0o600)


def test_external_preflight_rejects_real_writable_installation_before_tool(
    prepared: tuple[Path, PreparedCommandWitness],
    tmp_path: Path,
) -> None:
    installation = _external_installation(tmp_path / "installation")
    record = write_external_preflight((installation,), tmp_path / "control/access.bin")
    invocation = WitnessInvocation(prepared[1], tmp_path / "exits")
    marker = tmp_path / "ran"
    result = subprocess.run(
        invocation.wrap(["/bin/touch", str(marker)], external_access=record),
        check=False,
        capture_output=True,
    )
    assert result.returncode == 125
    assert b"External artifact access check failed" in result.stderr
    assert not marker.exists()
    assert not invocation.receipt.exists()


@pytest.mark.parametrize(
    "change",
    [
        "receipt",
        "missing",
        "size",
        "extra",
        "empty_directory",
        "symlink_file",
        "symlink_directory",
        "symlink_root",
    ],
)
def test_external_preflight_rejects_changed_layout_before_tool(
    readonly_witness: PreparedCommandWitness,
    tmp_path: Path,
    change: str,
) -> None:
    installation = _external_installation(tmp_path / "installation")
    root = installation.root
    if change == "receipt":
        (root / ".artisan-external-files.json").write_bytes(b"wrong")
    elif change == "missing":
        (root / "config.json").unlink()
    elif change == "size":
        (root / "config.json").write_bytes(b"different-size")
    elif change == "extra":
        (root / "extra.bin").write_bytes(b"extra")
    elif change == "empty_directory":
        (root / "extra").mkdir()
    elif change == "symlink_file":
        (root / "config.json").unlink()
        (root / "config.json").symlink_to(root / "nested/content.bin")
    elif change == "symlink_directory":
        (root / "nested").rename(tmp_path / "moved")
        (root / "nested").symlink_to(tmp_path / "moved", target_is_directory=True)
    else:
        alias = tmp_path / "alias"
        alias.symlink_to(root, target_is_directory=True)
        installation = installation.model_copy(update={"root": alias})
    record = write_external_preflight((installation,), tmp_path / "control/access.bin")
    invocation = WitnessInvocation(readonly_witness, tmp_path / "exits")
    marker = tmp_path / "ran"
    result = subprocess.run(
        invocation.wrap(["/bin/touch", str(marker)], external_access=record),
        check=False,
        capture_output=True,
    )
    assert result.returncode == 125
    assert not marker.exists()
    assert not invocation.receipt.exists()


@pytest.mark.parametrize(
    "malformed", ["magic", "truncated", "extra", "relative_escape"]
)
def test_external_preflight_rejects_malformed_evidence_before_tool(
    readonly_witness: PreparedCommandWitness,
    tmp_path: Path,
    malformed: str,
) -> None:
    installation = _external_installation(tmp_path / "installation")
    encoded = encode_external_preflight((installation,))
    if malformed == "magic":
        encoded = b"INVALID!" + encoded[8:]
    elif malformed == "truncated":
        encoded = encoded[:-1]
    elif malformed == "extra":
        encoded += b"trailing"
    else:
        offset = encoded.rfind(b"nested/content.bin")
        encoded = (
            encoded[:offset]
            + b"../bad/content.bin"
            + encoded[offset + len(b"nested/content.bin") :]
        )
    record = tmp_path / "access.bin"
    record.write_bytes(encoded)
    invocation = WitnessInvocation(readonly_witness, tmp_path / "exits")
    marker = tmp_path / "ran"
    result = subprocess.run(
        invocation.wrap(["/bin/touch", str(marker)], external_access=record),
        check=False,
        capture_output=True,
    )
    assert result.returncode == 125
    assert not marker.exists()
    assert not invocation.receipt.exists()


@pytest.mark.parametrize("receipt_state", ["valid", "missing", "wrong"])
def test_external_preflight_uses_real_docker_readonly_mounts(
    prepared: tuple[Path, PreparedCommandWitness],
    docker_image: str,
    tmp_path: Path,
    receipt_state: str,
) -> None:
    installation = _external_installation(tmp_path / "installation")
    record = write_external_preflight((installation,), tmp_path / "control/access.bin")
    receipt = installation.root / ".artisan-external-files.json"
    if receipt_state == "missing":
        receipt.unlink()
    elif receipt_state == "wrong":
        receipt.write_bytes(b"wrong")
    invocation = WitnessInvocation(prepared[1], tmp_path / "exits")
    marker = invocation.directory / "science"
    command = invocation.wrap(
        [
            "/bin/sh",
            "-c",
            'test "$(cat "$1/nested/content.bin")" = external-data || exit 90; '
            'if echo changed > "$1/nested/content.bin" 2>/dev/null; then exit 91; fi; '
            'touch "$2"; exit 17',
            "probe",
            str(installation.root),
            str(marker),
        ],
        external_access=record,
    )
    mounts = [
        f"type=bind,source={installation.root},target={installation.root},readonly,bind-propagation=rprivate,bind-recursive=readonly",
        f"type=bind,source={record},target={record},readonly",
        f"type=bind,source={prepared[1].executable},target={prepared[1].executable},readonly",
        f"type=bind,source={invocation.directory},target={invocation.directory}",
    ]
    result = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--pull=never",
            "--read-only",
            "--network=none",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            *[argument for mount in mounts for argument in ("--mount", mount)],
            "--entrypoint",
            command[0],
            docker_image,
            *command[1:],
        ],
        check=False,
        capture_output=True,
        timeout=60,
    )
    assert (installation.root / "nested/content.bin").read_bytes() == b"external-data"
    if receipt_state == "valid":
        assert result.returncode == 17, result.stderr
        assert marker.exists()
        assert invocation.verify_exit(17)
    else:
        assert result.returncode == 125, result.stderr
        assert b"External artifact access check failed" in result.stderr
        assert not marker.exists()
        assert not invocation.receipt.exists()
