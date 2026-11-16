"""Managed native targets require completion, not repeated installed-byte audits."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import IO
from unittest.mock import patch

import pytest

from artisan.execution.environments.native_target import (
    native_executable,
    publish_native_environment,
    verify_native_environment,
)
from artisan.schemas.execution.prepared_environment import (
    NATIVE_BINDING_FILENAME,
    EnvironmentFile,
    EnvironmentSource,
    PreparedNativeEnvironment,
)


@pytest.fixture
def target(tmp_path: Path) -> PreparedNativeEnvironment:
    workspace = tmp_path / "prepared"
    workspace.mkdir()
    files = []
    for name, content in (("pixi.toml", b"source"), ("pixi.lock", b"lock")):
        (workspace / name).write_bytes(content)
        files.append(
            EnvironmentFile(path=name, sha256=hashlib.sha256(content).hexdigest())
        )
    prefix = workspace / ".pixi/envs/default"
    (prefix / "bin").mkdir(parents=True)
    executable = prefix / "bin/tool"
    executable.write_text("#!/bin/sh\nexit 0\n")
    executable.chmod(0o755)
    (prefix / "bin/alias").symlink_to("tool")
    result = PreparedNativeEnvironment(
        source=EnvironmentSource(
            manifest_name="pixi.toml",
            name="default",
            files=tuple(sorted(files, key=lambda item: item.path)),
        ),
        platform="linux-64",
        workspace=workspace,
        prefix=prefix,
    )
    publish_native_environment(result)
    return result


def test_native_executable_stays_in_selected_prefix(
    target: PreparedNativeEnvironment,
) -> None:
    assert native_executable(target, "alias") == target.prefix / "bin/alias"
    with pytest.raises(FileNotFoundError):
        native_executable(target, "host-only")
    with pytest.raises(ValueError, match="inside the prepared"):
        native_executable(target, "/bin/sh")
    (target.prefix / "bin/escape").symlink_to("/bin/sh")
    with pytest.raises(ValueError, match="inside the prepared"):
        native_executable(target, "escape")
    (target.prefix / "bin/tool").chmod(0o644)
    with pytest.raises(ValueError, match="not executable"):
        native_executable(target, "tool")


def test_native_verification_never_reads_installed_payloads(
    target: PreparedNativeEnvironment,
) -> None:
    (target.prefix / "bin/tool").write_text("unsupported manual change")
    (target.prefix / "added.py").write_text("also unsupported")
    (target.prefix / "bin/alias").unlink()
    original_open = Path.open

    def bounded_open(
        path: Path,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> IO[str] | IO[bytes]:
        if path.is_relative_to(target.prefix):
            pytest.fail(f"Installed payload was opened: {path}")
        return original_open(path, mode, buffering, encoding, errors, newline)

    with (
        patch.object(Path, "open", bounded_open),
        patch.object(Path, "rglob", side_effect=AssertionError("installed scan")),
        patch("subprocess.run", side_effect=AssertionError("provisioning")),
    ):
        verify_native_environment(target)


@pytest.mark.parametrize("missing", ["record", "prefix", "manifest"])
def test_native_verification_rejects_incomplete_target(
    target: PreparedNativeEnvironment, missing: str
) -> None:
    if missing == "record":
        (target.workspace / NATIVE_BINDING_FILENAME).unlink()
    elif missing == "prefix":
        target.prefix.rename(target.prefix.with_name("unavailable"))
    else:
        (target.workspace / target.source.manifest_name).unlink()
    with pytest.raises(ValueError, match="prepare explicitly"):
        verify_native_environment(target)


def test_native_verification_rejects_different_completion_record(
    target: PreparedNativeEnvironment,
) -> None:
    changed = target.model_copy(update={"platform": "linux-aarch64"})
    publish_native_environment(changed)
    with pytest.raises(ValueError, match="completion record differs"):
        verify_native_environment(target)


def test_native_completion_rejects_old_inventory_schema(
    target: PreparedNativeEnvironment,
) -> None:
    data = target.model_dump(mode="json")
    data.update(schema_version=1, files=[])
    with pytest.raises(ValueError):
        PreparedNativeEnvironment.model_validate(data)
