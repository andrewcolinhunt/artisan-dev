"""Use completed managed native installations without auditing installed bytes."""

from __future__ import annotations

import stat
from pathlib import Path

from artisan.schemas.execution.prepared_environment import (
    NATIVE_BINDING_FILENAME,
    PreparedNativeEnvironment,
)


def verify_native_environment(target: PreparedNativeEnvironment) -> None:
    """Require the admitted completion record and usable installation paths."""
    record = target.workspace / NATIVE_BINDING_FILENAME
    if record.is_symlink() or not record.is_file():
        msg = "Prepared native completion record is missing; prepare explicitly"
        raise ValueError(msg)
    completed = PreparedNativeEnvironment.model_validate_json(record.read_bytes())
    if (
        completed.digest != target.digest
        or completed.workspace != target.workspace
        or completed.prefix != target.prefix
    ):
        msg = "Prepared native completion record differs from the selected target"
        raise ValueError(msg)
    if (
        not target.prefix.is_dir()
        or target.prefix.resolve() != target.prefix
        or not (target.workspace / target.source.manifest_name).is_file()
    ):
        msg = "Prepared native installation is unavailable; prepare explicitly"
        raise ValueError(msg)


def native_executable(target: PreparedNativeEnvironment, name: str) -> Path:
    """Resolve one existing executable within the selected managed prefix."""
    candidate = Path(name)
    if not candidate.is_absolute():
        candidate = target.prefix / "bin" / candidate
    resolved = candidate.resolve(strict=True)
    if (
        not resolved.is_relative_to(target.prefix)
        or not resolved.is_file()
        or not stat.S_IMODE(resolved.stat().st_mode) & 0o111
    ):
        msg = "Command is not executable inside the prepared native prefix"
        raise ValueError(msg)
    return candidate


def publish_native_environment(target: PreparedNativeEnvironment) -> None:
    """Publish completion after explicit installation and preparation checks."""
    temporary = target.workspace / f"{NATIVE_BINDING_FILENAME}.tmp"
    temporary.write_text(target.model_dump_json(indent=2))
    temporary.replace(target.workspace / NATIVE_BINDING_FILENAME)
