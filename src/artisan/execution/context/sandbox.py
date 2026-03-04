"""Sandbox helpers for creator execution."""

from __future__ import annotations

from pathlib import Path

from artisan.utils.path import shard_path


def create_sandbox(
    working_root: Path,
    execution_run_id: str,
    step_number: int,
    operation_name: str | None = None,
) -> tuple[Path, Path, Path, Path]:
    """Create sharded sandbox directories for creator execution.

    Returns:
        Tuple of (sandbox_path, preprocess_dir, execute_dir, postprocess_dir).
    """
    sandbox_path = shard_path(
        working_root, execution_run_id, step_number, operation_name=operation_name
    )
    sandbox_path.mkdir(parents=True, exist_ok=True)

    preprocess_dir = sandbox_path / "preprocess"
    execute_dir = sandbox_path / "execute"
    postprocess_dir = sandbox_path / "postprocess"

    preprocess_dir.mkdir(exist_ok=True)
    execute_dir.mkdir(exist_ok=True)
    postprocess_dir.mkdir(exist_ok=True)

    return sandbox_path, preprocess_dir, execute_dir, postprocess_dir


def output_snapshot(execute_dir: Path) -> list[Path]:
    """Return all files present in the execute-phase directory."""
    return [path for path in execute_dir.glob("**/*") if path.is_file()]
