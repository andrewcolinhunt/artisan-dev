"""Sandbox helpers for creator execution."""

from __future__ import annotations

from pathlib import Path


def create_sandbox(
    sandbox_path: Path,
) -> tuple[Path, Path, Path, Path]:
    """Create sandbox directories for creator execution.

    Args:
        sandbox_path: Pre-computed path for this execution unit's sandbox.

    Returns:
        Tuple of (sandbox_path, preprocess_dir, execute_dir, postprocess_dir).
    """
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
