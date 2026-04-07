"""Sandbox helpers for creator execution."""

from __future__ import annotations

import glob
import os


def create_sandbox(
    sandbox_path: str,
) -> tuple[str, str, str, str]:
    """Create sandbox directories for creator execution.

    Args:
        sandbox_path: Pre-computed path for this execution unit's sandbox.

    Returns:
        Tuple of (sandbox_path, preprocess_dir, execute_dir, postprocess_dir).
    """
    os.makedirs(sandbox_path, exist_ok=True)

    preprocess_dir = os.path.join(sandbox_path, "preprocess")
    execute_dir = os.path.join(sandbox_path, "execute")
    postprocess_dir = os.path.join(sandbox_path, "postprocess")

    os.makedirs(preprocess_dir, exist_ok=True)
    os.makedirs(execute_dir, exist_ok=True)
    os.makedirs(postprocess_dir, exist_ok=True)

    return sandbox_path, preprocess_dir, execute_dir, postprocess_dir


def output_snapshot(execute_dir: str) -> list[str]:
    """Return all files present in the execute-phase directory."""
    return [
        path
        for path in glob.glob(os.path.join(execute_dir, "**", "*"), recursive=True)
        if os.path.isfile(path)
    ]
