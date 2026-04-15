"""Snapshot and restore sandbox directories for remote transport.

Walks sandbox input directories and execute output directories,
packing files into ``dict[str, bytes]`` for shipping via cloudpickle
(e.g. to a Modal container). Enforces a 50 MB size limit per
direction to stay within gRPC payload constraints.
"""

from __future__ import annotations

import os
from typing import Any

_INPUT_DIRS = ("materialized_inputs", "preprocess", "execute")
_MAX_SNAPSHOT_BYTES = 50 * 1024 * 1024  # 50 MB


def snapshot_sandbox(root: str) -> dict[str, bytes]:
    """Snapshot sandbox input files as {relative_path: content}.

    Walks only ``_INPUT_DIRS`` under root. Skips postprocess/
    (not needed for execute phase). Returns empty dict when
    root contains no files (in-memory operations).

    Args:
        root: Path to the sandbox root directory.

    Returns:
        Dict mapping relative paths to file contents.

    Raises:
        ValueError: If total size exceeds 50 MB.
    """
    snapshot: dict[str, bytes] = {}
    total_size = 0

    for dir_name in _INPUT_DIRS:
        dir_path = os.path.join(root, dir_name)
        if not os.path.isdir(dir_path):
            continue
        for dirpath, _, filenames in os.walk(dir_path):
            for filename in filenames:
                abs_path = os.path.join(dirpath, filename)
                rel_path = os.path.relpath(abs_path, root)
                content = _read_file(abs_path)
                total_size += len(content)
                snapshot[rel_path] = content

    _check_size(total_size)
    return snapshot


def snapshot_outputs(execute_dir: str) -> dict[str, bytes]:
    """Snapshot execute output files as {relative_path: content}.

    Walks all files under execute_dir (the output directory).
    Returns empty dict when execute_dir contains no files
    (in-memory operations that return results directly).

    Args:
        execute_dir: Path to the execute output directory.

    Returns:
        Dict mapping relative paths to file contents.

    Raises:
        ValueError: If total size exceeds 50 MB.
    """
    snapshot: dict[str, bytes] = {}
    total_size = 0

    if not os.path.isdir(execute_dir):
        return snapshot

    for dirpath, _, filenames in os.walk(execute_dir):
        for filename in filenames:
            abs_path = os.path.join(dirpath, filename)
            rel_path = os.path.relpath(abs_path, execute_dir)
            content = _read_file(abs_path)
            total_size += len(content)
            snapshot[rel_path] = content

    _check_size(total_size)
    return snapshot


def restore_sandbox(root: str, snapshot: dict[str, bytes]) -> None:
    """Recreate sandbox files from a snapshot.

    Recreates the directory structure under root and writes
    each file at its original relative path.

    Args:
        root: Target root directory for restoration.
        snapshot: Dict mapping relative paths to file contents.
    """
    for rel_path, content in snapshot.items():
        abs_path = os.path.join(root, rel_path)
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        with open(abs_path, "wb") as f:
            f.write(content)


def _read_file(path: str) -> bytes:
    """Read a file as bytes."""
    with open(path, "rb") as f:
        return f.read()


def snapshot_sandbox_for_artifact(
    sandbox_root: str,
    execute_input: Any,
) -> dict[str, bytes]:
    """Snapshot only the files needed for a single artifact's execute.

    Captures materialized input files referenced in
    ``execute_input.inputs`` and the shared ``preprocess/`` directory.
    Excludes other artifacts' materialized files, keeping per-artifact
    payloads small for Modal transport.

    Args:
        sandbox_root: Unit-level sandbox root.
        execute_input: Per-artifact ``ExecuteInput`` with file
            references in its ``inputs`` dict.

    Returns:
        Snapshot dict suitable for ``restore_sandbox()``.

    Raises:
        ValueError: If total size exceeds 50 MB.
    """
    snapshot: dict[str, bytes] = {}
    total_size = 0

    # Collect file paths referenced in execute_input.inputs
    referenced_paths: set[str] = set()
    _collect_file_paths(execute_input.inputs, sandbox_root, referenced_paths)

    # Snapshot referenced files
    for abs_path in sorted(referenced_paths):
        if os.path.isfile(abs_path):
            rel_path = os.path.relpath(abs_path, sandbox_root)
            content = _read_file(abs_path)
            total_size += len(content)
            snapshot[rel_path] = content

    # Always include the shared preprocess/ directory
    preprocess_dir = os.path.join(sandbox_root, "preprocess")
    if os.path.isdir(preprocess_dir):
        for dirpath, _, filenames in os.walk(preprocess_dir):
            for filename in filenames:
                abs_path = os.path.join(dirpath, filename)
                rel_path = os.path.relpath(abs_path, sandbox_root)
                if rel_path not in snapshot:
                    content = _read_file(abs_path)
                    total_size += len(content)
                    snapshot[rel_path] = content

    _check_size(total_size)
    return snapshot


def _collect_file_paths(
    value: Any,
    sandbox_root: str,
    out: set[str],
) -> None:
    """Recursively find file paths under sandbox_root in a nested structure."""
    if isinstance(value, str):
        if value.startswith(sandbox_root) and os.path.isfile(value):
            out.add(value)
    elif isinstance(value, dict):
        for v in value.values():
            _collect_file_paths(v, sandbox_root, out)
    elif isinstance(value, list | tuple):
        for item in value:
            _collect_file_paths(item, sandbox_root, out)


def _check_size(total_size: int) -> None:
    """Raise if total snapshot size exceeds the limit."""
    if total_size > _MAX_SNAPSHOT_BYTES:
        size_mb = total_size / (1024 * 1024)
        msg = (
            f"Snapshot is {size_mb:.1f} MB, exceeding the 50 MB limit "
            "for Modal transport. Put large data on S3 and pass URIs "
            "instead of materializing to the sandbox."
        )
        raise ValueError(msg)
