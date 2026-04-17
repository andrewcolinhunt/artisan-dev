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


def snapshot_sandbox(root: str) -> tuple[dict[str, bytes], list[str]]:
    """Snapshot sandbox input files and empty directory shells.

    Walks only ``_INPUT_DIRS`` under root. Skips postprocess/
    (not needed for execute phase). Returns ``({}, [])`` when
    root contains no input dirs.

    Empty directories are captured explicitly so remote restoration
    reproduces the directory shells the local lifecycle created
    (e.g. an empty ``execute/`` the caller intends to use as a
    subprocess cwd).

    Args:
        root: Path to the sandbox root directory.

    Returns:
        Tuple ``(files, empty_dirs)``:
            * ``files`` maps relative paths to file contents.
            * ``empty_dirs`` lists relative paths of directories
              under ``_INPUT_DIRS`` that exist and have no
              descendant files (leaf empties).

    Raises:
        ValueError: If total size exceeds 50 MB.
    """
    files: dict[str, bytes] = {}
    empty_dirs: list[str] = []
    total_size = 0

    for dir_name in _INPUT_DIRS:
        dir_path = os.path.join(root, dir_name)
        if not os.path.isdir(dir_path):
            continue
        for dirpath, dirnames, filenames in os.walk(dir_path):
            if filenames:
                for filename in filenames:
                    abs_path = os.path.join(dirpath, filename)
                    rel_path = os.path.relpath(abs_path, root)
                    content = _read_file(abs_path)
                    total_size += len(content)
                    files[rel_path] = content
            elif not dirnames:
                # Leaf directory with no files — record so restore
                # can recreate the shell. Non-leaf empty dirs are
                # covered transitively via their descendants.
                empty_dirs.append(os.path.relpath(dirpath, root))

    _check_size(total_size)
    return files, empty_dirs


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


def restore_sandbox(
    root: str,
    snapshot: dict[str, bytes],
    empty_dirs: list[str] | None = None,
) -> None:
    """Recreate sandbox files and directory shells from a snapshot.

    Creates ``empty_dirs`` first (idempotent) so callers that pass
    a directory to ``subprocess.Popen(cwd=...)`` don't ENOENT on an
    expected-but-empty local directory that never crossed the wire.
    Then writes each file at its original relative path.

    Args:
        root: Target root directory for restoration.
        snapshot: ``{rel_path: bytes}`` mapping, typically from
            ``snapshot_sandbox()[0]``.
        empty_dirs: Optional list of relative paths to directories
            that should exist after restore. ``None`` (default)
            preserves the historical file-only behavior.
    """
    if empty_dirs:
        for rel_path in empty_dirs:
            os.makedirs(os.path.join(root, rel_path), exist_ok=True)

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
) -> tuple[dict[str, bytes], list[str]]:
    """Snapshot the files needed for a single artifact's execute.

    Captures materialized input files referenced in
    ``execute_input.inputs`` and the shared ``preprocess/`` directory.
    Excludes other artifacts' materialized files, keeping per-artifact
    payloads small for Modal transport.

    Also records the per-artifact ``execute_dir`` (typically
    ``execute/artifact_{i}/``) as an empty-dir entry when it exists
    and is empty — remote restoration needs the shell so that
    ``subprocess.Popen(cwd=execute_dir)`` doesn't ENOENT.

    Args:
        sandbox_root: Unit-level sandbox root.
        execute_input: Per-artifact ``ExecuteInput`` with file
            references in its ``inputs`` dict and ``execute_dir``.

    Returns:
        Tuple ``(files, empty_dirs)`` suitable for
        ``restore_sandbox()``.

    Raises:
        ValueError: If total size exceeds 50 MB.
    """
    files: dict[str, bytes] = {}
    empty_dirs: list[str] = []
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
            files[rel_path] = content

    # Always include the shared preprocess/ directory
    preprocess_dir = os.path.join(sandbox_root, "preprocess")
    if os.path.isdir(preprocess_dir):
        for dirpath, _, filenames in os.walk(preprocess_dir):
            for filename in filenames:
                abs_path = os.path.join(dirpath, filename)
                rel_path = os.path.relpath(abs_path, sandbox_root)
                if rel_path not in files:
                    content = _read_file(abs_path)
                    total_size += len(content)
                    files[rel_path] = content

    # Include the per-artifact execute_dir shell if empty. Non-empty
    # dirs will be reproduced by file writes during restore, so we
    # only need to record the empty case explicitly.
    execute_dir = execute_input.execute_dir
    if os.path.isdir(execute_dir) and not os.listdir(execute_dir):
        rel_execute = os.path.relpath(execute_dir, sandbox_root)
        if rel_execute not in empty_dirs:
            empty_dirs.append(rel_execute)

    _check_size(total_size)
    return files, empty_dirs


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
