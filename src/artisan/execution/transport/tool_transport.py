"""Snapshot and restore tool script files for remote transport.

Captures local tool scripts referenced by ``ToolSpec`` so they can
be shipped alongside the sandbox to a remote compute target.
"""

from __future__ import annotations

import os
from typing import Any


def snapshot_tool_files(operation: Any) -> dict[str, bytes]:
    """Snapshot tool script files referenced by ToolSpec.

    Reads the executable file if it is an absolute local path
    to an existing file. Returns empty dict if the operation has
    no tool, the tool has no executable, or the executable is a
    relative path (assumed to be on PATH in the container).

    Args:
        operation: Operation instance (may have ``tool: ToolSpec``).

    Returns:
        Dict of ``{absolute_path: content}`` for local tool files.
        Empty dict if no tool or tool is not a local file.

    Raises:
        FileNotFoundError: If executable is an absolute path but
            the file does not exist.
    """
    tool = getattr(operation, "tool", None)
    if tool is None:
        return {}

    executable = getattr(tool, "executable", None)
    if executable is None:
        return {}

    # Only snapshot absolute paths (local files to ship)
    if not executable.startswith("/"):
        return {}

    if not os.path.isfile(executable):
        msg = f"Tool executable not found: {executable}"
        raise FileNotFoundError(msg)

    with open(executable, "rb") as f:
        content = f.read()

    return {executable: content}


def restore_tool_files(tool_files: dict[str, bytes]) -> None:
    """Restore tool files at their original absolute paths.

    Creates parent directories as needed.

    Args:
        tool_files: Dict of ``{absolute_path: content}``.
    """
    for abs_path, content in tool_files.items():
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        with open(abs_path, "wb") as f:
            f.write(content)
