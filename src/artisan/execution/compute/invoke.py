"""Normalize per-artifact arguments for the selected command adapter."""

from __future__ import annotations

from typing import Any


def tool_command_inputs(prepared: dict[str, Any]) -> dict[str, Any]:
    """Normalize prepared inputs for ``execute_command``.

    Per-artifact dispatch delivers each sliced role as a one-element list
    (the list interface ``execute_function()`` implementations expect); an
    external tool's argv addresses one artifact's files, so the framework
    unwraps single-element lists before ``execute_command`` — identically
    under the selected command adapter. ``execute_as_tool``
    ops skip the unwrap: their generated argv embeds the dict as JSON, and
    the runner must re-deliver the exact list shape preprocess produced.

    Args:
        prepared: ``ExecuteInput.inputs`` for one artifact.

    Returns:
        The dict with one-element list values unwrapped to their item.
    """
    return {
        key: value[0] if isinstance(value, list) and len(value) == 1 else value
        for key, value in prepared.items()
    }
