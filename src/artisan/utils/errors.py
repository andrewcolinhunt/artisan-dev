"""Error formatting utilities for execution diagnostics."""

from __future__ import annotations

import traceback

_MAX_TRACEBACK_LINES = 50


def format_error(exc: BaseException) -> str:
    """Format an exception with its full traceback.

    Args:
        exc: The exception to format.

    Returns:
        Full traceback string, truncated to the last 50 lines if longer.
    """
    if exc.__traceback__ is None:
        return f"{type(exc).__name__}: {exc}"

    lines = traceback.format_exception(type(exc), exc, exc.__traceback__)
    text = "".join(lines)
    split = text.splitlines()

    if len(split) > _MAX_TRACEBACK_LINES:
        kept = split[-_MAX_TRACEBACK_LINES:]
        return "[truncated]\n" + "\n".join(kept)

    return text.rstrip("\n")
