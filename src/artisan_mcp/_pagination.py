"""Cursor pagination shared by the list tools.

Every paginated tool returns ``{items, has_more, next_cursor}`` per the
"refs, not blobs" criterion. The cursor is an opaque offset token the
server issues and the agent echoes back — never parsed by the agent.
"""

from __future__ import annotations

from typing import Any


def paginate(items: list[Any], limit: int, cursor: str | None) -> dict[str, Any]:
    """Return one page of ``items`` with ``has_more`` and ``next_cursor``.

    Args:
        items: The full, already-ordered result list.
        limit: Maximum items in the returned page.
        cursor: Opaque offset token from a prior page, or None for the first
            page. A non-integer cursor is treated as the start.

    Returns:
        ``{"items": [...], "has_more": bool, "next_cursor": str | None}``.
    """
    start = int(cursor) if cursor and cursor.isdigit() else 0
    window = items[start : start + limit]
    next_start = start + len(window)
    has_more = next_start < len(items)
    return {
        "items": window,
        "has_more": has_more,
        "next_cursor": str(next_start) if has_more else None,
    }
