"""Cursor pagination shared by the list tools.

Every paginated tool returns ``{items, has_more, next_cursor}`` per the
"refs, not blobs" criterion. The cursor is an opaque offset token the
server issues and the agent echoes back — never parsed by the agent.
"""

from __future__ import annotations

from typing import Any

from artisan_mcp._boundary import invalid_input, validate_int_range

MAX_PAGE_SIZE = 100


def paginate(items: list[Any], limit: int, cursor: str | None) -> dict[str, Any]:
    """Return one page of ``items`` with ``has_more`` and ``next_cursor``.

    Args:
        items: The full, already-ordered result list.
        limit: Maximum items in the returned page.
        cursor: Canonical ASCII offset token from a prior page, or None for
            the first page.

    Returns:
        ``{"items": [...], "has_more": bool, "next_cursor": str | None}``.
    """
    validate_int_range(limit, field="limit", minimum=1, maximum=MAX_PAGE_SIZE)
    start = _cursor_offset(cursor, len(items))
    window = items[start : start + limit]
    next_start = start + len(window)
    has_more = next_start < len(items)
    return {
        "items": window,
        "has_more": has_more,
        "next_cursor": str(next_start) if has_more else None,
    }


def _cursor_offset(cursor: str | None, item_count: int) -> int:
    """Decode a canonical cursor and reject stale offsets."""
    if cursor is None:
        return 0
    if (
        not cursor
        or not cursor.isascii()
        or not cursor.isdecimal()
        or (len(cursor) > 1 and cursor.startswith("0"))
        or len(cursor) > 20
    ):
        invalid_input("cursor", "cursor must be a canonical ASCII integer")

    start = int(cursor)
    if start == 0 or start >= item_count:
        invalid_input("cursor", "cursor is stale or outside the result set")
    return start
