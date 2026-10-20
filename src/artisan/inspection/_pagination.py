"""Byte-aware pages bound to the query and complete projected evidence view."""

from __future__ import annotations

import base64
import binascii
import json
import re
from typing import Any

from artisan.errors import ErrorCode
from artisan.inspection._boundary import invalid_input, validate_int_range
from artisan.inspection._evidence import (
    MAX_PAYLOAD_BYTES,
    encode_json,
    evidence_error,
    json_ready,
)
from artisan.utils.hashing import compute_content_digest

MAX_PAGE_SIZE = 100
MAX_CURSOR_BYTES = 512
_DIGEST = re.compile(r"[0-9a-f]{32}\Z")


def paginate(
    items: list[Any],
    limit: int,
    cursor: str | None,
    *,
    capability: str,
    arguments: dict[str, Any],
    scope: str = "catalog",
    header: dict[str, Any] | None = None,
    page_key: str | None = None,
    omitted_fields: list[str] | None = None,
    budget: int = MAX_PAYLOAD_BYTES,
    hint: str | None = None,
) -> dict[str, Any]:
    """Return a whole-row page, including a portable continuation.

    Args:
        items: Complete deterministic projection in its documented order.
        limit: Maximum rows, from one through 100.
        cursor: Opaque continuation from this same query and projected view.
        capability: Public inspection name for query identity and continuation.
        arguments: Normalized selectors excluding limit, cursor and store root.
        scope: Configured store identity or catalogue scope; only its digest escapes.
        header: Stable parent fields carried by every page.
        page_key: Named page field, or None for top-level page fields.
        omitted_fields: Explicit schema-level omission paths.
        budget: Complete canonical payload budget, including continuation.
        hint: Fixed narrower-query guidance for oversized individual rows.
    """
    validate_int_range(limit, field="limit", minimum=1, maximum=MAX_PAGE_SIZE)
    rows, parent = json_ready(items), json_ready(header or {})
    query_id = _digest(
        {
            "version": 2,
            "capability": capability,
            "arguments": arguments,
            "scope": _digest(scope),
            "page_key": page_key,
        }
    )
    view_id = _digest(
        {
            "query_id": query_id,
            "items": rows,
            "header": parent,
            "omitted_fields": omitted_fields or [],
        }
    )
    start = _cursor_offset(cursor, query_id, view_id, len(rows))

    def build(count: int) -> dict[str, Any]:
        stop = start + count
        next_cursor = _cursor(query_id, view_id, stop) if stop < len(rows) else None
        page = {
            "items": rows[start:stop],
            "returned_items": count,
            "total_items": len(rows),
            "has_more": next_cursor is not None,
            "next_cursor": next_cursor,
            "view_id": view_id,
            "omitted_fields": omitted_fields or [],
        }
        continuation = (
            None
            if next_cursor is None
            else {
                "capability": capability,
                "arguments": {**arguments, "limit": limit, "cursor": next_cursor},
            }
        )
        return {
            "projection_version": "2",
            "follow_up": continuation,
            **parent,
            **({page_key: page} if page_key else page),
        }

    count = min(limit, len(rows) - start)
    while count and len(encode_json(build(count))) > budget:
        count -= 1
    if count:
        return build(count)
    envelope = build(1) if rows else build(0)
    collection = envelope[page_key] if page_key else envelope
    collection.update(items=[], returned_items=0)
    if len(encode_json(envelope)) > budget:
        raise evidence_error(ErrorCode.EVIDENCE_RESPONSE_TOO_LARGE)
    if rows:
        raise evidence_error(
            ErrorCode.EVIDENCE_ITEM_TOO_LARGE, field=page_key or "items", hint=hint
        )
    return envelope


def reject_cursor(cursor: str | None) -> None:
    """Summary and indivisible views do not accept continuation cursors."""
    if cursor is not None:
        invalid_input("cursor", "This view does not accept a cursor")


def _digest(value: Any) -> str:
    return compute_content_digest(encode_json(value))


def _cursor(query_id: str, view_id: str, offset: int) -> str:
    payload = {"version": 2, "query_id": query_id, "view_id": view_id, "offset": offset}
    return base64.urlsafe_b64encode(encode_json(payload)).decode("ascii").rstrip("=")


def _cursor_offset(cursor: str | None, query_id: str, view_id: str, count: int) -> int:
    if cursor is None:
        return 0
    invalid = evidence_error(ErrorCode.INVALID_EVIDENCE_CURSOR, field="cursor")
    if len(cursor) > MAX_CURSOR_BYTES or not re.fullmatch(r"[A-Za-z0-9_-]+", cursor):
        raise invalid
    try:
        raw = base64.b64decode(
            cursor + "=" * (-len(cursor) % 4), altchars=b"-_", validate=True
        )
        token = json.loads(raw)
    except (ValueError, UnicodeError, binascii.Error) as exc:
        raise invalid from exc
    if not isinstance(token, dict) or set(token) != {
        "version",
        "query_id",
        "view_id",
        "offset",
    }:
        raise invalid
    if type(token["version"]) is not int or token["version"] != 2:
        raise invalid
    if any(
        not isinstance(token[key], str) or not _DIGEST.fullmatch(token[key])
        for key in ("query_id", "view_id")
    ):
        raise invalid
    if type(token["offset"]) is not int or token["offset"] <= 0:
        raise invalid
    if _cursor(token["query_id"], token["view_id"], token["offset"]) != cursor:
        raise invalid
    if token["query_id"] != query_id:
        raise invalid
    if token["view_id"] != view_id:
        raise evidence_error(ErrorCode.EVIDENCE_VIEW_CHANGED, field="cursor")
    if token["offset"] >= count:
        raise invalid
    return token["offset"]
