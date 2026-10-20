"""Behavioral coverage of exact byte limits and stateless evidence cursors."""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path

import pytest

from artisan.errors import ArtisanError, ErrorCode
from artisan_mcp._evidence import (
    MAX_PAYLOAD_BYTES,
    encode_json,
    ensure_fits,
    json_ready,
)
from artisan_mcp._pagination import paginate


def page(rows, limit=1, cursor=None, **kwargs):
    """Select the same synthetic query unless the test changes its identity."""
    defaults = {
        "tool": "artisan_test",
        "arguments": {"name": "selected"},
        "scope": "/private/store",
    }
    return paginate(rows, limit, cursor, **(defaults | kwargs))


def token_with(cursor, **changes):
    """Change a token field while retaining canonical base64url encoding."""
    data = json.loads(base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)))
    data.update(changes)
    return base64.urlsafe_b64encode(encode_json(data)).decode().rstrip("=")


def test_exact_payload_boundary_counts_escaping_and_unicode():
    value = {"text": '雪😀"\\\n' * 10}
    measured = len(encode_json(value))
    assert measured > len(json.dumps(value, ensure_ascii=False).encode())
    assert ensure_fits(value, budget=measured) == value
    with pytest.raises(ArtisanError) as caught:
        ensure_fits(value, budget=measured - 1)
    assert caught.value.code == ErrorCode.EVIDENCE_RESPONSE_TOO_LARGE


def test_exact_page_boundary_keeps_complete_row_and_continuation():
    rows = [{"value": '雪"\\\n' * 300}, {"value": "next"}]
    expected = page(rows)
    measured = len(encode_json(expected))
    assert page(rows, budget=measured) == expected
    with pytest.raises(ArtisanError) as caught:
        page(rows, budget=measured - 1)
    assert caught.value.code == ErrorCode.EVIDENCE_ITEM_TOO_LARGE


def test_last_page_does_not_reserve_an_unused_continuation():
    expected = page([{"id": 1}])
    assert page([{"id": 1}], budget=len(encode_json(expected))) == expected


def test_byte_driven_pages_advance_by_returned_count_without_skipping():
    rows = [{"id": index, "value": "雪" * 2_000} for index in range(9)]
    seen = []
    result = page(rows, limit=9)
    assert 0 < result["returned_items"] < 9
    while True:
        assert len(encode_json(result)) <= MAX_PAYLOAD_BYTES
        assert result["returned_items"] == len(result["items"]) > 0
        seen.extend(result["items"])
        if not result["has_more"]:
            assert result["next_cursor"] is None
            assert result["follow_up"] is None
            break
        follow = result["follow_up"]
        assert follow["tool"] == "artisan_test"
        assert follow["arguments"]["cursor"] == result["next_cursor"]
        result = page(rows, limit=2, cursor=result["next_cursor"])
    assert seen == rows


@pytest.mark.parametrize(
    "rows",
    [[{"huge": {"nested": ["x" * 60_000]}}], [{"tiny": True}, {"huge": "x" * 60_000}]],
)
def test_oversized_first_or_later_row_never_returns_empty_continuation(rows):
    cursor = None if len(rows) == 1 else page(rows)["next_cursor"]
    with pytest.raises(ArtisanError) as caught:
        page(rows, cursor=cursor)
    assert caught.value.code == ErrorCode.EVIDENCE_ITEM_TOO_LARGE
    assert "x" * 100 not in str(caught.value)


def test_required_header_error_differs_from_oversized_row():
    with pytest.raises(ArtisanError) as caught:
        page([{"id": 1}], header={"identity": "雪" * 20_000})
    assert caught.value.code == ErrorCode.EVIDENCE_RESPONSE_TOO_LARGE


def test_empty_page_is_complete_and_counted():
    result = page([])
    assert result["items"] == []
    assert result["returned_items"] == result["total_items"] == 0
    assert result["has_more"] is False
    assert result["next_cursor"] is result["follow_up"] is None


@pytest.mark.parametrize(
    "change",
    [
        {"arguments": {"name": "different"}},
        {"scope": "/another/private/store"},
        {"tool": "another_tool"},
        {"page_key": "edges"},
    ],
)
def test_cursor_is_bound_to_query_store_and_collection(change):
    rows = [1, 2, 3]
    cursor = page(rows)["next_cursor"]
    with pytest.raises(ArtisanError) as caught:
        page(rows, cursor=cursor, **change)
    assert caught.value.code == ErrorCode.INVALID_EVIDENCE_CURSOR
    assert cursor not in str(caught.value)
    decoded = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)).decode()
    assert "private" not in decoded


@pytest.mark.parametrize(
    "change",
    [
        {"rows": [1]},
        {"rows": [1, 2, 4]},
        {"header": {"status": "failed"}},
        {"omitted_fields": ["items.value"]},
    ],
)
def test_cursor_detects_changed_rows_header_coverage_and_omissions(change):
    initial = {"rows": [1, 2, 3], "header": {"status": "running"}}
    cursor = page(**initial)["next_cursor"]
    with pytest.raises(ArtisanError) as caught:
        page(**(initial | change), cursor=cursor)
    assert caught.value.code == ErrorCode.EVIDENCE_VIEW_CHANGED


@pytest.mark.parametrize(
    "changes",
    [
        {"version": 2},
        {"version": True},
        {"offset": 0},
        {"offset": -1},
        {"offset": True},
        {"offset": 1.0},
        {"offset": 3},
        {"view_id": "bad"},
        {"query_id": "f" * 32},
        {"extra": "unexpected"},
    ],
)
def test_cursor_rejects_invalid_fields_and_unchanged_out_of_range(changes):
    cursor = token_with(page([1, 2, 3])["next_cursor"], **changes)
    with pytest.raises(ArtisanError) as caught:
        page([1, 2, 3], cursor=cursor)
    assert caught.value.code == ErrorCode.INVALID_EVIDENCE_CURSOR


@pytest.mark.parametrize("cursor", ["", "0", "=", "_", "💡" * 200, "\ud800", "a" * 513])
def test_cursor_rejects_malformed_and_overlong_encoding(cursor):
    with pytest.raises(ArtisanError) as caught:
        page([1, 2, 3], cursor=cursor)
    assert caught.value.code == ErrorCode.INVALID_EVIDENCE_CURSOR


def test_cursor_rejects_duplicate_fields_and_padding():
    cursor = page([1, 2, 3])["next_cursor"]
    raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
    duplicate = base64.urlsafe_b64encode(b'{"offset":1,' + raw[1:]).decode().rstrip("=")
    for malformed in (duplicate, cursor + "="):
        with pytest.raises(ArtisanError) as caught:
            page([1, 2, 3], cursor=malformed)
        assert caught.value.code == ErrorCode.INVALID_EVIDENCE_CURSOR


def test_fresh_projection_accepts_old_cursor_and_new_page_size():
    before = page([1, 2, 3, 4])
    after = page(json.loads("[1,2,3,4]"), limit=3, cursor=before["next_cursor"])
    assert after["items"] == [2, 3, 4]
    assert after["view_id"] == before["view_id"]


def test_json_normalizes_enum_and_datetime_without_stringifying_other_objects():
    class State(Enum):
        READY = "ready"

    value = {"state": State.READY, "when": datetime(2026, 10, 20, tzinfo=UTC)}
    assert json.loads(encode_json(value)) == {
        "state": "ready",
        "when": "2026-10-20T00:00:00+00:00",
    }
    assert json_ready(value)["state"] == "ready"


@pytest.mark.parametrize(
    "value",
    [
        {"score": float("nan")},
        {"score": float("inf")},
        {"score": float("-inf")},
        {1: "not a JSON key"},
        {"path": Path("/private/path")},
        {"values": {1, 2}},
    ],
)
def test_json_rejects_unsupported_or_nonfinite_values(value):
    with pytest.raises((TypeError, ValueError)):
        encode_json(value)
