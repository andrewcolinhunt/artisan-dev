"""Shared JSON and byte policy for tool, resource and prompt evidence."""

from __future__ import annotations

import math
from datetime import date, datetime
from enum import Enum
from typing import Any, Never, cast

from pydantic import BaseModel

from artisan.errors import ArtisanError, ErrorCode
from artisan.utils.hashing import canonical_json_bytes

MAX_PAYLOAD_BYTES = 48 * 1024


def json_ready(value: Any) -> Any:
    """Normalize supported values without clipping or concealing invalid JSON."""
    if isinstance(value, BaseModel):
        return json_ready(value.model_dump(mode="python"))
    if isinstance(value, Enum):
        return json_ready(value.value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            message = "Evidence object keys must be strings"
            raise TypeError(message)
        return {key: json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_ready(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        message = "Evidence contains a nonfinite number"
        raise ValueError(message)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    message = "Evidence contains an unsupported value"
    raise TypeError(message)


def encode_json(value: Any) -> bytes:
    """Encode complete standards-compliant JSON with Artisan's canonical policy."""
    return canonical_json_bytes(json_ready(value))


def evidence_error(
    code: str, *, field: str = "evidence", hint: str | None = None
) -> ArtisanError:
    """Create fixed, bounded recovery guidance without echoing evidence values."""
    messages = {
        ErrorCode.INVALID_EVIDENCE_CURSOR: "Evidence cursor does not match this query",
        ErrorCode.EVIDENCE_VIEW_CHANGED: "Selected evidence changed between pages",
        ErrorCode.EVIDENCE_ITEM_TOO_LARGE: "One evidence item exceeds the response budget",
        ErrorCode.EVIDENCE_RESPONSE_TOO_LARGE: "Evidence response exceeds the response budget",
    }
    hints = {
        ErrorCode.INVALID_EVIDENCE_CURSOR: "Restart the intended query without a cursor.",
        ErrorCode.EVIDENCE_VIEW_CHANGED: "Discard earlier pages and restart without a cursor.",
        ErrorCode.EVIDENCE_ITEM_TOO_LARGE: "Select fewer fields or inspect the source with Python; a smaller page cannot shrink one item.",
        ErrorCode.EVIDENCE_RESPONSE_TOO_LARGE: "Select a smaller section or inspect the source with Python.",
    }
    return ArtisanError(
        code,
        messages[code],
        error_type="validation",
        field=field,
        hint=hint or hints[code],
        recovery_hint="CHECK_INPUT",
    )


def ensure_fits(
    value: dict[str, Any], *, budget: int = MAX_PAYLOAD_BYTES, hint: str | None = None
) -> dict[str, Any]:
    """Reject an indivisible response rather than shortening structured values."""
    normalized = json_ready(value)
    if len(encode_json(normalized)) > budget:
        raise evidence_error(ErrorCode.EVIDENCE_RESPONSE_TOO_LARGE, hint=hint)
    return cast("dict[str, Any]", normalized)


def response_too_large() -> dict[str, Any]:
    """Return the fixed fallback that fits even when an error itself is too large."""
    return evidence_error(ErrorCode.EVIDENCE_RESPONSE_TOO_LARGE).to_dict(
        include_cause=False
    )


def decode_json_object(data: bytes | str) -> dict[str, Any]:
    """Decode one strict UTF-8 JSON object without exposing malformed contents.

    Raises:
        ValueError: Input is malformed, duplicated, nonfinite or not an object.
        UnicodeError: Bytes are not UTF-8.
    """
    import json

    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                message = "Duplicate JSON object key"
                raise ValueError(message)
            result[key] = value
        return result

    def reject_constant(value: str) -> Never:
        message = "JSON numbers must be finite"
        raise ValueError(message)

    try:
        text = data.decode("utf-8") if isinstance(data, bytes) else data
        value = json.loads(
            text, object_pairs_hook=object_pairs, parse_constant=reject_constant
        )
        if not isinstance(value, dict):
            message = "Expected a JSON object"
            raise ValueError(message)
        json_ready(value)
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        message = "Expected one valid UTF-8 JSON object"
        raise ValueError(message) from exc
    return value
