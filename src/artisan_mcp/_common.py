"""Small values and serializers shared across the MCP surface."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

READ_ONLY = {"readOnlyHint": True, "idempotentHint": True}
MAX_RESOURCE_ITEMS = 100
MAX_RESOURCE_CHARS = 64_000


def jsonable_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert top-level datetime values to ISO strings.

    Args:
        rows: Flat records returned by the run-history reader.

    Returns:
        Records safe for JSON serialization.
    """
    return [
        {
            key: value.isoformat() if isinstance(value, datetime) else value
            for key, value in row.items()
        }
        for row in rows
    ]


def resource_fits(value: Any) -> bool:
    """Return whether a JSON resource fits the MCP response budget.

    Args:
        value: JSON-compatible resource payload.

    Returns:
        True when the serialized resource is within the character budget.
    """
    return len(json.dumps(value, default=str)) <= MAX_RESOURCE_CHARS
