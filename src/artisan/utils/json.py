"""Shared JSON serialization helpers."""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Any


def artisan_json_default(o: Any) -> Any:
    """JSON default handler for set, Path, and Enum types.

    Suitable for use as the ``default`` argument to ``json.dumps``.
    Operations may pass ``Path`` objects, ``set`` instances, or
    ``Enum`` values in their params dicts; this handler converts them
    to JSON-safe forms during serialization. Enums emit their
    ``.value`` — the canonical string for artisan's string-valued
    enums (e.g. ``GroupByStrategy.CROSS_PRODUCT`` → ``"cross_product"``).
    """
    if isinstance(o, set):
        return sorted(o)
    if isinstance(o, Path):
        return str(o)
    if isinstance(o, Enum):
        return o.value
    msg = f"Object of type {type(o).__name__} is not JSON serializable"
    raise TypeError(msg)
