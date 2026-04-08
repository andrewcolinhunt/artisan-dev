"""Shared JSON serialization helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def artisan_json_default(o: Any) -> Any:
    """JSON default handler for set and Path types.

    Suitable for use as the ``default`` argument to ``json.dumps``.
    Operations may pass ``Path`` objects in their params dicts,
    so this handler converts them to strings during serialization.
    """
    if isinstance(o, set):
        return sorted(o)
    if isinstance(o, Path):
        return str(o)
    msg = f"Object of type {type(o).__name__} is not JSON serializable"
    raise TypeError(msg)
