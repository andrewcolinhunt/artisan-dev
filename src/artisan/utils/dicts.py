"""Nested dict flattening with configurable key separators."""

from __future__ import annotations

from typing import Any


def flatten_dict(
    d: dict[str, Any],
    *,
    prefix: str = "",
    separator: str = ".",
) -> dict[str, Any]:
    """Flatten a nested dict into dot-separated keys.

    Recursively walks nested dicts and produces a single-level dict where
    keys are joined by *separator*. Leaf values are preserved as-is (no
    type casting).

    Args:
        d: Dict to flatten.
        prefix: Key prefix for recursion (internal).
        separator: Separator between key segments. Defaults to ``"."``.

    Returns:
        Flat dict with dot-separated keys.

    Examples:
        >>> flatten_dict({"a": 1, "b": {"c": 2}})
        {'a': 1, 'b.c': 2}
        >>> flatten_dict({"x": {"y": {"z": 3}}})
        {'x.y.z': 3}
    """
    result: dict[str, Any] = {}
    for key, val in d.items():
        full_key = f"{prefix}{separator}{key}" if prefix else key
        if isinstance(val, dict):
            result.update(flatten_dict(val, prefix=full_key, separator=separator))
        else:
            result[full_key] = val
    return result
