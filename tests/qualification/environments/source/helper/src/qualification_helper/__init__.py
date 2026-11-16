"""A second local distribution for native package lowering."""

from __future__ import annotations


def passthrough(value: int) -> int:
    """Preserve a numeric input across the package boundary."""
    return value
