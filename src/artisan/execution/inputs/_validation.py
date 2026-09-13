"""Shared validation helpers for execution inputs."""

from __future__ import annotations

import string
from typing import TypeGuard


def is_hex_id(value: object) -> TypeGuard[str]:
    """Return whether a value has the framework's 128-bit hex ID shape."""
    return (
        isinstance(value, str)
        and len(value) == 32
        and all(char in string.hexdigits for char in value)
    )
