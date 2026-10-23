"""Metric value encoding for scalar/compound metric columns."""

from __future__ import annotations

import json
import math
from typing import Any


def encode_metric_value(val: Any) -> tuple[str | None, str | None]:
    """Encode a metric value into scalar and compound JSON columns.

    Args:
        val: Any Python value from a metric JSON payload.

    Returns:
        Tuple of (scalar_json, compound_json). Exactly one is non-null
        for valid inputs, both null for None and non-finite floats.

    Raises:
        TypeError: If val is not a JSON-compatible type.
    """
    if val is None:
        return (None, None)
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return (None, None)
    if isinstance(val, bool | int | float | str):
        return (json.dumps(val), None)
    if isinstance(val, list | dict):
        return (None, json.dumps(val))
    msg = f"Unsupported metric value type: {type(val).__name__}"
    raise TypeError(msg)
