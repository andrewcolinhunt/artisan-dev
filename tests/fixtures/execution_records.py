"""Schema-driven builder for executions-table test DataFrames."""

from __future__ import annotations

from typing import Any

import polars as pl

from artisan.storage.core.table_schemas import EXECUTIONS_SCHEMA


def executions_df(**columns: list[Any]) -> pl.DataFrame:
    """Build an executions DataFrame, filling omitted schema columns with nulls.

    Row count is the length of the longest provided column; every
    unspecified ``EXECUTIONS_SCHEMA`` column is filled with that many typed
    nulls. Adding a schema column is therefore a zero-fixture-edit change for
    callers that do not exercise it, and a wrong-length null vector (the
    length-1-beside-length-3 broadcast bug) is structurally impossible.

    Args:
        **columns: Column name to value list. Each name must be an
            ``EXECUTIONS_SCHEMA`` column.

    Returns:
        A DataFrame typed to ``EXECUTIONS_SCHEMA`` with one row per element
        of the longest provided column.
    """
    n = max((len(v) for v in columns.values()), default=0)
    data: dict[str, list[Any]] = dict(columns)
    for name in EXECUTIONS_SCHEMA:
        if name not in data:
            data[name] = [None] * n
    return pl.DataFrame(data, schema=EXECUTIONS_SCHEMA)
