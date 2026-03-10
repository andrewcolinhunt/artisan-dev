"""DataFrame utilities for metric pivoting."""

from __future__ import annotations

import json
import math
from typing import Any

import polars as pl

_REQUIRED_COLS = {
    "artifact_id",
    "step_number",
    "step_name",
    "metric_name",
    "metric_value",
    "metric_compound",
}
_DEFAULT_INDEX = ["artifact_id"]


def is_scalar_metric(val: Any) -> bool:
    """Check whether a metric value is a scalar (filterable) type.

    Args:
        val: Any Python value from a metric JSON payload.

    Returns:
        True for int, float, bool, str, and None.
    """
    return val is None or isinstance(val, int | float | bool | str)


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


def pivot_metrics_wide(
    tidy: pl.DataFrame,
    *,
    index_cols: list[str] | None = None,
) -> pl.DataFrame:
    """Pivot a tidy metrics DataFrame to wide format.

    Column names use ``{step_name}.{metric_name}``. When the same step_name
    appears at multiple step_numbers, disambiguates with
    ``{step_number}.{step_name}.{metric_name}``.

    After pivoting, each column is cast to its natural type (Bool, Int64,
    Float64, or String) based on the JSON-encoded values.

    Args:
        tidy: Long-format DataFrame with columns: artifact_id, step_number,
            step_name, metric_name, metric_value, metric_compound.
        index_cols: Columns to keep as row identifiers in the wide output.
            Defaults to ``["artifact_id"]``.

    Returns:
        Wide-format DataFrame with one row per unique combination of
        ``index_cols``, and one column per qualified metric name.

    Raises:
        ValueError: If required columns are missing from *tidy*.
    """
    if index_cols is None:
        index_cols = list(_DEFAULT_INDEX)

    missing = _REQUIRED_COLS - set(tidy.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    if tidy.is_empty():
        schema = {col: tidy.schema[col] for col in index_cols if col in tidy.schema}
        return pl.DataFrame(schema=schema)

    # Filter to scalar rows only (compounds excluded from wide)
    tidy = tidy.filter(pl.col("metric_value").is_not_null())

    if tidy.is_empty():
        schema = {col: tidy.schema[col] for col in index_cols if col in tidy.schema}
        return pl.DataFrame(schema=schema)

    # Detect ambiguous step names (same name at multiple step_numbers)
    step_mapping = (
        tidy.select(["step_name", "step_number"])
        .unique()
        .group_by("step_name")
        .agg(pl.col("step_number").n_unique().alias("n_steps"))
    )
    ambiguous_names = set(
        step_mapping.filter(pl.col("n_steps") > 1)["step_name"].to_list()
    )

    if ambiguous_names:
        qualified = tidy.with_columns(
            pl.when(pl.col("step_name").is_in(list(ambiguous_names)))
            .then(
                pl.col("step_number").cast(pl.String)
                + "."
                + pl.col("step_name")
                + "."
                + pl.col("metric_name")
            )
            .otherwise(pl.col("step_name") + "." + pl.col("metric_name"))
            .alias("_qualified_name")
        )
    else:
        qualified = tidy.with_columns(
            (pl.col("step_name") + "." + pl.col("metric_name")).alias("_qualified_name")
        )

    result = qualified.pivot(
        on="_qualified_name",
        index=index_cols,
        values="metric_value",
    )

    return _infer_column_types(result, exclude=set(index_cols))


def _infer_column_types(df: pl.DataFrame, *, exclude: set[str]) -> pl.DataFrame:
    """Cast JSON-encoded string columns to their natural types."""
    for col_name in df.columns:
        if col_name in exclude:
            continue
        df = _cast_json_column(df, col_name)
    return df


def _cast_json_column(df: pl.DataFrame, col_name: str) -> pl.DataFrame:
    """Cast a single JSON-encoded string column to its natural type.

    Tries Boolean -> Int64 -> Float64 -> String (unquoted) in order.
    """
    values = df[col_name].drop_nulls().to_list()
    if not values:
        return df

    # Boolean: all values are "true" or "false"
    if all(v in ("true", "false") for v in values):
        return df.with_columns(
            pl.col(col_name)
            .str.replace("true", "1")
            .str.replace("false", "0")
            .cast(pl.Int8)
            .cast(pl.Boolean)
        )

    # Int64: all values parse as integers with exact round-trip
    try:
        if all(str(int(v)) == v for v in values):
            return df.with_columns(pl.col(col_name).cast(pl.Int64))
    except (ValueError, OverflowError):
        pass

    # Float64: all values parse as floats
    try:
        for v in values:
            float(v)
        return df.with_columns(pl.col(col_name).cast(pl.Float64))
    except (ValueError, OverflowError):
        pass

    # String: strip JSON quotes
    return df.with_columns(pl.col(col_name).str.strip_chars('"'))
