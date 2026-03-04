"""DataFrame utilities for metric pivoting."""

from __future__ import annotations

from typing import Any

import polars as pl

_REQUIRED_COLS = {
    "artifact_id",
    "step_number",
    "step_name",
    "metric_name",
    "metric_value",
}
_DEFAULT_INDEX = ["artifact_id"]


def to_float(val: Any) -> float | None:
    """Cast a metric value to float, or None if non-numeric."""
    if isinstance(val, bool):
        return float(val)
    if isinstance(val, int | float):
        return float(val)
    return None


def pivot_metrics_wide(
    tidy: pl.DataFrame,
    *,
    index_cols: list[str] | None = None,
) -> pl.DataFrame:
    """Pivot a tidy metrics DataFrame to wide format.

    Column names use ``{step_name}.{metric_name}``. When the same step_name
    appears at multiple step_numbers, disambiguates with
    ``{step_number}.{step_name}.{metric_name}``.

    Args:
        tidy: Long-format DataFrame with columns: artifact_id, step_number,
            step_name, metric_name, metric_value (plus any extra index cols).
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

    return qualified.pivot(
        on="_qualified_name",
        index=index_cols,
        values="metric_value",
    )
