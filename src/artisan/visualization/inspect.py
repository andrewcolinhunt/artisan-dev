"""Human-readable inspect helpers for Delta Lake pipeline data.

Read-only functions that present Delta Lake tables as clean Polars DataFrames
for use in tutorials and interactive exploration.

Usage::

    inspect_pipeline(delta_root)          # one row per step
    inspect_step(delta_root, 0)           # one row per artifact at step 0
    inspect_metrics(delta_root, 2)        # parsed metric values at step 2
    inspect_data(delta_root, name="d0")   # actual CSV content of a DataArtifact
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import polars as pl

from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.utils.dicts import flatten_dict

# ======================================================================
# Public API
# ======================================================================


def inspect_pipeline(
    delta_root: Path | str,
    *,
    pipeline_run_id: str | None = None,
    storage_options: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Pipeline-level overview — one row per step.

    Args:
        delta_root: Path to Delta Lake root.
        pipeline_run_id: Filter to a specific run. Latest if None.
        storage_options: Delta-rs storage options for cloud backends.

    Returns:
        DataFrame with columns: step, operation, status, produced, duration.

    Raises:
        FileNotFoundError: If steps table doesn't exist.
    """
    delta_root = Path(delta_root)
    steps_path = delta_root / TablePath.STEPS
    if not steps_path.exists():
        msg = f"Steps table not found at {steps_path}"
        raise FileNotFoundError(msg)

    # Load completed, skipped, and cancelled steps
    scanner = pl.scan_delta(str(steps_path), storage_options=storage_options).filter(
        pl.col("status").is_in(["completed", "skipped", "cancelled"])
    )
    if pipeline_run_id is not None:
        scanner = scanner.filter(pl.col("pipeline_run_id") == pipeline_run_id)

    steps_df = (
        scanner.select(
            "pipeline_run_id",
            "step_number",
            "step_name",
            "operation_class",
            "status",
            "succeeded_count",
            "duration_seconds",
        )
        .sort("step_number")
        .collect()
    )

    if steps_df.is_empty():
        return pl.DataFrame(
            schema={
                "step": pl.Int32,
                "operation": pl.String,
                "status": pl.String,
                "produced": pl.String,
                "duration": pl.String,
            }
        )

    # Resolve pipeline_run_id from first row if not provided
    run_id = pipeline_run_id or steps_df["pipeline_run_id"][0]
    if pipeline_run_id is None:
        steps_df = steps_df.filter(pl.col("pipeline_run_id") == run_id)

    # Deduplicate by step_number (keep last)
    steps_df = steps_df.unique(subset=["step_number"], keep="last").sort("step_number")

    # Load artifact index for counts
    index_path = delta_root / TablePath.ARTIFACT_INDEX
    index_counts: dict[int, dict[str, int]] = {}
    if index_path.exists():
        idx_df = pl.scan_delta(
            str(index_path), storage_options=storage_options
        ).collect()
        if not idx_df.is_empty():
            grouped = (
                idx_df.group_by("origin_step_number", "artifact_type")
                .len()
                .sort("origin_step_number")
            )
            for row in grouped.iter_rows(named=True):
                step_num = row["origin_step_number"]
                if step_num not in index_counts:
                    index_counts[step_num] = {}
                index_counts[step_num][row["artifact_type"]] = row["len"]

    # Build result rows
    rows: list[dict[str, Any]] = []
    for row in steps_df.iter_rows(named=True):
        step_num = row["step_number"]

        if row["status"] == "skipped":
            rows.append(
                {
                    "step": step_num,
                    "operation": row["step_name"],
                    "status": "skipped",
                    "produced": "-",
                    "duration": "-",
                }
            )
            continue

        if row["status"] == "cancelled":
            rows.append(
                {
                    "step": step_num,
                    "operation": row["step_name"],
                    "status": "cancelled",
                    "produced": "-",
                    "duration": "-",
                }
            )
            continue

        op_class = row["operation_class"] or ""
        is_filter = "Filter" in op_class or "filter" in (row["step_name"] or "")

        if is_filter:
            produced = f"{row['succeeded_count'] or 0} passed"
        else:
            counts = index_counts.get(step_num, {})
            if counts:
                parts = [f"{v} {k}" for k, v in sorted(counts.items())]
                produced = ", ".join(parts)
            else:
                produced = "-"

        duration_s = row["duration_seconds"]
        duration = f"{duration_s:.1f}s" if duration_s is not None else "-"

        rows.append(
            {
                "step": step_num,
                "operation": row["step_name"],
                "status": "ok",
                "produced": produced,
                "duration": duration,
            }
        )

    return pl.DataFrame(rows)


def inspect_step(
    delta_root: Path | str,
    step_number: int,
    *,
    storage_options: dict[str, str] | None = None,
) -> pl.DataFrame:
    """One-row-per-artifact summary for a given step.

    Args:
        delta_root: Path to Delta Lake root.
        step_number: Step number to inspect.
        storage_options: Delta-rs storage options for cloud backends.

    Returns:
        DataFrame with columns: name, artifact_type, step, details.
    """
    delta_root = Path(delta_root)
    empty = pl.DataFrame(
        schema={
            "name": pl.String,
            "artifact_type": pl.String,
            "step": pl.Int32,
            "details": pl.String,
        }
    )

    # Get artifact IDs at this step from index
    index_path = delta_root / TablePath.ARTIFACT_INDEX
    if not index_path.exists():
        return empty

    idx_df = (
        pl.scan_delta(str(index_path), storage_options=storage_options)
        .filter(pl.col("origin_step_number") == step_number)
        .collect()
    )

    if idx_df.is_empty():
        return empty

    # Group by artifact type
    type_groups = idx_df.group_by("artifact_type").agg(pl.col("artifact_id"))

    all_rows: list[dict[str, Any]] = []
    for group_row in type_groups.iter_rows(named=True):
        art_type = group_row["artifact_type"]
        art_ids = set(group_row["artifact_id"])

        try:
            table_path = delta_root / ArtifactTypeDef.get_table_path(art_type)
        except KeyError:
            continue

        if not table_path.exists():
            continue

        df = (
            pl.scan_delta(str(table_path), storage_options=storage_options)
            .filter(pl.col("origin_step_number") == step_number)
            .collect()
        )

        for row in df.iter_rows(named=True):
            if row["artifact_id"] not in art_ids:
                continue

            name = row.get("original_name") or row["artifact_id"][:16]
            details = _build_details(art_type, row)

            all_rows.append(
                {
                    "name": name,
                    "artifact_type": art_type,
                    "step": step_number,
                    "details": details,
                }
            )

    if not all_rows:
        return empty

    return pl.DataFrame(all_rows).sort("name")


def inspect_metrics(
    delta_root: Path | str,
    step_number: int | None = None,
    *,
    round_digits: int = 3,
    storage_options: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Parse metric artifacts into a human-readable table.

    Args:
        delta_root: Path to Delta Lake root.
        step_number: Filter to a specific step. All metric steps if None.
        round_digits: Decimal places for float rounding.
        storage_options: Delta-rs storage options for cloud backends.

    Returns:
        DataFrame with columns: name, step, {metric_key_1}, {metric_key_2}, ...

    Raises:
        FileNotFoundError: If metrics table doesn't exist.
    """
    delta_root = Path(delta_root)
    table_path = delta_root / ArtifactTypeDef.get_table_path("metric")
    if not table_path.exists():
        msg = f"Metrics table not found at {table_path}"
        raise FileNotFoundError(msg)

    scanner = pl.scan_delta(str(table_path), storage_options=storage_options)
    if step_number is not None:
        scanner = scanner.filter(pl.col("origin_step_number") == step_number)

    df = scanner.collect()

    if df.is_empty():
        return pl.DataFrame(schema={"name": pl.String, "step": pl.Int32})

    # Parse all metric values and collect unique keys
    parsed_rows: list[dict[str, Any]] = []
    all_keys: dict[str, None] = {}

    for row in df.iter_rows(named=True):
        name = row.get("original_name") or row["artifact_id"][:16]
        # Strip _metrics suffix for readability
        if name.endswith("_metrics"):
            name = name[: -len("_metrics")]

        content = row.get("content")
        if content is None:
            parsed_rows.append({"name": name, "step": row["origin_step_number"]})
            continue

        values = json.loads(
            content.decode("utf-8") if isinstance(content, bytes) else content
        )
        flat = flatten_dict(values)

        entry: dict[str, Any] = {"name": name, "step": row["origin_step_number"]}
        for k, v in flat.items():
            all_keys[k] = None
            if isinstance(v, float):
                entry[k] = round(v, round_digits)
            else:
                entry[k] = v
        parsed_rows.append(entry)

    result = pl.DataFrame(parsed_rows)

    # Round any float columns that weren't already rounded
    float_cols = [
        c for c in result.columns if result[c].dtype in (pl.Float64, pl.Float32)
    ]
    if float_cols:
        result = result.with_columns(
            [pl.col(c).round(round_digits) for c in float_cols]
        )

    return result.sort("step", "name")


def inspect_data(
    delta_root: Path | str,
    name: str | None = None,
    step_number: int | None = None,
    *,
    storage_options: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Read DataArtifact CSV content as a Polars DataFrame.

    Args:
        delta_root: Path to Delta Lake root.
        name: Filter by original_name. Takes the first match.
        step_number: Filter by step number.
        storage_options: Delta-rs storage options for cloud backends.

    Returns:
        DataFrame with the actual CSV data content.

    Raises:
        FileNotFoundError: If data table doesn't exist.
        ValueError: If no matching artifacts found or content is None.
    """
    delta_root = Path(delta_root)
    table_path = delta_root / ArtifactTypeDef.get_table_path("data")
    if not table_path.exists():
        msg = f"Data table not found at {table_path}"
        raise FileNotFoundError(msg)

    scanner = pl.scan_delta(str(table_path), storage_options=storage_options)
    if name is not None:
        scanner = scanner.filter(pl.col("original_name") == name)
    if step_number is not None:
        scanner = scanner.filter(pl.col("origin_step_number") == step_number)

    df = scanner.collect()

    if df.is_empty():
        # Build a helpful error message
        all_names = (
            pl.scan_delta(str(table_path), storage_options=storage_options)
            .select("original_name")
            .collect()["original_name"]
            .to_list()
        )
        msg = f"No matching data artifacts found. Available names: {all_names}"
        raise ValueError(msg)

    if name is not None:
        # Single artifact by name
        content = df["content"][0]
        if content is None:
            msg = f"Artifact '{name}' has no content (not hydrated)"
            raise ValueError(msg)
        return pl.read_csv(io.BytesIO(content))

    # Multiple artifacts (step filter) — concatenate with _source column
    frames: list[pl.DataFrame] = []
    for row in df.iter_rows(named=True):
        content = row["content"]
        if content is None:
            continue
        source_name = row.get("original_name") or row["artifact_id"][:16]
        frame = pl.read_csv(io.BytesIO(content)).with_columns(
            pl.lit(source_name).alias("_source")
        )
        frames.append(frame)

    if not frames:
        msg = "All matching artifacts have no content (not hydrated)"
        raise ValueError(msg)

    return pl.concat(frames)


# ======================================================================
# Private helpers
# ======================================================================


def _format_size(size: int) -> str:
    """Format a byte count as a human-readable string."""
    if size >= 1_000_000:
        return f"{size / 1_000_000:.1f} MB"
    if size >= 1_000:
        return f"{size / 1_000:.1f} KB"
    return f"{size} B"


def _build_details(artifact_type: str, row: dict[str, Any]) -> str:
    """Return a type-specific summary string for an artifact row."""
    if artifact_type == "data":
        row_count = row.get("row_count")
        columns_json = row.get("columns")
        if row_count is not None and columns_json:
            cols = (
                json.loads(columns_json)
                if isinstance(columns_json, str)
                else columns_json
            )
            return f"{row_count} rows, {len(cols)} cols"
        size = row.get("size_bytes")
        if size is not None:
            return _format_size(size)
        return "-"

    if artifact_type == "metric":
        content = row.get("content")
        if content is None:
            return "-"
        try:
            values = json.loads(
                content.decode("utf-8") if isinstance(content, bytes) else content
            )
            keys = list(values.keys())[:4]
            suffix = ", ..." if len(values) > 4 else ""
            return ", ".join(keys) + suffix
        except (json.JSONDecodeError, UnicodeDecodeError):
            return "-"

    elif artifact_type == "config":
        content = row.get("content")
        if content is None:
            return "-"
        try:
            values = json.loads(
                content.decode("utf-8") if isinstance(content, bytes) else content
            )
            return f"{len(values)} params"
        except (json.JSONDecodeError, UnicodeDecodeError):
            return "-"

    elif artifact_type == "file_ref" or artifact_type == "large_file":
        size = row.get("size_bytes")
        if size is not None:
            return _format_size(size)
        return "-"

    elif artifact_type == "appendable":
        record_id = row.get("record_id")
        if record_id is not None:
            return record_id
        return "-"

    else:
        return "-"
