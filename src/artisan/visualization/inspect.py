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
from typing import Any

import polars as pl
from fsspec import AbstractFileSystem

from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.utils.dicts import flatten_dict
from artisan.utils.path import uri_join

# ======================================================================
# Public API
# ======================================================================


def inspect_pipeline(
    delta_root: str,
    *,
    pipeline_run_id: str | None = None,
    storage_options: dict[str, str] | None = None,
    fs: AbstractFileSystem | None = None,
) -> pl.DataFrame:
    """Pipeline-level overview — one row per step.

    Args:
        delta_root: Path to Delta Lake root.
        pipeline_run_id: Filter to a specific run. Latest if None.
        storage_options: Delta-rs storage options for cloud backends.
        fs: Filesystem for existence checks. Local if None.

    Returns:
        DataFrame with columns: step, operation, status, produced, duration.

    Raises:
        FileNotFoundError: If steps table doesn't exist.
    """
    if fs is None:
        from fsspec.implementations.local import LocalFileSystem

        fs = LocalFileSystem()
    steps_path = uri_join(delta_root, TablePath.STEPS)
    if not fs.exists(steps_path):
        msg = f"Steps table not found at {steps_path}"
        raise FileNotFoundError(msg)

    # Load completed, skipped, cancelled, and failed steps
    scanner = pl.scan_delta(steps_path, storage_options=storage_options).filter(
        pl.col("status").is_in(["completed", "skipped", "cancelled", "failed"])
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
    index_path = uri_join(delta_root, TablePath.ARTIFACT_INDEX)
    index_counts: dict[int, dict[str, int]] = {}
    if fs.exists(index_path):
        idx_df = pl.scan_delta(index_path, storage_options=storage_options).collect()
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

        if row["status"] == "failed":
            rows.append(
                {
                    "step": step_num,
                    "operation": row["step_name"],
                    "status": "failed",
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


_FAILURES_SCHEMA = {
    "step": pl.Int32,
    "operation": pl.String,
    "execution_run_id": pl.String,
    "code": pl.String,
    "recovery_hint": pl.String,
    "field": pl.String,
    "suggestions": pl.List(pl.String),
    "error": pl.String,
    "log": pl.String,
}


def inspect_failures(
    delta_root: str,
    *,
    pipeline_run_id: str | None = None,
    storage_options: dict[str, str] | None = None,
    fs: AbstractFileSystem | None = None,
) -> pl.DataFrame:
    """Execution-level failure report — one row per failed execution.

    Scans ``executions`` for ``success == False``, deserializes each
    ``error_envelope`` into its structured fields (``code``,
    ``recovery_hint``, ``field``, ``suggestions``), and surfaces them
    alongside step/op identity, the error string, and a pointer to the
    human failure log. Rows whose failure carried no ``ArtisanError``
    (``error_envelope`` NULL) show the string and log only, with null
    structured fields.

    Complements ``inspect_pipeline`` (the step overview): this surfaces the
    failed *executions* within any step, including partial failures inside
    a step that completed.

    Args:
        delta_root: Path to Delta Lake root.
        pipeline_run_id: Filter to one run (joined via ``steps`` on
            ``step_run_id``). All runs if None. Failures with a null
            ``step_run_id`` (composite-internal lifecycles) do not match a
            run filter.
        storage_options: Delta-rs storage options for cloud backends.
        fs: Filesystem for existence checks. Local if None.

    Returns:
        DataFrame with columns: step, operation, execution_run_id, code,
        recovery_hint, field, suggestions, error, log. ``log`` is the
        relative fragment ``step_{step}_{operation}/{run_id}.log`` — prefix
        it with ``<runs_dir>/logs/failures/``.

    Raises:
        FileNotFoundError: If the executions table does not exist.
    """
    if fs is None:
        from fsspec.implementations.local import LocalFileSystem

        fs = LocalFileSystem()
    executions_path = uri_join(delta_root, TablePath.EXECUTIONS)
    if not fs.exists(executions_path):
        msg = f"Executions table not found at {executions_path}"
        raise FileNotFoundError(msg)

    failures = (
        pl.scan_delta(executions_path, storage_options=storage_options)
        .filter(~pl.col("success"))
        .select(
            "execution_run_id",
            "step_run_id",
            "origin_step_number",
            "operation_name",
            "error",
            "error_envelope",
        )
        .collect()
    )

    if pipeline_run_id is not None:
        failures = failures.filter(
            pl.col("step_run_id").is_in(
                _run_step_ids(delta_root, pipeline_run_id, storage_options, fs)
            )
        )

    rows: list[dict[str, Any]] = []
    for row in failures.iter_rows(named=True):
        code = recovery_hint = field = None
        suggestions: list[str] | None = None
        env_json = row["error_envelope"]
        if env_json is not None:
            env = json.loads(env_json)
            code = env.get("code")
            recovery_hint = env.get("recovery_hint")
            field = env.get("field")
            suggestions = env.get("suggestions")
        step = row["origin_step_number"]
        operation = row["operation_name"]
        rows.append(
            {
                "step": step,
                "operation": operation,
                "execution_run_id": row["execution_run_id"],
                "code": code,
                "recovery_hint": recovery_hint,
                "field": field,
                "suggestions": suggestions,
                "error": row["error"],
                "log": f"step_{step}_{operation}/{row['execution_run_id']}.log",
            }
        )

    if not rows:
        return pl.DataFrame(schema=_FAILURES_SCHEMA)
    return pl.DataFrame(rows, schema=_FAILURES_SCHEMA)


def inspect_step(
    delta_root: str,
    step_number: int,
    *,
    storage_options: dict[str, str] | None = None,
    fs: AbstractFileSystem | None = None,
) -> pl.DataFrame:
    """One-row-per-artifact summary for a given step.

    Args:
        delta_root: Path to Delta Lake root.
        step_number: Step number to inspect.
        storage_options: Delta-rs storage options for cloud backends.
        fs: Filesystem for existence checks. Local if None.

    Returns:
        DataFrame with columns: name, artifact_type, step, details.
    """
    empty = pl.DataFrame(
        schema={
            "name": pl.String,
            "artifact_type": pl.String,
            "step": pl.Int32,
            "details": pl.String,
        }
    )

    if fs is None:
        from fsspec.implementations.local import LocalFileSystem

        fs = LocalFileSystem()
    # Get artifact IDs at this step from index
    index_path = uri_join(delta_root, TablePath.ARTIFACT_INDEX)
    if not fs.exists(index_path):
        return empty

    idx_df = (
        pl.scan_delta(index_path, storage_options=storage_options)
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
            table_path = uri_join(delta_root, ArtifactTypeDef.get_table_path(art_type))
        except KeyError:
            continue

        if not fs.exists(table_path):
            continue

        df = (
            pl.scan_delta(table_path, storage_options=storage_options)
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
    delta_root: str,
    step_number: int | None = None,
    *,
    round_digits: int = 3,
    storage_options: dict[str, str] | None = None,
    fs: AbstractFileSystem | None = None,
) -> pl.DataFrame:
    """Parse metric artifacts into a human-readable table.

    Args:
        delta_root: Path to Delta Lake root.
        step_number: Filter to a specific step. All metric steps if None.
        round_digits: Decimal places for float rounding.
        storage_options: Delta-rs storage options for cloud backends.
        fs: Filesystem for existence checks. Local if None.

    Returns:
        DataFrame with columns: name, step, {metric_key_1}, {metric_key_2}, ...

    Raises:
        FileNotFoundError: If metrics table doesn't exist.
    """
    if fs is None:
        from fsspec.implementations.local import LocalFileSystem

        fs = LocalFileSystem()
    table_path = uri_join(delta_root, ArtifactTypeDef.get_table_path("metric"))
    if not fs.exists(table_path):
        msg = f"Metrics table not found at {table_path}"
        raise FileNotFoundError(msg)

    scanner = pl.scan_delta(table_path, storage_options=storage_options)
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
    delta_root: str,
    name: str | None = None,
    step_number: int | None = None,
    *,
    storage_options: dict[str, str] | None = None,
    fs: AbstractFileSystem | None = None,
) -> pl.DataFrame:
    """Read DataArtifact CSV content as a Polars DataFrame.

    Args:
        delta_root: Path to Delta Lake root.
        name: Filter by original_name. Takes the first match.
        step_number: Filter by step number.
        storage_options: Delta-rs storage options for cloud backends.
        fs: Filesystem for existence checks. Local if None.

    Returns:
        DataFrame with the actual CSV data content.

    Raises:
        FileNotFoundError: If data table doesn't exist.
        ValueError: If no matching artifacts found or content is None.
    """
    if fs is None:
        from fsspec.implementations.local import LocalFileSystem

        fs = LocalFileSystem()
    table_path = uri_join(delta_root, ArtifactTypeDef.get_table_path("data"))
    if not fs.exists(table_path):
        msg = f"Data table not found at {table_path}"
        raise FileNotFoundError(msg)

    scanner = pl.scan_delta(table_path, storage_options=storage_options)
    if name is not None:
        scanner = scanner.filter(pl.col("original_name") == name)
    if step_number is not None:
        scanner = scanner.filter(pl.col("origin_step_number") == step_number)

    df = scanner.collect()

    if df.is_empty():
        # Build a helpful error message
        all_names = (
            pl.scan_delta(table_path, storage_options=storage_options)
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


def _run_step_ids(
    delta_root: str,
    pipeline_run_id: str,
    storage_options: dict[str, str] | None,
    fs: AbstractFileSystem,
) -> list[str]:
    """Return the ``step_run_id``s belonging to one pipeline run.

    Reads ``steps`` and filters to ``pipeline_run_id``. Empty when the
    steps table is absent — an unknown run matches nothing.
    """
    steps_path = uri_join(delta_root, TablePath.STEPS)
    if not fs.exists(steps_path):
        return []
    return (
        pl.scan_delta(steps_path, storage_options=storage_options)
        .filter(pl.col("pipeline_run_id") == pipeline_run_id)
        .select("step_run_id")
        .collect()["step_run_id"]
        .to_list()
    )


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

    elif artifact_type in {"file_ref", "large_file"}:
        size = row.get("size_bytes")
        if size is not None:
            return _format_size(size)
        return "-"

    elif artifact_type == "appendable":
        record_id = row.get("record_id")
        if record_id is not None:
            return str(record_id)
        return "-"

    else:
        return "-"
