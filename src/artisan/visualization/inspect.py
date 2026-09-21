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
from typing import TYPE_CHECKING, Any, cast

import polars as pl
from fsspec import AbstractFileSystem
from pydantic import BaseModel, ValidationError

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.command_record import CommandRecording
from artisan.storage.core.committed_scan import read_committed, scan_committed
from artisan.storage.core.store_format import assert_store_format
from artisan.utils.dicts import flatten_dict
from artisan.utils.log_paths import failure_log_relative_path, worker_log_path

if TYPE_CHECKING:
    from polars.datatypes import DataType, DataTypeClass

    from artisan.schemas.execution.storage_config import StorageConfig
from artisan.utils.path import uri_join

# ======================================================================
# Public API
# ======================================================================


def _validated_fs(
    delta_root: str,
    fs: AbstractFileSystem | None,
    storage_options: dict[str, str] | None,
) -> AbstractFileSystem:
    """Resolve the filesystem and enforce the store-format gate."""
    if fs is None:
        from fsspec.implementations.local import LocalFileSystem

        fs = LocalFileSystem()
    assert_store_format(delta_root, fs, storage_options)
    return fs


def inspect_worker_log(
    delta_root: str,
    execution_run_id: str,
    *,
    fs: AbstractFileSystem | None = None,
    storage_options: dict[str, str] | None = None,
) -> str | None:
    """Read an exact execution's provider log, including uncommitted attempts.

    Separate provider diagnostics take precedence over a committed embedded log.
    Missing logs return None. The ID must be a literal path component.

    Args:
        delta_root: Store root containing execution records and diagnostics.
        execution_run_id: Original execution attempt ID.
        fs: Filesystem for the store; defaults to the local filesystem.
        storage_options: Delta-rs options for reading embedded diagnostics.
    """
    path = worker_log_path(delta_root, execution_run_id)
    fs = _validated_fs(delta_root, fs, storage_options)
    if fs.exists(path):
        with fs.open(path, "rb") as stream:
            return cast(bytes, stream.read()).decode("utf-8")
    executions = read_committed(
        delta_root, TablePath.EXECUTIONS, fs=fs, storage_options=storage_options
    ).filter(pl.col("execution_run_id") == execution_run_id)
    if executions.is_empty():
        return None
    return cast(str | None, executions.item(0, "worker_log"))


def inspect_commands(
    delta_root: str,
    execution_run_id: str,
    *,
    fs: AbstractFileSystem | None = None,
    storage_options: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Read the canonical command evidence for exactly one committed execution.

    Args:
        delta_root: Root of the current-format Artisan store.
        execution_run_id: Execution attempt to inspect.
        fs: Filesystem for store access; defaults to local storage.
        storage_options: Delta-rs options for cloud storage.

    Returns:
        Validated command recording, including omissions and missing invocations.

    Raises:
        FileNotFoundError: No committed execution has this ID.
        StoreIntegrityError: The ID is duplicated or its evidence is malformed.
    """
    fs = _validated_fs(delta_root, fs, storage_options)
    rows = (
        scan_committed(
            delta_root,
            TablePath.EXECUTIONS,
            fs=fs,
            storage_options=storage_options,
        )
        .filter(pl.col("execution_run_id") == execution_run_id)
        .select("command_recording")
        .collect()
    )
    if rows.height == 0:
        msg = f"Committed execution {execution_run_id!r} not found"
        raise FileNotFoundError(msg)
    if rows.height != 1:
        msg = "Duplicate committed execution ID"
        raise StoreIntegrityError(msg)
    try:
        return CommandRecording.model_validate_json(rows.item()).model_dump(mode="json")
    except (ValidationError, TypeError, ValueError):
        msg = "Invalid canonical command recording"
        raise StoreIntegrityError(msg) from None


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
        ``status`` uses the authoritative lifecycle vocabulary, including
        pending and running attempts.

    Raises:
        FileNotFoundError: If steps table doesn't exist.
    """
    fs = _validated_fs(delta_root, fs, storage_options)
    steps_path = uri_join(delta_root, TablePath.STEPS)
    if not fs.exists(steps_path):
        msg = f"Steps table not found at {steps_path}"
        raise FileNotFoundError(msg)

    from artisan.orchestration.engine.step_tracker import StepTracker
    from artisan.schemas.orchestration.step_lifecycle import StepStatus

    states = StepTracker(
        delta_root,
        storage_options=storage_options,
        fs=fs,
    ).load_current_states(pipeline_run_id)
    if not states:
        return pl.DataFrame(
            schema={
                "step": pl.Int32,
                "operation": pl.String,
                "status": pl.String,
                "produced": pl.String,
                "duration": pl.String,
            }
        )

    run_id = states[0].pipeline_run_id

    index_counts: dict[int, dict[str, int]] = {}
    from artisan.storage.core.run_scope import load_accepted_outputs

    outputs = load_accepted_outputs(
        delta_root,
        fs=fs,
        storage_options=storage_options,
        pipeline_run_id=run_id,
    )
    distinct_outputs = outputs.select(
        "current_step_number", "artifact_id", "artifact_type"
    ).unique()
    for output in distinct_outputs.iter_rows(named=True):
        counts = index_counts.setdefault(output["current_step_number"], {})
        artifact_type = output["artifact_type"]
        counts[artifact_type] = counts.get(artifact_type, 0) + 1

    # Build result rows
    rows: list[dict[str, Any]] = []
    usable = {StepStatus.SUCCEEDED, StepStatus.PARTIAL}
    for state in states:
        step_num = state.step_number
        if state.status not in usable:
            rows.append(
                {
                    "step": step_num,
                    "operation": state.step_name,
                    "status": state.status.value,
                    "produced": "-",
                    "duration": "-",
                }
            )
            continue

        op_class = state.operation_class or ""
        is_filter = "Filter" in op_class or "filter" in state.step_name

        if is_filter:
            produced = f"{state.succeeded_count or 0} passed"
        else:
            counts = index_counts.get(step_num, {})
            if counts:
                parts = [f"{v} {k}" for k, v in sorted(counts.items())]
                produced = ", ".join(parts)
            else:
                produced = "-"
        duration_s = state.duration_seconds
        duration = f"{duration_s:.1f}s" if duration_s is not None else "-"

        rows.append(
            {
                "step": step_num,
                "operation": state.step_name,
                "status": state.status.value,
                "produced": produced,
                "duration": duration,
            }
        )

    return pl.DataFrame(rows)


_FAILURES_SCHEMA: dict[str, DataType | DataTypeClass] = {
    "step": pl.Int32,
    "operation": pl.String,
    "execution_run_id": pl.String,
    "timestamp_start": pl.Datetime("us", "UTC"),
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
    """Report failed executions and their structured errors.

    Scans ``executions`` for ``success == False``, deserializes each
    ``error_envelope`` into its structured fields (``code``,
    ``recovery_hint``, ``field``, ``suggestions``), and surfaces them
    alongside step/op identity, the error string, and a pointer to the
    human failure log. Rows whose failure carried no ``ArtisanError``
    (``error_envelope`` NULL) show the string and log only, with null
    structured fields.

    Complements ``inspect_pipeline`` (the step overview): this surfaces the
    failed *executions* within any step, including partial failures inside
    a terminal step attempt.

    Args:
        delta_root: Path to Delta Lake root.
        pipeline_run_id: Exact run whose execution participation to inspect.
            Reused executions appear at their current logical positions while
            retaining source execution/log identity. None reports executions
            across all runs at their origin steps.
        storage_options: Delta-rs storage options for cloud backends.
        fs: Filesystem for existence checks. Local if None.

    Returns:
        DataFrame with columns: step, operation, execution_run_id, timestamp_start, code,
        recovery_hint, field, suggestions, error, log. ``log`` is the
        relative fragment ``YYYYMMDD/YYYYMMDDTHHMMSSffffffZ_executionID.log``;
        prefix it with ``<runs_dir>/logs/failures/``. Rows sort by source
        start time, execution ID, then current step, oldest first.

    Raises:
        IncompatibleStoreError: The root lacks the current store-format contract.
        FileNotFoundError: Both execution and step tables are absent after
            store-format validation. An initialized store without failures
            returns an empty frame.
    """
    fs = _validated_fs(delta_root, fs, storage_options)
    executions_path = uri_join(delta_root, TablePath.EXECUTIONS)
    if not fs.exists(executions_path):
        steps_path = uri_join(delta_root, TablePath.STEPS)
        if fs.exists(steps_path):
            return pl.DataFrame(schema=_FAILURES_SCHEMA)
        msg = f"Executions table not found at {executions_path}"
        raise FileNotFoundError(msg)

    if pipeline_run_id is not None:
        from artisan.storage.core.run_scope import load_execution_membership

        failures = load_execution_membership(
            delta_root,
            fs=fs,
            storage_options=storage_options,
            pipeline_run_id=pipeline_run_id,
        ).filter(~pl.col("success"))
        failures = failures.select(
            "execution_run_id",
            "timestamp_start",
            pl.col("current_step_number").alias("origin_step_number"),
            "operation_name",
            "error",
            "error_envelope",
        )
    else:
        failures = (
            scan_committed(
                delta_root,
                TablePath.EXECUTIONS,
                fs=fs,
                storage_options=storage_options,
            )
            .filter(~pl.col("success"))
            .select(
                "execution_run_id",
                "timestamp_start",
                "origin_step_number",
                "operation_name",
                "error",
                "error_envelope",
            )
            .collect()
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
                "timestamp_start": row["timestamp_start"],
                "code": code,
                "recovery_hint": recovery_hint,
                "field": field,
                "suggestions": suggestions,
                "error": row["error"],
                "log": failure_log_relative_path(
                    row["execution_run_id"], row["timestamp_start"]
                ),
            }
        )

    if not rows:
        return pl.DataFrame(schema=_FAILURES_SCHEMA)
    return pl.DataFrame(rows, schema=_FAILURES_SCHEMA).sort(
        "timestamp_start", "execution_run_id", "step"
    )


_RECOVERY_ACTIONS = {
    "CHECK_INPUT": "Check the operation's inputs and parameters against its schema.",
    "RETRY_LATER": "Retry the run; the failure may be transient.",
    "TRY_ALTERNATIVE": "Try an alternative operation or configuration.",
    "REPORT_TO_USER": "Surface this failure to the user; it needs human attention.",
}
"""Deterministic recovery_hint → next-action mapping (no heuristics)."""

_DEFAULT_ACTION = "Read the failure log for the full error and traceback."


class RunDiagnosis(BaseModel):
    """Composite failure diagnosis for one run — pure composition, no new state.

    Attributes:
        pipeline_run_id: The diagnosed run.
        last_status: The run's most recent step status, or None if unknown.
        failed_steps: One deserialized ``inspect_failures`` row per failed
            execution in the run.
        similar_runs: Recent runs whose ``last_status`` was ``failed``,
            excluding this run (context for a recurring failure).
        upstream_edges: Backward provenance edges from the artifacts the
            failed steps produced, deduplicated and depth-bounded.
        suggested_actions: Deterministic next actions derived from the
            failed steps' ``recovery_hint`` values.
    """

    pipeline_run_id: str
    last_status: str | None
    failed_steps: list[dict[str, Any]]
    similar_runs: list[dict[str, Any]]
    upstream_edges: list[dict[str, str]]
    suggested_actions: list[str]


def diagnose_run(
    delta_root: str,
    pipeline_run_id: str,
    *,
    storage: StorageConfig | None = None,
) -> RunDiagnosis:
    """Diagnose one run's failures by composing the shipped readers.

    Composes ``inspect_failures`` (failed executions + envelopes),
    ``run_history.list_runs`` (this run's status and recent failed runs),
    and ``provenance_edges`` (backward lineage from the failed steps'
    artifacts). ``suggested_actions`` is a fixed mapping from the failures'
    ``recovery_hint`` values — no inference.

    Args:
        delta_root: Path to Delta Lake root.
        pipeline_run_id: The run to diagnose.
        storage: Storage configuration for cloud backends. Defaults to
            local filesystem.

    Returns:
        A ``RunDiagnosis``. An initialized store without failed executions
        yields a diagnosis with no failed steps.

    Raises:
        IncompatibleStoreError: The root lacks the current store-format contract.
        FileNotFoundError: Required tables are missing after store-format
            validation, propagated from the underlying readers.
    """
    from artisan.orchestration.run_history import list_runs
    from artisan.schemas.execution.storage_config import StorageConfig

    storage = storage or StorageConfig()
    failed_steps = inspect_failures(
        delta_root,
        pipeline_run_id=pipeline_run_id,
        storage_options=storage.delta_storage_options(),
        fs=storage.filesystem(),
    ).to_dicts()

    runs = list_runs(delta_root, storage=storage).to_dicts()
    last_status = next(
        (r["last_status"] for r in runs if r["pipeline_run_id"] == pipeline_run_id),
        None,
    )
    similar_runs = [
        r
        for r in runs
        if r["pipeline_run_id"] != pipeline_run_id and r["last_status"] == "failed"
    ][:5]

    upstream_edges = _failure_upstream_edges(delta_root, failed_steps, storage)

    hints = {
        hint
        for step in failed_steps
        if isinstance(hint := step.get("recovery_hint"), str)
    }
    suggested = [_RECOVERY_ACTIONS.get(hint, _DEFAULT_ACTION) for hint in sorted(hints)]
    if not suggested and failed_steps:
        suggested = [_DEFAULT_ACTION]

    return RunDiagnosis(
        pipeline_run_id=pipeline_run_id,
        last_status=last_status,
        failed_steps=failed_steps,
        similar_runs=similar_runs,
        upstream_edges=upstream_edges,
        suggested_actions=suggested,
    )


def _failure_upstream_edges(
    delta_root: str,
    failed_steps: list[dict[str, Any]],
    storage: StorageConfig,
) -> list[dict[str, str]]:
    """Walk backward provenance from the failed steps' artifacts, deduplicated.

    Trace at most ten output artifacts from failed executions, to depth two.
    Missing execution or artifact provenance edges yield no upstream edges.
    """
    from artisan.provenance import provenance_edges

    execution_ids = {step["execution_run_id"] for step in failed_steps}
    if not execution_ids:
        return []
    fs = storage.filesystem()
    edge_path = uri_join(delta_root, TablePath.EXECUTION_EDGES)
    if not fs.exists(edge_path):
        return []
    artifact_ids = (
        scan_committed(
            delta_root,
            TablePath.EXECUTION_EDGES,
            fs=fs,
            storage_options=storage.delta_storage_options(),
        )
        .filter(
            pl.col("execution_run_id").is_in(execution_ids)
            & (pl.col("direction") == "output")
        )
        .select("artifact_id")
        .unique(maintain_order=True)
        .limit(10)
        .collect()["artifact_id"]
        .to_list()
    )
    seen: set[tuple[str, str]] = set()
    edges: list[dict[str, str]] = []
    for artifact_id in artifact_ids:
        walk = provenance_edges(
            delta_root, artifact_id, direction="backward", depth=2, storage=storage
        )
        for edge in walk.edges:
            key = (edge["source_artifact_id"], edge["target_artifact_id"])
            if key not in seen:
                seen.add(key)
                edges.append(edge)
    return edges


def inspect_step(
    delta_root: str,
    step_number: int,
    *,
    pipeline_run_id: str | None = None,
    storage_options: dict[str, str] | None = None,
    fs: AbstractFileSystem | None = None,
) -> pl.DataFrame:
    """One-row-per-artifact summary for a given step.

    Args:
        delta_root: Path to Delta Lake root.
        step_number: Origin step by default, or current logical step when a
            run is selected.
        pipeline_run_id: Exact run whose accepted outputs to inspect. None
            selects artifacts by origin step across all runs.
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

    fs = _validated_fs(delta_root, fs, storage_options)
    index_path = uri_join(delta_root, TablePath.ARTIFACT_INDEX)
    if not fs.exists(index_path):
        return empty

    if pipeline_run_id is None:
        idx_df = (
            scan_committed(
                delta_root,
                TablePath.ARTIFACT_INDEX,
                fs=fs,
                storage_options=storage_options,
            )
            .filter(pl.col("origin_step_number") == step_number)
            .collect()
        )
    else:
        from artisan.storage.core.run_scope import load_accepted_outputs

        outputs = load_accepted_outputs(
            delta_root,
            fs=fs,
            storage_options=storage_options,
            pipeline_run_id=pipeline_run_id,
        )
        rows = [
            {
                "artifact_id": row["artifact_id"],
                "artifact_type": row["artifact_type"],
                "origin_step_number": row["origin_step_number"],
                "metadata": "{}",
            }
            for row in outputs.iter_rows(named=True)
            if row["current_step_number"] == step_number
        ]
        idx_df = pl.DataFrame(rows) if rows else pl.DataFrame()

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
            scan_committed(
                delta_root,
                ArtifactTypeDef.get_table_path(art_type),
                fs=fs,
                storage_options=storage_options,
            )
            .filter(pl.col("artifact_id").is_in(art_ids))
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
    pipeline_run_id: str | None = None,
    round_digits: int = 3,
    storage_options: dict[str, str] | None = None,
    fs: AbstractFileSystem | None = None,
) -> pl.DataFrame:
    """Parse metric artifacts into a human-readable table.

    Args:
        delta_root: Path to Delta Lake root.
        step_number: Origin step by default, or current logical step when a
            run is selected. None includes all steps in the selected scope.
        pipeline_run_id: Exact run whose accepted metric outputs to inspect.
            None includes metrics across all runs at their origin steps.
        round_digits: Decimal places for float rounding.
        storage_options: Delta-rs storage options for cloud backends.
        fs: Filesystem for existence checks. Local if None.

    Returns:
        DataFrame with columns: name, step, {metric_key_1}, {metric_key_2}, ...

    Raises:
        FileNotFoundError: If metrics table doesn't exist.
    """
    fs = _validated_fs(delta_root, fs, storage_options)
    table_path = uri_join(delta_root, ArtifactTypeDef.get_table_path("metric"))
    if not fs.exists(table_path):
        msg = f"Metrics table not found at {table_path}"
        raise FileNotFoundError(msg)

    scanner = scan_committed(
        delta_root,
        ArtifactTypeDef.get_table_path("metric"),
        fs=fs,
        storage_options=storage_options,
    )
    current_steps: pl.DataFrame | None = None
    if pipeline_run_id is not None:
        from artisan.storage.core.run_scope import load_accepted_outputs

        outputs = load_accepted_outputs(
            delta_root,
            fs=fs,
            storage_options=storage_options,
            pipeline_run_id=pipeline_run_id,
        )
        current_steps = outputs.filter(pl.col("artifact_type") == "metric").select(
            "artifact_id", "current_step_number"
        )
        if step_number is not None:
            current_steps = current_steps.filter(
                pl.col("current_step_number") == step_number
            )
        current_steps = current_steps.unique()
        scanner = scanner.filter(
            pl.col("artifact_id").is_in(current_steps["artifact_id"].to_list())
        )
    elif step_number is not None:
        scanner = scanner.filter(pl.col("origin_step_number") == step_number)

    df = scanner.collect()
    if current_steps is not None:
        df = current_steps.join(df, on="artifact_id", how="inner")

    if df.is_empty():
        return pl.DataFrame(schema={"name": pl.String, "step": pl.Int32})

    parsed_rows: list[dict[str, Any]] = []

    for row in df.iter_rows(named=True):
        name = row.get("original_name") or row["artifact_id"][:16]
        current_step = row.get("current_step_number")
        display_step = (
            current_step if current_step is not None else row["origin_step_number"]
        )
        # Strip _metrics suffix for readability
        if name.endswith("_metrics"):
            name = name[: -len("_metrics")]

        content = row.get("content")
        if content is None:
            parsed_rows.append({"name": name, "step": display_step})
            continue

        values = json.loads(
            content.decode("utf-8") if isinstance(content, bytes) else content
        )
        flat = flatten_dict(values)

        entry: dict[str, Any] = {"name": name, "step": display_step}
        for k, v in flat.items():
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
    pipeline_run_id: str | None = None,
    storage_options: dict[str, str] | None = None,
    fs: AbstractFileSystem | None = None,
) -> pl.DataFrame:
    """Read DataArtifact CSV content as a Polars DataFrame.

    Args:
        delta_root: Path to Delta Lake root.
        name: Filter by original_name. Takes the first match.
        step_number: Origin step by default, or current logical step when a
            run is selected. None includes all steps in the selected scope.
        pipeline_run_id: Exact run whose accepted data outputs may match.
            None selects artifacts across all runs by origin step.
        storage_options: Delta-rs storage options for cloud backends.
        fs: Filesystem for existence checks. Local if None.

    Returns:
        DataFrame with the actual CSV data content.

    Raises:
        FileNotFoundError: If data table doesn't exist.
        ValueError: If no matching artifacts found or content is None.
    """
    fs = _validated_fs(delta_root, fs, storage_options)
    table_path = uri_join(delta_root, ArtifactTypeDef.get_table_path("data"))
    if not fs.exists(table_path):
        msg = f"Data table not found at {table_path}"
        raise FileNotFoundError(msg)

    scanner = scan_committed(
        delta_root,
        ArtifactTypeDef.get_table_path("data"),
        fs=fs,
        storage_options=storage_options,
    )
    if name is not None:
        scanner = scanner.filter(pl.col("original_name") == name)
    if pipeline_run_id is not None:
        from artisan.storage.core.run_scope import load_accepted_outputs

        outputs = load_accepted_outputs(
            delta_root,
            fs=fs,
            storage_options=storage_options,
            pipeline_run_id=pipeline_run_id,
        )
        artifact_ids = [
            row["artifact_id"]
            for row in outputs.filter(pl.col("artifact_type") == "data").iter_rows(
                named=True
            )
            if step_number is None or row["current_step_number"] == step_number
        ]
        scanner = scanner.filter(pl.col("artifact_id").is_in(artifact_ids))
    elif step_number is not None:
        scanner = scanner.filter(pl.col("origin_step_number") == step_number)

    df = scanner.collect()

    if df.is_empty():
        # Build a helpful error message
        all_names = (
            read_committed(
                delta_root,
                ArtifactTypeDef.get_table_path("data"),
                fs=fs,
                storage_options=storage_options,
            )
            .select("original_name")["original_name"]
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
