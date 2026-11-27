"""Human-readable inspect helpers for Delta Lake pipeline data.

Read-only functions that present Delta Lake tables as clean Polars DataFrames
for use in tutorials and interactive exploration.

Usage::

    inspect_pipeline(delta_root)          # one row per step
    inspect_step(delta_root, 0)           # one row per artifact at step 0
    inspect_metrics(delta_root, 2)        # parsed metric values at step 2
    inspect_csv(delta_root, name="d0")   # actual CSV content of a CSVArtifact
"""

from __future__ import annotations

import io
import json
from typing import TYPE_CHECKING, Any

import polars as pl
from fsspec import AbstractFileSystem
from pydantic import BaseModel

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.utils.dicts import flatten_dict
from artisan.utils.log_paths import worker_log_path

if TYPE_CHECKING:
    from polars.datatypes import DataType, DataTypeClass

    from artisan.schemas.execution.storage_config import StorageConfig
    from artisan.storage.core.curator_view import CuratorView
    from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_store import open_configured_store
from artisan.visualization._read import artifact_membership, read_view, registered_rows


def inspect_worker_log(
    delta_root: str,
    execution_run_id: str,
    *,
    fs: AbstractFileSystem | None = None,
    files_root: str | None = None,
    storage_options: dict[str, str] | None = None,
) -> str | None:
    """Read an exact execution's provider log, including uncommitted attempts.

    Separate provider diagnostics take precedence over a committed embedded log.
    Missing logs return None. The ID must be a literal path component.

    Args:
        delta_root: Store root containing execution records and diagnostics.
        execution_run_id: Original execution attempt ID.
        fs: Filesystem for the store; defaults to the local filesystem.
        files_root: Managed carrier root; required for nonlocal/custom storage.
        storage_options: Delta-rs options for reading embedded diagnostics.
    """
    from artisan.storage.core.execution_source import (
        CuratorExecutionSource,
        read_execution_source,
    )

    path = worker_log_path(delta_root, execution_run_id)
    committer = open_configured_store(
        delta_root, files_root=files_root, fs=fs, storage_options=storage_options
    )
    if committer.fs.exists(path):
        with committer.fs.open(path, "rb") as stream:
            return bytes(stream.read()).decode("utf-8")
    source = read_execution_source(committer, execution_run_id)
    if not isinstance(source, CuratorExecutionSource):
        return None
    value = source.execution_record["worker_log"]
    if value is not None and not isinstance(value, str):
        msg = "Worker log evidence is malformed"
        raise StoreIntegrityError(msg)
    return value


def inspect_commands(
    delta_root: str,
    execution_run_id: str,
    *,
    fs: AbstractFileSystem | None = None,
    files_root: str | None = None,
    storage_options: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Read the canonical command evidence for exactly one committed execution.

    Args:
        delta_root: Root of the current-format Artisan store.
        execution_run_id: Execution attempt to inspect.
        fs: Filesystem for store access; defaults to local storage.
        files_root: Managed carrier root; required for nonlocal/custom storage.
        storage_options: Delta-rs options for cloud storage.

    Returns:
        Validated command recording, including omissions and missing invocations.

    Raises:
        FileNotFoundError: No committed execution has this ID.
        StoreIntegrityError: The ID is duplicated or its evidence is malformed.
    """
    from artisan.visualization.retained_inspect import retained_commands

    return retained_commands(
        open_configured_store(
            delta_root, files_root=files_root, fs=fs, storage_options=storage_options
        ),
        execution_run_id,
    ).model_dump(mode="json")


def inspect_pipeline(
    delta_root: str,
    *,
    pipeline_run_id: str | None = None,
    storage_options: dict[str, str] | None = None,
    fs: AbstractFileSystem | None = None,
    files_root: str | None = None,
) -> pl.DataFrame:
    """Pipeline-level overview — one row per step.

    Args:
        delta_root: Path to Delta Lake root.
        pipeline_run_id: Filter to a specific run. Latest if None.
        storage_options: Delta-rs storage options for cloud backends.
        fs: Filesystem for existence checks. Local if None.
        files_root: Managed carrier root; required for nonlocal/custom storage.

    Returns:
        DataFrame with columns: step, operation, status, produced, duration.
        ``status`` uses the authoritative lifecycle vocabulary, including
        pending and running attempts.

    Raises:
        FileNotFoundError: If steps table doesn't exist.
    """
    from collections import Counter

    from artisan.orchestration.engine.retained_artifact_query import (
        _references_for_states,
    )
    from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
    from artisan.orchestration.run_status import _step_status

    committer = open_configured_store(
        delta_root, files_root=files_root, fs=fs, storage_options=storage_options
    )
    with committer._read_snapshot() as snapshot:
        tracker = RetainedStepTracker(committer, "")
        _, attempts = tracker._read_lifecycle(snapshot)
        states = tracker._select_current_states(attempts, pipeline_run_id)
        counts: dict[int, Counter[str]] = {}
        if states:
            for reference in _references_for_states(
                snapshot, states, pipeline_run_id=states[0].pipeline_run_id
            ):
                assert reference.current_step_number is not None
                counts.setdefault(reference.current_step_number, Counter())[
                    reference.artifact_type
                ] += 1
        summaries = [
            _step_status(state, counts.get(state.step_number, {})) for state in states
        ]
    return pl.DataFrame(
        [
            {
                "step": item.step_number,
                "operation": item.name,
                "status": item.status.value,
                "produced": item.produced,
                "duration": item.duration,
            }
            for item in summaries
        ],
        schema={
            "step": pl.Int32,
            "operation": pl.String,
            "status": pl.String,
            "produced": pl.String,
            "duration": pl.String,
        },
    )


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
    files_root: str | None = None,
) -> pl.DataFrame:
    """Report failed executions and their structured errors.

    Exact terminal selections identify original failed deliveries and curator
    executions. Optional curator and scientific diagnostics supply structured
    fields and log references. Failures retain their durable error and original
    ID; unavailable timestamps and logs remain null.

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
        files_root: Managed carrier root; required for nonlocal/custom storage.

    Returns:
        DataFrame with columns: step, operation, execution_run_id, timestamp_start, code,
        recovery_hint, field, suggestions, error, log. ``log`` is the
        relative fragment ``YYYYMMDD/YYYYMMDDTHHMMSSffffffZ_executionID.log``;
        prefix it with ``<runs_dir>/logs/failures/``. Rows sort by source
        start time, execution ID, then current step, oldest first.

    Raises:
        IncompatibleStoreError: The root lacks the current store-format contract.
        StoreIntegrityError: Required source or lifecycle evidence is incomplete.
            An initialized store without failures returns an empty frame.
    """
    from artisan.visualization.retained_inspect import retained_failures

    return retained_failures(
        open_configured_store(
            delta_root, files_root=files_root, fs=fs, storage_options=storage_options
        ),
        pipeline_run_id=pipeline_run_id,
    )


_RECOVERY_ACTIONS = {
    "CHECK_INPUT": "Check the operation's inputs and parameters against its schema.",
    "RETRY_LATER": "Retry the run; the failure may be transient.",
    "TRY_ALTERNATIVE": "Try an alternative operation or configuration.",
    "REPORT_TO_USER": "Surface this failure to the user; it needs human attention.",
}
"""Deterministic recovery_hint → next-action mapping (no heuristics)."""

_DEFAULT_ACTION = "Read the failure log for the full error and traceback."


class DiagnosisUpstreamScope(BaseModel):
    """Coverage of the selected failed-output seed and depth-limited walks."""

    seed_limit: int = 10
    seed_count: int = 0
    total_seed_count: int | None = 0
    seed_selection_truncated: bool | None = False
    depth: int = 2
    traversal_truncated: bool | None = False


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
        matching_failed_run_count: All other failed run rollups before the
            recent-run window is applied; not a causal similarity count.
        similar_run_limit: Maximum number of recent failed runs retained.
        upstream_scope: Seed selection and depth coverage of the selected
            upstream edge union, independent of response pagination.
    """

    pipeline_run_id: str
    last_status: str | None
    failed_steps: list[dict[str, Any]]
    similar_runs: list[dict[str, Any]]
    upstream_edges: list[dict[str, str]]
    suggested_actions: list[str]
    matching_failed_run_count: int
    similar_run_limit: int = 5
    upstream_scope: DiagnosisUpstreamScope


def diagnose_run(
    delta_root: str,
    pipeline_run_id: str,
    *,
    storage: StorageConfig | None = None,
    files_root: str | None = None,
) -> RunDiagnosis:
    """Diagnose one run's failures from one captured store view.

    Failure membership, run history and backward lineage use the same lifecycle
    and table versions. ``suggested_actions`` maps the recorded failures'
    ``recovery_hint`` values to fixed actions.

    Args:
        delta_root: Path to Delta Lake root.
        pipeline_run_id: The run to diagnose.
        storage: Storage configuration for cloud backends. Defaults to
            local filesystem.
        files_root: Managed carrier root; required for nonlocal/custom storage.

    Returns:
        A ``RunDiagnosis``. An initialized store without failed executions
        yields a diagnosis with no failed steps.

    Raises:
        IncompatibleStoreError: The root lacks the current store-format contract.
        FileNotFoundError: Required tables are missing after store-format
            validation, propagated from the underlying readers.
    """
    from artisan.orchestration.engine.retained_execution_query import (
        _execution_membership,
    )
    from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
    from artisan.visualization.retained_inspect import _failure_rows

    committer = open_configured_store(
        delta_root, files_root=files_root, storage=storage
    )
    with read_view(committer) as (snapshot, view):
        tracker = RetainedStepTracker(committer, pipeline_run_id)
        lifecycle, attempts = tracker._read_lifecycle(snapshot)
        states = tracker._select_current_states(attempts, pipeline_run_id)
        failed_steps = _failure_rows(
            committer, _execution_membership(snapshot, states)
        ).to_dicts()
        runs = tracker._summarize_runs(lifecycle, attempts).to_dicts()
        edges, scope = _failure_upstream_edges(committer, view, failed_steps)
    return _diagnosis(pipeline_run_id, failed_steps, runs, edges, scope)


def _diagnosis(
    pipeline_run_id: str,
    failed_steps: list[dict[str, Any]],
    runs: list[dict[str, Any]],
    upstream_edges: list[dict[str, str]],
    upstream_scope: DiagnosisUpstreamScope,
) -> RunDiagnosis:
    """Rank and summarize one already captured report without further store reads."""
    last_status = next(
        (r["last_status"] for r in runs if r["pipeline_run_id"] == pipeline_run_id),
        None,
    )
    matching_runs = [
        r
        for r in runs
        if r["pipeline_run_id"] != pipeline_run_id and r["last_status"] == "failed"
    ]
    matching_runs.sort(key=lambda row: row["pipeline_run_id"])
    matching_runs.sort(
        key=lambda row: (row["started_at"] is not None, row["started_at"]),
        reverse=True,
    )
    similar_runs = matching_runs[:5]

    hints = {
        hint
        for step in failed_steps
        if isinstance(hint := step.get("recovery_hint"), str)
    }
    suggested = list(
        dict.fromkeys(
            _RECOVERY_ACTIONS.get(hint, _DEFAULT_ACTION) for hint in sorted(hints)
        )
    )
    if not suggested and failed_steps:
        suggested = [_DEFAULT_ACTION]

    return RunDiagnosis(
        pipeline_run_id=pipeline_run_id,
        last_status=last_status,
        failed_steps=failed_steps,
        similar_runs=similar_runs,
        upstream_edges=upstream_edges,
        suggested_actions=suggested,
        matching_failed_run_count=len(matching_runs),
        upstream_scope=upstream_scope,
    )


def _failure_upstream_edges(
    committer: RetainedCommitter,
    view: CuratorView,
    failed_steps: list[dict[str, Any]],
) -> tuple[list[dict[str, str]], DiagnosisUpstreamScope]:
    """Read failed-output membership and provenance from the same captured view."""
    from artisan.storage.core.provenance_store import ProvenanceStore

    execution_ids = {step["execution_run_id"] for step in failed_steps}
    if not execution_ids:
        return [], DiagnosisUpstreamScope()
    output_ids = (
        view.scan(TablePath.EXECUTION_EDGES)
        .filter(
            pl.col("execution_run_id").is_in(execution_ids)
            & (pl.col("direction") == "output")
        )
        .select("artifact_id")
        .collect()["artifact_id"]
        .to_list()
    )
    adjacency = ProvenanceStore(
        committer.delta_root,
        fs=committer.fs,
        storage_options=committer.storage_options,
        files_root=committer.files_root,
        _view=view,
    ).load_backward_map()
    return _upstream_from_seeds(output_ids, adjacency)


def _upstream_from_seeds(
    output_ids: list[str],
    adjacency: dict[str, list[str]],
) -> tuple[list[dict[str, str]], DiagnosisUpstreamScope]:
    """Walk ten deterministic seeds and preserve selection/depth coverage."""
    from artisan.provenance.reader import _walk_edges

    all_ids = sorted(set(output_ids))
    scope = DiagnosisUpstreamScope(
        seed_count=min(10, len(all_ids)),
        total_seed_count=len(all_ids),
        seed_selection_truncated=len(all_ids) > 10,
    )
    pairs: set[tuple[str, str]] = set()
    for artifact_id in all_ids[: scope.seed_limit]:
        walk = _walk_edges(
            adjacency, artifact_id, direction="backward", depth=scope.depth
        )
        scope.traversal_truncated = bool(scope.traversal_truncated or walk.truncated)
        pairs.update(
            (edge["source_artifact_id"], edge["target_artifact_id"])
            for edge in walk.edges
        )
    edges = [
        {"source_artifact_id": source, "target_artifact_id": target}
        for source, target in sorted(pairs)
    ]
    return edges, scope


def inspect_step(
    delta_root: str,
    step_number: int,
    *,
    pipeline_run_id: str | None = None,
    step_run_id: str | None = None,
    storage_options: dict[str, str] | None = None,
    fs: AbstractFileSystem | None = None,
    files_root: str | None = None,
) -> pl.DataFrame:
    """One-row-per-artifact summary for a given step.

    Args:
        delta_root: Path to Delta Lake root.
        step_number: Origin step by default, or current logical step when a
            run is selected.
        pipeline_run_id: Exact run whose accepted outputs to inspect. None
            selects artifacts by origin step across all runs.
        step_run_id: Exact attempt, including history. Must match the required
            step_number and any supplied pipeline_run_id.
        storage_options: Delta-rs storage options for cloud backends.
        fs: Filesystem for existence checks. Local if None.
        files_root: Managed carrier root; required for nonlocal/custom storage.

    Returns:
        DataFrame with columns: name, artifact_type, step, details.

    Raises:
        ValueError: An explicit attempt is unknown or conflicts with run/step.
    """
    empty = pl.DataFrame(
        schema={
            "name": pl.String,
            "artifact_type": pl.String,
            "step": pl.Int32,
            "details": pl.String,
        }
    )

    committer = open_configured_store(
        delta_root, files_root=files_root, fs=fs, storage_options=storage_options
    )
    all_rows: list[dict[str, Any]] = []
    with read_view(committer) as (snapshot, view):
        membership = artifact_membership(
            snapshot,
            view,
            pipeline_run_id=pipeline_run_id,
            step_run_id=step_run_id,
            step_number=step_number,
        )
        for kind in sorted(set(membership["artifact_type"])):
            ids = set(membership.filter(pl.col("artifact_type") == kind)["artifact_id"])
            rows = registered_rows(snapshot, ArtifactTypeDef.get_table_path(kind), ids)
            for row in rows.iter_rows(named=True):
                all_rows.append(
                    {
                        "name": row.get("original_name") or row["artifact_id"][:16],
                        "artifact_type": kind,
                        "step": step_number,
                        "details": _build_details(kind, row),
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
    step_run_id: str | None = None,
    round_digits: int = 3,
    storage_options: dict[str, str] | None = None,
    fs: AbstractFileSystem | None = None,
    files_root: str | None = None,
) -> pl.DataFrame:
    """Parse metric artifacts into a human-readable table.

    Args:
        delta_root: Path to Delta Lake root.
        step_number: Origin step by default, or current logical step when a
            run is selected. None includes all steps in the selected scope.
        pipeline_run_id: Exact run whose accepted metric outputs to inspect.
            None includes metrics across all runs at their origin steps.
        step_run_id: Exact attempt, including history. Must match any supplied
            run or step; omitted positions are inferred from this attempt.
        round_digits: Decimal places for float rounding.
        storage_options: Delta-rs storage options for cloud backends.
        fs: Filesystem for existence checks. Local if None.
        files_root: Managed carrier root; required for nonlocal/custom storage.

    Returns:
        DataFrame with columns: name, step, {metric_key_1}, {metric_key_2}, ...

    Raises:
        FileNotFoundError: If metrics table doesn't exist.
        ValueError: An explicit attempt is unknown or conflicts with run/step.
    """
    committer = open_configured_store(
        delta_root, files_root=files_root, fs=fs, storage_options=storage_options
    )
    with read_view(committer) as (snapshot, view):
        membership = artifact_membership(
            snapshot,
            view,
            pipeline_run_id=pipeline_run_id,
            step_run_id=step_run_id,
            step_number=step_number,
            artifact_type="metric",
        )
        rows = registered_rows(
            snapshot,
            ArtifactTypeDef.get_table_path("metric"),
            set(membership["artifact_id"]),
        )
        df = membership.select("artifact_id", "current_step_number").join(
            rows, on="artifact_id"
        )

    if df.is_empty():
        return pl.DataFrame(schema={"name": pl.String, "step": pl.Int32})

    parsed_rows: list[dict[str, Any]] = []

    for row in df.iter_rows(named=True):
        name = row.get("original_name") or row["artifact_id"][:16]
        current_step = row.get("current_step_number")
        display_step = (
            current_step if current_step is not None else row["origin_step_number"]
        )
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

    float_cols = [
        c for c in result.columns if result[c].dtype in (pl.Float64, pl.Float32)
    ]
    if float_cols:
        result = result.with_columns(
            [pl.col(c).round(round_digits) for c in float_cols]
        )

    return result.sort("step", "name")


def inspect_csv(
    delta_root: str,
    name: str | None = None,
    step_number: int | None = None,
    *,
    pipeline_run_id: str | None = None,
    step_run_id: str | None = None,
    storage_options: dict[str, str] | None = None,
    fs: AbstractFileSystem | None = None,
    files_root: str | None = None,
) -> pl.DataFrame:
    """Read CSVArtifact CSV content as a Polars DataFrame.

    Args:
        delta_root: Path to Delta Lake root.
        name: Filter by original_name. Takes the first match.
        step_number: Origin step by default, or current logical step when a
            run is selected. None includes all steps in the selected scope.
        pipeline_run_id: Exact run whose accepted CSV outputs may match.
            None selects artifacts across all runs by origin step.
        step_run_id: Exact attempt, including history. Must match any supplied
            run or step; omitted positions are inferred from this attempt.
        storage_options: Delta-rs storage options for cloud backends.
        fs: Filesystem for existence checks. Local if None.
        files_root: Managed carrier root; required for nonlocal/custom storage.

    Returns:
        DataFrame with the actual CSV data content.

    Raises:
        FileNotFoundError: If CSV table doesn't exist.
        ValueError: If no matching artifacts found, content is None, or an
            explicit attempt is unknown or conflicts with run/step.
    """
    committer = open_configured_store(
        delta_root, files_root=files_root, fs=fs, storage_options=storage_options
    )
    with read_view(committer) as (snapshot, view):
        membership = artifact_membership(
            snapshot,
            view,
            pipeline_run_id=pipeline_run_id,
            step_run_id=step_run_id,
            step_number=step_number,
            artifact_type="csv",
        )
        selected = view.scan(ArtifactTypeDef.get_table_path("csv")).filter(
            pl.col("artifact_id").is_in(membership["artifact_id"].to_list())
        )
        if name is not None:
            selected = selected.filter(pl.col("original_name") == name)
        ids = set(selected.select("artifact_id").collect()["artifact_id"])
        df = registered_rows(snapshot, ArtifactTypeDef.get_table_path("csv"), ids)
    if df.is_empty():
        msg = "No matching CSV artifacts found"
        raise ValueError(msg)

    if name is not None:
        content = df["content"][0]
        if content is None:
            msg = f"Artifact '{name}' has no content (not hydrated)"
            raise ValueError(msg)
        return pl.read_csv(io.BytesIO(content))

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


def _format_size(size: int) -> str:
    """Format a byte count as a human-readable string."""
    if size >= 1_000_000:
        return f"{size / 1_000_000:.1f} MB"
    if size >= 1_000:
        return f"{size / 1_000:.1f} KB"
    return f"{size} B"


def _build_details(artifact_type: str, row: dict[str, Any]) -> str:
    """Return a type-specific summary string for an artifact row."""
    if artifact_type == "csv":
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

    elif artifact_type == "file_ref":
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
