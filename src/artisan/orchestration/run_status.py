"""Composite run status — per-step terminal state plus the run rollup.

Composes ``inspect_pipeline`` (per-step terminal status from the steps
table) with ``run_history.list_runs`` (the run rollup). "Running" is never
persisted, so only terminal statuses surface — the honest read a polling,
read-only server can serve. Shapes live beside this reader (the
``ProvenanceEdges`` precedent) so the MCP ``artisan_get_run_status`` tool
and any future CLI command share one contract.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem

    from artisan.schemas.execution.storage_config import StorageConfig


class StepStatus(BaseModel):
    """Terminal state of one pipeline step.

    Fields mirror ``inspect_pipeline`` columns. ``status`` is one of
    ``ok`` / ``partial`` / ``failed`` / ``skipped`` / ``cancelled``;
    ``produced`` and ``duration`` are the human-readable summaries that
    reader emits.

    Attributes:
        step_number: The step's number within the run.
        name: The step name.
        status: Terminal status (ok/partial/failed/skipped/cancelled).
            ``partial`` means some units failed under CONTINUE while others
            succeeded; ``failed`` means every unit failed.
        produced: Human summary of what the step produced.
        duration: Human-readable duration (e.g. ``"1.2s"``).
    """

    step_number: int
    name: str
    status: str
    produced: str
    duration: str


class RunStatus(BaseModel):
    """Run rollup plus its per-step terminal statuses.

    Attributes:
        pipeline_run_id: The run this status describes.
        last_status: Status of the most recent step event, or None when the
            run is not found.
        step_count: Distinct steps recorded for the run.
        started_at: ISO timestamp of the first step event, or None.
        ended_at: ISO timestamp of the last step event, or None.
        steps: Per-step terminal statuses, ordered by step number.
    """

    pipeline_run_id: str
    last_status: str | None
    step_count: int
    started_at: str | None
    ended_at: str | None
    steps: list[StepStatus]


def run_status(
    delta_root: str,
    pipeline_run_id: str,
    *,
    storage: StorageConfig | None = None,
) -> RunStatus:
    """Assemble the terminal status of one pipeline run.

    Args:
        delta_root: Root path for Delta Lake tables.
        pipeline_run_id: The run to describe.
        storage: Storage configuration for cloud backends. Defaults to
            local filesystem.

    Returns:
        A ``RunStatus``; an unknown run yields empty steps and null rollup
        fields (the steps table itself must exist).

    Raises:
        FileNotFoundError: If the steps table does not exist.
    """
    import polars as pl

    from artisan.orchestration.run_history import list_runs
    from artisan.schemas.execution.storage_config import StorageConfig
    from artisan.visualization.inspect import inspect_pipeline

    storage = storage or StorageConfig()
    steps_df = inspect_pipeline(
        delta_root,
        pipeline_run_id=pipeline_run_id,
        storage_options=storage.delta_storage_options(),
        fs=storage.filesystem(),
    )
    steps = [
        StepStatus(
            step_number=row["step"],
            name=row["operation"],
            status=row["status"],
            produced=row["produced"],
            duration=row["duration"],
        )
        for row in steps_df.iter_rows(named=True)
    ]

    rollup = list_runs(delta_root, storage=storage).filter(
        pl.col("pipeline_run_id") == pipeline_run_id
    )
    if rollup.is_empty():
        return RunStatus(
            pipeline_run_id=pipeline_run_id,
            last_status=None,
            step_count=0,
            started_at=None,
            ended_at=None,
            steps=steps,
        )
    row = rollup.row(0, named=True)
    return RunStatus(
        pipeline_run_id=pipeline_run_id,
        last_status=row["last_status"],
        step_count=int(row["step_count"]),
        started_at=_iso(row["started_at"]),
        ended_at=_iso(row["ended_at"]),
        steps=steps,
    )


def resolve_step_number(
    delta_root: str,
    pipeline_run_id: str,
    step_name: str,
    *,
    storage_options: dict[str, str] | None = None,
    fs: AbstractFileSystem | None = None,
) -> int | None:
    """Resolve a step name to its number within a run via the steps table.

    Args:
        delta_root: Root path for Delta Lake tables.
        pipeline_run_id: The run the step belongs to.
        step_name: The step name to resolve.
        storage_options: Delta-rs storage options for cloud backends.
        fs: Filesystem for existence checks. Local if None.

    Returns:
        The step number, or None when no such step exists in the run.

    Raises:
        FileNotFoundError: If the steps table does not exist.
    """
    import polars as pl

    from artisan.schemas.enums import TablePath
    from artisan.utils.path import uri_join

    if fs is None:
        from fsspec.implementations.local import LocalFileSystem

        fs = LocalFileSystem()
    steps_path = uri_join(delta_root, TablePath.STEPS)
    if not fs.exists(steps_path):
        msg = f"Steps table not found at {steps_path}"
        raise FileNotFoundError(msg)

    matches = (
        pl.scan_delta(steps_path, storage_options=storage_options)
        .filter(pl.col("pipeline_run_id") == pipeline_run_id)
        .filter(pl.col("step_name") == step_name)
        .select("step_number")
        .collect()
    )
    if matches.is_empty():
        return None
    return int(matches["step_number"][0])


def _iso(value: object) -> str | None:
    """Return a timestamp's ISO string, or None."""
    return value.isoformat() if value is not None else None
