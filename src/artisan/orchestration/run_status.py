"""Composite run status — per-step current state plus the run rollup.

Composes ``inspect_pipeline`` (per-step current status from the steps
table) with ``run_history.list_runs`` (the run rollup). Pending and running
attempts remain visible to polling readers. The core reader and MCP
``artisan_get_run_status`` tool share these shapes as one contract.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING

from pydantic import BaseModel

from artisan.schemas.orchestration.step_lifecycle import StepStatus

if TYPE_CHECKING:
    from collections.abc import Mapping

    from fsspec import AbstractFileSystem

    from artisan.schemas.execution.storage_config import StorageConfig
    from artisan.schemas.orchestration.step_state import StepState


class RunStepStatus(BaseModel):
    """Current state of one logical pipeline step.

    Fields mirror ``inspect_pipeline`` columns. ``status`` is the shared
    lifecycle enum;
    ``produced`` and ``duration`` are the human-readable summaries that
    reader emits.

    Attributes:
        step_number: The step's number within the run.
        name: The step name.
        status: Authoritative lifecycle status.
        produced: Human summary of what the step produced.
        duration: Human-readable duration (e.g. ``"1.2s"``).
    """

    step_number: int
    name: str
    status: StepStatus
    produced: str
    duration: str


class RunStatus(BaseModel):
    """Run rollup plus each step's current lifecycle status.

    Attributes:
        pipeline_run_id: The run this status describes.
        last_status: Status of the latest authoritative attempt, or None when
            the run is not found.
        step_count: Distinct steps recorded for the run.
        started_at: ISO timestamp of the first pending snapshot, or None.
        ended_at: ISO timestamp of the last terminal snapshot, or None while
            the run is active.
        steps: Per-step current statuses, ordered by step number.
    """

    pipeline_run_id: str
    last_status: StepStatus | None
    step_count: int
    started_at: str | None
    ended_at: str | None
    steps: list[RunStepStatus]


def _step_status(state: StepState, counts: Mapping[str, int]) -> RunStepStatus:
    """Format authoritative lifecycle and unique-content counts for inspection."""
    produced, duration = "-", "-"
    if state.status in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}:
        if "Filter" in (state.operation_class or "") or "filter" in state.step_name:
            produced = f"{state.succeeded_count or 0} passed"
        elif counts:
            produced = ", ".join(
                f"{count} {kind}" for kind, count in sorted(counts.items())
            )
        if state.duration_seconds is not None:
            duration = f"{state.duration_seconds:.1f}s"
    return RunStepStatus(
        step_number=state.step_number,
        name=state.step_name,
        status=state.status,
        produced=produced,
        duration=duration,
    )


def run_status(
    delta_root: str,
    pipeline_run_id: str,
    *,
    files_root: str | None = None,
    storage: StorageConfig | None = None,
) -> RunStatus:
    """Assemble the current status of one pipeline run.

    Args:
        delta_root: Root path for Delta Lake tables.
        pipeline_run_id: The run to describe.
        files_root: Managed carrier root. Required for nonlocal/custom storage.
        storage: Storage configuration for cloud backends. Defaults to
            local filesystem.

    Returns:
        A ``RunStatus``; an unknown run yields empty steps and null rollup
        fields (the steps table itself must exist).

    Raises:
        StoreIntegrityError: The store or selected lifecycle evidence is incomplete.
    """
    from artisan.orchestration.engine.retained_run_status import retained_run_status
    from artisan.storage.io.retained_store import open_configured_store

    return retained_run_status(
        open_configured_store(delta_root, files_root=files_root, storage=storage),
        pipeline_run_id,
    )


def resolve_step_number(
    delta_root: str,
    pipeline_run_id: str,
    step_name: str,
    *,
    files_root: str | None = None,
    storage_options: dict[str, str] | None = None,
    fs: AbstractFileSystem | None = None,
) -> int | None:
    """Resolve a step name to its number within a run via the steps table.

    Args:
        delta_root: Root path for Delta Lake tables.
        pipeline_run_id: The run the step belongs to.
        step_name: The step name to resolve.
        files_root: Managed carrier root. Required for nonlocal/custom storage.
        storage_options: Delta-rs storage options for cloud backends.
        fs: Filesystem for existence checks. Local if None.

    Returns:
        The step number, or None when no such step exists in a supported store.

    Raises:
        IncompatibleStoreError: If the store manifest or schema is unsupported.
    """
    from artisan.orchestration.engine.retained_run_status import (
        resolve_retained_step_number,
    )
    from artisan.storage.io.retained_store import open_configured_store

    return resolve_retained_step_number(
        open_configured_store(
            delta_root, files_root=files_root, storage_options=storage_options, fs=fs
        ),
        pipeline_run_id,
        step_name,
    )


def _iso(value: date | datetime | None) -> str | None:
    """Return a timestamp's ISO string, or None."""
    return value.isoformat() if value is not None else None
