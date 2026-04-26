"""List and inspect persisted pipeline runs.

Module-level entry point that reads the persisted ``steps`` Delta table
and aggregates one row per pipeline run. Replaces the historical
``PipelineManager.list_runs`` classmethod, which never referenced
``cls`` and hard-coded ``StorageConfig()``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from artisan.orchestration.engine.step_tracker import StepTracker

if TYPE_CHECKING:
    import polars as pl

    from artisan.schemas.execution.storage_config import StorageConfig


def list_runs(
    delta_root: str,
    *,
    storage: StorageConfig | None = None,
) -> pl.DataFrame:
    """List all pipeline runs recorded in the Delta root.

    Args:
        delta_root: Root path for Delta Lake tables.
        storage: Storage configuration for cloud backends. Defaults to
            local filesystem.

    Returns:
        DataFrame with columns ``pipeline_run_id``, ``step_count``,
        ``last_status``, ``started_at``, ``ended_at`` — one row per run.
    """
    from artisan.schemas.execution.storage_config import StorageConfig

    storage = storage or StorageConfig()
    tracker = StepTracker(
        delta_root,
        storage_options=storage.delta_storage_options(),
        fs=storage.filesystem(),
    )
    return tracker.list_runs()
