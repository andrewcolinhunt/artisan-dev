"""List and inspect persisted pipeline runs.

Read one verified lifecycle snapshot and aggregate one row per pipeline run.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

    from artisan.schemas.execution.storage_config import StorageConfig


def list_runs(
    delta_root: str,
    *,
    files_root: str | None = None,
    storage: StorageConfig | None = None,
) -> pl.DataFrame:
    """List all pipeline runs recorded in the Delta root.

    Args:
        delta_root: Root path for Delta Lake tables.
        files_root: Managed carrier root. Local stores default to sibling files;
            nonlocal stores require an explicit root.
        storage: Storage configuration for cloud backends. Defaults to
            local filesystem.

    Returns:
        DataFrame with columns ``pipeline_run_id``, ``step_count``,
        ``last_status``, ``started_at``, ``ended_at`` — one row per run.
    """
    from artisan.orchestration.engine.retained_run_status import list_retained_runs
    from artisan.storage.io.retained_store import open_configured_store

    return list_retained_runs(
        open_configured_store(delta_root, files_root=files_root, storage=storage)
    )
