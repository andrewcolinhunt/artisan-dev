"""By-type / by-step artifact reference scan over ``artifact_index``.

Returns references and index metadata only — never artifact payloads. The
``artifact_index`` table carries ``artifact_id``, ``artifact_type``,
``origin_step_number`` and a JSON ``metadata`` blob; it has no
``pipeline_run_id`` column. Run filtering therefore uses authoritative
current-step output membership rather than global origin step numbers.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

if TYPE_CHECKING:
    from artisan.schemas.execution.storage_config import StorageConfig


class ArtifactRef(BaseModel):
    """A reference to one artifact — never its payload.

    Attributes:
        artifact_id: The artifact's identifier.
        artifact_type: Registered artifact type (csv, metric, file_ref, …).
        origin_step_number: The step number that produced the artifact.
        current_step_number: The artifact's logical step in a run-scoped
            projection. None for an unscoped global query.
        pipeline_run_id: The run this ref was matched under when a run
            filter was applied, else None. The ``artifact_index`` carries no
            run column, so this echoes the query filter rather than a stored
            value.
        metadata: The index row's parsed JSON metadata (lightweight; not the
            artifact content).
    """

    artifact_id: str
    artifact_type: str
    origin_step_number: int
    current_step_number: int | None = None
    pipeline_run_id: str | None = None
    metadata: dict[str, Any] = {}


def query_artifacts(
    delta_root: str,
    *,
    artifact_type: str | None = None,
    pipeline_run_id: str | None = None,
    storage: StorageConfig | None = None,
    files_root: str | None = None,
) -> list[ArtifactRef]:
    """Scan ``artifact_index`` for artifact references, optionally filtered.

    Filters AND together. ``pipeline_run_id`` resolves exact current-step
    output membership through direct and cached executions.

    Args:
        delta_root: Root path for Delta Lake tables.
        artifact_type: When set, restrict to this artifact type.
        pipeline_run_id: When set, restrict to artifacts produced by the
            run's steps.
        storage: Storage configuration for cloud backends. Defaults to
            local filesystem.
        files_root: Managed carrier root. Required for nonlocal/custom storage.

    Returns:
        Artifact references ordered by (step number, artifact id).

    Raises:
        StoreIntegrityError: The store or selected metadata is incomplete.
    """
    from artisan.orchestration.engine.retained_artifact_query import (
        query_retained_artifacts,
    )
    from artisan.schemas.enums import TablePath
    from artisan.storage.core.store_read_scope import RetainedStoreReadScope
    from artisan.storage.io.retained_store import open_configured_store

    committer = open_configured_store(
        delta_root, files_root=files_root, storage=storage
    )
    if pipeline_run_id is not None:
        return query_retained_artifacts(
            committer, pipeline_run_id=pipeline_run_id, artifact_type=artifact_type
        )
    scope = RetainedStoreReadScope(committer)
    with scope.activate():
        assert scope.view is not None
        query = scope.view.scan(TablePath.ARTIFACT_INDEX)
        if artifact_type is not None:
            import polars as pl

            query = query.filter(pl.col("artifact_type") == artifact_type)
        rows = query.sort("origin_step_number", "artifact_id").collect()
    return [
        ArtifactRef(
            artifact_id=row["artifact_id"],
            artifact_type=row["artifact_type"],
            origin_step_number=row["origin_step_number"],
            metadata=_parse_metadata(row["metadata"]),
        )
        for row in rows.iter_rows(named=True)
    ]


def _parse_metadata(raw: str | None) -> dict[str, Any]:
    """Parse the index ``metadata`` JSON column into a dict."""
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}
