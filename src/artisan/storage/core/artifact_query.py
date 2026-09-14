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

from artisan.storage.core.store_format import assert_store_format

if TYPE_CHECKING:
    from artisan.schemas.execution.storage_config import StorageConfig


class ArtifactRef(BaseModel):
    """A reference to one artifact — never its payload.

    Attributes:
        artifact_id: The artifact's identifier.
        artifact_type: Registered artifact type (data, metric, file_ref, …).
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

    Returns:
        Artifact references ordered by (step number, artifact id).

    Raises:
        FileNotFoundError: If the artifact index table does not exist.
    """
    import polars as pl

    from artisan.schemas.enums import TablePath
    from artisan.schemas.execution.storage_config import StorageConfig
    from artisan.utils.path import uri_join

    storage = storage or StorageConfig()
    opts = storage.delta_storage_options()
    fs = storage.filesystem()
    assert_store_format(delta_root, fs, opts)

    index_path = uri_join(delta_root, TablePath.ARTIFACT_INDEX)
    if not fs.exists(index_path):
        msg = f"Artifact index table not found at {index_path}"
        raise FileNotFoundError(msg)

    if pipeline_run_id is not None:
        from artisan.storage.core.run_scope import load_accepted_outputs

        membership = load_accepted_outputs(
            delta_root,
            fs=fs,
            storage_options=opts,
            pipeline_run_id=pipeline_run_id,
        ).select("artifact_id", "current_step_number")
        if artifact_type is not None:
            membership = membership.join(
                pl.scan_delta(index_path, storage_options=opts)
                .filter(pl.col("artifact_type") == artifact_type)
                .select("artifact_id")
                .collect(),
                on="artifact_id",
                how="inner",
            )
        rows = (
            membership.unique(
                subset=["current_step_number", "artifact_id"], maintain_order=True
            )
            .join(
                pl.scan_delta(index_path, storage_options=opts).collect(),
                on="artifact_id",
                how="inner",
            )
            .sort("current_step_number", "artifact_id")
        )
    else:
        scanner = pl.scan_delta(index_path, storage_options=opts)
        if artifact_type is not None:
            scanner = scanner.filter(pl.col("artifact_type") == artifact_type)
        rows = scanner.sort("origin_step_number", "artifact_id").collect()
        rows = rows.with_columns(
            pl.lit(None, dtype=pl.Int32).alias("current_step_number")
        )

    return [
        ArtifactRef(
            artifact_id=row["artifact_id"],
            artifact_type=row["artifact_type"],
            origin_step_number=row["origin_step_number"],
            current_step_number=row["current_step_number"],
            pipeline_run_id=pipeline_run_id,
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
