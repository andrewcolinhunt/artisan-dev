"""By-type / by-step artifact reference scan over ``artifact_index``.

Returns references and index metadata only — never artifact payloads. The
``artifact_index`` table carries ``artifact_id``, ``artifact_type``,
``origin_step_number`` and a JSON ``metadata`` blob; it has no
``pipeline_run_id`` column, so run filtering resolves the run's step
numbers from the steps table and matches ``origin_step_number`` (see
``query_artifacts``). Shape lives beside the reader (the ``ProvenanceEdges``
precedent).
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem

    from artisan.schemas.execution.storage_config import StorageConfig


class ArtifactRef(BaseModel):
    """A reference to one artifact — never its payload.

    Attributes:
        artifact_id: The artifact's identifier.
        artifact_type: Registered artifact type (data, metric, file_ref, …).
        origin_step_number: The step number that produced the artifact.
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

    Filters AND together. ``pipeline_run_id`` resolves the run's step
    numbers from the steps table and matches ``origin_step_number``; because
    the index is not run-scoped, a store holding multiple runs that reuse
    step numbers can over-return (acceptable at single-run scale).

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

    index_path = uri_join(delta_root, TablePath.ARTIFACT_INDEX)
    if not fs.exists(index_path):
        msg = f"Artifact index table not found at {index_path}"
        raise FileNotFoundError(msg)

    scanner = pl.scan_delta(index_path, storage_options=opts)
    if artifact_type is not None:
        scanner = scanner.filter(pl.col("artifact_type") == artifact_type)
    if pipeline_run_id is not None:
        step_numbers = _run_step_numbers(delta_root, pipeline_run_id, opts, fs)
        scanner = scanner.filter(pl.col("origin_step_number").is_in(step_numbers))

    rows = scanner.sort("origin_step_number", "artifact_id").collect()
    return [
        ArtifactRef(
            artifact_id=row["artifact_id"],
            artifact_type=row["artifact_type"],
            origin_step_number=row["origin_step_number"],
            pipeline_run_id=pipeline_run_id,
            metadata=_parse_metadata(row["metadata"]),
        )
        for row in rows.iter_rows(named=True)
    ]


def _run_step_numbers(
    delta_root: str,
    pipeline_run_id: str,
    storage_options: dict[str, str] | None,
    fs: AbstractFileSystem,
) -> list[int]:
    """Return the distinct step numbers recorded for one run.

    Empty when the steps table is absent — an unknown run matches nothing.
    """
    import polars as pl

    from artisan.schemas.enums import TablePath
    from artisan.utils.path import uri_join

    steps_path = uri_join(delta_root, TablePath.STEPS)
    if not fs.exists(steps_path):
        return []
    return (
        pl.scan_delta(steps_path, storage_options=storage_options)
        .filter(pl.col("pipeline_run_id") == pipeline_run_id)
        .select("step_number")
        .unique()
        .collect()["step_number"]
        .to_list()
    )


def _parse_metadata(raw: str | None) -> dict[str, Any]:
    """Parse the index ``metadata`` JSON column into a dict."""
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}
