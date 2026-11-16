"""Protected original curator relations for portable, read-only query views."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

import polars as pl

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.provenance import ArtifactProvenanceEdge
from artisan.storage.core.table_schemas import (
    ARTIFACT_EDGES_SCHEMA,
    EXECUTION_EDGES_SCHEMA,
)

if TYPE_CHECKING:
    from artisan.storage._retained_publication import (
        InspectedCurator,
        RetainedInspectionManifest,
    )


def curator_execution_edges(
    sources: Sequence[InspectedCurator], *, ordinary: bool = True
) -> pl.DataFrame:
    """Derive the exact set of original input/output relations, never reuse owners."""
    rows: set[tuple[str, str, str, str]] = set()
    for source in sources:
        if ordinary and source.replay_of_execution_run_id is not None:
            continue
        identity = source.execution_run_id
        rows.update(
            (identity, "input", role.role, member.artifact_id)
            for role in source.inputs
            for member in role.members
        )
        inventory = {
            member.occurrence.address: member.occurrence.artifact_id
            for member in source.projection.occurrences
        }
        rows.update(
            (identity, "output", role.role, inventory[address])
            for role in source.projection.outputs
            for address in role.occurrences
        )
    return pl.DataFrame(sorted(rows), schema=EXECUTION_EDGES_SCHEMA, orient="row")


def curator_artifact_edges(
    frame: pl.DataFrame,
    manifest: RetainedInspectionManifest,
    index: pl.DataFrame,
    *,
    ordinary: bool = True,
) -> pl.DataFrame:
    """Validate copied declared relations before excluding diagnostic discovery.

    Roles, parent groups and boundary flags are preserved original assertions.
    Selection source sets and frozen query memberships never become edges.
    """
    if (
        dict(frame.schema) != ARTIFACT_EDGES_SCHEMA
        or frame.unique().height != frame.height
    ):
        msg = "Inspection curator edges have an invalid schema or duplicate rows"
        raise StoreIntegrityError(msg)
    sources = {source.execution_run_id: source for source in manifest.curator_sources}
    types = dict(index.select("artifact_id", "artifact_type").iter_rows())
    pairs: dict[str, set[tuple[str, str]]] = {}
    for row in frame.iter_rows(named=True):
        edge = ArtifactProvenanceEdge.model_validate(row)
        source = sources.get(edge.execution_run_id)
        if source is None or not source.success:
            msg = "Inspection curator edge lacks an original successful execution"
            raise StoreIntegrityError(msg)
        if (
            types.get(edge.source_artifact_id) != edge.source_artifact_type
            or types.get(edge.target_artifact_id) != edge.target_artifact_type
        ):
            msg = "Inspection curator edge differs from protected content types"
            raise StoreIntegrityError(msg)
        if edge.source_artifact_id != edge.target_artifact_id:
            pairs.setdefault(edge.execution_run_id, set()).add(
                (edge.source_artifact_id, edge.target_artifact_id)
            )
    for source in sources.values():
        inventory = {
            member.occurrence.address: member.occurrence.artifact_id
            for member in source.projection.occurrences
        }
        expected = {
            (inventory[edge.source], inventory[edge.target])
            for edge in source.projection.lineage
        }
        if pairs.get(source.execution_run_id, set()) != expected:
            msg = "Inspection curator relations differ from retained lineage"
            raise StoreIntegrityError(msg)
        if any(
            types.get(member.artifact_id) != member.artifact_type
            for role in source.inputs
            for member in role.members
        ):
            msg = "Inspection curator inputs lack their protected content types"
            raise StoreIntegrityError(msg)
    if not ordinary:
        return frame
    diagnostics = [
        source.execution_run_id
        for source in sources.values()
        if source.replay_of_execution_run_id is not None
    ]
    return frame.filter(~pl.col("execution_run_id").is_in(diagnostics))
