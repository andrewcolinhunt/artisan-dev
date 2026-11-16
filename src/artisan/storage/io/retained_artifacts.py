"""Artifact membership and semantic closure for a captured retained result."""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

from fsspec import AbstractFileSystem

from artisan.errors import StoreIntegrityError
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.provenance.occurrence import (
    ArtifactOccurrence,
    OccurrenceAddress,
    ReferenceContext,
)
from artisan.schemas.provenance.projection import BoundaryProjection
from artisan.storage.io.bounded_io import CapturedParquet, iter_parquet_frames


def retained_content(
    projection: BoundaryProjection,
    artifacts: Mapping[OccurrenceAddress, Artifact],
) -> dict[str, Artifact]:
    """Select new content without merging producer occurrences or rewriting origins."""
    boundary = projection.boundary_addresses()
    expected = {
        item.address: item
        for item in projection.occurrences
        if item.address not in boundary
    }
    if set(artifacts) != set(expected):
        msg = "Retained artifact inventory must match non-boundary occurrences exactly"
        raise StoreIntegrityError(msg)
    boundary_ids = {
        item.artifact_id for item in projection.occurrences if item.address in boundary
    }
    selected: dict[str, Artifact] = {}
    for address, artifact in artifacts.items():
        ArtifactOccurrence(address, artifact)
        item = expected[address]
        if (artifact.artifact_id, artifact.artifact_type) != (
            item.artifact_id,
            item.artifact_type,
        ):
            msg = "Retained artifact identity or type differs from its occurrence"
            raise StoreIntegrityError(msg)
        row = artifact.to_row()
        ArtifactTypeDef.get(artifact.artifact_type).reference_policy()
        if item.artifact_id in boundary_ids:
            continue
        previous = selected.get(item.artifact_id)
        if previous is not None:
            previous_row = previous.to_row()
            if {
                key: value for key, value in row.items() if key != "origin_step_number"
            } != {
                key: value
                for key, value in previous_row.items()
                if key != "origin_step_number"
            }:
                msg = "Equal artifact identities have conflicting retained content"
                raise StoreIntegrityError(msg)
        else:
            selected[item.artifact_id] = artifact
    return selected


def validate_retained_membership(
    projection: BoundaryProjection,
    staged: Mapping[str, Artifact],
    boundary_artifacts: Mapping[OccurrenceAddress, Artifact],
    *,
    artifact_fs: AbstractFileSystem | None = None,
) -> dict[OccurrenceAddress, Artifact]:
    """Validate trusted input bytes, staged content and exact required references."""
    boundary = projection.boundary_addresses()
    if set(boundary_artifacts) != boundary:
        msg = "Trusted artifact inventory must match the frozen boundary exactly"
        raise StoreIntegrityError(msg)
    expected = {item.address: item for item in projection.occurrences}
    boundary_content: dict[str, Artifact] = {}
    for address, artifact in boundary_artifacts.items():
        ArtifactOccurrence(address, artifact)
        artifact.to_row()
        artifact.verify_external_content(fs=artifact_fs)
        item = expected[address]
        if (artifact.artifact_id, artifact.artifact_type) != (
            item.artifact_id,
            item.artifact_type,
        ):
            msg = "Trusted boundary artifact does not match admitted occurrence"
            raise StoreIntegrityError(msg)
        boundary_content[item.artifact_id] = artifact
    required = {item.artifact_id for item in projection.occurrences} - set(
        boundary_content
    )
    if set(staged) != required:
        msg = "Staged content must match retained content absent from trusted inputs"
        raise StoreIntegrityError(msg)
    content = dict(boundary_content)
    content.update(staged)
    artifacts = {}
    for address, item in expected.items():
        artifact = (
            boundary_artifacts[address]
            if address in boundary
            else content[item.artifact_id]
        )
        ArtifactOccurrence(address, artifact)
        artifact.to_row()
        if (artifact.artifact_id, artifact.artifact_type) != (
            item.artifact_id,
            item.artifact_type,
        ):
            msg = "Captured content does not match retained occurrence identity or type"
            raise StoreIntegrityError(msg)
        artifacts[address] = artifact
    _validate_references(projection, artifacts, boundary_artifacts)
    return artifacts


def _validate_references(
    projection: BoundaryProjection,
    artifacts: Mapping[OccurrenceAddress, Artifact],
    boundary_artifacts: Mapping[OccurrenceAddress, Artifact],
) -> None:
    graph = OccurrenceGraph()
    for group in projection.boundary_inputs:
        graph.declare_input_role(group.role)
        for address in group.occurrences:
            graph.add_input(group.role, artifacts[address], address=address)
    for address, artifact in boundary_artifacts.items():
        graph.add_context_input(address, artifact)
    graph.validate_boundary_context(
        projection.frozen_context, input_attribution=projection.input_attribution
    )
    references: set[tuple[OccurrenceAddress, OccurrenceAddress]] = set()
    retained = {
        address for group in projection.exports for address in group.occurrences
    } | set(projection.support)
    for address in retained:
        if address in boundary_artifacts:
            resolved = graph.required_references(
                address, frozen_context=projection.frozen_context
            )
        else:
            direct = tuple(
                edge.source
                for edge in projection.associations
                if edge.target == address
            )
            artifact = artifacts[address]
            resolved = ArtifactTypeDef.get(
                artifact.artifact_type
            ).resolve_required_references(
                artifact, ReferenceContext(artifacts, direct_parents=direct)
            )
        references.update((address, reference.address) for reference in resolved)
    if references != {(edge.source, edge.target) for edge in projection.references}:
        msg = (
            "Sealed required-reference membership differs from artifact type contracts"
        )
        raise StoreIntegrityError(msg)


def hydrate_artifact_content(
    tables: Mapping[str, CapturedParquet],
    carriers: Mapping[str, str],
    bindings: Mapping[str, str],
    *,
    artifact_fs: AbstractFileSystem | None = None,
) -> dict[str, Artifact]:
    """Hydrate registered artifact rows and verify their exact carrier membership."""
    by_path = {
        definition.table_path: definition
        for definition in ArtifactTypeDef.get_all().values()
    }
    content, external = {}, set()
    for table_path, captured in tables.items():
        definition = by_path.get(table_path)
        if definition is None:
            continue
        for frame in iter_parquet_frames(captured.path):
            for row in frame.iter_rows(named=True):
                artifact = cast(type[Artifact], definition.model).from_row(row)
                artifact_id = cast(str, artifact.artifact_id)
                if artifact_id in content:
                    msg = "Retained artifact rows repeat a content identity"
                    raise StoreIntegrityError(msg)
                if artifact.EXTERNALLY_BACKED:
                    if artifact_id not in bindings:
                        msg = (
                            "Retained external artifact is missing its carrier binding"
                        )
                        raise StoreIntegrityError(msg)
                    setattr(
                        artifact,
                        next(iter(artifact.LOCATOR_FIELDS)),
                        carriers[bindings[artifact_id]],
                    )
                    artifact.verify_external_content(fs=artifact_fs)
                    external.add(artifact_id)
                content[artifact_id] = artifact
    if external != set(bindings):
        msg = "Retained carrier bindings contain unowned or nonexternal artifacts"
        raise StoreIntegrityError(msg)
    return content
