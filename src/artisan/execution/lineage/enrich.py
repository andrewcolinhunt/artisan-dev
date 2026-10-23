"""Enrich explicitly declared artifact pairs with types and execution metadata."""

from __future__ import annotations

from typing import TYPE_CHECKING

from artisan.execution.exceptions import LineageIntegrityError
from artisan.schemas.artifact.provenance import ArtifactProvenanceEdge
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.provenance.source_target_pair import SourceTargetPair

if TYPE_CHECKING:
    from artisan.storage.core.artifact_store import ArtifactStore


def require_artifact_type(artifact_id: str, artifact_types: dict[str, str]) -> str:
    """Resolve a concrete type or fail before recording an untyped edge."""
    artifact_type = artifact_types.get(artifact_id)
    if not artifact_type or artifact_type in ("UNKNOWN", ArtifactTypes.ANY):
        msg = f"Missing concrete artifact type for {artifact_id!r}"
        raise LineageIntegrityError(msg)
    return artifact_type


def build_artifact_edges_from_store(
    source_target_pairs: list[SourceTargetPair],
    execution_run_id: str,
    artifact_store: ArtifactStore,
) -> list[ArtifactProvenanceEdge]:
    """Resolve all declared endpoint types in one lookup and enrich the pairs.

    Raises:
        LineageIntegrityError: If the store cannot resolve any endpoint type.
    """
    if not source_target_pairs:
        return []
    all_ids = {
        endpoint
        for pair in source_target_pairs
        for endpoint in (pair.source, pair.target)
    }
    types = artifact_store.provenance.load_type_map(sorted(all_ids))
    return build_artifact_edges_from_types(source_target_pairs, execution_run_id, types)


def build_artifact_edges_from_types(
    source_target_pairs: list[SourceTargetPair],
    execution_run_id: str,
    artifact_types: dict[str, str],
) -> list[ArtifactProvenanceEdge]:
    """Enrich declared pairs using an ID-to-type map, preserving roles and groups.

    Args:
        source_target_pairs: Explicit artifact relationships after resolution.
        execution_run_id: Execution recording these declarations.
        artifact_types: Concrete type for each source and target ID.

    Raises:
        LineageIntegrityError: If any declared endpoint lacks a concrete type.
    """
    return [
        ArtifactProvenanceEdge(
            execution_run_id=execution_run_id,
            source_artifact_id=pair.source,
            target_artifact_id=pair.target,
            source_artifact_type=require_artifact_type(pair.source, artifact_types),
            target_artifact_type=require_artifact_type(pair.target, artifact_types),
            source_role=pair.source_role,
            target_role=pair.target_role,
            group_id=pair.group_id,
        )
        for pair in source_target_pairs
    ]
