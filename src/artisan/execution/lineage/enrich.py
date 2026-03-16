"""Enrich source-target lineage pairs into fully typed provenance edges.

Provides three builders: from an in-memory artifact dict, from the
artifact store, and for config-reference edges.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.provenance import ArtifactProvenanceEdge
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.provenance.source_target_pair import SourceTargetPair

if TYPE_CHECKING:
    from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
    from artisan.storage.core.artifact_store import ArtifactStore

UNKNOWN_ARTIFACT_TYPE = "UNKNOWN"
logger = logging.getLogger(__name__)


def _artifact_type_or_unknown(artifact: Artifact | None) -> str:
    """Return artifact type string or UNKNOWN when missing."""
    if artifact is None:
        return UNKNOWN_ARTIFACT_TYPE
    return artifact.artifact_type


def build_artifact_edges_from_store(
    source_target_pairs: list[SourceTargetPair],
    execution_run_id: str,
    artifact_store: ArtifactStore,
) -> list[ArtifactProvenanceEdge]:
    """Build typed provenance edges by resolving types from the artifact store.

    Uses a single bulk ``load_artifact_type_map`` call instead of per-pair
    lookups, reducing delta scans from 2N to 1.

    Args:
        source_target_pairs: Untyped source/target pairs from lineage capture.
        execution_run_id: ID of the current execution run.
        artifact_store: Store used to resolve artifact type strings.

    Returns:
        Fully typed provenance edges ready for staging.
    """
    if not source_target_pairs:
        return []

    # Bulk-resolve all artifact types (1 scan instead of 2N)
    all_ids: set[str] = set()
    for pair in source_target_pairs:
        all_ids.add(pair.source)
        all_ids.add(pair.target)
    type_map = artifact_store.provenance.load_type_map(list(all_ids))

    artifact_edges: list[ArtifactProvenanceEdge] = []
    for pair in source_target_pairs:
        source_type = type_map.get(pair.source, UNKNOWN_ARTIFACT_TYPE)
        target_type = type_map.get(pair.target, UNKNOWN_ARTIFACT_TYPE)

        artifact_edges.append(
            ArtifactProvenanceEdge(
                execution_run_id=execution_run_id,
                source_artifact_id=pair.source,
                target_artifact_id=pair.target,
                source_artifact_type=source_type,
                target_artifact_type=target_type,
                source_role=pair.source_role,
                target_role=pair.target_role,
                group_id=pair.group_id,
            )
        )

    return artifact_edges


def build_artifact_edges_from_dict(
    source_target_pairs: list[SourceTargetPair],
    execution_run_id: str,
    built_artifacts: dict[str, Artifact],
) -> list[ArtifactProvenanceEdge]:
    """Build typed provenance edges from an in-memory artifact dict.

    Args:
        source_target_pairs: Untyped source/target pairs from lineage capture.
        execution_run_id: ID of the current execution run.
        built_artifacts: Mapping of artifact ID to Artifact for type lookup.

    Returns:
        Fully typed provenance edges ready for staging.
    """
    artifact_edges: list[ArtifactProvenanceEdge] = []

    for pair in source_target_pairs:
        source_type = _artifact_type_or_unknown(built_artifacts.get(pair.source))
        target_type = _artifact_type_or_unknown(built_artifacts.get(pair.target))

        artifact_edges.append(
            ArtifactProvenanceEdge(
                execution_run_id=execution_run_id,
                source_artifact_id=pair.source,
                target_artifact_id=pair.target,
                source_artifact_type=source_type,
                target_artifact_type=target_type,
                source_role=pair.source_role,
                target_role=pair.target_role,
                group_id=pair.group_id,
            )
        )

    return artifact_edges


def build_config_reference_edges(
    config_artifacts: list[ExecutionConfigArtifact],
    artifact_store: ArtifactStore,
    execution_run_id: str,
) -> list[ArtifactProvenanceEdge]:
    """Build provenance edges from referenced artifacts to config artifacts.

    Uses a single bulk ``load_artifact_type_map`` call for all references.

    Args:
        config_artifacts: ExecutionConfigArtifact instances that reference
            other artifacts.
        artifact_store: Store used to resolve referenced artifact types.
        execution_run_id: ID of the current execution run.

    Returns:
        Provenance edges pointing from each referenced artifact to its
        config artifact.
    """
    from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact

    # Collect all reference IDs across all configs
    all_ref_ids: set[str] = set()
    config_refs: list[tuple[ExecutionConfigArtifact, list[str]]] = []
    for config in config_artifacts:
        if not isinstance(config, ExecutionConfigArtifact):
            continue
        refs = list(config.get_artifact_references())
        config_refs.append((config, refs))
        all_ref_ids.update(refs)

    # Bulk-resolve types (1 scan instead of N)
    type_map = (
        artifact_store.provenance.load_type_map(list(all_ref_ids))
        if all_ref_ids
        else {}
    )

    edges: list[ArtifactProvenanceEdge] = []
    for config, refs in config_refs:
        for ref_id in refs:
            source_type = type_map.get(ref_id, UNKNOWN_ARTIFACT_TYPE)
            edges.append(
                ArtifactProvenanceEdge(
                    execution_run_id=execution_run_id,
                    source_artifact_id=ref_id,
                    target_artifact_id=config.artifact_id,
                    source_artifact_type=source_type,
                    target_artifact_type=ArtifactTypes.CONFIG,
                    source_role="referenced",
                    target_role="config",
                )
            )

    return edges
