"""Build provenance edges from captured lineage metadata."""

from __future__ import annotations

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.provenance.lineage_mapping import LineageMapping
from artisan.schemas.provenance.source_target_pair import SourceTargetPair


def build_edges(
    lineage: dict[str, list[LineageMapping]],
    finalized_artifacts: dict[str, list[Artifact]],
) -> list[SourceTargetPair]:
    """Resolve lineage mappings into concrete source-target artifact pairs.

    Mappings carrying ``source_original_name`` are resolved against
    finalized output artifact names within the role named by
    ``source_role``. Mappings carrying ``source_artifact_id`` are used
    directly. The ``LineageMapping`` schema guarantees exactly one of
    those two fields is set.

    Args:
        lineage: Role-keyed lineage mappings from capture or user code.
        finalized_artifacts: Role-keyed finalized output artifacts.

    Returns:
        List of source-target pairs with role and group metadata.

    Raises:
        ValueError: If a ``source_original_name`` cannot be resolved
            against finalized outputs in the declared role.
    """
    # Per-role lookup retains repeated occurrence names in artifact order.
    role_name_to_ids: dict[str, dict[str, list[str]]] = {}
    for role, artifacts in finalized_artifacts.items():
        lookup: dict[str, list[str]] = {}
        for artifact in artifacts:
            original_name = getattr(artifact, "original_name", None)
            if original_name is not None and artifact.artifact_id is not None:
                lookup.setdefault(original_name, []).append(artifact.artifact_id)
        role_name_to_ids[role] = lookup

    edges: list[SourceTargetPair] = []
    target_occurrences: dict[tuple[str, str, str], int] = {}
    for role, mappings in lineage.items():
        for mapping in mappings:
            occurrence_key = (role, mapping.draft_original_name, mapping.source_role)
            occurrence = target_occurrences.get(occurrence_key, 0)
            target_occurrences[occurrence_key] = occurrence + 1
            target_ids = role_name_to_ids.get(role, {}).get(
                mapping.draft_original_name, []
            )
            target_id = target_ids[occurrence] if occurrence < len(target_ids) else None
            source_id: str | None
            if mapping.source_original_name is not None:
                source_ids = role_name_to_ids.get(mapping.source_role, {}).get(
                    mapping.source_original_name, []
                )
                if not source_ids:
                    msg = (
                        f"Source '{mapping.source_original_name}' not found "
                        f"in role '{mapping.source_role}'. "
                        f"source_original_name resolves only against "
                        f"finalized outputs; use source_artifact_id for "
                        f"input sources."
                    )
                    raise ValueError(msg)
                source_id = (
                    source_ids[occurrence]
                    if occurrence < len(source_ids)
                    else source_ids[0]
                )
            else:
                source_id = mapping.source_artifact_id
                if source_id is None:
                    msg = "Lineage mapping has no source reference"
                    raise ValueError(msg)
            if target_id:
                edges.append(
                    SourceTargetPair(
                        source=source_id,
                        target=target_id,
                        source_role=mapping.source_role,
                        target_role=role,
                        group_id=mapping.group_id,
                    )
                )

    return edges
