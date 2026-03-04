"""Build provenance edges from captured lineage metadata."""

from __future__ import annotations

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.provenance.lineage_mapping import LineageMapping
from artisan.schemas.provenance.source_target_pair import SourceTargetPair
from artisan.schemas.specs.output_spec import OutputSpec


def build_edges(
    lineage: dict[str, list[LineageMapping]],
    finalized_artifacts: dict[str, list[Artifact]],
    input_artifacts: dict[str, list[Artifact]],  # noqa: ARG001
    output_specs: dict[str, OutputSpec],  # noqa: ARG001
) -> list[SourceTargetPair]:
    """Resolve lineage mappings into concrete source-target artifact pairs.

    Draft references (``__draft__`` prefixed IDs) are resolved against
    finalized artifact names within the appropriate role.

    Args:
        lineage: Role-keyed lineage mappings from capture or user code.
        finalized_artifacts: Role-keyed finalized output artifacts.
        input_artifacts: Role-keyed input artifacts (unused; reserved).
        output_specs: Role-keyed output specs (unused; reserved).

    Returns:
        List of source-target pairs with role and group metadata.

    Raises:
        ValueError: If a draft reference cannot be resolved.
    """
    # Per-role lookup: role -> (original_name -> artifact_id).
    # Must be scoped by role because Artifact.draft() strips extensions,
    # so different roles can have artifacts with the same original_name.
    role_name_to_id: dict[str, dict[str, str]] = {}
    for role, artifacts in finalized_artifacts.items():
        lookup: dict[str, str] = {}
        for artifact in artifacts:
            original_name = getattr(artifact, "original_name", None)
            if original_name is not None and artifact.artifact_id is not None:
                lookup[original_name] = artifact.artifact_id
        role_name_to_id[role] = lookup

    edges: list[SourceTargetPair] = []
    for role, mappings in lineage.items():
        for mapping in mappings:
            target_id = role_name_to_id.get(role, {}).get(mapping.draft_original_name)
            source_id = mapping.source_artifact_id
            if source_id.startswith("__draft__"):
                draft_name = source_id[len("__draft__") :]
                if draft_name == "":
                    msg = f"Invalid draft name: {source_id} (no draft prefix)"
                    raise ValueError(msg)
                source_id = role_name_to_id.get(mapping.source_role, {}).get(draft_name)
                if source_id is None:
                    msg = f"Draft {draft_name} not found in role {mapping.source_role}"
                    raise ValueError(msg)
            if target_id:
                edges.append(
                    SourceTargetPair(
                        source=source_id,
                        target=target_id,
                        source_role=mapping.source_role,
                        target_role=role,
                        group_id=getattr(mapping, "group_id", None),
                    )
                )

    return edges
