"""Validation helpers for execution artifacts and lineage."""

from __future__ import annotations

from artisan.execution.exceptions import (
    ArtifactValidationError,
    LineageCompletenessError,
    LineageIntegrityError,
)
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.provenance.lineage_mapping import LineageMapping
from artisan.schemas.specs.output_spec import OutputSpec


def validate_artifacts_match_specs(
    artifacts: dict[str, list[Artifact]],
    output_specs: dict[str, OutputSpec],
) -> None:
    """Verify artifacts satisfy output specs (presence, types, no extras).

    Raises:
        ArtifactValidationError: On missing roles, empty required lists,
            type mismatches, or undeclared output roles.
    """
    for role, spec in output_specs.items():
        if spec.required and role not in artifacts:
            msg = f"Missing required output role: {role}"
            raise ArtifactValidationError(msg)
        if role not in artifacts:
            continue
        if not artifacts[role] and spec.required:
            msg = f"Empty artifact list for required role: {role}"
            raise ArtifactValidationError(msg)
        for artifact in artifacts[role]:
            if not ArtifactTypes.matches(spec.artifact_type, artifact.artifact_type):
                msg = (
                    f"Artifact type mismatch for role '{role}': "
                    f"expected {spec.artifact_type!r}, got {artifact.artifact_type!r}"
                )
                raise ArtifactValidationError(msg)

    if output_specs:
        extra_roles = set(artifacts.keys()) - set(output_specs.keys())
        if extra_roles:
            msg = f"Unexpected output roles: {extra_roles}"
            raise ArtifactValidationError(msg)


def validate_lineage_completeness(
    artifacts: dict[str, list[Artifact]],
    output_specs: dict[str, OutputSpec],
    lineage: dict[str, list[LineageMapping]],
) -> None:
    """Verify every non-orphan output artifact has a lineage mapping.

    Raises:
        LineageCompletenessError: If any artifact lacks a lineage entry.
    """
    for role, artifact_list in artifacts.items():
        spec = output_specs.get(role)
        if spec is None:
            continue
        lineage_config = spec.infer_lineage_from
        if lineage_config is not None and lineage_config.get("inputs") == []:
            continue
        if not artifact_list:
            continue

        mapped_names = {
            mapping.draft_original_name for mapping in lineage.get(role, [])
        }
        for artifact in artifact_list:
            if artifact.original_name not in mapped_names:
                msg = (
                    f"Artifact '{artifact.original_name}' in role '{role}' "
                    f"has no lineage mapping"
                )
                raise LineageCompletenessError(msg)


def validate_lineage_integrity(
    lineage: dict[str, list[LineageMapping]],
    input_artifacts: dict[str, list[Artifact]],
    output_artifacts: dict[str, list[Artifact]],
) -> None:
    """Verify lineage references point to real artifacts with no duplicates.

    Raises:
        LineageIntegrityError: On non-existent source/target references
            or duplicate mappings for the same draft.
    """
    input_ids = {
        artifact.artifact_id
        for artifacts in input_artifacts.values()
        for artifact in artifacts
    }
    output_ids = {
        artifact.artifact_id
        for artifacts in output_artifacts.values()
        for artifact in artifacts
        if artifact.artifact_id
    }
    all_source_ids = input_ids | output_ids
    output_names = {
        artifact.original_name
        for artifacts in output_artifacts.values()
        for artifact in artifacts
    }

    for mappings in lineage.values():
        seen_drafts: set[str] = set()
        for mapping in mappings:
            if mapping.source_artifact_id not in all_source_ids:
                msg = f"Lineage references non-existent source: {mapping.source_artifact_id}"
                raise LineageIntegrityError(msg)
            if mapping.draft_original_name not in output_names:
                msg = f"Lineage references non-existent output: {mapping.draft_original_name}"
                raise LineageIntegrityError(msg)
            if mapping.draft_original_name in seen_drafts:
                msg = f"Duplicate lineage mapping for: {mapping.draft_original_name}"
                raise LineageIntegrityError(msg)
            seen_drafts.add(mapping.draft_original_name)
