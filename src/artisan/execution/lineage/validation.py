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
            original_name = getattr(artifact, "original_name", None)
            if original_name not in mapped_names:
                msg = (
                    f"Artifact '{original_name}' in role '{role}' "
                    f"has no lineage mapping"
                )
                raise LineageCompletenessError(msg)


def _check_mapping_uniqueness(
    mapping: LineageMapping,
    seen_triples: set[tuple[str, str, str]],
) -> None:
    """Reject duplicate or same-role-conflicting mappings within a role.

    Multiple mappings per draft are allowed when ``source_role`` differs —
    the canonical co-input edge shape that auto-detect already emits (see
    ``capture.py``). Same draft + same ``source_role`` is a modeling
    error whether the source is identical (literal duplicate) or distinct
    (ambiguous parent-in-role); both are rejected with distinct messages.

    Args:
        mapping: The mapping to check.
        seen_triples: Accumulator of ``(draft, source_role, source_identity)``
            triples already accepted in this role. Mutated by appending the
            new triple when accepted.

    Raises:
        LineageIntegrityError: If the mapping duplicates a triple already in
            ``seen_triples`` or conflicts with a same-(draft, source_role)
            entry that points at a different source.
    """
    # _require_one_source_ref on LineageMapping guarantees exactly one of
    # source_artifact_id or source_original_name is set.
    source_identity: str = (
        mapping.source_artifact_id
        if mapping.source_artifact_id is not None
        else mapping.source_original_name
    )
    triple = (
        mapping.draft_original_name,
        mapping.source_role,
        source_identity,
    )
    if triple in seen_triples:
        msg = (
            f"Duplicate lineage mapping for "
            f"draft '{mapping.draft_original_name}': "
            f"source_role={mapping.source_role!r}, "
            f"source={source_identity!r}"
        )
        raise LineageIntegrityError(msg)
    for seen_draft, seen_role, _seen_source in seen_triples:
        if (
            seen_draft == mapping.draft_original_name
            and seen_role == mapping.source_role
        ):
            msg = (
                f"Conflicting lineage mappings for "
                f"draft '{mapping.draft_original_name}': "
                f"source_role={mapping.source_role!r} appears "
                "twice with different sources. A draft may have "
                "multiple parents only across distinct source_roles."
            )
            raise LineageIntegrityError(msg)
    seen_triples.add(triple)


def validate_lineage_integrity(
    lineage: dict[str, list[LineageMapping]],
    input_artifacts: dict[str, list[Artifact]],
    output_artifacts: dict[str, list[Artifact]],
) -> None:
    """Verify lineage references point to real artifacts with no duplicates.

    Raises:
        LineageIntegrityError: On non-existent source/target references,
            literal duplicate mappings, or two mappings for the same draft
            within a single ``source_role`` (split into separate roles
            instead).
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
        getattr(artifact, "original_name", None)
        for artifacts in output_artifacts.values()
        for artifact in artifacts
    }
    output_names_by_role: dict[str, set[str]] = {}
    for role, artifacts in output_artifacts.items():
        output_names_by_role[role] = {
            artifact.original_name
            for artifact in artifacts
            if getattr(artifact, "original_name", None)
        }

    for mappings in lineage.values():
        seen_triples: set[tuple[str, str, str]] = set()
        for mapping in mappings:
            if mapping.source_original_name is not None:
                role_names = output_names_by_role.get(mapping.source_role, set())
                if mapping.source_original_name not in role_names:
                    msg = (
                        f"Lineage references non-existent output source: "
                        f"'{mapping.source_original_name}' "
                        f"in role '{mapping.source_role}'"
                    )
                    raise LineageIntegrityError(msg)
            elif mapping.source_artifact_id not in all_source_ids:
                msg = f"Lineage references non-existent source: {mapping.source_artifact_id}"
                raise LineageIntegrityError(msg)
            if mapping.draft_original_name not in output_names:
                msg = f"Lineage references non-existent output: {mapping.draft_original_name}"
                raise LineageIntegrityError(msg)
            _check_mapping_uniqueness(mapping, seen_triples)
