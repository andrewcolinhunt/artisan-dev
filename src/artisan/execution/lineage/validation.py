"""Validation helpers for execution artifacts and lineage."""

from __future__ import annotations

from artisan.execution.exceptions import (
    ArtifactValidationError,
    LineageCompletenessError,
    LineageIntegrityError,
)
from artisan.execution.inputs._validation import is_hex_id
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.provenance.lineage_mapping import LineageMapping
from artisan.schemas.specs.output_spec import OutputSpec


def validate_artifacts_match_specs(
    artifacts: dict[str, list[Artifact]],
    output_specs: dict[str, OutputSpec],
    *,
    allow_dynamic_outputs: bool = False,
) -> None:
    """Verify artifacts satisfy output specs (presence, types, no extras).

    Only curators may allow dynamic output roles, and only with empty specs.

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

    if output_specs or not allow_dynamic_outputs:
        extra_roles = set(artifacts.keys()) - set(output_specs.keys())
        if extra_roles:
            msg = f"Unexpected output roles: {extra_roles}"
            raise ArtifactValidationError(msg)


def validate_lineage_completeness(
    artifacts: dict[str, list[Artifact]],
    output_specs: dict[str, OutputSpec],
    lineage: dict[str, list[LineageMapping]],
) -> None:
    """Require every derived occurrence to declare all required parent roles.

    Raises:
        LineageCompletenessError: If a role or required parent is missing.
    """
    missing = set(artifacts) - set(lineage)
    if missing:
        msg = f"Missing lineage for output roles: {sorted(missing)}"
        raise LineageCompletenessError(msg)
    for role, artifact_list in artifacts.items():
        spec = output_specs.get(role)
        required_roles = (
            set(next(iter(spec.derives_from.values())))
            if spec and spec.derives_from
            else set()
        )
        roles_by_index: dict[int, set[str]] = {}
        for mapping in lineage[role]:
            roles_by_index.setdefault(mapping.draft_index, set()).add(
                mapping.source_role
            )
        for index in range(len(artifact_list)):
            missing_roles = required_roles - roles_by_index.get(index, set())
            if missing_roles:
                msg = (
                    f"Output {role!r}[{index}] is missing lineage mappings "
                    f"from source roles: {sorted(missing_roles)}"
                )
                raise LineageCompletenessError(msg)


def _lineage_contract(
    target_role: str,
    output_specs: dict[str, OutputSpec],
) -> tuple[str, set[str]]:
    """Return the source namespace and roles allowed for an emitted role."""
    if not output_specs:
        return "inputs", set()
    spec = output_specs.get(target_role)
    if spec is None or spec.derives_from is None:
        msg = f"Artifact output role {target_role!r} must declare derives_from"
        raise LineageIntegrityError(msg)
    kind, roles = next(iter(spec.derives_from.items()))
    return kind, set(roles)


def _validate_mapping_reference(
    mapping: LineageMapping,
    input_ids: dict[str, set[str]],
    output_artifacts: dict[str, list[Artifact]],
    reference_kind: str,
) -> None:
    """Check a source in its explicitly declared input or output namespace."""
    if reference_kind == "inputs":
        if mapping.source_artifact_id is None:
            msg = "Input lineage requires source_artifact_id"
            raise LineageIntegrityError(msg)
        if not is_hex_id(mapping.source_artifact_id):
            msg = f"Malformed source ID: {mapping.source_artifact_id!r}"
            raise LineageIntegrityError(msg)
        if mapping.source_artifact_id not in input_ids.get(mapping.source_role, set()):
            msg = (
                f"Lineage references non-existent input source {mapping.source_artifact_id} "
                f"in role {mapping.source_role!r}"
            )
            raise LineageIntegrityError(msg)
    else:
        index = mapping.source_output_index
        if index is None:
            msg = "Output lineage requires source_output_index"
            raise LineageIntegrityError(msg)
        if not 0 <= index < len(output_artifacts.get(mapping.source_role, [])):
            msg = f"Lineage references non-existent output source {mapping.source_role!r}[{index}]"
            raise LineageIntegrityError(msg)


def validate_lineage_integrity(
    lineage: dict[str, list[LineageMapping]],
    input_artifact_ids: dict[str, list[str]],
    output_artifacts: dict[str, list[Artifact]],
    output_specs: dict[str, OutputSpec],
) -> None:
    """Validate explicit role coverage, contracts, references, and uniqueness.

    Different parents in the same role are valid fan-in. Only an identical
    reference for the same target occurrence is a duplicate.

    Raises:
        LineageIntegrityError: If role coverage, contracts, or references fail.
    """
    if set(lineage) != set(output_artifacts):
        msg = (
            "Lineage roles must exactly match artifact roles: "
            f"missing={sorted(set(output_artifacts) - set(lineage))}, "
            f"extra={sorted(set(lineage) - set(output_artifacts))}"
        )
        raise LineageIntegrityError(msg)
    input_ids = {role: set(ids) for role, ids in input_artifact_ids.items()}
    for target_role, mappings in lineage.items():
        kind, allowed_roles = _lineage_contract(target_role, output_specs)
        if not allowed_roles and mappings:
            msg = f"Root output role {target_role!r} must have empty lineage"
            raise LineageIntegrityError(msg)
        seen: set[tuple[int, str, str | None, int | None]] = set()
        for mapping in mappings:
            if not 0 <= mapping.draft_index < len(output_artifacts[target_role]):
                msg = f"Lineage references non-existent output {target_role!r}[{mapping.draft_index}]"
                raise LineageIntegrityError(msg)
            if mapping.source_role not in allowed_roles:
                msg = (
                    f"Output {target_role!r}[{mapping.draft_index}] has forbidden source role "
                    f"{mapping.source_role!r}; allowed roles: {sorted(allowed_roles)}"
                )
                raise LineageIntegrityError(msg)
            _validate_mapping_reference(mapping, input_ids, output_artifacts, kind)
            key = (
                mapping.draft_index,
                mapping.source_role,
                mapping.source_artifact_id,
                mapping.source_output_index,
            )
            if key in seen:
                msg = (
                    f"Duplicate lineage mapping for output {target_role!r}[{mapping.draft_index}]: "
                    f"source role {mapping.source_role!r}"
                )
                raise LineageIntegrityError(msg)
            seen.add(key)
