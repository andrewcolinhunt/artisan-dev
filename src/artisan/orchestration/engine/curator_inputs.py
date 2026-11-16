"""Prepare ordered curator inputs against one fixed, verified store view."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from artisan.errors import StoreIntegrityError
from artisan.execution.curator_definition import capture_curator_definition
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.execution.curator_admission import (
    CuratorAdmission,
    CuratorInputAssociation,
    CuratorInputMember,
    CuratorInputRole,
)
from artisan.schemas.execution.curator_definition import CuratorDefinitionContract
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_view import CuratorView, capture_curator_view
from artisan.storage.core.registered_content import RegisteredContent
from artisan.storage.core.retained_reader import AcceptedArtifact
from artisan.storage.io.curator_control import verify_curator_inputs
from artisan.storage.io.retained_commit import RetainedCommitter


def prepare_curator_inputs(
    operation: OperationDefinition,
    record: StepStartRecord,
    inputs: Mapping[str, Sequence[AcceptedArtifact | str]],
    *,
    software_digest: str,
    software_cacheable: bool,
    committer: RetainedCommitter,
) -> CuratorAdmission:
    """Validate content and capture associations without publishing or dispatching.

    Explicit accepted references retain their exact producer membership. Raw
    registered IDs remain unscoped, including when their bytes equal accepted
    outputs. Content rows and protected origins are verified here; external
    carriers are checked at supported read and selected-output boundaries.
    """
    definition = capture_curator_definition(operation)
    _validate_roles(definition, inputs)
    selected = {role: tuple(values) for role, values in inputs.items()}
    view = CuratorView(committer, capture_curator_view(committer))
    content = RegisteredContent(view)
    content.load(
        {_artifact_id(value) for values in selected.values() for value in values}
    )
    roles = tuple(
        CuratorInputRole(
            role=role, members=tuple(_member(value, content) for value in values)
        )
        for role, values in selected.items()
    )
    _validate_types(definition, roles)
    associations = _capture_associations(definition, roles, content)
    admission = CuratorAdmission(
        record=record,
        operation_name=definition.name,
        operation_version=definition.version,
        definition=definition,
        software_digest=software_digest,
        software_cacheable=software_cacheable,
        cacheable=definition.cacheable,
        inputs=roles,
        associations=associations,
        view=view.snapshot,
    )
    verify_curator_inputs(admission, committer)
    return admission


def _validate_roles(
    definition: CuratorDefinitionContract,
    inputs: Mapping[str, Sequence[AcceptedArtifact | str]],
) -> None:
    declared = {item.role for item in definition.inputs}
    required = {item.role for item in definition.inputs if item.required}
    if not definition.runtime_defined_inputs and (
        inputs.keys() - declared or required - inputs.keys()
    ):
        msg = "Curator inputs have missing required or undeclared roles"
        raise ValueError(msg)


def _artifact_id(value: AcceptedArtifact | str) -> str:
    if isinstance(value, str):
        return value
    if value.artifact.artifact_id is None:
        msg = "Curator inputs require registered IDs or finalized accepted artifacts"
        raise ValueError(msg)
    return value.artifact.artifact_id


def _member(
    value: AcceptedArtifact | str, content: RegisteredContent
) -> CuratorInputMember:
    artifact = content.artifacts[_artifact_id(value)]
    if (
        isinstance(value, AcceptedArtifact)
        and value.artifact.to_row() != artifact.to_row()
    ):
        msg = "Selected curator input differs from its authoritative registered content"
        raise StoreIntegrityError(msg)
    return CuratorInputMember(
        artifact_id=_artifact_id(value),
        artifact_type=artifact.artifact_type,
        producers=(value.reference,) if isinstance(value, AcceptedArtifact) else (),
    )


def _validate_types(
    definition: CuratorDefinitionContract, roles: tuple[CuratorInputRole, ...]
) -> None:
    specs = {item.role: item for item in definition.inputs}
    for group in roles:
        spec = specs.get(group.role)
        if spec is not None and any(
            not ArtifactTypes.matches(spec.artifact_type, member.artifact_type)
            for member in group.members
        ):
            msg = f"Curator input role {group.role!r} has an incompatible artifact type"
            raise ValueError(msg)


def _capture_associations(
    definition: CuratorDefinitionContract,
    roles: tuple[CuratorInputRole, ...],
    content: RegisteredContent,
) -> tuple[CuratorInputAssociation, ...]:
    specs = {item.role: item for item in definition.inputs}
    requests: dict[str, set[str]] = {}
    for group in roles:
        spec = specs.get(group.role)
        if spec is not None:
            for member in group.members:
                requests.setdefault(member.artifact_id, set()).update(
                    spec.with_associated
                )
    view, committer = content.view, content.view.committer
    store = ArtifactStore(
        committer.delta_root,
        fs=committer.fs,
        storage_options=committer.storage_options,
        files_root=committer.files_root,
        _view=view,
    )
    captured = store.capture_associated(
        {primary: tuple(sorted(kinds)) for primary, kinds in requests.items()}
    )
    content.load({member for members in captured.values() for member in members})
    for (_, kind), members in captured.items():
        ArtifactTypeDef.get(kind)
        if any(content.artifacts[member].artifact_type != kind for member in members):
            msg = "Captured curator association disagrees with registered content type"
            raise StoreIntegrityError(msg)
    return tuple(
        CuratorInputAssociation(
            primary_id=primary, artifact_type=kind, artifact_ids=members
        )
        for (primary, kind), members in captured.items()
    )
