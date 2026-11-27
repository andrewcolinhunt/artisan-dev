"""Freeze submission inputs, then resolve exact sources after pending ownership."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import cast

from artisan.errors import StoreIntegrityError
from artisan.execution.inputs._validation import is_hex_id
from artisan.orchestration.engine.current_outputs import (
    CurrentStepOutputs,
    read_current_step_outputs,
)
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.output_reference import (
    OutputReference,
    StoreIdentity,
)
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.schemas.specs.input_defaults import capture_input_defaults
from artisan.schemas.specs.input_spec import InputSpec
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_view import CuratorView, capture_curator_view
from artisan.storage.core.registered_content import RegisteredContent
from artisan.storage.core.retained_reader import AcceptedArtifact
from artisan.storage.io.curator_external_reads import (
    curator_carrier_read,
    verify_curator_carrier_read,
)
from artisan.storage.io.input_registration import register_raw_inputs
from artisan.storage.io.retained_commit import RetainedCommitter

type InputMember = OutputReference | str | Artifact | AcceptedArtifact
type PipelineInputs = (
    Mapping[str, OutputReference | Sequence[InputMember]]
    | Sequence[OutputReference]
    | Sequence[str]
    | None
)


type FrozenMember = OutputReference | str | Artifact | AcceptedArtifact
type ResolvedMember = str | Artifact | AcceptedArtifact


@dataclass(frozen=True)
class FrozenInputRole:
    role: str
    members: tuple[FrozenMember, ...]


@dataclass(frozen=True)
class FrozenPipelineInputs:
    """Detached caller containers; source resolution never follows later retries."""

    roles: tuple[FrozenInputRole, ...] = ()
    raw_paths: tuple[str, ...] | None = None

    @property
    def dependency_step_run_ids(self) -> tuple[str, ...]:
        """Return ordered unique attempt IDs for predecessor waits."""
        return tuple(
            dict.fromkeys(
                member.source.step_run_id
                for group in self.roles
                for member in group.members
                if isinstance(member, OutputReference)
            )
        )

    def diagnostic_data(self) -> dict[str, object]:
        """Return descriptors only; callers must sanitize before durable recording."""
        if self.raw_paths is not None:
            return {"raw_paths": list(self.raw_paths)}
        return {
            group.role: [_diagnostic_member(member) for member in group.members]
            for group in self.roles
        }


def normalize_pipeline_inputs(
    inputs: PipelineInputs, *, store_identity: StoreIdentity
) -> FrozenPipelineInputs:
    """Copy input containers and preserve exact sources within the receiving store."""
    if inputs is None:
        return FrozenPipelineInputs()
    if isinstance(inputs, Mapping):
        roles = []
        for role, value in inputs.items():
            if not isinstance(cast(object, role), str) or not role:
                msg = "Input roles must be nonempty strings"
                raise TypeError(msg)
            values = (
                (value,) if isinstance(value, OutputReference) else _sequence(value)
            )
            roles.append(
                FrozenInputRole(
                    role, tuple(_freeze(item, store_identity) for item in values)
                )
            )
        return FrozenPipelineInputs(tuple(roles))
    values = _sequence(inputs)
    if not values:
        return FrozenPipelineInputs(raw_paths=())
    if all(isinstance(value, str) for value in values):
        return FrozenPipelineInputs(raw_paths=cast(tuple[str, ...], values))
    if not all(isinstance(value, OutputReference) for value in values):
        msg = "Top-level inputs must contain only raw paths or only output references"
        raise TypeError(msg)
    return FrozenPipelineInputs(
        (
            FrozenInputRole(
                "_merged_streams",
                tuple(_freeze(item, store_identity) for item in values),
            ),
        )
    )


def validate_pipeline_input_contract(
    inputs: FrozenPipelineInputs,
    specs: Mapping[str, InputSpec],
    *,
    runtime_defined_inputs: bool,
) -> None:
    """Reject invalid declared roles/types before an empty boundary can skip.

    An empty top-level sequence names no roles and retains the pipeline's explicit
    no-work convention. Named empty streams must still satisfy the input contract.
    """
    if inputs.raw_paths == ():
        return
    roles = {group.role for group in inputs.roles}
    if inputs.raw_paths is not None:
        roles.add("file")
    if not runtime_defined_inputs and (
        roles - specs.keys()
        or {role for role, spec in specs.items() if spec.required} - roles
    ):
        msg = "Pipeline inputs have missing required or undeclared roles"
        raise ValueError(msg)
    for group in inputs.roles:
        spec = specs.get(group.role)
        if spec is None:
            continue
        for member in group.members:
            value = member.artifact if isinstance(member, AcceptedArtifact) else member
            kind = (
                value.artifact_type
                if isinstance(value, (Artifact, OutputReference))
                else None
            )
            if (
                kind is not None
                and kind != ArtifactTypes.ANY
                and not spec.accepts_type(kind)
            ):
                msg = f"Pipeline input role {group.role!r} has an incompatible artifact type"
                raise ValueError(msg)


def complete_pipeline_inputs(
    inputs: FrozenPipelineInputs, specs: Mapping[str, InputSpec]
) -> FrozenPipelineInputs:
    """Capture fixed artifacts only for omitted roles, preserving explicit no-work."""
    if inputs.raw_paths == ():
        return inputs
    present = {group.role for group in inputs.roles}
    if inputs.raw_paths is not None:
        present.add("file")
    roles = list(inputs.roles)
    roles.extend(
        FrozenInputRole(role, artifacts)
        for role, artifacts in capture_input_defaults(specs, present).items()
    )
    return FrozenPipelineInputs(tuple(roles), inputs.raw_paths)


def resolve_creator_inputs(
    inputs: FrozenPipelineInputs,
    *,
    record: StepStartRecord,
    committer: RetainedCommitter,
    storage: StorageConfig,
    check_cancelled: Callable[[], None],
    input_specs: Mapping[str, InputSpec] | None = None,
) -> dict[str, tuple[AcceptedArtifact | Artifact, ...]]:
    """Resolve registered IDs as verified unscoped artifacts for scientific admission."""
    roles = _resolve(inputs, record, committer, storage, check_cancelled, input_specs)
    raw_ids = {
        value for values in roles.values() for value in values if isinstance(value, str)
    }
    content = _registered(committer, raw_ids)
    _hydrate_external(content, raw_ids)
    result = {
        role: tuple(
            content.artifacts[value] if isinstance(value, str) else _finalized(value)
            for value in values
        )
        for role, values in roles.items()
    }
    check_cancelled()
    return result


def resolve_curator_inputs(
    inputs: FrozenPipelineInputs,
    *,
    record: StepStartRecord,
    committer: RetainedCommitter,
    storage: StorageConfig,
    check_cancelled: Callable[[], None],
    input_specs: Mapping[str, InputSpec] | None = None,
) -> dict[str, tuple[AcceptedArtifact | str, ...]]:
    """Require existing registration for bare artifacts without reading carriers."""
    roles = _resolve(inputs, record, committer, storage, check_cancelled, input_specs)
    ids = {_artifact_id(value) for values in roles.values() for value in values}
    content = _registered(committer, ids)
    result = {
        role: tuple(_curator_member(value, content) for value in values)
        for role, values in roles.items()
    }
    check_cancelled()
    return result


def _freeze(value: object, store_identity: StoreIdentity) -> FrozenMember:
    if isinstance(value, OutputReference):
        value = OutputReference.model_validate(value)
        if value.source.store != store_identity:
            msg = "Output reference belongs to a different store; import its data explicitly"
            raise ValueError(msg)
        return value
    if isinstance(value, AcceptedArtifact):
        return AcceptedArtifact(
            value.reference.model_copy(deep=True), value.artifact.model_copy(deep=True)
        )
    if isinstance(value, Artifact):
        return value.model_copy(deep=True)
    if isinstance(value, str) and is_hex_id(value):
        return value
    msg = "Role inputs require output references, registered IDs or typed artifacts"
    raise TypeError(msg)


def _sequence(value: object) -> tuple[object, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        msg = "Input values must be ordered sequences or output references"
        raise TypeError(msg)
    return tuple(value)


def _resolve(
    inputs: FrozenPipelineInputs,
    record: StepStartRecord,
    committer: RetainedCommitter,
    storage: StorageConfig,
    check_cancelled: Callable[[], None],
    input_specs: Mapping[str, InputSpec] | None,
) -> dict[str, tuple[ResolvedMember, ...]]:
    check_cancelled()
    tracker = RetainedStepTracker(committer, "")
    state = tracker.current_state(record.step_run_id)
    if state.status != StepStatus.PENDING or any(
        getattr(state, field) != getattr(record, field)
        for field in ("step_number", "step_name", "operation_class")
    ):
        msg = "Input resolution requires its exact durable pending step"
        raise StoreIntegrityError(msg)
    roles: dict[str, tuple[ResolvedMember, ...]] = {}
    if inputs.raw_paths is not None:
        if not inputs.raw_paths:
            return {"file": ()}
        plan = register_raw_inputs(
            committer,
            inputs.raw_paths,
            step_run_id=record.step_run_id,
            step_number=record.step_number,
            operation_name=record.step_name,
            storage=storage,
            check_cancelled=check_cancelled,
        )
        check_cancelled()
        roles["file"] = tuple(item.artifact_id for item in plan.inputs)
    outputs: dict[str, CurrentStepOutputs] = {}
    for group in inputs.roles:
        members: list[ResolvedMember] = []
        for member in group.members:
            if isinstance(member, OutputReference):
                attempt = member.source.step_run_id
                if attempt not in outputs:
                    source = tracker.current_state(attempt)
                    outputs[attempt] = CurrentStepOutputs(
                        source, read_current_step_outputs(committer, attempt)
                    )
                members.extend(
                    _output_members(
                        member,
                        outputs[attempt],
                        (input_specs or {}).get(group.role),
                    )
                )
            else:
                members.append(member)
        roles[group.role] = tuple(members)
    return roles


def _output_members(
    reference: OutputReference,
    selected: CurrentStepOutputs,
    input_spec: InputSpec | None = None,
) -> tuple[AcceptedArtifact, ...]:
    for group in selected.outputs:
        if group.role == reference.role:
            declared_type = selected.state.output_types.get(group.role)
            if (
                declared_type is not None
                and declared_type != ArtifactTypes.ANY
                and not ArtifactTypes.matches(reference.artifact_type, declared_type)
            ):
                msg = "Output reference type differs from its accepted role"
                raise StoreIntegrityError(msg)
            if (
                input_spec is not None
                and declared_type is not None
                and declared_type != ArtifactTypes.ANY
                and not input_spec.accepts_type(declared_type)
            ):
                msg = "Selected output role has an incompatible artifact type"
                raise ValueError(msg)
            if any(
                not ArtifactTypes.matches(
                    reference.artifact_type, item.artifact.artifact_type
                )
                for item in group.occurrences
            ):
                msg = "Output reference type differs from its accepted membership"
                raise StoreIntegrityError(msg)
            return group.occurrences
    if selected.state.status not in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}:
        return ()
    msg = f"Selected source has no output role {reference.role!r}"
    raise ValueError(msg)


def _registered(committer: RetainedCommitter, ids: set[str]) -> RegisteredContent:
    content = RegisteredContent(CuratorView(committer, capture_curator_view(committer)))
    content.load(ids)
    return content


def _finalized(value: Artifact | AcceptedArtifact) -> Artifact | AcceptedArtifact:
    artifact = value.artifact if isinstance(value, AcceptedArtifact) else value
    if not artifact.is_finalized:
        msg = "Pipeline artifact inputs must be finalized"
        raise ValueError(msg)
    artifact.to_row()
    return value


def _artifact_id(value: ResolvedMember) -> str:
    if isinstance(value, str):
        return value
    _finalized(value)
    artifact = value.artifact if isinstance(value, AcceptedArtifact) else value
    assert artifact.artifact_id is not None
    return artifact.artifact_id


def _curator_member(
    value: ResolvedMember, content: RegisteredContent
) -> AcceptedArtifact | str:
    artifact_id = _artifact_id(value)
    artifact = value.artifact if isinstance(value, AcceptedArtifact) else value
    if (
        isinstance(artifact, Artifact)
        and artifact.to_row() != content.artifacts[artifact_id].to_row()
    ):
        msg = "Curator artifact input differs from its registered content"
        raise StoreIntegrityError(msg)
    return value if isinstance(value, AcceptedArtifact) else artifact_id


def _hydrate_external(content: RegisteredContent, ids: set[str]) -> None:
    view, committer = content.view, content.view.committer
    store = ArtifactStore(
        committer.delta_root,
        fs=committer.fs,
        storage_options=committer.storage_options,
        files_root=committer.files_root,
        _view=view,
    )
    by_type: dict[str, list[str]] = {}
    for artifact_id in ids:
        artifact = content.artifacts[artifact_id]
        if artifact.EXTERNALLY_BACKED:
            by_type.setdefault(artifact.artifact_type, []).append(artifact_id)
    for kind, members in by_type.items():
        hydrated = store.get_artifacts_by_type(members, kind)
        if set(hydrated) != set(members):
            msg = "Registered input lacks complete captured location membership"
            raise StoreIntegrityError(msg)
        for artifact_id, artifact in hydrated.items():
            if artifact.to_row() != content.artifacts[artifact_id].to_row():
                msg = "Hydrated input differs from its captured registered content"
                raise StoreIntegrityError(msg)
            uri = getattr(artifact, next(iter(artifact.LOCATOR_FIELDS)))
            verify_curator_carrier_read(
                view, curator_carrier_read(view, artifact_id, uri)
            )
            content.artifacts[artifact_id] = artifact


def _diagnostic_member(member: FrozenMember) -> object:
    if isinstance(member, OutputReference):
        return {
            "step_run_id": member.source.step_run_id,
            "role": member.role,
            "artifact_type": member.artifact_type,
        }
    artifact = member.artifact if isinstance(member, AcceptedArtifact) else member
    return {
        "artifact_id": artifact.artifact_id
        if isinstance(artifact, Artifact)
        else artifact
    }
