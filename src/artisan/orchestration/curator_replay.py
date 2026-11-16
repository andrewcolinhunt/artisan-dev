"""Restore trusted curator code and frozen explicit inputs for diagnostic replay."""

from __future__ import annotations

import json
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from types import MappingProxyType
from uuid import uuid4

from pydantic import JsonValue

from artisan.errors import ArtisanError, ErrorCode, StoreIntegrityError
from artisan.execution.compute.prepared import freeze_runtime_injections
from artisan.execution.curator_definition import (
    capture_curator_definition,
    verify_curator_definition,
)
from artisan.execution.recording.commands import CommandRecorder
from artisan.execution.recording.curator_replay import curator_replay_inputs
from artisan.execution.recording.durable import configuration_recorder
from artisan.execution.recording.replay_snapshot import (
    operation_behavior,
    same_configuration_structure,
    set_pointer,
)
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.registry.resolve import operation_identity
from artisan.schemas.execution.curator_admission import CuratorAdmission
from artisan.schemas.execution.curator_definition import CuratorDefinitionContract
from artisan.schemas.execution.replay import (
    ReplayDiagnostic,
    ReplaySnapshot,
    captured_associations,
)
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.core.curator_view import capture_curator_view
from artisan.storage.core.execution_source import (
    CuratorExecutionSource,
    read_execution_source,
)
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.utils.hashing import serialize_params


@dataclass(frozen=True)
class CuratorReplayPreparation:
    """Restored live code; a new admission still selects software and store visibility."""

    execution_run_id: str
    source: CuratorExecutionSource
    snapshot: ReplaySnapshot
    operation: OperationDefinition = field(repr=False)
    definition: CuratorDefinitionContract
    runtime_env: Mapping[str, str] = field(repr=False)
    recorder: CommandRecorder = field(repr=False)
    allow_code_change: bool


def prepare_curator_replay(
    committer: RetainedCommitter,
    execution_run_id: str,
    *,
    operation_class: type[OperationDefinition],
    configuration_replacements: Mapping[str, JsonValue],
    runtime_replacements: Mapping[str, str],
    allow_code_change: bool = False,
) -> CuratorReplayPreparation:
    """Read original authority and restore only the caller's already imported class.

    No imports, software provisioning, cache lookup, admission or recovery occur.
    Explicit input occurrences and associations remain captured; the eventual
    diagnostic admission will choose its current committed-store view separately.
    """
    source = read_execution_source(committer, execution_run_id)
    if not isinstance(source, CuratorExecutionSource):
        raise _error(
            ErrorCode.REPLAY_EXECUTION_NOT_FOUND,
            "Replay requires an original completed curator execution",
        )
    snapshot = _source_snapshot(source)
    if set(runtime_replacements) != set(snapshot.required_runtime_keys or ()):
        raise _error(
            ErrorCode.REPLAY_REPLACEMENT_REQUIRED,
            "Supply exactly the captured runtime replacement keys",
        )
    runtime_env = freeze_runtime_injections(runtime_replacements)
    operation, recorder = _restore_operation(
        source, snapshot, operation_class, configuration_replacements, allow_code_change
    )
    recorder.add_sensitive_data(dict(runtime_env))
    return CuratorReplayPreparation(
        execution_run_id,
        source,
        snapshot,
        operation,
        capture_curator_definition(operation),
        MappingProxyType(dict(runtime_env)),
        recorder,
        allow_code_change,
    )


def curator_replay_record(
    replay: CuratorReplayPreparation, *, runner_name: str
) -> StepStartRecord:
    """Create fresh diagnostic step ownership without admitting or executing it."""
    definition = replay.definition
    return StepStartRecord(
        step_run_id=uuid4().hex,
        replay_of_execution_run_id=replay.execution_run_id,
        step_number=replay.source.admission.record.step_number,
        step_name=replay.source.admission.record.step_name,
        operation_class=f"{definition.module}.{definition.qualname}",
        params_json=json.dumps(
            replay.recorder.sanitize_json(serialize_params(replay.operation))
        ),
        input_refs_json=replay.source.admission.record.input_refs_json,
        compute_backend=runner_name,
        compute_options_json=json.dumps(
            {"dynamic_output_roles": not definition.outputs}
        ),
        output_roles_json=json.dumps([item.role for item in definition.outputs]),
        output_types_json=json.dumps(
            {item.role: item.artifact_type for item in definition.outputs}
        ),
    )


def prepare_curator_replay_admission(
    replay: CuratorReplayPreparation,
    record: StepStartRecord,
    *,
    software_digest: str,
    software_cacheable: bool,
    committer: RetainedCommitter,
) -> tuple[CuratorAdmission, ReplayDiagnostic]:
    """Freeze a new current-store view while keeping original explicit membership.

    Returns data for the ordinary curator admission writer; this function neither
    publishes nor consults a cache. Worker and acceptance checks remain shared.
    """
    source = replay.source.admission
    if (
        record.step_run_id == source.record.step_run_id
        or record.replay_of_execution_run_id != replay.execution_run_id
    ):
        raise _error(
            ErrorCode.REPLAY_CONFIGURATION_INVALID, "Replay requires a new owner"
        )
    verify_curator_definition(replay.operation, replay.definition)
    if not replay.allow_code_change and software_digest != source.software_digest:
        raise _error(ErrorCode.REPLAY_CODE_CHANGED, "Selected curator software changed")
    admission = CuratorAdmission(
        record=record,
        operation_name=replay.definition.name,
        operation_version=replay.definition.version,
        definition=replay.definition,
        software_digest=software_digest,
        software_cacheable=software_cacheable,
        cacheable=replay.definition.cacheable,
        inputs=source.inputs,
        associations=source.associations,
        view=capture_curator_view(committer),
    )
    assert replay.snapshot.operation is not None
    diagnostic = ReplayDiagnostic(
        source_execution_run_id=replay.execution_run_id,
        source_execution_spec_id=source.digest,
        source_identity=replay.snapshot.operation.identity,
        selected_identity=operation_identity(type(replay.operation)),
        source_software_digest=source.software_digest,
        selected_software_digest=software_digest,
        allow_code_change=replay.allow_code_change,
        runner=record.compute_backend,
        roots={},
        reproducibility_notes=[
            "Explicit inputs and framework associations are frozen; authored reads "
            "use the current committed store, files, environment and network."
        ],
    )
    return admission, diagnostic


def _source_snapshot(source: CuratorExecutionSource) -> ReplaySnapshot:
    encoded = source.execution_record.get("replay_snapshot")
    if not isinstance(encoded, str):
        raise _error(
            ErrorCode.REPLAY_EVIDENCE_UNAVAILABLE,
            "Curator replay configuration is unavailable",
        )
    try:
        snapshot = ReplaySnapshot.model_validate_json(encoded)
    except ValueError:
        msg = "Curator replay configuration is malformed"
        raise StoreIntegrityError(msg) from None
    admission = source.admission
    if (
        snapshot.status == "unavailable"
        or snapshot.required_runtime_keys is None
        or not snapshot.associated_complete
        or admission.definition is None
        or admission.software_digest is None
    ):
        raise _error(
            ErrorCode.REPLAY_EVIDENCE_UNAVAILABLE,
            "Curator replay requires captured configuration, runtime keys and associations",
        )
    assert snapshot.source is not None
    assert snapshot.operation is not None
    record = admission.record
    expected = curator_replay_inputs(admission)
    identity = snapshot.operation.identity
    definition = admission.definition
    if (
        (
            snapshot.source.execution_spec_id,
            snapshot.source.step_run_id,
            snapshot.source.step_number,
            snapshot.source.runner,
        )
        != (
            admission.digest,
            record.step_run_id,
            record.step_number,
            record.compute_backend,
        )
        or (identity.name, identity.version, identity.module, identity.qualname)
        != (definition.name, definition.version, definition.module, definition.qualname)
        or snapshot.group_ids is not None
        or snapshot.inputs != expected
        or snapshot.associated != captured_associations(expected)
    ):
        msg = "Curator replay evidence differs from its required source admission"
        raise StoreIntegrityError(msg)
    return snapshot


def _restore_operation(
    source: CuratorExecutionSource,
    snapshot: ReplaySnapshot,
    operation_class: object,
    replacements: Mapping[str, JsonValue],
    allow_code_change: bool,
) -> tuple[OperationDefinition, CommandRecorder]:
    if (
        not isinstance(operation_class, type)
        or not issubclass(operation_class, OperationDefinition)
        or operation_class._kind() != "curator"
    ):
        raise _error(
            ErrorCode.REPLAY_DEPENDENCY_UNAVAILABLE,
            "Supply an already imported curator definition class",
        )
    assert snapshot.operation is not None
    original = snapshot.operation.identity
    selected = operation_identity(operation_class)
    if (selected.name, selected.version) != (original.name, original.version) or (
        not allow_code_change
        and (selected != original or original.module_digest is None)
    ):
        raise _error(ErrorCode.REPLAY_CODE_CHANGED, "Selected curator code changed")
    if set(replacements) != {slot.pointer for slot in snapshot.required_replacements}:
        raise _error(
            ErrorCode.REPLAY_REPLACEMENT_REQUIRED,
            "Supply exactly the captured configuration replacement pointers",
        )
    payload = deepcopy(snapshot.operation.configuration)
    recorder = configuration_recorder(operation_class)
    recorder.add_sensitive_data(dict(replacements))
    for pointer, value in replacements.items():
        set_pointer(payload, pointer, deepcopy(value))
    try:
        operation = operation_class.model_validate(payload, by_name=True)
        if not same_configuration_structure(
            operation.model_dump(mode="json", round_trip=True), payload
        ):
            msg = "Captured curator configuration acquired new defaults or lost fields"
            raise ValueError(msg)
        definition = capture_curator_definition(operation)
    except (TypeError, ValueError):
        raise _error(
            ErrorCode.REPLAY_CONFIGURATION_INVALID,
            "Restored configuration does not match the concrete curator definition",
        ) from None
    assert source.admission.definition is not None
    excluded = (
        {"params_digest"}
        if any(
            pointer == "/params" or pointer.startswith("/params/")
            for pointer in replacements
        )
        else set()
    )
    if not allow_code_change and (
        definition.model_dump(exclude=excluded)
        != source.admission.definition.model_dump(exclude=excluded)
        or operation_behavior(operation) != snapshot.operation.behavior
    ):
        raise _error(
            ErrorCode.REPLAY_CODE_CHANGED,
            "Restored curator declarations or parameters differ from the source",
        )
    recorder.add_operation(operation)
    return operation, recorder


def _error(code: str, message: str) -> ArtisanError:
    return ArtisanError(
        code, message, error_type="validation", recovery_hint="CHECK_INPUT"
    )
