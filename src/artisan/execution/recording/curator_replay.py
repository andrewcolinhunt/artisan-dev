"""Optional curator reconstruction evidence from a qualified invocation."""

from __future__ import annotations

from collections.abc import Mapping
from importlib.metadata import version

from artisan.execution.compute.prepared import freeze_runtime_injections
from artisan.execution.curator_definition import verify_curator_definition
from artisan.execution.recording.durable import configuration_recorder
from artisan.execution.recording.replay_snapshot import (
    _configuration,
    operation_behavior,
    runtime_roots,
)
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.registry.resolve import operation_identity
from artisan.schemas.execution.curator_admission import CuratorAdmission
from artisan.schemas.execution.replay import (
    ReplayOperation,
    ReplaySnapshot,
    ReplaySource,
    captured_associations,
)
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.utils.hashing import AssociatedInputIdentity, CacheInputIdentity


def capture_curator_replay(
    operation: OperationDefinition,
    admission: CuratorAdmission,
    runtime: RuntimeEnvironment,
    runtime_env: Mapping[str, str],
    *,
    sensitive_values: tuple[str, ...] = (),
) -> ReplaySnapshot:
    """Capture optional configuration without replacing required admission evidence.

    Runtime values are replacement obligations, never persisted configuration.
    A caller must supply trusted code when replaying; capture performs no imports
    selected by stored names. Failure to serialize optional configuration leaves
    the qualified execution usable with explicitly unavailable replay evidence.
    """
    if admission.definition is None:
        msg = "Curator replay capture requires a qualified admission"
        raise ValueError(msg)
    verify_curator_definition(operation, admission.definition)
    injections = freeze_runtime_injections(runtime_env)
    recorder = configuration_recorder(operation)
    recorder._learn_json(runtime.storage)
    recorder.add_sensitive_data(dict(injections))
    recorder.add_sensitive_data(sensitive_values)
    try:
        configuration, replacements = _configuration(
            operation, recorder.sensitive_values
        )
        inputs = curator_replay_inputs(admission)
        return ReplaySnapshot(
            status="requires_operation_class",
            unavailable_reason=None,
            operation=ReplayOperation(
                identity=operation_identity(type(operation)),
                configuration=configuration,
                behavior=operation_behavior(operation),
                artisan_version=version("dexterity-artisan"),
            ),
            inputs=inputs,
            group_ids=None,
            associated=captured_associations(inputs),
            associated_complete=True,
            source=ReplaySource(
                step_number=admission.record.step_number,
                step_run_id=admission.record.step_run_id,
                runner=admission.record.compute_backend,
                application_modules=runtime.application_modules,
                execution_spec_id=admission.digest,
                roots=recorder.sanitize_data(runtime_roots(runtime)),
            ),
            required_replacements=replacements,
            required_runtime_keys=tuple(sorted(injections)),
            diagnostic=None,
        )
    except (TypeError, ValueError, OverflowError) as error:
        return ReplaySnapshot.unavailable(
            f"curator_configuration_unavailable:{type(error).__name__}"
        )


def curator_replay_inputs(
    admission: CuratorAdmission,
) -> dict[str, list[CacheInputIdentity]]:
    """Project exact ordered inputs and association capture into optional evidence."""
    associated: dict[str, list[AssociatedInputIdentity]] = {}
    for item in admission.associations:
        associated.setdefault(item.primary_id, []).append(
            AssociatedInputIdentity(item.artifact_type, item.artifact_ids)
        )
    return {
        role.role: [
            CacheInputIdentity(
                role.role,
                None,
                position,
                member.artifact_type,
                member.artifact_id,
                tuple(associated.get(member.artifact_id, ())),
            )
            for position, member in enumerate(role.members)
        ]
        for role in admission.inputs
    }
