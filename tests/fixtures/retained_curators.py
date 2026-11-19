"""Real curator computation, existing seals and candidate completion fixtures."""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import UTC, datetime
from uuid import uuid4

import polars as pl

from artisan.execution.curator_definition import capture_curator_definition
from artisan.execution.executors.curator import (
    _handle_artifact_result,
    _handle_passthrough_result,
)
from artisan.execution.executors.curator_stream import handle_artifact_stream
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.recording.commands import capture_commands, command_snapshot
from artisan.execution.recording.parquet_writer import StagingResult
from artisan.execution.recording.recorder import record_execution_failure
from artisan.execution.recording.replay_snapshot import capture_replay
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.engine.retained_curator import CuratorResults
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.execution.curator_admission import (
    CuratorAdmission,
    CuratorAdmissionReference,
    CuratorInputAssociation,
    CuratorInputMember,
    CuratorInputRole,
)
from artisan.schemas.execution.curator_result import (
    ArtifactResult,
    ArtifactStreamResult,
    PassthroughResult,
)
from artisan.schemas.execution.execution_context import ExecutionContext
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_view import CuratorView, capture_curator_view
from artisan.storage.core.retained_reader import AcceptedArtifact
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.utils.command_supervisor import managed_invocation
from artisan.utils.hashing import serialize_params
from fixtures.retained_acceptance import open_committer


def admit_curator(
    committer: RetainedCommitter,
    operation: OperationDefinition,
    inputs: dict[str, tuple[AcceptedArtifact, ...]],
    *,
    step_run_id: str,
    step_number: int = 8,
    pipeline_run_id: str = "synthetic-pipeline",
    software_digest: str | None = None,
    software_cacheable: bool = True,
    associations: tuple[CuratorInputAssociation, ...] = (),
) -> tuple[CuratorResults, CuratorAdmission, CuratorAdmissionReference]:
    coordinator = CuratorResults(
        RetainedStepTracker(committer, pipeline_run_id),
        f"{committer.delta_root}/curator-staging",
    )
    coordinator.initialize()
    record = StepStartRecord(
        step_run_id=step_run_id,
        step_number=step_number,
        step_name=operation.name,
        operation_class=f"{type(operation).__module__}.{type(operation).__qualname__}",
        params_json=json.dumps(serialize_params(operation)),
        input_refs_json="{}",
        compute_backend="local",
        compute_options_json=json.dumps(
            {"dynamic_output_roles": not operation.outputs}
        ),
        output_roles_json=json.dumps(list(operation.outputs)),
        output_types_json=json.dumps(
            {role: spec.artifact_type for role, spec in operation.outputs.items()}
        ),
    )
    admission = CuratorAdmission(
        record=record,
        operation_name=operation.name,
        operation_version=operation.version,
        definition=capture_curator_definition(operation)
        if software_digest is not None
        else None,
        software_digest=software_digest,
        software_cacheable=software_cacheable,
        cacheable=operation.cacheable,
        associations=associations,
        inputs=tuple(
            CuratorInputRole(
                role=role,
                members=tuple(
                    CuratorInputMember(
                        artifact_id=item.artifact.artifact_id,
                        artifact_type=item.artifact.artifact_type,
                        producers=(item.reference,),
                    )
                    for item in members
                ),
            )
            for role, members in inputs.items()
        ),
        view=capture_curator_view(committer),
    )
    return coordinator, admission, coordinator.admit(admission)


def stage_curator(
    coordinator: CuratorResults,
    admission: CuratorAdmission,
    operation: OperationDefinition,
) -> tuple[StagingResult, StepResult]:
    committer = coordinator.committer
    inputs = {
        role.role: [item.artifact_id for item in role.members]
        for role in admission.inputs
    }
    store = ArtifactStore(
        committer.delta_root,
        fs=committer.fs,
        storage_options=committer.storage_options,
        files_root=committer.files_root,
        _view=CuratorView(committer, admission.view),
    )
    replay = ReplaySnapshot.unavailable("curator-storage-fixture")
    if admission.record.replay_of_execution_run_id is not None:
        from artisan.registry.resolve import operation_identity
        from artisan.schemas.execution.replay import ReplayDiagnostic

        identity = operation_identity(type(operation))
        replay = replay.model_copy(
            update={
                "diagnostic": ReplayDiagnostic(
                    source_execution_run_id=admission.record.replay_of_execution_run_id,
                    source_execution_spec_id="0" * 32,
                    source_identity=identity,
                    selected_identity=identity,
                    allow_code_change=False,
                    runner="local",
                    roots={},
                    status="unavailable",
                )
            }
        )
    unit = ExecutionUnit(
        operation=operation,
        inputs=inputs,
        step_number=admission.record.step_number,
        step_run_id=admission.record.step_run_id,
        execution_spec_id=admission.digest,
        replay_snapshot=replay,
        replay_of_execution_run_id=admission.record.replay_of_execution_run_id,
    )
    context = ExecutionContext(
        execution_run_id=uuid4().hex,
        execution_spec_id=admission.digest,
        step_number=admission.record.step_number,
        timestamp_start=datetime.now(UTC),
        worker_id=0,
        artifact_store=store,
        staging_root=coordinator.staging.staging_dir,
        fs=committer.fs,
        operation_name=operation.name,
        operation=operation,
        step_run_id=admission.record.step_run_id,
    )
    with (
        managed_invocation(),
        capture_commands(operation),
        capture_replay(unit),
    ):
        output = operation.execute_curator(
            {
                role: pl.DataFrame(
                    {"artifact_id": ids}, schema={"artifact_id": pl.String}
                )
                for role, ids in inputs.items()
            },
            admission.record.step_number,
            store,
        )
        if (
            isinstance(output, (ArtifactResult, PassthroughResult))
            and not output.success
        ):
            staged = record_execution_failure(
                execution_context=context,
                error=output.error or "Unknown error",
                inputs=inputs,
                timestamp_end=datetime.now(UTC),
                command_recording=command_snapshot(),
                replay_snapshot=replay,
                replay_of_execution_run_id=admission.record.replay_of_execution_run_id,
                params=serialize_params(operation),
            )
        elif isinstance(output, ArtifactStreamResult):
            staged = handle_artifact_stream(
                output,
                context,
                inputs,
                {},
                time.perf_counter(),
                time.perf_counter(),
                serialize_params(operation),
                None,
                None,
            )
        elif isinstance(output, ArtifactResult):
            staged = _handle_artifact_result(
                output,
                operation,
                store,
                context,
                inputs,
                datetime.now(UTC),
                command_snapshot(),
                replay,
                admission.record.replay_of_execution_run_id,
            )
        elif isinstance(output, PassthroughResult):
            staged = _handle_passthrough_result(
                output,
                operation,
                context,
                inputs,
                datetime.now(UTC),
                command_snapshot(),
                replay,
                admission.record.replay_of_execution_run_id,
            )
        else:
            raise TypeError(type(output))
    result = StepResult(
        step_run_id=admission.record.step_run_id,
        step_number=admission.record.step_number,
        step_name=operation.name,
        status=StepStatus.SUCCEEDED if staged.success else StepStatus.FAILED,
        disposition=StepDisposition.EXECUTED if staged.success else None,
        total_count=1,
        succeeded_count=int(staged.success),
        failed_count=int(not staged.success),
        output_roles=frozenset(operation.outputs) if staged.success else frozenset(),
        output_types={
            role: spec.artifact_type for role, spec in operation.outputs.items()
        }
        if staged.success
        else {},
        metadata=output.metadata,
        error=staged.error,
    )
    return staged, result


def main() -> None:
    request = json.load(sys.stdin)
    storage = StorageConfig.model_validate(request["storage"])
    committer = open_committer(request["root"], storage.filesystem(), storage)

    def checkpoint(event: str) -> None:
        if event == request["boundary"]:
            os._exit(41)

    coordinator = CuratorResults(
        RetainedStepTracker(committer, "synthetic-pipeline"),
        request["staging"],
        checkpoint=checkpoint,
    )
    coordinator.finish(
        CuratorAdmissionReference.model_validate(request["reference"]),
        request["execution_run_id"],
        StepResult.model_validate(request["result"]),
    )
    msg = "Curator controller did not reach the selected interruption"
    raise RuntimeError(msg)


if __name__ == "__main__":
    main()
