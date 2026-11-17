"""Run one qualified, persisted curator admission through shared worker ownership."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from artisan.errors import StoreIntegrityError
from artisan.execution.application_bootstrap import bootstrap_worker_applications
from artisan.execution.assignment_transport import AssignmentDispatch
from artisan.execution.curator_definition import verify_curator_definition
from artisan.execution.executors.curator import run_curator_flow
from artisan.execution.local_assignment import PreparedLocalBindings
from artisan.execution.local_call import InertWorkerCall, LocalCallHandle
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.recording.curator_replay import capture_curator_replay
from artisan.execution.recording.durable import configuration_recorder
from artisan.execution.recording.parquet_writer import StagingResult
from artisan.execution.recording.replay_snapshot import runtime_roots
from artisan.execution.runtime_injections import runtime_injections
from artisan.execution.worker_bootstrap import serialize_worker_call
from artisan.execution.worker_directory import resolve_worker_directory
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.registry.resolve import operation_identity
from artisan.schemas.execution.curator_admission import (
    CuratorAdmission,
    CuratorAdmissionReference,
)
from artisan.schemas.execution.replay import ReplayAssociation, ReplayDiagnostic
from artisan.schemas.execution.worker_config import WorkerConfig
from artisan.schemas.execution.worker_transport import RuntimeCompatibility
from artisan.schemas.orchestration.step_lifecycle import CancellationStatus, StepStatus
from artisan.storage.cache.curator_dependencies import CuratorReadRecorder
from artisan.storage.core.curator_view import CuratorView
from artisan.storage.io.assignment_journal import AssignmentJournal
from artisan.storage.io.curator_admission import read_curator_admission
from artisan.storage.io.curator_control import verify_curator_inputs
from artisan.storage.io.curator_plan import read_curator_plan
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.utils.command_supervisor import CommandOwnershipError
from artisan.utils.path import uri_join, uri_parent


@dataclass(frozen=True)
class AdmittedCurator:
    """Trusted live definition paired with an exact persisted curator admission."""

    operation: OperationDefinition
    admission: CuratorAdmissionReference
    diagnostic: ReplayDiagnostic | None = None
    sensitive_values: tuple[str, ...] = field(default=(), repr=False)


class PreparedCuratorSoftware(Protocol):
    """Qualified provider software, independent of its node-local launch profile."""

    @property
    def software_digest(self) -> str: ...

    @property
    def software_cacheable(self) -> bool: ...

    @property
    def compatibility(self) -> RuntimeCompatibility: ...

    @property
    def application_modules(self) -> tuple[str, ...]: ...

    def verify_worker(self) -> None:
        """Remeasure the worker and all provider additions before authored entry."""
        ...


def _capture_entry(
    curator: AdmittedCurator,
    software: PreparedCuratorSoftware,
    dispatch: AssignmentDispatch,
) -> tuple[CuratorAdmission, RetainedCommitter]:
    fs = dispatch.storage.filesystem()
    admission = read_curator_admission(
        curator.admission, delta_root=dispatch.delta_root, fs=fs
    )
    if admission.definition is None or (
        admission.software_digest != software.software_digest
        or admission.software_cacheable != software.software_cacheable
    ):
        msg = "Curator worker differs from its qualified admitted software"
        raise StoreIntegrityError(msg)
    verify_curator_definition(curator.operation, admission.definition)
    diagnostic = curator.diagnostic
    if diagnostic is not None:
        diagnostic = ReplayDiagnostic.model_validate(diagnostic.model_dump())
    if diagnostic is None:
        if admission.record.replay_of_execution_run_id is not None:
            msg = "Diagnostic curator admission is missing its replay selection"
            raise StoreIntegrityError(msg)
    elif (
        diagnostic.source_execution_run_id
        != admission.record.replay_of_execution_run_id
        or diagnostic.selected_identity != operation_identity(type(curator.operation))
        or diagnostic.selected_software_digest != admission.software_digest
        or diagnostic.source_software_digest is None
        or diagnostic.runner != admission.record.compute_backend
    ):
        msg = "Curator replay selection differs from its admitted worker"
        raise StoreIntegrityError(msg)
    software.verify_worker()
    bootstrap_worker_applications(
        dispatch.application_modules,
        qualified_modules=software.application_modules,
        compatibility=software.compatibility if dispatch.application_modules else None,
    )
    journal = AssignmentJournal(
        dispatch.delta_root,
        fs=fs,
        storage_options=dispatch.storage.delta_storage_options(),
    )
    committer = RetainedCommitter(
        dispatch.delta_root,
        dispatch.files_root,
        journal,
        fs=fs,
        storage_options=dispatch.storage.delta_storage_options(),
    )
    tracker = RetainedStepTracker(committer, pipeline_run_id="")
    state = tracker.current_state(curator.admission.step_run_id)
    if (
        state.status != StepStatus.RUNNING
        or state.step_spec_id != admission.digest
        or state.cancellation_status not in {None, CancellationStatus.REJECTED}
        or read_curator_plan(dispatch.delta_root, state.step_run_id, fs) is not None
    ):
        msg = "Curator worker requires the exact active persisted admission"
        raise StoreIntegrityError(msg)
    verify_curator_inputs(admission, committer)
    return admission, committer


def execute_admitted_curator(
    curator: AdmittedCurator,
    software: PreparedCuratorSoftware,
    dispatch: AssignmentDispatch,
    *,
    working_root: Path | None = None,
) -> StagingResult:
    """Verify authority before authored code, then stage one complete stream."""
    working_root = resolve_worker_directory(
        working_root, preserve_working=dispatch.preserve_working
    )
    admission, committer = _capture_entry(curator, software, dispatch)
    unit = ExecutionUnit(
        operation=curator.operation,
        inputs={
            role.role: [item.artifact_id for item in role.members]
            for role in admission.inputs
        },
        associated_inputs=[
            ReplayAssociation(
                primary_id=item.primary_id,
                artifact_type=item.artifact_type,
                artifact_ids=list(item.artifact_ids),
            )
            for item in admission.associations
        ],
        execution_spec_id=admission.digest,
        step_number=admission.record.step_number,
        step_run_id=admission.record.step_run_id,
        replay_of_execution_run_id=admission.record.replay_of_execution_run_id,
        replay_sensitive_values=(
            *curator.sensitive_values,
            *dispatch.runtime_env.values(),
        ),
    )
    worker_config = WorkerConfig(
        delta_root=dispatch.delta_root,
        files_root=dispatch.files_root,
        staging_root=dispatch.staging_root,
        working_root=str(working_root),
        storage=dispatch.storage,
        failure_logs_root=uri_join(uri_parent(dispatch.delta_root), "logs", "failures"),
        preserve_staging=dispatch.preserve_staging,
        preserve_working=dispatch.preserve_working,
        worker_id_env_var=None,
        shared_filesystem=True,
        compute_backend_name=admission.record.compute_backend,
        application_modules=dispatch.application_modules,
    )
    unit.replay_snapshot = capture_curator_replay(
        curator.operation,
        admission,
        worker_config,
        dispatch.runtime_env,
        sensitive_values=unit.replay_sensitive_values,
    )
    if curator.diagnostic is not None:
        recorder = configuration_recorder(curator.operation)
        recorder._learn_json(dispatch.storage)
        recorder.add_sensitive_data(unit.replay_sensitive_values)
        unit.replay_snapshot = unit.replay_snapshot.model_copy(
            update={
                "diagnostic": curator.diagnostic.model_copy(
                    update={
                        "roots": recorder.sanitize_data(runtime_roots(worker_config))
                    }
                )
            }
        )
    view = CuratorView(committer, admission.view)
    with runtime_injections(dispatch.runtime_env):
        result = run_curator_flow(
            unit,
            worker_config,
            curator_view=view,
            read_recorder=CuratorReadRecorder(view, admission),
        )
    if (
        result.cancellation_acknowledgement is not None
        and result.cancellation_acknowledgement.status == CancellationStatus.UNKNOWN
    ):
        msg = "Curator command ownership is unknown"
        raise CommandOwnershipError(msg)
    return result


class LocalCuratorHandle(LocalCallHandle[StagingResult]):
    """Bind a whole curator stream to the same supervised call lifecycle."""

    def __init__(
        self,
        curator: AdmittedCurator,
        software: PreparedLocalBindings,
        dispatch: AssignmentDispatch,
    ) -> None:
        self._curator = curator
        self._software = software
        self._storage_dispatch = dispatch
        dispatch.working_root.mkdir(parents=True, exist_ok=True)
        super().__init__(
            software.worker_profile,
            working_root=dispatch.working_root,
            runtime_env=dispatch.runtime_env,
            preserve_working=dispatch.preserve_working,
        )

    @property
    def admission(self) -> CuratorAdmissionReference:
        return self._curator.admission

    def _preflight(self) -> None:
        _capture_entry(self._curator, self._software, self._storage_dispatch)

    def _write_payload(self, directory: Path) -> None:
        from artisan.execution.curator_reply import CuratorReplyContext

        call = serialize_worker_call(
            execute_admitted_curator,
            compatibility=self._software.compatibility,
            args=(self._curator, self._software, self._storage_dispatch),
            kwargs={"working_root": directory / "worker"},
            application_modules=self._software.application_modules,
        )
        self._check_cancelled()
        context = CuratorReplyContext(
            admission=self.admission,
            payload_sha256=call.header.payload_sha256,
            sensitive_values=self.sensitive_values,
        )
        InertWorkerCall(
            call.header, call.payload, context.model_dump_json().encode()
        ).write(directory)

    def _decode_reply(self, encoded: bytes) -> StagingResult:
        from artisan.execution.curator_reply import CuratorReply

        return CuratorReply.model_validate_json(encoded).result(
            self.admission, sensitive_values=self.sensitive_values
        )
