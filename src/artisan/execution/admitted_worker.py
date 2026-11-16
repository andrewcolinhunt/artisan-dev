"""Execute exact persisted admissions without transporting controller temporary files."""

from __future__ import annotations

import shutil
from pathlib import Path
from tempfile import mkdtemp

from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.execution.admission_localization import localize_admission
from artisan.execution.application_bootstrap import bootstrap_worker_applications
from artisan.execution.assignment import (
    PreparedAssignmentSoftware,
    execute_assignment,
    verify_assignment,
)
from artisan.execution.assignment_transport import (
    AdmittedAssignment,
    AssignmentDispatch,
)
from artisan.execution.composition import PlanExecution
from artisan.execution.exceptions import AssignmentExecutionError
from artisan.execution.recording.delivery_diagnostics import (
    publish_delivery_diagnostics,
)
from artisan.execution.recording.retained_replay import record_replay_configuration
from artisan.execution.worker_directory import resolve_worker_directory
from artisan.schemas.execution.assignment import SealedDelivery, SuccessfulDelivery
from artisan.schemas.execution.delivery_diagnostics import DeliveryDiagnostics
from artisan.storage.io.admission import capture_admission
from artisan.storage.io.assignment_journal import AssignmentJournal
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_seal import stage_retained_result
from artisan.utils.command_supervisor import CommandOwnershipError


def execute_admitted_assignment(
    admitted: AdmittedAssignment,
    software: PreparedAssignmentSoftware,
    dispatch: AssignmentDispatch,
    *,
    working_root: Path | None = None,
) -> SealedDelivery:
    """Verify inputs and seal outputs, retaining scratch if worker closure is unknown."""
    working_root = resolve_worker_directory(
        working_root, preserve_working=dispatch.preserve_working
    )
    verify_assignment(admitted, software)
    bootstrap_worker_applications(
        dispatch.application_modules,
        qualified_modules=software.application_modules,
        compatibility=software.compatibility if dispatch.application_modules else None,
    )
    fs = dispatch.storage.filesystem()
    journal = AssignmentJournal(
        dispatch.delta_root,
        fs=fs,
        storage_options=dispatch.storage.delta_storage_options(),
    )
    state = journal.state(admitted.attempt.assignment)
    if (
        state is None
        or state.attempt != admitted.attempt
        or state.admission_digest != admitted.admission.record_digest
        or state.cancelled
        or state.winner is not None
        or state.reuse is not None
    ):
        msg = "Worker entry requires the exact active persisted admission"
        raise StoreIntegrityError(msg)
    committer = RetainedCommitter(
        dispatch.delta_root,
        dispatch.files_root,
        journal,
        fs=fs,
        storage_options=dispatch.storage.delta_storage_options(),
    )

    def seal(successful: SuccessfulDelivery, result: PlanExecution) -> SealedDelivery:
        return stage_retained_result(
            dispatch.staging_root,
            successful,
            result.projection,
            result.retained_artifacts,
            fs=fs,
            artifact_fs=LocalFileSystem(),
        )

    def record_diagnostics(diagnostic: DeliveryDiagnostics) -> None:
        publish_delivery_diagnostics(diagnostic, delta_root=dispatch.delta_root, fs=fs)

    working_root.mkdir(parents=True, exist_ok=True)
    temporary = Path(mkdtemp(prefix="admitted-inputs-", dir=working_root))
    preserve_inputs = dispatch.preserve_working
    try:
        with capture_admission(
            admitted.admission,
            delta_root=dispatch.delta_root,
            files_root=dispatch.files_root,
            fs=fs,
            verify_producer=committer._verify_producer,
        ) as captured:
            assignment = localize_admission(
                admitted,
                captured,
                files_root=dispatch.files_root,
                directory=temporary / "carriers",
                fs=fs,
            )
        receipt = execute_assignment(
            assignment,
            software,
            seal,
            working_root=working_root,
            runtime_env=dispatch.runtime_env,
            preserve_working=dispatch.preserve_working,
            diagnostic_sink=record_diagnostics,
            diagnostic_storage=dispatch.storage,
        )
        record_replay_configuration(
            receipt,
            dispatch.runtime_env,
            admission=admitted.admission,
            delta_root=dispatch.delta_root,
            fs=fs,
        )
        return receipt
    except AssignmentExecutionError as error:
        if error.delivery is not None:
            record_replay_configuration(
                error.delivery,
                dispatch.runtime_env,
                admission=admitted.admission,
                delta_root=dispatch.delta_root,
                fs=fs,
            )
        raise
    except CommandOwnershipError:
        # An unresolved child may still be reading these localized carriers.
        preserve_inputs = True
        raise
    finally:
        if not preserve_inputs:
            shutil.rmtree(temporary)
