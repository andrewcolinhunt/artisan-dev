"""Native creator and curator worker fixture for pipeline controller tests."""

from __future__ import annotations

import threading
from concurrent.futures import Future
from dataclasses import dataclass

from artisan.composites.base.compiled import CompiledPlan
from artisan.execution.assignment_transport import AssignmentDispatch
from artisan.execution.curator_worker import AdmittedCurator, execute_admitted_curator
from artisan.execution.recording.parquet_writer import StagingResult
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.runners.assignment import AssignmentSoftware
from artisan.schemas.execution.curator_admission import CuratorAdmissionReference
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.schemas.execution.worker_transport import RuntimeCompatibility
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
)
from artisan.storage.io.retained_commit import RetainedCommitter
from fixtures.retained_execution import NativeRunner, NativeSoftware


@dataclass(frozen=True)
class NativeCuratorSoftware(NativeSoftware):
    """Synthetic qualification for controller tests that never serialize a worker."""

    @property
    def compatibility(self) -> RuntimeCompatibility:
        msg = "The synthetic runner has no object transport"
        raise AssertionError(msg)

    def verify_worker(self) -> None:
        pass


@dataclass
class _CuratorHandle:
    admission: CuratorAdmissionReference
    future: Future[StagingResult]
    worker_owner: None = None

    def done(self) -> bool:
        return self.future.done()

    def result(self, timeout: float | None = None) -> StagingResult:
        return self.future.result(timeout)

    def ownership_state(self) -> OwnershipState:
        return OwnershipState.CLOSED if self.done() else OwnershipState.PENDING

    def cancel(self) -> CancellationAcknowledgement:
        return CancellationAcknowledgement(CancellationStatus.REJECTED)


class NativePipelineRunner(NativeRunner):
    def __init__(self, committer: RetainedCommitter) -> None:
        super().__init__(committer)
        self.curator_launches = 0
        self.curator_resolutions = 0
        self.entered = threading.Event()
        self.release = threading.Event()
        self.release.set()

    def resolve(
        self,
        plan: CompiledPlan,
        runner_resources: RunnerResources,
        *,
        application_modules: tuple[str, ...] = (),
    ) -> NativeSoftware:
        self.entered.set()
        assert self.release.wait(10)
        return super().resolve(plan, runner_resources)

    def resolve_curator(
        self,
        operation: OperationDefinition,
        runner_resources: RunnerResources,
        *,
        application_modules: tuple[str, ...] = (),
    ) -> NativeCuratorSoftware:
        self.curator_resolutions += 1
        return NativeCuratorSoftware()

    def launch_curator(
        self,
        curator: AdmittedCurator,
        *,
        software: AssignmentSoftware,
        dispatch: AssignmentDispatch,
    ) -> _CuratorHandle:
        self.curator_launches += 1
        assert isinstance(software, NativeCuratorSoftware)
        future: Future[StagingResult] = Future()
        try:
            future.set_result(
                execute_admitted_curator(
                    curator, software, dispatch, working_root=dispatch.working_root
                )
            )
        except Exception as error:
            future.set_exception(error)
        return _CuratorHandle(curator.admission, future)
