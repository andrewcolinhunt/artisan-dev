"""Re-enter the exact live SDK payload before acceptance, without inventing results."""

from __future__ import annotations

import time
from collections.abc import Sequence
from pathlib import Path

from artisan_modal import ModalRunner
from artisan_modal.handles import ModalAssignmentHandle
from artisan_modal.pack_control import ModalPack
from artisan_modal.pack_owner import ModalPackOwner
from final_cases import BATCH, metrics, seed
from final_control import ControlledRunner, wait_closed
from modal_qualification_app import NativeProbe

from artisan.errors import StoreIntegrityError
from artisan.orchestration import PipelineManager
from artisan.orchestration.runner_api import (
    AdmittedAssignment,
    AssignmentDispatch,
    AssignmentHandle,
    AssignmentLaunchError,
    AssignmentSoftware,
    CancellationAcknowledgement,
    CancellationStatus,
    RunnerResources,
    SealedDelivery,
)
from artisan.schemas.orchestration.step_lifecycle import StepStatus


class RedeliveryRunner(ControlledRunner):
    """Delay controller acceptance until two SDK entries have sealed real deliveries."""

    def __init__(self, runner: ModalRunner) -> None:
        super().__init__(runner, "observe")
        self.redelivered: tuple[ModalAssignmentHandle, ...] = ()

    def launch(
        self,
        assignments: Sequence[AdmittedAssignment],
        *,
        software: AssignmentSoftware,
        dispatch: AssignmentDispatch,
    ) -> tuple[AssignmentHandle, ...]:
        handles = super().launch(assignments, software=software, dispatch=dispatch)
        owners = []
        try:
            first = handles[0].worker_owner
            assert isinstance(first, ModalPackOwner)
            owners.append(first)
            deadline = time.monotonic() + 180
            for handle in handles:
                handle.result(timeout=max(0, deadline - time.monotonic()))
            wait_closed(handles, deadline)
            request = first._request
            assert isinstance(request, ModalPack)
            second = ModalPackOwner(
                first._function,
                lambda: request,
                count=len(handles),
                sensitive_values=request.sensitive_values,
            )
            owners.append(second)
            self.redelivered = tuple(
                ModalAssignmentHandle(
                    item.attempt, second, index, request.sensitive_values
                )
                for index, item in enumerate(assignments)
            )
            second.start()
            deadline = time.monotonic() + 180
            for handle in self.redelivered:
                handle.result(timeout=max(0, deadline - time.monotonic()))
            wait_closed(self.redelivered, deadline)
            original = first.outcome(0)
            repeated = second.outcome(0)
            assert original is not None
            assert repeated is not None
            assert original.pack_id == repeated.pack_id
            assert original.request_sha256 == repeated.request_sha256
            assert original.entry_id != repeated.entry_id
            core_entries = []
            for index in range(len(handles)):
                for owner in (first, second):
                    progress = owner.outcome(index)
                    assert progress is not None
                    assert progress.outcome is not None
                    core_entries.append(str(progress.outcome.entry_id))
            assert len(set(core_entries)) == 2 * len(handles)
            self.evidence["core_entries"] = core_entries
            self.evidence["pack_entries"] = [
                str(original.entry_id),
                str(repeated.entry_id),
            ]
            self.evidence["owner_ids"] = [
                str(owner.observations[0].owner_id) for owner in owners
            ]
            return handles
        except BaseException as error:
            message = "Qualification redelivery did not settle both real SDK entries"
            raise AssignmentLaunchError(
                message,
                handles=(*handles, *self.redelivered),
                worker_owners=owners,
                cancellation=CancellationAcknowledgement(CancellationStatus.UNKNOWN),
            ) from error


def redelivery(
    pipeline: PipelineManager,
    runner: ModalRunner,
    _manifest: Path,
    resources: RunnerResources,
) -> dict[str, object]:
    source = seed(pipeline)
    repeated = RedeliveryRunner(runner)
    result = pipeline.run(
        NativeProbe,
        inputs={"source": source},
        step_runner=repeated,
        runner_resources=resources,
        batch_strategy=BATCH,
        skip_cache=True,
    )
    assert result.status == StepStatus.SUCCEEDED, result.error
    first = pipeline.executions[-1].handles
    original = []
    for handle in first:
        receipt = handle.result()
        assert isinstance(receipt, SealedDelivery)
        original.append(receipt)
    second = [handle.result() for handle in repeated.redelivered]
    assert len(original) == len(second) == 2
    before = metrics(pipeline, result.step_number)
    for selected, duplicate in zip(original, second, strict=True):
        assert selected.delivery.attempt == duplicate.delivery.attempt
        assert selected.delivery.delivery_id != duplicate.delivery.delivery_id
        assert selected.seal_uri != duplicate.seal_uri
        try:
            pipeline.committer.accept(duplicate)
        except StoreIntegrityError:
            pass
        else:
            message = "A second delivery replaced the original accepted winner"
            raise AssertionError(message)
        assert pipeline.committer.journal.require_winner(selected).receipt == selected
    assert metrics(pipeline, result.step_number) == before
    return {
        "status": result.status.value,
        "pack_entries": repeated.evidence["pack_entries"],
        "owner_ids": repeated.evidence["owner_ids"],
        "core_entries": repeated.evidence["core_entries"],
        "sdk_inputs": 2,
        "original_receipts": [receipt.model_dump(mode="json") for receipt in original],
        "repeated_receipts": [receipt.model_dump(mode="json") for receipt in second],
        "second_acceptance_rejected": True,
        "accepted_outputs_unchanged": True,
        "scope": "repeated computation is allowed; acceptance remains one exact winner",
    }
