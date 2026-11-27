"""Qualification-only actual-handle interception; it never invents a receipt."""

from __future__ import annotations

import json
import time
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from typing import Literal
from uuid import uuid4

from artisan_modal import ModalRunner
from artisan_modal.pack_owner import ModalPackOwner

from artisan.orchestration.runner_api import (
    AdmittedAssignment,
    AdmittedCurator,
    AssignmentDispatch,
    AssignmentHandle,
    AssignmentLaunchError,
    AssignmentSoftware,
    CancellationAcknowledgement,
    CancellationStatus,
    CompiledPlan,
    CuratorHandle,
    OperationDefinition,
    OwnershipState,
    RunnerResources,
)


def wait_closed(handles: Sequence[AssignmentHandle], deadline: float) -> None:
    while True:
        states = [handle.ownership_state() for handle in handles]
        if all(state == OwnershipState.CLOSED for state in states):
            return
        if OwnershipState.UNKNOWN in states or time.monotonic() >= deadline:
            msg = "Qualification owners did not establish closure"
            raise AssertionError(msg)
        time.sleep(0.05)


@dataclass
class ControlledRunner:
    """Observe real handle cancellation separately from controller failure policy."""

    delegate: ModalRunner
    mode: Literal["sibling", "unknown", "observe"]
    evidence: dict[str, object] = field(default_factory=dict)
    name: str = "modal"

    def resolve(
        self,
        plan: CompiledPlan,
        runner_resources: RunnerResources,
        *,
        application_modules: tuple[str, ...] = (),
    ) -> AssignmentSoftware:
        return self.delegate.resolve(
            plan, runner_resources, application_modules=application_modules
        )

    def resolve_curator(
        self,
        operation: OperationDefinition,
        runner_resources: RunnerResources,
        *,
        application_modules: tuple[str, ...] = (),
    ) -> AssignmentSoftware:
        return self.delegate.resolve_curator(
            operation, runner_resources, application_modules=application_modules
        )

    def launch_curator(
        self,
        curator: AdmittedCurator,
        *,
        software: AssignmentSoftware,
        dispatch: AssignmentDispatch,
    ) -> CuratorHandle:
        return self.delegate.launch_curator(
            curator, software=software, dispatch=dispatch
        )

    def launch(
        self,
        assignments: Sequence[AdmittedAssignment],
        *,
        software: AssignmentSoftware,
        dispatch: AssignmentDispatch,
    ) -> tuple[AssignmentHandle, ...]:
        assert len(assignments) == 2
        nonce = uuid4().hex
        root = dispatch.staging_root.rstrip("/") + "/qualification-readiness/" + nonce
        channel = {
            "nonce": nonce,
            "root": root,
            "options": dispatch.storage.options,
            "attempts": {
                str(index): item.attempt.model_dump(mode="json")
                for index, item in enumerate(assignments)
            },
        }
        scoped = replace(
            dispatch,
            runtime_env={
                **dispatch.runtime_env,
                "ARTISAN_QUAL_CHANNEL": json.dumps(channel),
            },
        )
        handles = self.delegate.launch(assignments, software=software, dispatch=scoped)
        self.evidence["handles"] = handles
        self.evidence["readiness_root"] = root
        if self.mode == "observe":
            return handles
        try:
            assert handles[0].worker_owner is handles[1].worker_owner
            fs = dispatch.storage.filesystem()
            marker = root + "/0.json"
            deadline = time.monotonic() + 120
            while not fs.exists(marker):
                if handles[0].done() or time.monotonic() >= deadline:
                    msg = "Exact first unit did not publish its readiness"
                    raise AssertionError(msg)
                time.sleep(0.1)
            with fs.open(marker, "rb") as stream:
                encoded = stream.read(65537)
            assert len(encoded) <= 65536
            observed = json.loads(encoded)
            assert observed["nonce"] == nonce
            assert observed["attempt"] == assignments[0].attempt.model_dump(mode="json")
            acknowledgement = handles[0].cancel()
            self.evidence.update(
                nonce=nonce,
                first_attempt=assignments[0].attempt.model_dump(mode="json"),
                cancellation=acknowledgement.status.value,
                readiness=observed,
            )
            if self.mode == "sibling":
                assert acknowledgement.status == CancellationStatus.CONFIRMED
                deadline = time.monotonic() + 120
                receipt = handles[1].result(
                    timeout=max(0.0, deadline - time.monotonic())
                )
                assert receipt.delivery.attempt == assignments[1].attempt
                wait_closed(handles, deadline)
                self.evidence["sibling_receipt"] = receipt.model_dump(mode="json")
                self.evidence["owners_closed"] = True
            else:
                assert acknowledgement.status == CancellationStatus.UNKNOWN
                assert handles[0].ownership_state() == OwnershipState.UNKNOWN
                owner = handles[0].worker_owner
                assert isinstance(owner, ModalPackOwner)
                finished = time.monotonic() + 60
                while owner.outcome(0) is None:
                    if time.monotonic() >= finished:
                        message = (
                            "Native UNKNOWN did not return bounded worker evidence"
                        )
                        raise AssertionError(message)
                    time.sleep(0.1)
                outcome = owner.outcome(0)
                assert outcome is not None
                assert outcome.outcome is not None
                assert outcome.outcome.ownership == OwnershipState.UNKNOWN
                sibling = owner.outcome(1)
                assert sibling is None or (sibling.skipped and sibling.outcome is None)
                assert not fs.exists(root + "/1.json")
                self.evidence["unknown_retained"] = True
                self.evidence["native_returned_unknown"] = True
                self.evidence["queued_sibling_not_entered"] = True
        except BaseException as error:
            message = "Qualification interception failed after launch"
            raise AssignmentLaunchError(
                message,
                handles=handles,
                worker_owners=tuple(
                    dict.fromkeys(
                        handle.worker_owner
                        for handle in handles
                        if handle.worker_owner is not None
                    )
                ),
                cancellation=CancellationAcknowledgement(CancellationStatus.UNKNOWN),
            ) from error
        return handles
