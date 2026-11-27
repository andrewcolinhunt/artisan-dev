"""A completed local future does not prove that its command owner has closed."""

from __future__ import annotations

from pathlib import Path
from typing import Literal
from unittest.mock import Mock

import pytest

from artisan.execution.assignment import Assignment
from artisan.execution.local_assignment import PreparedLocalBindings
from artisan.execution.local_worker import LocalAssignmentHandle
from artisan.schemas.execution.worker_ownership import OwnershipState, WorkerOwner
from artisan.schemas.orchestration.step_lifecycle import CancellationStatus
from artisan.utils.command_supervisor import (
    CommandOwnershipError,
    CommandSupervisor,
    OwnershipOutcome,
)


@pytest.mark.parametrize("closure", ["closed", "unknown"])
def test_failed_future_preserves_actual_cleanup_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    closure: Literal["closed", "unknown"],
) -> None:
    owner = Mock(spec=CommandSupervisor)
    owner.finish.return_value = OwnershipOutcome(closure)
    monkeypatch.setattr(CommandSupervisor, "start", lambda: owner)
    monkeypatch.setattr(
        "artisan.execution.assignment.verify_assignment", lambda *_: None
    )

    def fail(handle: LocalAssignmentHandle, directory: Path) -> None:
        (directory / "worker-evidence.txt").write_text("Unsettled worker evidence")
        msg = "Synthetic worker failure"
        raise RuntimeError(msg)

    monkeypatch.setattr(LocalAssignmentHandle, "_invoke", fail)
    working_root = tmp_path / "worker"
    working_root.mkdir()
    handle = LocalAssignmentHandle(
        Mock(spec=Assignment),
        Mock(spec=PreparedLocalBindings),
        Mock(),
        working_root=working_root,
        runtime_env={},
    )
    expected = CommandOwnershipError if closure == "unknown" else RuntimeError
    with pytest.raises(expected):
        handle.result(timeout=5)
    assert handle.done()
    assert handle.ownership_state() == OwnershipState(closure)
    outcome = handle.cancel()
    assert outcome.status == (
        CancellationStatus.UNKNOWN
        if closure == "unknown"
        else CancellationStatus.REJECTED
    )
    assert bool(list(working_root.iterdir())) is (closure == "unknown")
    assert handle.cancel() == outcome


@pytest.mark.parametrize("transport", list(OwnershipState))
def test_completed_unit_retains_shared_transport_ownership(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, transport: OwnershipState
) -> None:
    owner = Mock(spec=CommandSupervisor)
    owner.finish.return_value = OwnershipOutcome("closed")
    monkeypatch.setattr(CommandSupervisor, "start", lambda: owner)
    monkeypatch.setattr(
        "artisan.execution.assignment.verify_assignment", lambda *_: None
    )
    receipt = Mock()
    monkeypatch.setattr(LocalAssignmentHandle, "_invoke", lambda *_: receipt)
    state = OwnershipState.PENDING
    transport_owner = Mock(spec=WorkerOwner)
    transport_owner.ownership_state.side_effect = lambda: state
    handle = LocalAssignmentHandle(
        Mock(spec=Assignment),
        Mock(spec=PreparedLocalBindings),
        Mock(),
        working_root=tmp_path,
        runtime_env={},
        worker_owner=transport_owner,
    )
    assert handle.result(timeout=5) is receipt
    assert handle.done()
    assert handle.ownership_state() == OwnershipState.PENDING
    state = transport
    assert handle.ownership_state() == transport
    assert handle.cancel().status == CancellationStatus.REJECTED
    owner.request_close.assert_not_called()
    transport_owner.request_close.assert_not_called()


def test_scratch_cleanup_failure_remains_unknown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner = Mock(spec=CommandSupervisor)
    owner.finish.return_value = OwnershipOutcome("closed")
    monkeypatch.setattr(CommandSupervisor, "start", lambda: owner)
    monkeypatch.setattr(
        "artisan.execution.assignment.verify_assignment", lambda *_: None
    )
    monkeypatch.setattr(LocalAssignmentHandle, "_invoke", lambda *_: Mock())
    with monkeypatch.context() as cleanup:
        cleanup.setattr(
            "artisan.execution.local_call.shutil.rmtree",
            Mock(side_effect=OSError("Synthetic cleanup failure")),
        )
        handle = LocalAssignmentHandle(
            Mock(spec=Assignment),
            Mock(spec=PreparedLocalBindings),
            Mock(),
            working_root=tmp_path,
            runtime_env={},
        )
        with pytest.raises(CommandOwnershipError, match="scratch cleanup"):
            handle.result(timeout=5)
    assert handle.ownership_state() == OwnershipState.UNKNOWN
    assert handle.cancel().status == CancellationStatus.UNKNOWN
    assert list(tmp_path.glob("artisan-assignment-*"))
