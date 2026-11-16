"""Partial startup retains unit and transport authority until cleanup settles."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Literal, cast
from unittest.mock import Mock

import pytest

from artisan.execution.assignment import Assignment
from artisan.execution.local_assignment import PreparedLocalBindings
from artisan.execution.local_packing import (
    LocalAssignmentRequest,
    LocalPackStartupError,
    launch_local_assignments,
)
from artisan.execution.local_profile import LocalWorkerProfile
from artisan.execution.local_worker import LocalAssignmentHandle
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
)
from artisan.utils.command_supervisor import CommandSupervisor, OwnershipOutcome


@pytest.fixture
def assignment_request(monkeypatch: pytest.MonkeyPatch) -> LocalAssignmentRequest:
    """Exercise real local threads without preparing software or starting processes."""
    bindings = Mock(spec=PreparedLocalBindings)
    bindings.worker = Mock()
    bindings.worker.digest = "qualified-worker"
    bindings.compatibility = Mock()
    bindings.launcher = None
    bindings.native_gpu = None
    bindings.worker_is_observed = False
    bindings.worker_profile = Mock(spec=LocalWorkerProfile)
    bindings.worker_profile.observed = False
    monkeypatch.setattr(
        "artisan.execution.assignment.verify_assignment", lambda *_: None
    )
    monkeypatch.setattr(LocalAssignmentHandle, "_write_payload", lambda *_: None)
    return LocalAssignmentRequest(Mock(spec=Assignment), bindings, Mock())


def _owner(closure: Literal["closed", "unknown"] = "closed") -> Mock:
    owner = Mock(spec=CommandSupervisor)
    owner.attachment = {"socket": "owned", "secret": "synthetic", "scope": "root"}
    owner.finish.return_value = OwnershipOutcome(closure)
    return owner


def test_later_startup_failure_retains_paused_transport_until_thread_finishes(
    tmp_path: Path,
    assignment_request: LocalAssignmentRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered, release = threading.Event(), threading.Event()
    pack_threads: list[threading.Thread] = []
    original_start = threading.Thread.start

    def start(thread: threading.Thread) -> None:
        if thread.name == "artisan-local-pack":
            pack_threads.append(thread)
            if len(pack_threads) == 2:
                assert entered.wait(5)
                msg = "Second pack thread could not start"
                raise RuntimeError(msg)
        original_start(thread)

    def environment(*_args: object) -> Mock:
        entered.set()
        assert release.wait(5)
        return Mock()

    cast(
        Mock, assignment_request.bindings.worker_profile.environment
    ).side_effect = environment
    monkeypatch.setattr(CommandSupervisor, "start", _owner)
    monkeypatch.setattr(threading.Thread, "start", start)
    monkeypatch.setattr("artisan.execution.local_packing.CLEANUP_SECONDS", 0.1)
    launch = Mock(side_effect=AssertionError("Stopped pack attempted a new launch"))
    monkeypatch.setattr("artisan.execution.local_packing.ManagedCommand", launch)
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(
            launch_local_assignments,
            [assignment_request, assignment_request],
            working_root=tmp_path,
            units_per_worker=1,
        )
        try:
            with pytest.raises(LocalPackStartupError) as caught:
                pending.result(timeout=5)
            first = caught.value.worker_owners[0]
            assert first.ownership_state() == OwnershipState.UNKNOWN
            assert pack_threads[0].is_alive()
            assert list(tmp_path.glob("artisan-pack-*"))
            assert any(
                outcome.state == "unknown"
                for outcome in caught.value.transport_outcomes
            )
        finally:
            release.set()
            for thread in pack_threads:
                if thread.ident is not None:
                    thread.join(timeout=5)
                    assert not thread.is_alive()
    launch.assert_not_called()
    assert first.ownership_state() == OwnershipState.UNKNOWN
    assert list(tmp_path.glob("artisan-pack-*"))


@pytest.mark.parametrize("cleanup", ["raises", "unknown"])
def test_pack_abort_preserves_all_handles_when_one_cancellation_is_unresolved(
    tmp_path: Path,
    assignment_request: LocalAssignmentRequest,
    monkeypatch: pytest.MonkeyPatch,
    cleanup: str,
) -> None:
    cancelled: list[LocalAssignmentHandle] = []
    original_cancel = LocalAssignmentHandle.cancel
    original_start = threading.Thread.start

    def cancel(handle: LocalAssignmentHandle) -> CancellationAcknowledgement:
        cancelled.append(handle)
        outcome = original_cancel(handle)
        if len(cancelled) == 1:
            if cleanup == "raises":
                msg = "Unit cancellation reporting failed"
                raise OSError(msg)
            return CancellationAcknowledgement(CancellationStatus.UNKNOWN)
        return outcome

    def start(thread: threading.Thread) -> None:
        if thread.name == "artisan-local-pack":
            msg = "Pack thread could not start"
            raise RuntimeError(msg)
        original_start(thread)

    monkeypatch.setattr(CommandSupervisor, "start", _owner)
    monkeypatch.setattr(LocalAssignmentHandle, "cancel", cancel)
    monkeypatch.setattr(threading.Thread, "start", start)
    with pytest.raises(LocalPackStartupError) as caught:
        launch_local_assignments(
            [assignment_request, assignment_request],
            working_root=tmp_path,
            units_per_worker=2,
        )
    error = caught.value
    assert len(error.handles) == 2
    assert all(handle in cancelled for handle in error.handles)
    assert all(handle.done() for handle in error.handles)
    assert len(error.worker_owners) == 1
    assert error.worker_owners[0].ownership_state() == OwnershipState.UNKNOWN
    assert any(outcome.state == "unknown" for outcome in error.transport_outcomes)


@pytest.mark.parametrize("cleanup", ["unknown", "raises"])
def test_failed_unit_thread_start_retains_its_distinct_cleanup_authority(
    tmp_path: Path,
    assignment_request: LocalAssignmentRequest,
    monkeypatch: pytest.MonkeyPatch,
    cleanup: str,
) -> None:
    pack_owner, unit_owner = _owner(), _owner("unknown")
    if cleanup == "raises":
        unit_owner.finish.side_effect = OSError("Unit owner collection failed")
    monkeypatch.setattr(
        CommandSupervisor, "start", Mock(side_effect=[pack_owner, unit_owner])
    )
    original_start = threading.Thread.start

    def start(thread: threading.Thread) -> None:
        if thread.name == "artisan-local-call":
            msg = "Unit thread could not start"
            raise RuntimeError(msg)
        original_start(thread)

    monkeypatch.setattr(threading.Thread, "start", start)
    with pytest.raises(LocalPackStartupError) as caught:
        launch_local_assignments(
            [assignment_request], working_root=tmp_path, units_per_worker=1
        )
    error = caught.value
    assert len(error.handles) == 1
    retained = error.handles[0]
    assert retained._owner is unit_owner
    assert retained.done()
    assert retained.ownership_state() == OwnershipState.UNKNOWN
    assert retained.cancel().status == CancellationStatus.UNKNOWN
    assert retained.worker_owner in error.worker_owners
    assert any(outcome.state == "unknown" for outcome in error.transport_outcomes)
    unit_owner.request_close.assert_called()
