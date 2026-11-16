"""Actual core lifetime cancellation inside the filesystem-backed SDK pack loop."""

from __future__ import annotations

import os
import sys
import threading
from concurrent.futures import CancelledError
from pathlib import Path
from types import ModuleType
from unittest.mock import Mock

import pytest
from artisan_modal import pack_worker, transport
from artisan_modal.pack_control import (
    CancelRequest,
    ModalPack,
    PackChannel,
    PackResult,
    UnitProgress,
)
from artisan_modal.pack_owner import ModalPackOwner
from artisan_modal.transport import ModalCall
from fsspec import AbstractFileSystem
from test_pack_owner import Function, finish

from artisan.execution import inert_worker
from artisan.execution.worker_directory import resolve_worker_directory
from artisan.orchestration.runner_api import (
    CancellationStatus,
    CommandOwnershipError,
    CuratorReply,
    CuratorReplyContext,
    InertCallControl,
    InertCallResult,
    OwnershipState,
    StagingResult,
)
from artisan.utils.command_supervisor import (
    ManagedCommand,
    attached_worker_unit,
    managed_invocation,
)


@pytest.fixture
def real_core(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(inert_worker, "_lease", threading.Lock())
    monkeypatch.setattr(inert_worker, "_retained", [])
    monkeypatch.setattr(transport, "WORKING_ROOT", tmp_path / "workers")
    monkeypatch.setattr(transport, "verify_worker_activation", Mock())
    monkeypatch.setattr(transport, "verify_modal_function", Mock())
    monkeypatch.setattr(transport, "verify_modal_runtime", Mock())


@pytest.mark.usefixtures("control_fs", "pack_runtime", "real_core")
def test_real_detached_command_cancel_preserves_next_siblings(
    pack_request: ModalPack, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    ready = threading.Event()
    children = []
    seen = []

    def execute(directory: Path, *, attachment: dict[str, str]) -> bytes:
        context = CuratorReplyContext.model_validate_json(
            (directory / "reply-context.json").read_bytes()
        )
        seen.append(context.admission.step_run_id)
        program = (
            "import subprocess,sys; subprocess.run([sys.executable,'-c',"
            "'import os,time; os.setsid(); print(os.getpid(),flush=True); time.sleep(20)'])"
            if len(seen) == 1
            else "print('sibling completed')"
        )
        with attached_worker_unit(attachment), managed_invocation():
            command = ManagedCommand(
                attachment,
                [sys.executable, "-c", program],
                dict(os.environ),
                str(resolve_worker_directory(None)),
                merged=False,
                external=False,
            )
            try:

                def output(line: str) -> None:
                    if line.strip().isdigit():
                        children.append(int(line.strip()))
                        ready.set()

                command.communicate(on_line=output)
            finally:
                command.dispose()
        return (
            CuratorReply.staged(
                context, StagingResult(success=False, error="fixture result")
            )
            .model_dump_json()
            .encode()
        )

    monkeypatch.setattr(inert_worker, "execute_call_directory", execute)
    function = Function()
    owner = ModalPackOwner(function, lambda: pack_request, count=3)
    owner.start()
    assert ready.wait(4)
    assert owner.cancel(0).status == CancellationStatus.CONFIRMED
    with pytest.raises(CancelledError):
        owner.result(0, 0)
    assert owner.result(1, 4)
    assert owner.result(2, 4)
    finish(owner, function)
    assert owner.ownership_state() == OwnershipState.CLOSED
    assert len(seen) == 3
    assert not function.cancelled.is_set()
    with pytest.raises(ProcessLookupError):
        os.kill(children[0], 0)
    assert not list((tmp_path / "workers").glob("call-*"))


@pytest.mark.usefixtures("control_fs", "pack_runtime", "real_core")
def test_real_blocked_native_deadline_is_sticky_and_stops_later_units(
    pack_request: ModalPack, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, release = threading.Event(), threading.Event()
    seen = []

    def execute(directory: Path, **_kwargs: object) -> bytes:
        seen.append(directory)
        entered.set()
        assert release.wait(4)
        context = CuratorReplyContext.model_validate_json(
            (directory / "reply-context.json").read_bytes()
        )
        return (
            CuratorReply.staged(
                context, StagingResult(success=False, error="fixture result")
            )
            .model_dump_json()
            .encode()
        )

    monkeypatch.setattr(inert_worker, "execute_call_directory", execute)
    function = Function()
    owner = ModalPackOwner(function, lambda: pack_request, count=3)
    owner.start()
    try:
        assert entered.wait(3)
        observed = threading.Event()
        original = inert_worker.InertCallControl.request_close

        def close(control: InertCallControl, deadline: float) -> None:
            original(control, deadline)
            observed.set()

        monkeypatch.setattr(inert_worker.InertCallControl, "request_close", close)
        PackChannel(pack_request).cancel(0, 0)
        assert observed.wait(2)
    finally:
        release.set()
    for index in range(3):
        with pytest.raises(CommandOwnershipError):
            owner.result(index, 4)
    finish(owner, function)
    assert len(seen) == 1
    assert owner.ownership_state() == OwnershipState.UNKNOWN
    assert seen[0].exists()


@pytest.mark.parametrize("stage", ["publish", "monitor"])
def test_control_io_failure_prevents_further_science(
    pack_request: ModalPack,
    control_fs: AbstractFileSystem,
    pack_runtime: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    from test_pack_owner import outcome

    entered, release = threading.Event(), threading.Event()
    seen = []
    monitor_failed = threading.Event()
    original_cancel = PackChannel.cancellation

    def cancellation(channel: PackChannel, index: int) -> CancelRequest | None:
        try:
            return original_cancel(channel, index)
        except Exception:
            monitor_failed.set()
            raise

    monkeypatch.setattr(PackChannel, "cancellation", cancellation)
    original = PackChannel.publish

    def publish(channel: PackChannel, result: UnitProgress) -> None:
        if stage == "publish" and result.index == 1:
            msg = "selected-secret"
            raise OSError(msg)
        return original(channel, result)

    monkeypatch.setattr(PackChannel, "publish", publish)

    def execute(call: ModalCall, _control: InertCallControl) -> InertCallResult:
        seen.append(call)
        if stage == "monitor":
            entered.set()
            assert release.wait(3)
        return outcome()

    monkeypatch.setattr(pack_runtime, "execute_call", execute)
    function = Function()
    owner = ModalPackOwner(function, lambda: pack_request, count=3)
    owner.start()
    if stage == "monitor":
        try:
            assert entered.wait(2)
            channel = PackChannel(pack_request)
            control_fs.pipe_file(channel.root + "/cancel-pack.json", b"malformed")
            assert monitor_failed.wait(2)
        finally:
            release.set()
    if stage == "publish":
        assert owner.result(0, 3)
    for index in (1, 2):
        with pytest.raises(CommandOwnershipError):
            owner.result(index, 3)
    finish(owner, function)
    assert owner.ownership_state() == OwnershipState.UNKNOWN
    assert len(seen) <= 2
    assert "selected-secret" not in "".join(
        event.model_dump_json() for event in owner.observations
    )


@pytest.mark.usefixtures("control_fs")
def test_sdk_base_exception_propagates_and_retains_pack_lease(
    pack_request: ModalPack, pack_runtime: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    class InputCancellation(BaseException):
        pass

    monkeypatch.setattr(
        pack_runtime, "execute_call", Mock(side_effect=InputCancellation)
    )
    with pytest.raises(InputCancellation):
        pack_worker.invoke_pack(pack_request.model_dump_json().encode())
    with pytest.raises(CommandOwnershipError, match="unresolved pack"):
        pack_worker.invoke_pack(pack_request.model_dump_json().encode())


@pytest.mark.usefixtures("control_fs")
def test_worker_failure_diagnostic_redacts_storage_and_uri_values(
    pack_request: ModalPack, pack_runtime: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    def execute(*_args: object) -> InertCallResult:
        msg = "selected-secret https://user:capability@example.test/?token=other-secret"
        raise OSError(msg)

    monkeypatch.setattr(pack_runtime, "execute_call", execute)
    result = PackResult.model_validate_json(
        pack_worker.invoke_pack(pack_request.model_dump_json().encode())
    )
    assert result.ownership == OwnershipState.UNKNOWN
    assert "OSError" in result.failure
    assert not any(
        value in result.failure
        for value in ("selected-secret", "capability", "other-secret")
    )


@pytest.mark.usefixtures("control_fs")
def test_blocked_monitor_prevents_pack_closure_even_after_unit_results(
    pack_request: ModalPack, pack_runtime: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    from test_pack_owner import outcome

    entered, release = threading.Event(), threading.Event()
    original = PackChannel.cancellation

    def cancellation(channel: PackChannel, index: int | None) -> CancelRequest | None:
        if threading.current_thread().name.endswith("_monitor)"):
            entered.set()
            assert release.wait(3)
        return original(channel, index)

    monkeypatch.setattr(PackChannel, "cancellation", cancellation)
    monkeypatch.setattr(pack_runtime, "_CLEANUP_SECONDS", 0.05)

    def execute(_call: ModalCall, _control: InertCallControl) -> InertCallResult:
        assert entered.wait(2)
        return outcome()

    monkeypatch.setattr(pack_runtime, "execute_call", execute)
    function = Function()
    owner = ModalPackOwner(function, lambda: pack_request, count=3)
    owner.start()
    try:
        assert owner.result(0, 2)
        finish(owner, function)
        assert owner.ownership_state() == OwnershipState.UNKNOWN
        execution = pack_runtime._retained[0]
        assert execution.monitor.is_alive()
        assert PackChannel(pack_request).entry() is not None
    finally:
        release.set()
        if pack_runtime._retained:
            pack_runtime._retained[0].monitor.join(2)
    assert owner.ownership_state() == OwnershipState.UNKNOWN
