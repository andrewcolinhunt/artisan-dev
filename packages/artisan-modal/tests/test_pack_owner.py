"""Ordered progress, SDK ownership and per-unit cancellation over actual file records."""

from __future__ import annotations

import json
import threading
import time
from concurrent.futures import CancelledError, Future
from types import ModuleType
from unittest.mock import Mock
from uuid import uuid4

import pytest
from artisan_modal import pack_owner, pack_worker
from artisan_modal.pack_control import (
    ModalPack,
    PackChannel,
)
from artisan_modal.pack_owner import ModalPackOwner
from artisan_modal.transport import ModalCall

from artisan.orchestration.runner_api import (
    CancellationStatus,
    CommandOwnershipError,
    InertCallControl,
    InertCallResult,
    OwnershipState,
)


def outcome(*, unknown: bool = False, cancelled: bool = False) -> InertCallResult:
    return InertCallResult(
        entry_id=uuid4(),
        ownership=OwnershipState.UNKNOWN if unknown else OwnershipState.CLOSED,
        reply=None if unknown or cancelled else b'{"kind":"typed"}',
        error="unresolved cleanup"
        if unknown
        else "Worker call cancelled after confirmed cleanup"
        if cancelled
        else None,
        retained_working=unknown,
        cancelled=cancelled,
    )


class Function:
    def __init__(self) -> None:
        self.future = Future()
        self.cancelled = threading.Event()
        self.object_id = "fc-example123"
        self.thread = None
        self.encoded = None

    def spawn(self, encoded: bytes) -> Function:
        self.encoded = encoded

        def run() -> None:
            try:
                self.future.set_result(pack_worker.invoke_pack(encoded))
            except BaseException as error:
                self.future.set_exception(error)

        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()
        return self

    def get(self) -> object:
        return self.future.result(5)

    def cancel(self, *, terminate_containers: object = False) -> None:
        assert not terminate_containers
        self.cancelled.set()


def finish(owner: ModalPackOwner, function: Function) -> None:
    assert owner._finished.wait(5)
    if function.thread is not None:
        function.thread.join(2)
        assert not function.thread.is_alive()
    if owner._collector is not None:
        # UNKNOWN may return at its deadline with collection still retained.
        owner._collector.join(2)
        assert not owner._collector.is_alive()
    if owner._cancel_thread is not None:
        owner._cancel_thread.join(2)
        assert not owner._cancel_thread.is_alive()


@pytest.mark.usefixtures("control_fs")
def test_completed_unit_is_available_while_shared_pack_stays_owned(
    pack_request: ModalPack, pack_runtime: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, release = threading.Event(), threading.Event()
    seen = []

    def execute(call: ModalCall, _control: InertCallControl) -> InertCallResult:
        seen.append(call.payload)
        if len(seen) == 2:
            entered.set()
            assert release.wait(3)
        return outcome()

    monkeypatch.setattr(pack_runtime, "execute_call", execute)
    function = Function()
    owner = ModalPackOwner(function, lambda: pack_request, count=3)
    owner.start()
    try:
        assert entered.wait(2)
        assert owner.result(0, 2) == b'{"kind":"typed"}'
        assert owner.ownership_state() == OwnershipState.PENDING
        assert not owner.done(1)
    finally:
        release.set()
    assert owner.result(2, 3)
    finish(owner, function)
    assert seen == [item.payload for item in pack_request.calls]
    assert owner.ownership_state() == OwnershipState.CLOSED
    assert len({owner.outcome(i).outcome.entry_id for i in range(3)}) == 3
    assert not function.cancelled.is_set()
    assert PackChannel(pack_request).entry() is None


@pytest.mark.usefixtures("control_fs")
def test_queued_cancel_confirms_without_touching_active_sibling(
    pack_request: ModalPack, pack_runtime: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, release = threading.Event(), threading.Event()
    seen = []

    def execute(call: ModalCall, _control: InertCallControl) -> InertCallResult:
        seen.append(call.payload)
        if call == pack_request.calls[0]:
            entered.set()
            assert release.wait(3)
        return outcome()

    monkeypatch.setattr(pack_runtime, "execute_call", execute)
    function = Function()
    owner = ModalPackOwner(function, lambda: pack_request, count=3)
    owner.start()
    try:
        assert entered.wait(2)
        assert owner.cancel(1).status == CancellationStatus.CONFIRMED
        with pytest.raises(CancelledError):
            owner.result(1, 0)
        assert not function.cancelled.is_set()
        assert not owner.done(0)
    finally:
        release.set()
    assert owner.result(0, 3)
    assert owner.result(2, 3)
    finish(owner, function)
    assert seen == [pack_request.calls[0].payload, pack_request.calls[2].payload]
    assert owner.ownership_state() == OwnershipState.CLOSED


@pytest.mark.usefixtures("control_fs")
def test_later_unknown_preserves_earlier_result_and_prevents_next_entry(
    pack_request: ModalPack, pack_runtime: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen = []

    def execute(call: ModalCall, _control: InertCallControl) -> InertCallResult:
        seen.append(call.payload)
        return outcome(unknown=len(seen) == 2)

    monkeypatch.setattr(pack_runtime, "execute_call", execute)
    function = Function()
    owner = ModalPackOwner(function, lambda: pack_request, count=3)
    owner.start()
    assert owner.result(0, 3)
    for index in (1, 2):
        with pytest.raises(CommandOwnershipError):
            owner.result(index, 3)
    finish(owner, function)
    assert len(seen) == 2
    assert owner.ownership_state() == OwnershipState.UNKNOWN
    assert PackChannel(pack_request).entry() is not None
    with pytest.raises(CommandOwnershipError, match="unresolved pack"):
        pack_worker.invoke_pack(pack_request.model_dump_json().encode())


@pytest.mark.parametrize("stage", ["prepare", "spawn", "get"])
@pytest.mark.usefixtures("control_fs")
def test_failure_authority_and_secret_observations(
    pack_request: ModalPack, stage: str
) -> None:
    function = Mock()
    function.spawn.return_value.object_id = "fc-example123"
    prepare = Mock(return_value=pack_request)
    secret = "quoted ' value \\ token"
    target = (
        prepare
        if stage == "prepare"
        else function.spawn
        if stage == "spawn"
        else function.spawn.return_value.get
    )
    target.side_effect = ValueError(
        f"failure {secret} escaped {json.dumps(secret)} https://user:uri-secret@example.test/path?token=capability"
    )
    owner = ModalPackOwner(function, prepare, count=3, sensitive_values=(secret,))
    owner.start()
    for index in range(3):
        with pytest.raises(
            RuntimeError if stage == "prepare" else CommandOwnershipError
        ):
            owner.result(index, 3)
    assert owner._finished.wait(3)
    assert owner.ownership_state() == (
        OwnershipState.CLOSED if stage == "prepare" else OwnershipState.UNKNOWN
    )
    encoded = "\n".join(event.model_dump_json() for event in owner.observations)
    assert not any(
        value in encoded for value in (secret, "quoted", "uri-secret", "capability")
    )


@pytest.mark.usefixtures("control_fs")
def test_blocked_preparation_timeout_is_sticky_and_never_submits(
    pack_request: ModalPack,
) -> None:
    entered, release = threading.Event(), threading.Event()
    function = Mock()

    def prepare() -> ModalPack:
        entered.set()
        assert release.wait(3)
        return pack_request

    owner = ModalPackOwner(function, prepare, count=3)
    owner.start()
    try:
        assert entered.wait(2)
        owner.request_close(time.monotonic())
        assert owner.ownership_state() == OwnershipState.UNKNOWN
    finally:
        release.set()
    assert owner._finished.wait(3)
    function.spawn.assert_not_called()
    assert owner.ownership_state() == OwnershipState.UNKNOWN


@pytest.mark.usefixtures("control_fs")
def test_sdk_cancel_acknowledgement_never_closes_unknown_worker(
    pack_request: ModalPack, pack_runtime: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, release = threading.Event(), threading.Event()

    def execute(_call: ModalCall, _control: InertCallControl) -> InertCallResult:
        entered.set()
        assert release.wait(3)
        return outcome(unknown=True)

    monkeypatch.setattr(pack_runtime, "execute_call", execute)
    function = Function()
    owner = ModalPackOwner(function, lambda: pack_request, count=3)
    owner.start()
    try:
        assert entered.wait(2)
        owner.request_close(time.monotonic())
        assert function.cancelled.wait(2)
        assert owner.ownership_state() == OwnershipState.UNKNOWN
    finally:
        release.set()
    finish(owner, function)
    assert owner.ownership_state() == OwnershipState.UNKNOWN


@pytest.mark.parametrize("failure", ["logger", "formatter", "sanitizer", "model"])
@pytest.mark.usefixtures("control_fs")
def test_observation_failures_do_not_replace_completed_results(
    pack_request: ModalPack,
    pack_runtime: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    class BrokenError(RuntimeError):
        def __str__(self) -> None:
            msg = "selected-secret"
            raise RuntimeError(msg)

    monkeypatch.setattr(pack_runtime, "execute_call", lambda *_: outcome())
    owner = ModalPackOwner(Function(), lambda: pack_request, count=3)
    if failure == "logger":
        monkeypatch.setattr(
            pack_owner._LOGGER, "info", Mock(side_effect=OSError("selected-secret"))
        )
    elif failure in {"sanitizer", "model"}:
        monkeypatch.setattr(
            pack_owner,
            "redacted_message" if failure == "sanitizer" else "CallObservation",
            Mock(side_effect=RuntimeError("selected-secret")),
        )
    owner._observe("failed", error=BrokenError())
    owner.start()
    assert owner.result(2, 3)
    finish(owner, owner._function)
    assert owner.ownership_state() == OwnershipState.CLOSED
    assert owner.diagnostic_write_error == "Modal diagnostic observation failed"


def test_observations_are_bounded(pack_request: ModalPack) -> None:
    owner = ModalPackOwner(Mock(), lambda: pack_request, count=3)
    for _ in range(100):
        owner._observe("failed", error=ValueError("x" * 10000))
    assert len(owner.observations) == 64
    assert all(len(event.message) == 1024 for event in owner.observations)


@pytest.mark.usefixtures("control_fs")
def test_unit_cancel_during_preparation_is_published_before_submission(
    pack_request: ModalPack, pack_runtime: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, release = threading.Event(), threading.Event()
    seen = []

    def prepare() -> ModalPack:
        entered.set()
        assert release.wait(3)
        return pack_request

    monkeypatch.setattr(
        pack_runtime,
        "execute_call",
        lambda call, _control: (seen.append(call.payload), outcome())[1],
    )
    function = Function()
    owner = ModalPackOwner(function, prepare, count=3)
    owner.start()
    assert entered.wait(2)
    cancelled = Future()
    thread = threading.Thread(target=lambda: cancelled.set_result(owner.cancel(1)))
    thread.start()
    try:
        assert owner._wake.wait(2)
        release.set()
        assert cancelled.result(3).status == CancellationStatus.CONFIRMED
        assert owner.result(2, 3)
    finally:
        release.set()
        thread.join(3)
    finish(owner, function)
    assert pack_request.calls[1].payload not in seen
    assert owner.ownership_state() == OwnershipState.CLOSED


@pytest.mark.usefixtures("control_fs")
def test_whole_pack_close_requests_active_core_before_sdk_stop(
    pack_request: ModalPack, pack_runtime: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, cancelled = threading.Event(), threading.Event()
    seen = []
    original = pack_runtime.InertCallControl.request_close

    def request_close(control: InertCallControl, deadline: float) -> None:
        original(control, deadline)
        cancelled.set()

    monkeypatch.setattr(pack_runtime.InertCallControl, "request_close", request_close)

    def execute(call: ModalCall, _control: InertCallControl) -> InertCallResult:
        seen.append(call)
        entered.set()
        assert cancelled.wait(3)
        return outcome(cancelled=True)

    monkeypatch.setattr(pack_runtime, "execute_call", execute)
    function = Function()
    owner = ModalPackOwner(function, lambda: pack_request, count=3)
    owner.start()
    assert entered.wait(2)
    owner.request_close(time.monotonic() + 3)
    finish(owner, function)
    assert owner.ownership_state() == OwnershipState.CLOSED
    assert not function.cancelled.is_set()
    assert len(seen) == 1
    for index in range(3):
        with pytest.raises(CancelledError):
            owner.result(index, 0)


@pytest.mark.usefixtures("control_fs", "pack_runtime")
def test_ambiguous_spawn_retains_owner_and_eventually_attempts_control_then_sdk(
    pack_request: ModalPack,
) -> None:
    entered, release = threading.Event(), threading.Event()
    function = Mock()
    function.spawn.return_value.get.side_effect = OSError("SDK interrupted")
    cancelled = threading.Event()
    function.spawn.return_value.cancel.side_effect = lambda **_kwargs: cancelled.set()

    def spawn(_encoded: bytes) -> Mock:
        entered.set()
        assert release.wait(3)
        return function.spawn.return_value

    function.spawn.side_effect = spawn
    owner = ModalPackOwner(function, lambda: pack_request, count=3)
    owner.start()
    try:
        assert entered.wait(2)
        owner.request_close(time.monotonic())
        assert owner.ownership_state() == OwnershipState.UNKNOWN
    finally:
        release.set()
    assert cancelled.wait(3)
    assert owner._finished.wait(3)
    assert PackChannel(pack_request).cancellation(None) is not None
    assert owner.ownership_state() == OwnershipState.UNKNOWN


@pytest.mark.usefixtures("control_fs")
def test_slow_channel_cleanup_does_not_block_cancel_or_clear_unknown(
    pack_request: ModalPack, pack_runtime: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, release = threading.Event(), threading.Event()
    monkeypatch.setattr(pack_runtime, "execute_call", lambda *_: outcome())
    original = PackChannel.remove

    def remove(channel: PackChannel) -> None:
        entered.set()
        assert release.wait(3)
        original(channel)

    monkeypatch.setattr(PackChannel, "remove", remove)
    function = Function()
    owner = ModalPackOwner(function, lambda: pack_request, count=3)
    owner.start()
    try:
        assert entered.wait(2)
        before = time.monotonic()
        owner.request_close(before)
        assert time.monotonic() - before < 0.5
        assert owner.ownership_state() == OwnershipState.UNKNOWN
        assert owner.result(0, 0)
    finally:
        release.set()
    finish(owner, function)
    assert owner.ownership_state() == OwnershipState.UNKNOWN
