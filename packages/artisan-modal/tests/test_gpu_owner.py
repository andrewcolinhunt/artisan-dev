"""Fixed-probe ownership survives submission ambiguity and late SDK returns."""

from __future__ import annotations

import threading
import time
from unittest.mock import Mock
from uuid import uuid4

import pytest
from artisan_modal.gpu import GPUProbeReply
from artisan_modal.gpu_owner import GPUProbeOwner
from test_gpu import allocation, request

from artisan.orchestration.runner_api import CommandOwnershipError, OwnershipState


def test_probe_success_and_reported_failure_are_closed(prepared: tuple) -> None:
    probe = request(prepared[0])
    for failed in (False, True):
        reply = GPUProbeReply(
            nonce=probe.nonce,
            request_sha256=probe.digest,
            entry_id=uuid4(),
            software=None if failed else allocation().software_profile(),
            error="unavailable GPU" if failed else None,
        )
        owner = GPUProbeOwner(
            Mock(
                spawn=Mock(
                    return_value=Mock(
                        get=Mock(return_value=reply.model_dump_json().encode())
                    )
                )
            ),
            probe,
        )
        owner.start()
        if failed:
            with pytest.raises(RuntimeError, match="unavailable GPU"):
                owner.result(2)
        else:
            assert owner.result(2) == reply.software
        owner._thread.join(2)
        assert owner.ownership_state() == OwnershipState.CLOSED


@pytest.mark.parametrize("phase", ["spawn", "get", "decode", "nonce"])
def test_post_submission_ambiguity_is_unknown(prepared: tuple, phase: str) -> None:
    probe = request(prepared[0])
    reply = GPUProbeReply(
        nonce=uuid4(),
        request_sha256=probe.digest,
        entry_id=uuid4(),
        software=allocation().software_profile(),
    )
    call = Mock(
        get=Mock(side_effect=OSError("get"))
        if phase == "get"
        else Mock(
            return_value=reply.model_dump_json().encode()
            if phase == "nonce"
            else b"malformed"
        )
    )
    function = Mock(
        spawn=Mock(side_effect=OSError("spawn"))
        if phase == "spawn"
        else Mock(return_value=call)
    )
    owner = GPUProbeOwner(function, probe)
    owner.start()
    with pytest.raises(CommandOwnershipError):
        owner.result(2)
    owner._thread.join(2)
    assert owner.ownership_state() == OwnershipState.UNKNOWN


def test_deadline_unknown_is_sticky_after_late_valid_reply(prepared: tuple) -> None:
    probe = request(prepared[0])
    entered, release = threading.Event(), threading.Event()
    reply = GPUProbeReply(
        nonce=probe.nonce,
        request_sha256=probe.digest,
        entry_id=uuid4(),
        software=allocation().software_profile(),
    )

    def receive() -> bytes:
        entered.set()
        assert release.wait(3)
        return reply.model_dump_json().encode()

    call = Mock(get=receive)
    owner = GPUProbeOwner(Mock(spawn=lambda _: call), probe)
    owner.start()
    try:
        assert entered.wait(2)
        owner.request_close(time.monotonic())
        assert owner.ownership_state() == OwnershipState.UNKNOWN
    finally:
        release.set()
    with pytest.raises(CommandOwnershipError):
        owner.result(2)
    owner._thread.join(2)
    if owner._cancel_thread is not None:
        owner._cancel_thread.join(2)
    call.cancel.assert_called_once_with(terminate_containers=False)
    assert owner.ownership_state() == OwnershipState.UNKNOWN


def test_blocked_sdk_cancel_is_retained_after_probe_reply(prepared: tuple) -> None:
    probe = request(prepared[0])
    receiving, return_reply = threading.Event(), threading.Event()
    cancelling, return_cancel = threading.Event(), threading.Event()
    reply = GPUProbeReply(
        nonce=probe.nonce,
        request_sha256=probe.digest,
        entry_id=uuid4(),
        software=allocation().software_profile(),
    )

    def get() -> bytes:
        receiving.set()
        assert return_reply.wait(3)
        return reply.model_dump_json().encode()

    def cancel(**_kwargs: object) -> None:
        cancelling.set()
        assert return_cancel.wait(3)

    owner = GPUProbeOwner(Mock(spawn=lambda _: Mock(get=get, cancel=cancel)), probe)
    owner.start()
    try:
        assert receiving.wait(2)
        owner.request_close(time.monotonic())
        assert cancelling.wait(2)
        return_reply.set()
        with pytest.raises(CommandOwnershipError):
            owner.result(2)
        assert owner.ownership_state() == OwnershipState.UNKNOWN
        assert owner._cancel_thread.is_alive()
    finally:
        return_reply.set()
        return_cancel.set()
        owner._thread.join(2)
        if owner._cancel_thread is not None:
            owner._cancel_thread.join(2)
    assert owner.ownership_state() == OwnershipState.UNKNOWN


def test_failed_thread_start_retains_owner_without_waiting_full_deadline(
    prepared: tuple, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner = GPUProbeOwner(Mock(), request(prepared[0]))
    monkeypatch.setattr(
        owner._thread, "start", Mock(side_effect=RuntimeError("thread start failed"))
    )
    with pytest.raises(RuntimeError, match="thread start"):
        owner.start()
    assert owner._finished.is_set()
    owner.request_close(time.monotonic())
    assert owner.ownership_state() == OwnershipState.UNKNOWN
