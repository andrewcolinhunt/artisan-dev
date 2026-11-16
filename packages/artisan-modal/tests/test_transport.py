"""Provider metadata and call bytes are checked before entering the live wrapper."""

from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import Mock
from uuid import uuid4

import pytest
from artisan_modal import transport
from artisan_modal.bindings import ModalFunctionRuntime, ModalRuntimePayload
from artisan_modal.transport import ModalCall
from pydantic import ValidationError

from artisan.orchestration.runner_api import (
    CuratorAdmissionReference,
    CuratorReplyContext,
    InertCallControl,
    InertCallResult,
    OwnershipState,
    RuntimeCompatibility,
    WorkerTransportHeader,
)


@pytest.fixture
def submitted(monkeypatch: pytest.MonkeyPatch) -> ModalCall:
    monkeypatch.setattr(transport, "verify_worker_activation", Mock())
    payload = b"\x80opaque live pickle bytes\xff"
    header = WorkerTransportHeader(
        compatibility=RuntimeCompatibility.model_validate(
            {
                "python_implementation": "CPython",
                "python_version": "3.12.12",
                "cache_tag": "cpython-312",
                "packages": [
                    {
                        "name": name,
                        "version": "1",
                    }
                    for name in (
                        "cloudpickle",
                        "dexterity-artisan",
                        "pydantic",
                        "pydantic-core",
                    )
                ],
            }
        ),
        payload_sha256=hashlib.sha256(payload).hexdigest(),
    )
    context = CuratorReplyContext(
        admission=CuratorAdmissionReference(
            step_run_id="a" * 32, admission_digest="b" * 32
        ),
        payload_sha256=header.payload_sha256,
        sensitive_values=(),
    )
    return ModalCall(
        function=ModalFunctionRuntime(
            image_id="im-qualified",
            milli_cpu=1000,
            memory_mb=1024,
            gpu_count=0,
            gpu_type="",
            max_inputs=1,
            timeout_seconds=60,
        ),
        runtime=ModalRuntimePayload(sdk_version="1.4.1"),
        header=header,
        payload=payload,
        reply_context=context.model_dump_json().encode(),
        preserve_working=True,
    )


def test_transport_preserves_binary_payload_and_exchanges_only_closed_result(
    submitted: ModalCall, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen = []
    monkeypatch.setattr(
        transport, "verify_modal_runtime", lambda expected: seen.append(expected)
    )
    monkeypatch.setattr(
        transport, "verify_modal_function", lambda expected: seen.append(expected)
    )
    result = InertCallResult(
        entry_id=uuid4(),
        ownership=OwnershipState.UNKNOWN,
        error="uncertain cleanup",
        retained_working=True,
    )

    def execute(
        call: object,
        *,
        working_root: Path,
        preserve_working: bool,
        control: InertCallControl,
    ) -> InertCallResult:
        assert isinstance(control, InertCallControl)
        assert call == submitted.inert()
        assert working_root == transport.WORKING_ROOT
        assert preserve_working
        assert seen == [submitted.runtime, submitted.function]
        return result

    monkeypatch.setattr(transport, "execute_inert_call", execute)
    encoded = transport.execute_call(
        ModalCall.model_validate_json(submitted.model_dump_json()), InertCallControl()
    )
    assert encoded == result
    assert encoded.reply is None


def test_changed_payload_rejects_before_platform_or_live_entry(
    submitted: ModalCall, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = Mock()
    entered = Mock()
    monkeypatch.setattr(transport, "verify_modal_runtime", runtime)
    monkeypatch.setattr(transport, "execute_inert_call", entered)
    changed = submitted.model_copy(update={"payload": b"changed"})
    with pytest.raises(ValidationError, match="exact header"):
        transport.execute_call(
            ModalCall.model_validate_json(changed.model_dump_json()), InertCallControl()
        )
    runtime.assert_not_called()
    entered.assert_not_called()


@pytest.mark.parametrize(
    "phase",
    ["verify_worker_activation", "verify_modal_runtime", "verify_modal_function"],
)
def test_platform_mismatch_rejects_before_live_entry(
    submitted: ModalCall, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    monkeypatch.setattr(transport, "verify_modal_runtime", Mock())
    monkeypatch.setattr(transport, "verify_modal_function", Mock())
    monkeypatch.setattr(
        transport, phase, Mock(side_effect=ValueError("mismatched runtime"))
    )
    entered = Mock()
    monkeypatch.setattr(transport, "execute_inert_call", entered)
    with pytest.raises(ValueError, match="mismatched runtime"):
        transport.execute_call(
            ModalCall.model_validate_json(submitted.model_dump_json()),
            InertCallControl(),
        )
    entered.assert_not_called()


def test_sdk_cancellation_is_not_converted_into_a_return_value(
    submitted: ModalCall, monkeypatch: pytest.MonkeyPatch
) -> None:
    class InputCancellation(BaseException):
        pass

    monkeypatch.setattr(transport, "verify_modal_runtime", Mock())
    monkeypatch.setattr(transport, "verify_modal_function", Mock())
    monkeypatch.setattr(
        transport, "execute_inert_call", Mock(side_effect=InputCancellation)
    )
    with pytest.raises(InputCancellation):
        transport.execute_call(
            ModalCall.model_validate_json(submitted.model_dump_json()),
            InertCallControl(),
        )
