"""Controller serialization uses the admitted application selection without imports."""

from __future__ import annotations

import hashlib

import pytest

from artisan.execution.application_bootstrap import worker_application_scope
from artisan.execution.runtime_compatibility import capture_runtime_compatibility
from artisan.execution.worker_bootstrap import serialize_worker_call
from artisan.schemas.execution.worker_transport import TRANSPORT_PACKAGES


def test_serialization_does_not_reopen_admitted_application_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    compatibility = capture_runtime_compatibility(TRANSPORT_PACKAGES)
    monkeypatch.setattr(
        "artisan.execution.application_bootstrap.observed_editable_sources",
        lambda *_: pytest.fail("Serialization reopened authored sources"),
    )
    call = serialize_worker_call(
        str,
        compatibility=compatibility,
        args=(1,),
        application_modules=("already_admitted_application",),
    )
    assert hashlib.sha256(call.payload).hexdigest() == call.header.payload_sha256


def test_serialization_rejects_changed_live_application_selection() -> None:
    compatibility = capture_runtime_compatibility(TRANSPORT_PACKAGES)
    with (
        worker_application_scope(("admitted_application",)),
        pytest.raises(ValueError, match="differs from the live call selection"),
    ):
        serialize_worker_call(
            str,
            compatibility=compatibility,
            application_modules=("another_application",),
        )
