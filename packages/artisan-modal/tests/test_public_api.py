"""Public Modal diagnostics preserve immutable snapshots and ownership authority."""

from __future__ import annotations

import json
from dataclasses import is_dataclass
from unittest.mock import Mock
from uuid import uuid4

import artisan_modal
import pytest
from artisan_modal import CallObservation, ModalSoftware, modal_owner_observations
from artisan_modal.pack_control import ModalPack
from artisan_modal.pack_owner import ModalPackOwner
from artisan_modal.software import ModalSoftware as InternalModalSoftware
from pydantic import ValidationError

from artisan.orchestration.runner_api import InertCallResult, OwnershipState


def test_public_facade_exposes_records_without_mutable_owner() -> None:
    assert ModalSoftware is InternalModalSoftware
    assert is_dataclass(ModalSoftware)
    assert ModalSoftware.__dataclass_params__.frozen
    assert CallObservation.model_config["frozen"]
    assert "ModalPackOwner" not in artisan_modal.__all__
    assert not hasattr(artisan_modal, "ModalPackOwner")


def test_modal_owner_observations_rejects_foreign_owner() -> None:
    class ForeignOwner:
        def ownership_state(self) -> OwnershipState:
            return OwnershipState.CLOSED

        def request_close(self, deadline: float) -> None:
            msg = f"Inspection must not request closure (deadline={deadline})"
            raise AssertionError(msg)

        @property
        def observations(self) -> tuple[CallObservation, ...]:
            msg = "Inspection must not trust foreign observations"
            raise AssertionError(msg)

    with pytest.raises(TypeError, match="Expected a Modal worker owner"):
        modal_owner_observations(ForeignOwner())


def test_modal_owner_observations_returns_immutable_bounded_snapshot(
    pack_request: ModalPack,
) -> None:
    function, prepare = Mock(), Mock(return_value=pack_request)
    owner = ModalPackOwner(function, prepare, count=3)
    assert modal_owner_observations(owner) == ()
    owner._observe("start")
    snapshot = modal_owner_observations(owner)
    assert isinstance(snapshot, tuple)
    assert len(snapshot) == 1
    assert isinstance(snapshot[0], CallObservation)
    with pytest.raises(ValidationError, match="frozen"):
        snapshot[0].phase = "complete"
    for _ in range(100):
        owner._observe("failed", error=ValueError("x" * 2000))
    current = modal_owner_observations(owner)
    assert len(snapshot) == 1
    assert snapshot[0].phase == "start"
    assert len(current) == 64
    assert [event.sequence for event in current] == list(range(64))
    assert all(len(event.message) == 1024 for event in current[1:])
    assert {event.owner_id for event in current} == {snapshot[0].owner_id}
    function.spawn.assert_not_called()
    prepare.assert_not_called()


def test_modal_owner_observations_preserves_redaction_and_call_identity(
    pack_request: ModalPack,
) -> None:
    secret = "quoted ' value \\ token"
    owner = ModalPackOwner(
        Mock(), lambda: pack_request, count=3, sensitive_values=(secret,)
    )
    owner._call_id = "fc-example123"
    owner._observe(
        "failed",
        error=ValueError(
            f"failure {secret} escaped {json.dumps(secret)} "
            "https://user:uri-secret@example.test/path?token=capability"
        ),
    )
    snapshot = modal_owner_observations(owner)
    assert snapshot[0].call_id == "fc-example123"
    assert snapshot[0].error_type == "ValueError"
    encoded = snapshot[0].model_dump_json()
    assert not any(
        value in encoded for value in (secret, "quoted", "uri-secret", "capability")
    )


def test_modal_owner_observations_does_not_establish_worker_closure(
    pack_request: ModalPack,
) -> None:
    owner = ModalPackOwner(Mock(), lambda: pack_request, count=3)
    owner._observe(
        "core_result",
        outcome=InertCallResult(
            entry_id=uuid4(),
            ownership=OwnershipState.CLOSED,
            reply=b"opaque reply",
            error=None,
            retained_working=False,
            cancelled=False,
        ),
    )
    assert modal_owner_observations(owner)[0].core_ownership == OwnershipState.CLOSED
    assert owner.ownership_state() == OwnershipState.PENDING
