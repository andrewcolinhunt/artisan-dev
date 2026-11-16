"""Assignment ownership stays distinct from attempts and provider entries."""

from __future__ import annotations

from uuid import UUID

import pytest
from pydantic import ValidationError

from artisan.schemas.execution.assignment import (
    AssignmentAttempt,
    AssignmentIdentity,
    DeliveryIdentity,
    SuccessfulDelivery,
)


def attempt() -> AssignmentAttempt:
    return AssignmentAttempt(
        assignment=AssignmentIdentity(step_run_id="a" * 32, unit_index=2),
        attempt=0,
        generation=1,
        recipe_digest="b" * 32,
        software_digest="c" * 64,
        input_context_digest="d" * 32,
    )


def test_assignment_identity_preserves_owner_and_outer_order() -> None:
    identity = attempt().assignment
    assert (
        identity.assignment_id
        == AssignmentIdentity(**identity.model_dump()).assignment_id
    )
    assert (
        identity.assignment_id
        != AssignmentIdentity(
            step_run_id=identity.step_run_id, unit_index=3
        ).assignment_id
    )
    assert (
        identity.assignment_id
        != AssignmentIdentity(
            step_run_id="e" * 32, unit_index=identity.unit_index
        ).assignment_id
    )
    assert set(identity.model_dump()) == {"step_run_id", "unit_index"}


def test_provider_entries_get_fresh_delivery_ids_for_one_attempt() -> None:
    admitted = attempt()
    first, second = DeliveryIdentity.new(admitted), DeliveryIdentity.new(admitted)
    assert first.attempt == second.attempt == admitted
    assert first.delivery_id != second.delivery_id
    assert UUID(hex=first.delivery_id).version == 4
    assert DeliveryIdentity.model_validate_json(first.model_dump_json()) == first
    with pytest.raises(ValidationError):
        DeliveryIdentity(attempt=admitted)
    with pytest.raises(ValidationError, match="UUID4"):
        DeliveryIdentity(attempt=admitted, delivery_id="0" * 32)


@pytest.mark.parametrize("field", ["computation_complete", "cleanup_complete"])
@pytest.mark.parametrize("incomplete", [False, 1, None])
def test_successful_delivery_requires_observed_completion(
    field: str, incomplete: object
) -> None:
    values = {
        "delivery": DeliveryIdentity.new(attempt()),
        "computation_complete": True,
        "cleanup_complete": True,
    }
    with pytest.raises(ValidationError, match="completed computation and cleanup"):
        SuccessfulDelivery.model_validate(values | {field: incomplete})
    assert SuccessfulDelivery.model_validate(values).cleanup_complete is True


@pytest.mark.parametrize("field", ["attempt", "generation"])
@pytest.mark.parametrize("invalid", [True, -1, "1"])
def test_attempt_counters_are_strict_and_nonnegative(
    field: str, invalid: object
) -> None:
    with pytest.raises(ValidationError):
        AssignmentAttempt.model_validate(attempt().model_dump() | {field: invalid})


def test_attempt_evidence_is_closed_and_deeply_immutable() -> None:
    admitted = attempt()
    with pytest.raises(ValidationError, match="frozen"):
        admitted.assignment.unit_index = 7
    with pytest.raises(ValidationError, match="Extra inputs"):
        AssignmentAttempt.model_validate(admitted.model_dump() | {"accepted": True})
    damaged = admitted.model_copy(update={"generation": -1})
    with pytest.raises(ValidationError):
        DeliveryIdentity.new(damaged)
    diagnostic = AssignmentAttempt.model_validate(
        admitted.model_dump() | {"replay_of_execution_run_id": "f" * 32}
    )
    assert diagnostic.replay_of_execution_run_id == "f" * 32
