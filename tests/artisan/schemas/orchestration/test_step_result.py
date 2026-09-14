"""Tests for terminal StepResult invariants."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from artisan.schemas.orchestration.step_lifecycle import (
    CancellationStatus,
    StepDisposition,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult, StepResultBuilder


def _result(**updates: object) -> StepResult:
    values: dict[str, object] = {
        "step_name": "op",
        "step_number": 0,
        "status": StepStatus.SUCCEEDED,
        "disposition": StepDisposition.EXECUTED,
        "total_count": 2,
        "succeeded_count": 2,
        "failed_count": 0,
    }
    values.update(updates)
    return StepResult(**values)


def test_succeeded_result_has_no_stored_success_boolean() -> None:
    result = _result()
    assert "success" not in result.model_dump()
    assert result.status is StepStatus.SUCCEEDED


def test_partial_requires_a_known_mix() -> None:
    result = _result(
        status=StepStatus.PARTIAL,
        total_count=3,
        succeeded_count=2,
        failed_count=1,
    )
    assert result.has_failures
    with pytest.raises(ValidationError, match="both successful and failed"):
        _result(status=StepStatus.PARTIAL)


def test_failed_requires_error_and_has_no_disposition() -> None:
    failed = _result(
        status=StepStatus.FAILED,
        disposition=None,
        error="dispatch failed",
        total_count=0,
        succeeded_count=0,
    )
    assert failed.error == "dispatch failed"
    with pytest.raises(ValidationError, match="failed requires an error"):
        _result(status=StepStatus.FAILED, disposition=None)


def test_cancelled_requires_confirmed_acknowledgement() -> None:
    result = _result(
        status=StepStatus.CANCELLED,
        disposition=None,
        cancellation_status=CancellationStatus.CONFIRMED,
        total_count=0,
        succeeded_count=0,
    )
    assert result.status is StepStatus.CANCELLED
    with pytest.raises(ValidationError, match="confirmed cancellation"):
        _result(
            status=StepStatus.CANCELLED,
            disposition=None,
            total_count=0,
            succeeded_count=0,
        )


def test_skipped_rejects_execution_facts() -> None:
    result = _result(
        status=StepStatus.SKIPPED,
        disposition=None,
        total_count=0,
        succeeded_count=0,
    )
    assert result.status is StepStatus.SKIPPED
    with pytest.raises(ValidationError, match="zero counts"):
        _result(status=StepStatus.SKIPPED, disposition=None)

    with pytest.raises(ValidationError, match="must not expose outputs"):
        _result(
            status=StepStatus.SKIPPED,
            disposition=None,
            total_count=0,
            succeeded_count=0,
            failed_count=0,
            output_roles=frozenset({"data"}),
        )


def test_failed_cannot_be_a_cache_hit() -> None:
    with pytest.raises(ValidationError, match="failed must not carry a disposition"):
        _result(
            status=StepStatus.FAILED,
            disposition=StepDisposition.CACHE_HIT,
            error="cache source failed",
            total_count=0,
            succeeded_count=0,
        )


def test_succeeded_cannot_contain_failed_groups() -> None:
    with pytest.raises(ValidationError, match="succeeded requires failed_count"):
        _result(total_count=2, succeeded_count=1, failed_count=1)


def test_unknown_cancellation_requires_failed() -> None:
    with pytest.raises(ValidationError, match="Unknown cancellation requires failed"):
        _result(cancellation_status=CancellationStatus.UNKNOWN)


def test_counts_must_balance() -> None:
    with pytest.raises(ValidationError, match="total_count must equal"):
        _result(total_count=3)


def test_metadata_cannot_reintroduce_lifecycle_flags() -> None:
    with pytest.raises(ValidationError, match="Lifecycle facts"):
        _result(metadata={"cancelled": True})


def test_builder_requires_explicit_classification() -> None:
    builder = StepResultBuilder("op", 0, {}, "a" * 32)
    builder.add_success(2)
    builder.add_failure(1)
    result = builder.build(
        StepStatus.PARTIAL,
        disposition=StepDisposition.EXECUTED,
    )
    assert result.status is StepStatus.PARTIAL
    assert result.total_count == 3


def test_builder_hides_outputs_for_unusable_terminal_state() -> None:
    builder = StepResultBuilder("op", 0, {"data": "data"}, "a" * 32)
    builder.add_failure()

    result = builder.build(StepStatus.FAILED, error="worker failed")

    assert result.output_roles == frozenset()
    assert result.output_types == {}
