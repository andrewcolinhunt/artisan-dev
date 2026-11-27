"""Tests for terminal StepResult invariants."""

from __future__ import annotations

import pytest
from fixtures.output_references import output_reference
from pydantic import ValidationError

from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationStatus,
    StepDisposition,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult


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


@pytest.mark.parametrize(
    ("status", "error", "cancellation_status"),
    [
        (StepStatus.FAILED, "worker failed", None),
        (StepStatus.CANCELLED, None, CancellationStatus.CONFIRMED),
        (StepStatus.SKIPPED, None, None),
    ],
)
def test_unusable_terminal_result_rejects_outputs(
    status: StepStatus,
    error: str | None,
    cancellation_status: CancellationStatus | None,
) -> None:
    with pytest.raises(ValidationError, match="must not expose outputs"):
        _result(
            status=status,
            disposition=None,
            error=error,
            cancellation_status=cancellation_status,
            total_count=0,
            succeeded_count=0,
            output_roles=frozenset({"data"}),
            output_types={"data": "csv"},
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


class TestStepResult:
    """Tests for StepResult schema model."""

    def test_create_minimal(self):
        """Test minimal StepResult creation."""
        result = StepResult(
            step_name="ingest",
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
        )
        assert result.step_name == "ingest"
        assert result.step_number == 0
        assert result.status == StepStatus.SUCCEEDED
        assert result.disposition == StepDisposition.EXECUTED
        assert result.total_count == 0
        assert result.succeeded_count == 0
        assert result.failed_count == 0
        assert result.output_roles == frozenset()
        assert result.output_types == {}

    def test_create_full(self):
        """Test StepResult with all fields."""
        result = StepResult(
            step_name="score",
            step_number=1,
            status=StepStatus.PARTIAL,
            disposition=StepDisposition.EXECUTED,
            total_count=100,
            succeeded_count=95,
            failed_count=5,
            output_roles=frozenset(["data", "metrics"]),
            output_types={"data": "csv", "metrics": "metric"},
        )
        assert result.total_count == 100
        assert result.succeeded_count == 95
        assert result.failed_count == 5
        assert "data" in result.output_roles
        assert "metrics" in result.output_roles

    def test_output_returns_reference(self):
        """Test that output() returns an OutputReference."""
        result = StepResult(
            step_name="ingest",
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            output_roles=frozenset(["data"]),
            output_types={"data": "csv"},
            step_run_id="a" * 32,
        )._bind_source(output_reference("a" * 32).source)
        ref = result.output("data")
        assert isinstance(ref, OutputReference)
        assert ref.source.step_run_id == result.step_run_id
        assert ref.role == "data"
        assert ref.artifact_type == "csv"

    def test_output_missing_role_raises(self):
        """Test that output() raises ValueError for missing role."""
        result = StepResult(
            step_name="ingest",
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            output_roles=frozenset(["data"]),
        )
        with pytest.raises(ValueError, match="Output role 'missing' not available"):
            result.output("missing")

    def test_output_error_message_includes_available(self):
        """Test that error message includes available roles."""
        result = StepResult(
            step_name="ingest",
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            output_roles=frozenset(["alpha", "beta"]),
        )
        with pytest.raises(ValueError, match="Available roles: alpha, beta"):
            result.output("gamma")

    def test_has_failures_property(self):
        """Test has_failures property."""
        no_failures = StepResult(
            step_name="test",
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            failed_count=0,
        )
        assert no_failures.has_failures is False

        with_failures = StepResult(
            step_name="test",
            step_number=0,
            status=StepStatus.FAILED,
            error="five items failed",
            total_count=5,
            failed_count=5,
        )
        assert with_failures.has_failures is True

    def test_frozen(self):
        """Test that StepResult is frozen (immutable)."""
        result = StepResult(
            step_name="test",
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
        )
        with pytest.raises(ValidationError):
            result.step_name = "changed"


def test_binding_copies_result_without_changing_durable_payload() -> None:
    result = _result(
        step_run_id="a" * 32,
        output_roles=frozenset({"data"}),
        output_types={"data": "csv"},
    )
    before = result.model_dump_json()
    bound = result._bind_source(output_reference("a" * 32).source)
    assert bound is not result
    assert bound.model_dump_json() == result.model_dump_json() == before
    assert bound.output("data").source.step_run_id == "a" * 32
    for unbound in (result, StepResult.model_validate_json(before)):
        with pytest.raises(ValueError, match="not bound to a source store"):
            unbound.output("data")


def test_result_rejects_another_attempt_binding() -> None:
    result = _result(step_run_id="a" * 32, output_roles=frozenset({"data"}))
    with pytest.raises(ValueError, match="does not match"):
        result._bind_source(output_reference("b" * 32).source)
    bound = result._bind_source(output_reference("a" * 32).source)
    changed = bound.model_copy(update={"step_run_id": "b" * 32})
    with pytest.raises(ValueError, match="does not match"):
        changed.output("data")
