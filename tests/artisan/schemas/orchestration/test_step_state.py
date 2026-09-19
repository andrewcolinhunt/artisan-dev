"""Tests for persisted step state models."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.schemas.orchestration.step_state import StepState


def _state() -> StepState:
    return StepState(
        pipeline_run_id="run",
        step_run_id="a" * 32,
        step_number=1,
        step_name="op",
        step_spec_id="b" * 32,
        status=StepStatus.PARTIAL,
        state_sequence=2,
        disposition=StepDisposition.EXECUTED,
        operation_class="tests.Op",
        params_json="{}",
        input_refs_json="{}",
        compute_backend="local",
        compute_options_json="{}",
        total_count=3,
        succeeded_count=2,
        failed_count=1,
        timestamp=datetime.now(UTC),
        duration_seconds=1.0,
        output_roles=frozenset({"data"}),
        output_types={"data": "data"},
    )


def test_to_step_result_preserves_status() -> None:
    result = _state().to_step_result()
    assert result.status is StepStatus.PARTIAL
    assert result.disposition is StepDisposition.EXECUTED
    assert "success" not in result.model_dump()


def test_step_state_is_frozen() -> None:
    state = _state()
    with pytest.raises(ValidationError):
        state.step_number = 4


def test_start_record_allows_unknown_concrete_spec() -> None:
    record = StepStartRecord(
        step_run_id="a" * 32,
        step_number=0,
        step_name="op",
        operation_class="tests.Op",
        params_json="{}",
        input_refs_json="{}",
        compute_backend="local",
        compute_options_json="{}",
        output_roles_json="[]",
        output_types_json="{}",
    )
    assert record.step_spec_id is None
