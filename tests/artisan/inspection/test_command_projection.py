"""Command response coverage preserves grouped observations and honest absence."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from artisan.inspection.models import (
    ExecutionCommands,
    FailureIdentity,
    InvocationCommands,
)
from artisan.schemas.execution.command_record import CommandRecording


def _group(identity: str, *, unavailable: bool = False) -> InvocationCommands:
    return InvocationCommands(
        invocation_id=identity,
        call_path=(identity,),
        operation_name="synthetic",
        operation_version="1",
        recording=CommandRecording.unavailable()
        if unavailable
        else CommandRecording.empty(),
    )


@pytest.mark.parametrize(
    ("omitted", "status"), [(None, "unavailable"), (0, "complete"), (2, "unavailable")]
)
def test_absent_empty_and_omitted_observations_remain_distinct(
    omitted: int | None, status: str
) -> None:
    model = ExecutionCommands.model_validate(
        {
            "execution_run_id": "source",
            "status": status,
            "recording": None,
            "invocations": (),
            "omitted_invocations": omitted,
        }
    )
    assert model.omitted_invocations == omitted
    assert ExecutionCommands.model_validate_json(model.model_dump_json()) == model


def test_group_order_and_partial_coverage_are_preserved() -> None:
    groups = (_group("z"), _group("a", unavailable=True))
    result = ExecutionCommands(
        execution_run_id="source",
        status="partial",
        recording=None,
        invocations=groups,
        omitted_invocations=0,
    )
    assert result.invocations == groups
    with pytest.raises(ValidationError, match="coverage"):
        ExecutionCommands.model_validate({**result.model_dump(), "status": "complete"})


@pytest.mark.parametrize("omitted", [True, -1, 1.5])
def test_omission_counts_are_strict_nonnegative_integers(omitted: object) -> None:
    with pytest.raises(ValidationError):
        ExecutionCommands.model_validate(
            {
                "execution_run_id": "source",
                "status": "unavailable",
                "recording": None,
                "invocations": (),
                "omitted_invocations": omitted,
            }
        )


def test_direct_and_grouped_recordings_cannot_coexist() -> None:
    with pytest.raises(ValidationError, match="coexist"):
        ExecutionCommands(
            execution_run_id="source",
            status="complete",
            recording=CommandRecording.empty(),
            invocations=(_group("child"),),
            omitted_invocations=0,
        )
    with pytest.raises(ValidationError, match="repeat"):
        ExecutionCommands(
            execution_run_id="source",
            status="complete",
            recording=None,
            invocations=(_group("child"), _group("child")),
            omitted_invocations=0,
        )


def test_failure_timestamp_and_log_are_required_but_nullable() -> None:
    row = {
        "step": 1,
        "operation": "synthetic",
        "execution_run_id": "source",
        "timestamp_start": None,
        "code": None,
        "recovery_hint": None,
        "field": None,
        "log": None,
    }
    assert FailureIdentity.model_validate(row).log is None
    del row["log"]
    with pytest.raises(ValidationError):
        FailureIdentity.model_validate(row)
