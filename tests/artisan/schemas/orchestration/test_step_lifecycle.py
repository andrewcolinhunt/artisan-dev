"""Tests for the authoritative step lifecycle graph."""

from __future__ import annotations

import pytest

from artisan.schemas.orchestration.step_lifecycle import (
    TERMINAL_STEP_STATUSES,
    CancellationStatus,
    StepStatus,
    validate_cancellation_transition,
    validate_step_transition,
)


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (StepStatus.PENDING, StepStatus.RUNNING),
        (StepStatus.PENDING, StepStatus.SKIPPED),
        (StepStatus.PENDING, StepStatus.CANCELLED),
        (StepStatus.RUNNING, StepStatus.SUCCEEDED),
        (StepStatus.RUNNING, StepStatus.PARTIAL),
        (StepStatus.RUNNING, StepStatus.FAILED),
        (StepStatus.RUNNING, StepStatus.CANCELLED),
    ],
)
def test_validate_step_transition_accepts_graph_edges(
    current: StepStatus, target: StepStatus
) -> None:
    validate_step_transition(current, target)


@pytest.mark.parametrize("terminal", sorted(TERMINAL_STEP_STATUSES, key=str))
@pytest.mark.parametrize("target", list(StepStatus))
def test_validate_step_transition_rejects_terminal_edges(
    terminal: StepStatus, target: StepStatus
) -> None:
    with pytest.raises(ValueError, match="Invalid step transition"):
        validate_step_transition(terminal, target)


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (None, CancellationStatus.REQUESTED),
        (CancellationStatus.REQUESTED, CancellationStatus.CONFIRMED),
        (CancellationStatus.REQUESTED, CancellationStatus.REJECTED),
        (CancellationStatus.REQUESTED, CancellationStatus.UNKNOWN),
    ],
)
def test_validate_cancellation_transition_accepts_graph_edges(
    current: CancellationStatus | None, target: CancellationStatus
) -> None:
    validate_cancellation_transition(current, target)


def test_enums_parse_persisted_values() -> None:
    assert StepStatus("partial") is StepStatus.PARTIAL
    assert CancellationStatus("unknown") is CancellationStatus.UNKNOWN
