"""Authoritative lifecycle types for one pipeline step attempt."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class StepStatus(StrEnum):
    """Durable lifecycle state for one step attempt."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


class StepDisposition(StrEnum):
    """How a usable step result was obtained."""

    EXECUTED = "executed"
    CACHE_HIT = "cache_hit"


class CancellationStatus(StrEnum):
    """Evidence returned for a cancellation request."""

    REQUESTED = "requested"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class CancellationAcknowledgement:
    """Provider evidence for one cancellation request."""

    status: CancellationStatus
    message: str | None = None


TERMINAL_STEP_STATUSES = frozenset(
    {
        StepStatus.SUCCEEDED,
        StepStatus.PARTIAL,
        StepStatus.FAILED,
        StepStatus.CANCELLED,
        StepStatus.SKIPPED,
    }
)

_STEP_TRANSITIONS = {
    StepStatus.PENDING: frozenset(
        {StepStatus.RUNNING, StepStatus.SKIPPED, StepStatus.CANCELLED}
    ),
    StepStatus.RUNNING: frozenset(
        {
            StepStatus.SUCCEEDED,
            StepStatus.PARTIAL,
            StepStatus.FAILED,
            StepStatus.CANCELLED,
        }
    ),
}

_CANCELLATION_TRANSITIONS = {
    None: frozenset({CancellationStatus.REQUESTED}),
    CancellationStatus.REQUESTED: frozenset(
        {
            CancellationStatus.CONFIRMED,
            CancellationStatus.REJECTED,
            CancellationStatus.UNKNOWN,
        }
    ),
}


def validate_step_transition(current: StepStatus, target: StepStatus) -> None:
    """Validate one lifecycle transition.

    Raises:
        ValueError: If the transition is not in the lifecycle graph.
    """
    if target not in _STEP_TRANSITIONS.get(current, frozenset()):
        msg = f"Invalid step transition: {current.value} -> {target.value}"
        raise ValueError(msg)


def validate_cancellation_transition(
    current: CancellationStatus | None,
    target: CancellationStatus,
) -> None:
    """Validate one cancellation-acknowledgement transition.

    Raises:
        ValueError: If the acknowledgement progression is invalid.
    """
    if target not in _CANCELLATION_TRANSITIONS.get(current, frozenset()):
        current_value = current.value if current is not None else "none"
        msg = f"Invalid cancellation transition: {current_value} -> {target.value}"
        raise ValueError(msg)
