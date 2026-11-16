"""Mark direct operation failures inside the assignment execution boundary."""

from __future__ import annotations

from collections.abc import Iterator
from concurrent.futures import CancelledError
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

from artisan.execution.exceptions import AssignmentExecutionError
from artisan.execution.recording.commands import (
    CommandRecorder,
    current_recorder,
    sanitize_diagnostic,
)
from artisan.execution.recording.durable import sanitize_error_envelope
from artisan.utils.command_supervisor import CommandOwnershipError
from artisan.utils.external_tools import redact_values
from artisan.utils.traceback import format_error

_assignment_science: ContextVar[tuple[str, ...] | None] = ContextVar(
    "assignment_science", default=None
)


@contextmanager
def assignment_science(*, sensitive_values: tuple[str, ...] = ()) -> Iterator[None]:
    """Enable scientific markers without changing the legacy invocation contract."""
    token = _assignment_science.set(sensitive_values)
    try:
        yield
    finally:
        _assignment_science.reset(token)


@contextmanager
def scientific_phase(phase: str, *, call_path: tuple[str, ...] = ()) -> Iterator[None]:
    """Mark an authored hook, preserving cancellation and ownership uncertainty."""
    try:
        yield
    except (AssignmentExecutionError, CommandOwnershipError, CancelledError):
        raise
    except Exception as error:
        sensitive_values = _assignment_science.get()
        if sensitive_values is None:
            raise
        raise AssignmentExecutionError(
            redact_values(sanitize_diagnostic(format_error(error)), sensitive_values),
            phase=phase,
            call_path=call_path,
            error_envelope=_error_envelope(error, sensitive_values),
        ) from None


def _error_envelope(
    error: Exception, sensitive_values: tuple[str, ...]
) -> dict[str, Any] | None:
    """Keep optional structured observations from replacing the authored error."""
    from artisan.execution.recording.recorder import error_envelope_dict

    try:
        recorder = current_recorder() or CommandRecorder()
        recorder.add_sensitive_data(sensitive_values)
        return sanitize_error_envelope(recorder, error_envelope_dict(error))
    except Exception:
        return None
