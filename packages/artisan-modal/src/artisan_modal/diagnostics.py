"""Bounded redacted observations, separate from SDK or worker closure authority."""

from __future__ import annotations

import json
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from artisan.orchestration.runner_api import CommandRecorder, OwnershipState

CallPhase = Literal[
    "start",
    "prepare",
    "submit",
    "receive",
    "decode",
    "core_result",
    "complete",
    "failed",
    "cancel_requested",
    "cancel_returned",
    "cancel_failed",
]


class CallObservation(BaseModel):
    """One transport phase; it never carries request, reply, or credential bytes."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    owner_id: UUID
    sequence: int = Field(ge=0, lt=64)
    phase: CallPhase
    elapsed_seconds: float = Field(ge=0)
    call_id: str | None = Field(
        default=None, pattern=r"^fc-[A-Za-z0-9]+$", max_length=128
    )
    error_type: str | None = Field(default=None, max_length=128)
    message: str | None = Field(default=None, max_length=1024)
    core_ownership: OwnershipState | None = None
    core_entry_id: UUID | None = None
    core_retained_working: bool | None = None


def redacted_message(message: str, sensitive_values: tuple[str, ...]) -> str:
    """Apply captured literal/escaped-value redaction before bounding diagnostics."""
    recorder = CommandRecorder()
    recorder.add_sensitive_data(
        tuple(
            encoding
            for value in sensitive_values
            if value
            for encoding in (value, json.dumps(value)[1:-1], repr(value)[1:-1])
        )
    )
    return recorder.sanitize(message)[:1024]


def sensitive_strings(value: object) -> tuple[str, ...]:
    """Capture explicit nested injected values before bounded redaction."""
    if isinstance(value, str):
        return (value,)
    if isinstance(value, dict):
        return tuple(
            item for child in value.values() for item in sensitive_strings(child)
        )
    if isinstance(value, (tuple, list)):
        return tuple(item for child in value for item in sensitive_strings(child))
    return ()
