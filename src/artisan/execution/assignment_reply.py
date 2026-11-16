"""Inert local worker replies preserving scientific failure authority."""

from __future__ import annotations

from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from artisan.execution.exceptions import AssignmentExecutionError
from artisan.execution.recording.commands import sanitize_diagnostic
from artisan.schemas.execution.assignment import (
    AssignmentAttempt,
    DeliveryIdentity,
    SealedDelivery,
)
from artisan.schemas.execution.worker_transport import WorkerTransportHeader
from artisan.utils.command_supervisor import CommandOwnershipError
from artisan.utils.external_tools import redact_values
from artisan.utils.traceback import format_error


class AssignmentReplyContext(BaseModel):
    """Controller-captured attempt and redaction bound to the exact live payload."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    domain: Literal["assignment"] = "assignment"
    attempt: AssignmentAttempt
    payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$", strict=True)
    sensitive_values: tuple[str, ...] = Field(repr=False)

    def verify(self, header: WorkerTransportHeader) -> None:
        """Reject a swapped unit payload before deserialization or science."""
        if self.payload_sha256 != header.payload_sha256:
            msg = "Assignment reply context differs from its exact worker payload"
            raise ValueError(msg)


class AssignmentReply(BaseModel):
    """Exactly one receipt or classified failure for one admitted attempt."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    attempt: AssignmentAttempt
    kind: Literal["success", "science", "infrastructure", "ownership"]
    receipt: SealedDelivery | None = None
    failed_delivery: DeliveryIdentity | None = None
    message: str | None = None
    phase: str | None = None
    call_path: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_outcome(self) -> Self:
        """Reject incomplete, contradictory and cross-attempt worker replies."""
        if self.kind == "success":
            if (
                self.receipt is None
                or self.receipt.delivery.attempt != self.attempt
                or self.message is not None
                or self.phase is not None
                or self.call_path
                or self.failed_delivery is not None
            ):
                msg = "Successful assignment reply requires its exact receipt only"
                raise ValueError(msg)
        elif self.receipt is not None or not self.message:
            msg = "Failed assignment reply requires a diagnostic and no receipt"
            raise ValueError(msg)
        elif self.kind == "science":
            if (
                self.failed_delivery is None
                or self.failed_delivery.attempt != self.attempt
            ):
                msg = "Scientific failure requires its exact closed delivery"
                raise ValueError(msg)
        elif (
            self.phase is not None or self.call_path or self.failed_delivery is not None
        ):
            msg = "Infrastructure failures cannot claim a scientific call location"
            raise ValueError(msg)
        return self

    @classmethod
    def failed(
        cls,
        attempt: AssignmentAttempt,
        error: Exception,
        *,
        sensitive_values: tuple[str, ...] = (),
    ) -> Self:
        """Copy known failure authority into JSON without serializing exceptions."""
        if isinstance(error, AssignmentExecutionError):
            return cls(
                attempt=attempt,
                kind="science",
                failed_delivery=error.delivery,
                message=redact_values(
                    sanitize_diagnostic(str(error)), sensitive_values
                ),
                phase=(
                    redact_values(error.phase, sensitive_values)
                    if error.phase is not None
                    else None
                ),
                call_path=tuple(
                    redact_values(part, sensitive_values) for part in error.call_path
                ),
            )
        return cls(
            attempt=attempt,
            kind="ownership"
            if isinstance(error, CommandOwnershipError)
            else "infrastructure",
            message=redact_values(
                sanitize_diagnostic(format_error(error)), sensitive_values
            ),
        )

    def result(
        self, expected: AssignmentAttempt, *, sensitive_values: tuple[str, ...] = ()
    ) -> SealedDelivery:
        """Check attempt binding before exposing a receipt or classified failure."""
        if self.attempt != expected:
            msg = "Worker reply belongs to a different admitted attempt"
            raise ValueError(msg)
        if self.receipt is not None:
            return self.receipt
        assert self.message is not None
        message = redact_values(self.message, sensitive_values)
        if self.kind == "science":
            raise AssignmentExecutionError(
                message,
                phase=self.phase,
                call_path=self.call_path,
                delivery=self.failed_delivery,
            )
        if self.kind == "ownership":
            raise CommandOwnershipError(message)
        raise RuntimeError(message)
