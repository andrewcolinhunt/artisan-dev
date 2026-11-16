"""Optional observed execution details, separate from retained-result authority."""

from __future__ import annotations

from typing import Annotated, Any, Literal, Self

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from artisan.errors import ArtisanErrorEnvelope
from artisan.schemas.execution.assignment import ContentDigest, DeliveryIdentity
from artisan.schemas.execution.command_record import CommandRecording
from artisan.utils.hashing import canonical_json_bytes

MAX_DIAGNOSTIC_BYTES = 8 * 1024**2
MAX_DIAGNOSTIC_MESSAGE = 32 * 1024
Seconds = Annotated[float, Field(ge=0, allow_inf_nan=False)]
type DiagnosticOutcome = Literal[
    "success", "science", "infrastructure", "cancelled", "ownership"
]


class _DiagnosticModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )

    error_envelope: dict[str, Any] | None = None

    @field_validator("error_envelope")
    @classmethod
    def _bounded_envelope(cls, value: dict[str, Any] | None) -> dict[str, Any] | None:
        if value is not None:
            ArtisanErrorEnvelope.model_validate(value)
            if len(canonical_json_bytes(value)) > MAX_DIAGNOSTIC_MESSAGE:
                msg = "Structured error diagnostics exceed their byte limit"
                raise ValueError(msg)
        return value


class InvocationDiagnostic(_DiagnosticModel):
    """One observed lifecycle partition; this is not a persistent execution node."""

    invocation_id: str
    call_path: tuple[str, ...]
    operation_name: str
    operation_version: str
    succeeded: bool = Field(strict=True)
    cleanup: Literal["closed", "unknown"]
    duration_seconds: Seconds
    timings: dict[str, Seconds]
    command_recording: CommandRecording
    tool_output: str | None
    phase: str | None
    error: str | None = Field(max_length=MAX_DIAGNOSTIC_MESSAGE)

    @model_validator(mode="after")
    def _consistent_outcome(self) -> Self:
        if self.succeeded and (
            self.cleanup != "closed"
            or self.error is not None
            or self.error_envelope is not None
        ):
            msg = (
                "Successful invocation diagnostics require closed cleanup and no error"
            )
            raise ValueError(msg)
        if self.cleanup == "unknown" and (
            self.tool_output is not None
            or self.command_recording.status != "unavailable"
        ):
            msg = "Unknown cleanup cannot claim final command or file diagnostics"
            raise ValueError(msg)
        return self


class DeliveryDiagnostics(_DiagnosticModel):
    """Bounded worker observations; only independent acceptance selects a result."""

    domain: Literal["artisan.delivery-diagnostics"] = "artisan.delivery-diagnostics"
    schema_version: Literal[1] = 1
    delivery: DeliveryIdentity
    timestamp_start: AwareDatetime
    seal_digest: ContentDigest | None
    outcome: DiagnosticOutcome
    cleanup: Literal["closed", "unknown"]
    duration_seconds: Seconds
    invocations: tuple[InvocationDiagnostic, ...]
    omitted_invocations: int = Field(ge=0, strict=True)
    phase: str | None
    call_path: tuple[str, ...]
    error: str | None = Field(max_length=MAX_DIAGNOSTIC_MESSAGE)

    @field_validator("schema_version", mode="before")
    @classmethod
    def _strict_version(cls, value: object) -> object:
        if type(value) is not int:
            msg = "Delivery diagnostic version must be an integer"
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def _consistent_outcome(self) -> Self:
        if self.outcome == "success" and (
            self.seal_digest is None
            or self.cleanup != "closed"
            or self.error is not None
            or self.error_envelope is not None
            or self.phase is not None
            or self.call_path
        ):
            msg = (
                "Successful delivery diagnostics require their seal and closed cleanup"
            )
            raise ValueError(msg)
        if self.outcome != "success" and not self.error:
            msg = "Failed delivery diagnostics require an explicit diagnostic"
            raise ValueError(msg)
        if self.outcome == "ownership" and self.cleanup != "unknown":
            msg = "Unresolved command ownership requires unknown cleanup diagnostics"
            raise ValueError(msg)
        identifiers = [item.invocation_id for item in self.invocations]
        if len(identifiers) != len(set(identifiers)):
            msg = "Invocation diagnostics repeat an observed lifecycle partition"
            raise ValueError(msg)
        return self
