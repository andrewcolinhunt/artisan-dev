"""Closed curator replies bind staged evidence to its exact admitted invocation."""

from __future__ import annotations

from dataclasses import replace
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from artisan.execution.recording.commands import sanitize_diagnostic
from artisan.execution.recording.parquet_writer import StagingResult
from artisan.schemas.execution.curator_admission import CuratorAdmissionReference
from artisan.schemas.execution.worker_transport import WorkerTransportHeader
from artisan.utils.command_supervisor import CommandOwnershipError
from artisan.utils.external_tools import redact_values
from artisan.utils.traceback import format_error


class CuratorReplyContext(BaseModel):
    """Controller-captured identity and redaction bound before live-call loading."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    domain: Literal["curator"] = "curator"
    admission: CuratorAdmissionReference
    payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$", strict=True)
    sensitive_values: tuple[str, ...] = Field(repr=False)

    def verify(self, header: WorkerTransportHeader) -> None:
        if self.payload_sha256 != header.payload_sha256:
            msg = "Curator reply context differs from its exact worker payload"
            raise ValueError(msg)


class CuratorReply(BaseModel):
    """Staging is evidence for controller validation, never acceptance authority."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    admission: CuratorAdmissionReference
    kind: Literal["staged", "infrastructure", "ownership"]
    staging: StagingResult | None = None
    message: str | None = None

    @model_validator(mode="after")
    def validate_outcome(self) -> Self:
        if self.kind == "staged":
            if self.staging is None or self.message is not None:
                msg = "Staged curator reply requires its result only"
                raise ValueError(msg)
        elif self.staging is not None or not self.message:
            msg = "Failed curator reply requires a diagnostic only"
            raise ValueError(msg)
        return self

    @classmethod
    def staged(cls, context: CuratorReplyContext, result: StagingResult) -> Self:
        return cls(
            admission=context.admission,
            kind="staged",
            staging=_redact_staging(result, context.sensitive_values),
        )

    @classmethod
    def failed(cls, context: CuratorReplyContext, error: Exception) -> Self:
        return cls(
            admission=context.admission,
            kind="ownership"
            if isinstance(error, CommandOwnershipError)
            else "infrastructure",
            message=redact_values(
                sanitize_diagnostic(format_error(error)), context.sensitive_values
            ),
        )

    def result(
        self,
        expected: CuratorAdmissionReference,
        *,
        sensitive_values: tuple[str, ...] = (),
    ) -> StagingResult:
        if self.admission != expected:
            msg = "Worker reply belongs to a different curator admission"
            raise ValueError(msg)
        if self.staging is not None:
            return _redact_staging(self.staging, sensitive_values)
        assert self.message is not None
        message = redact_values(self.message, sensitive_values)
        if self.kind == "ownership":
            raise CommandOwnershipError(message)
        raise RuntimeError(message)


def _redact_staging(result: StagingResult, sensitive: tuple[str, ...]) -> StagingResult:
    acknowledgement = result.cancellation_acknowledgement
    if acknowledgement is not None and acknowledgement.message is not None:
        acknowledgement = replace(
            acknowledgement, message=redact_values(acknowledgement.message, sensitive)
        )
    return replace(
        result,
        error=redact_values(result.error, sensitive)
        if result.error is not None
        else None,
        cancellation_acknowledgement=acknowledgement,
    )
