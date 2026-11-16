"""Optional replay configuration and diagnostic selection for retained results."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from artisan.schemas.execution.assignment import (
    ContentDigest,
    DeliveryIdentity,
    SoftwareDigest,
)


class _ReplayModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )

    @field_validator("schema_version", mode="before", check_fields=False)
    @classmethod
    def _strict_version(cls, value: object) -> object:
        if type(value) is not int:
            msg = "Replay evidence version must be an integer"
            raise ValueError(msg)
        return value


class RetainedReplayConfiguration(_ReplayModel):
    """Capture required runtime keys without persisting injected values.

    An available configuration with no keys is a captured empty configuration.
    Missing capture does not imply that replay can run without replacements.
    """

    domain: Literal["artisan.retained-replay-configuration"] = (
        "artisan.retained-replay-configuration"
    )
    schema_version: Literal[1] = 1
    delivery: DeliveryIdentity
    admission_digest: ContentDigest
    seal_digest: ContentDigest | None
    status: Literal["available", "unavailable"]
    required_runtime_keys: tuple[
        Annotated[str, Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$", strict=True)], ...
    ] = ()
    unavailable_reason: str | None = None

    @model_validator(mode="after")
    def _validate_capture(self) -> RetainedReplayConfiguration:
        keys = self.required_runtime_keys
        if list(keys) != sorted(set(keys)) or any(not key for key in keys):
            msg = "Replay runtime keys must be nonempty, unique and sorted"
            raise ValueError(msg)
        if (self.status == "unavailable") != bool(self.unavailable_reason):
            msg = "Unavailable replay configuration requires an explicit reason"
            raise ValueError(msg)
        if self.status == "unavailable" and keys:
            msg = "Unavailable replay configuration cannot claim captured runtime keys"
            raise ValueError(msg)
        return self


class RetainedReplayDiagnostic(_ReplayModel):
    """Preserve source and selected identities without asserting equivalence."""

    domain: Literal["artisan.retained-replay-diagnostic"] = (
        "artisan.retained-replay-diagnostic"
    )
    schema_version: Literal[1] = 1
    source_delivery: DeliveryIdentity
    source_admission_digest: ContentDigest
    source_seal_digest: ContentDigest | None
    selected_recipe_digest: ContentDigest
    selected_software_digest: SoftwareDigest
    allow_code_change: bool = Field(strict=True)

    @model_validator(mode="after")
    def _validate_selection(self) -> RetainedReplayDiagnostic:
        original = self.source_delivery.attempt
        if not self.allow_code_change and (
            original.recipe_digest != self.selected_recipe_digest
            or original.software_digest != self.selected_software_digest
        ):
            msg = "Changed replay identity requires explicit changed-code selection"
            raise ValueError(msg)
        return self


class RetainedReplayDiagnosticRecord(_ReplayModel):
    """Bind optional changed-code evidence to its own diagnostic result only."""

    domain: Literal["artisan.retained-replay-result"] = "artisan.retained-replay-result"
    schema_version: Literal[1] = 1
    delivery: DeliveryIdentity
    seal_digest: ContentDigest | None
    diagnostic: RetainedReplayDiagnostic

    @model_validator(mode="after")
    def _validate_owner(self) -> RetainedReplayDiagnosticRecord:
        attempt = self.delivery.attempt
        if (
            attempt.replay_of_execution_run_id
            != self.diagnostic.source_delivery.delivery_id
            or attempt.recipe_digest != self.diagnostic.selected_recipe_digest
            or attempt.software_digest != self.diagnostic.selected_software_digest
            or self.delivery.delivery_id == self.diagnostic.source_delivery.delivery_id
            or attempt.assignment == self.diagnostic.source_delivery.attempt.assignment
        ):
            msg = "Replay diagnostic does not match its diagnostic result owner"
            raise ValueError(msg)
        return self
