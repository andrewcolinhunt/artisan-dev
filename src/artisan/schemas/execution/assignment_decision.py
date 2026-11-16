"""Controller decisions ordering admission, cancellation, winner and cache reuse."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from artisan.schemas.execution.assignment import (
    AssignmentAttempt,
    ContentDigest,
    SealedDelivery,
)


class AssignmentDecision(BaseModel):
    """One immutable decision in a logical unit's contiguous journal."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )

    sequence: int = Field(ge=0, strict=True)
    action: Literal["admit", "cancel", "claim", "reuse"]
    attempt: AssignmentAttempt
    admission_digest: ContentDigest
    winner: SealedDelivery | None = None
    cache_source: SealedDelivery | None = None

    @model_validator(mode="after")
    def _validate_winner(self) -> AssignmentDecision:
        if (self.action == "claim") != (self.winner is not None):
            msg = "Only a winner claim must contain a sealed delivery"
            raise ValueError(msg)
        if self.winner is not None and self.winner.delivery.attempt != self.attempt:
            msg = "Winner claim and sealed delivery must name the same attempt"
            raise ValueError(msg)
        if (self.action == "reuse") != (self.cache_source is not None):
            msg = "Only a reuse decision must contain a cache source"
            raise ValueError(msg)
        if self.cache_source is not None and (
            self.attempt.replay_of_execution_run_id is not None
            or self.cache_source.delivery.attempt.replay_of_execution_run_id is not None
        ):
            msg = "Diagnostic assignments cannot participate in ordinary reuse"
            raise ValueError(msg)
        return self
