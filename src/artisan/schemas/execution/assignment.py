"""Immutable assignment, attempt and delivery ownership for retained results."""

from __future__ import annotations

from typing import Annotated, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

from artisan.utils.hashing import canonical_json_bytes, compute_content_digest

type ContentDigest = Annotated[str, Field(pattern=r"^[0-9a-f]{32}$", strict=True)]
type SoftwareDigest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$", strict=True)]


class _AssignmentModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )


class AssignmentIdentity(_AssignmentModel):
    """One logical outer unit and its stable order within a step attempt."""

    step_run_id: ContentDigest
    unit_index: int = Field(ge=0, strict=True)

    @property
    def assignment_id(self) -> str:
        """Derive the unit owner without duplicating authoritative root fields."""
        return compute_content_digest(
            b"artisan.assignment-v1\0"
            + canonical_json_bytes(self.model_dump(mode="json"))
        )


class AssignmentAttempt(_AssignmentModel):
    """Controller-admitted attempt with frozen scientific and input evidence.

    The input-context digest binds the exact admission snapshot. It is not by
    itself an owner-independent scientific cache key. Diagnostic source identity
    is explicit so diagnostic results cannot enter ordinary acceptance or caches.
    An observed runtime can have a transport identity without qualifying its
    full dependency set for scientific cache reuse.
    """

    assignment: AssignmentIdentity
    attempt: int = Field(ge=0, strict=True)
    generation: int = Field(ge=0, strict=True)
    recipe_digest: ContentDigest
    software_digest: SoftwareDigest
    software_cacheable: bool = Field(default=True, strict=True)
    input_context_digest: ContentDigest
    replay_of_execution_run_id: ContentDigest | None = None


class DeliveryIdentity(_AssignmentModel):
    """A fresh provider entry of one admitted attempt, including redeliveries."""

    attempt: AssignmentAttempt
    delivery_id: str = Field(pattern=r"^[0-9a-f]{32}$", strict=True)

    @field_validator("delivery_id")
    @classmethod
    def _validate_delivery_id(cls, value: str) -> str:
        if UUID(hex=value).version != 4:
            msg = "Delivery identity must be a canonical UUID4 hexadecimal value"
            raise ValueError(msg)
        return value

    @classmethod
    def new(cls, attempt: AssignmentAttempt) -> DeliveryIdentity:
        """Create at worker entry; never reuse a delivery ID from submitted work."""
        return cls(attempt=attempt, delivery_id=uuid4().hex)


class SuccessfulDelivery(_AssignmentModel):
    """Completed computation and resolved cleanup, ready for worker sealing.

    The worker constructs this only after observing both outcomes. These fields
    carry that evidence; they do not grant controller acceptance authority.
    """

    delivery: DeliveryIdentity
    computation_complete: Literal[True]
    cleanup_complete: Literal[True]

    @field_validator("computation_complete", "cleanup_complete", mode="before")
    @classmethod
    def _require_complete(cls, value: object) -> object:
        if value is not True:
            msg = "Successful sealing requires completed computation and cleanup"
            raise ValueError(msg)
        return value


class SealedDelivery(_AssignmentModel):
    """A completed staging locator and digest, without acceptance authority."""

    delivery: DeliveryIdentity
    seal_uri: str = Field(min_length=1, strict=True)
    seal_digest: ContentDigest
