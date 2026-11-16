"""Finite ordered assignment membership frozen by one controller step."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from artisan.schemas.composites.plan import ExecutionRecipe
from artisan.schemas.execution.admission import AdmissionReference
from artisan.schemas.execution.assignment import ContentDigest, SoftwareDigest
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest

ADMISSION_DIGEST = "assignment_admission_digest"


class StepAdmission(BaseModel):
    """Bind a complete finite unit list before cache reuse or worker dispatch.

    The step lifecycle records this object's digest. Individual assignments retain
    their own cancellation, result selection and recovery decisions. Empty
    membership records a skipped step, never a fabricated successful delivery.
    """

    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )

    domain: Literal["artisan.step-admission"] = "artisan.step-admission"
    schema_version: Literal[1] = 1
    step_run_id: ContentDigest
    step_number: int = Field(ge=0, strict=True)
    recipe: ExecutionRecipe
    software_digest: SoftwareDigest | None
    assignments: tuple[AdmissionReference, ...]

    @field_validator("schema_version", mode="before")
    @classmethod
    def _strict_version(cls, value: object) -> object:
        if type(value) is not int:
            msg = "Step admission version must be an integer"
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def _validate_membership(self) -> StepAdmission:
        if bool(self.assignments) != (self.software_digest is not None):
            msg = "Only a step with assigned work binds execution software"
            raise ValueError(msg)
        for index, reference in enumerate(self.assignments):
            attempt = reference.attempt
            if (
                attempt.assignment.step_run_id != self.step_run_id
                or attempt.assignment.unit_index != index
                or attempt.recipe_digest != self.recipe.digest
                or attempt.software_digest != self.software_digest
            ):
                msg = "Step admission requires complete ordered units of one recipe and software"
                raise ValueError(msg)
        diagnostics = {
            reference.attempt.replay_of_execution_run_id
            for reference in self.assignments
        }
        if len(diagnostics) > 1:
            msg = "Step admission cannot mix ordinary and diagnostic ownership"
            raise ValueError(msg)
        if len({item.attempt.software_cacheable for item in self.assignments}) > 1:
            msg = "Step admission cannot mix software cache qualification"
            raise ValueError(msg)
        return self

    @property
    def digest(self) -> str:
        """Bind finite membership and all controller-frozen assignment digests."""
        return compute_content_digest(
            b"artisan.step-admission-v1\0"
            + canonical_json_bytes(self.model_dump(mode="json"))
        )
