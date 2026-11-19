"""Immutable scientific input admission, independent of optional diagnostics."""

from __future__ import annotations

import json
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from artisan.schemas.composites.plan import ExecutionRecipe
from artisan.schemas.execution.assignment import AssignmentAttempt, ContentDigest
from artisan.schemas.execution.retained_result import RetainedTableFile
from artisan.schemas.provenance.projection import BoundaryProjection


class _AdmissionModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )


class ManagedCarrier(_AdmissionModel):
    """Complete managed file identity, separate from an artifact's record hash."""

    digest: ContentDigest
    size_bytes: int = Field(ge=0, strict=True)

    @property
    def relative_path(self) -> str:
        """Return the deterministic path within the configured managed files root."""
        return f"carriers/{self.digest[:2]}/{self.digest}.bin"


class ManagedArtifactBinding(_AdmissionModel):
    """Associate externally backed artifact content with its complete carrier."""

    artifact_id: ContentDigest
    carrier_digest: ContentDigest


class AdmissionReference(_AdmissionModel):
    """Expected immutable record identity, bound by the controller journal."""

    attempt: AssignmentAttempt
    record_digest: ContentDigest


class _AdmissionRecord(_AdmissionModel):
    """Required recipe and exact input evidence persisted before dispatch."""

    domain: Literal["artisan.assignment-admission"] = "artisan.assignment-admission"
    schema_version: int
    attempt: AssignmentAttempt
    step_number: int = Field(ge=0, strict=True)
    recipe: ExecutionRecipe
    boundary: BoundaryProjection
    tables: tuple[RetainedTableFile, ...]
    carriers: tuple[ManagedCarrier, ...] = ()
    external_bindings: tuple[ManagedArtifactBinding, ...] = ()

    @field_validator("schema_version", mode="before")
    @classmethod
    def _strict_version(cls, value: object) -> object:
        if type(value) is not int:
            msg = "Admission version must be an integer"
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def _validate_evidence(self) -> Self:
        if (
            self.recipe.digest != self.attempt.recipe_digest
            or self.boundary.input_context_digest != self.attempt.input_context_digest
        ):
            msg = "Admission does not match its frozen recipe and input identity"
            raise ValueError(msg)
        if any(
            (
                self.boundary.exports,
                self.boundary.support,
                self.boundary.parents,
                self.boundary.associations,
                self.boundary.references,
            )
        ):
            msg = "Admission contains only boundary inputs and frozen context"
            raise ValueError(msg)
        filenames = [item.filename for item in self.tables]
        paths = [item.table_path for item in self.tables]
        digests = [item.digest for item in self.carriers]
        artifacts = [item.artifact_id for item in self.external_bindings]
        if any(
            values != sorted(set(values)) for values in (filenames, digests, artifacts)
        ) or len(paths) != len(set(paths)):
            msg = "Admission inventories must be unique and canonically ordered"
            raise ValueError(msg)
        if {item.carrier_digest for item in self.external_bindings} != set(digests):
            msg = "Admission carrier membership contains missing or orphaned files"
            raise ValueError(msg)
        return self


class AdmissionRecord(_AdmissionRecord):
    """Canonical version-one admission for managed or embedded artifacts."""

    schema_version: Literal[1] = 1


class AdmissionRecordWithAccess(_AdmissionRecord):
    """Admission that authenticates an external access record and its membership."""

    schema_version: Literal[2] = 2
    access_digest: ContentDigest
    access_artifact_ids: tuple[ContentDigest, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_access_membership(self) -> Self:
        if list(self.access_artifact_ids) != sorted(set(self.access_artifact_ids)):
            msg = "Admitted external artifact IDs must be unique and sorted"
            raise ValueError(msg)
        if set(self.access_artifact_ids) & {
            binding.artifact_id for binding in self.external_bindings
        }:
            msg = "An artifact cannot have both managed and external access bindings"
            raise ValueError(msg)
        return self


type AnyAdmissionRecord = AdmissionRecord | AdmissionRecordWithAccess


def parse_admission_record(encoded: bytes) -> AnyAdmissionRecord:
    """Decode an explicit version without changing historical canonical fields."""
    data = json.loads(encoded)
    version = data.get("schema_version") if isinstance(data, dict) else None
    if type(version) is not int:
        msg = "Admission requires an explicit integer schema version"
        raise ValueError(msg)
    if version == 1:
        return AdmissionRecord.model_validate(data)
    if version == 2:
        return AdmissionRecordWithAccess.model_validate(data)
    msg = f"Unsupported admission schema version: {version}"
    raise ValueError(msg)
