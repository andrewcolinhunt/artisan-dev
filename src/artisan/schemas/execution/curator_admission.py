"""Immutable store visibility and input evidence for store-bound curators."""

from __future__ import annotations

import json
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from artisan.schemas.execution.assignment import ContentDigest, SoftwareDigest
from artisan.schemas.execution.curator_definition import CuratorDefinitionContract
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.schemas.provenance.projection import AcceptedProducer
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest


class _CuratorModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )


class CuratorTableVersion(_CuratorModel):
    """A registered table's fixed Delta version, without copying its payloads."""

    table_path: str = Field(pattern=r"^[a-zA-Z_][a-zA-Z0-9_/-]*$", strict=True)
    version: int = Field(ge=0, strict=True)


class CuratorCompletedControl(_CuratorModel):
    """Exact completed logical ownership visible to one curator invocation."""

    logical_commit_id: str = Field(min_length=1, strict=True)
    commit_kind: str = Field(min_length=1, strict=True)
    step_run_id: ContentDigest
    recovery_batch_id: ContentDigest | None = None
    plan_digest: ContentDigest


class CuratorReadSnapshot(_CuratorModel):
    """A complete-owner view at fixed table versions, separate from live queries.

    Versions freeze physical membership; control identities freeze logical
    visibility. Readers still validate registered schemas and exact owner plans.
    This record contains no artifact payloads and grants no import authority.
    """

    domain: Literal["artisan.curator-read-snapshot"] = "artisan.curator-read-snapshot"
    schema_version: Literal[1] = 1
    tables: tuple[CuratorTableVersion, ...]
    controls: tuple[CuratorCompletedControl, ...]

    @field_validator("schema_version", mode="before")
    @classmethod
    def _strict_version(cls, value: object) -> object:
        if type(value) is not int:
            msg = "Curator snapshot version must be an integer"
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def _validate_membership(self) -> CuratorReadSnapshot:
        for names in (
            [item.table_path for item in self.tables],
            [item.logical_commit_id for item in self.controls],
        ):
            if names != sorted(set(names)):
                msg = "Curator snapshot members must be unique and canonically ordered"
                raise ValueError(msg)
        return self

    @property
    def digest(self) -> str:
        """Bind the complete frozen view independently of its storage locator."""
        return compute_content_digest(
            b"artisan.curator-read-snapshot-v1\0"
            + canonical_json_bytes(self.model_dump(mode="json"))
        )

    def table(self, table_path: str) -> CuratorTableVersion | None:
        return next(
            (item for item in self.tables if item.table_path == table_path), None
        )


class CuratorInputMember(_CuratorModel):
    """One supplied content item and every explicitly supplied accepted owner."""

    artifact_id: ContentDigest
    artifact_type: str = Field(min_length=1, strict=True)
    producers: tuple[AcceptedProducer, ...] = ()

    @model_validator(mode="after")
    def _validate_producers(self) -> CuratorInputMember:
        if len(self.producers) != len(set(self.producers)):
            msg = "Curator input producer membership must not repeat an exact owner"
            raise ValueError(msg)
        return self


class CuratorInputRole(_CuratorModel):
    """An existing curator's ordered content-ID input stream."""

    role: str = Field(min_length=1, strict=True)
    members: tuple[CuratorInputMember, ...]


class CuratorInputAssociation(_CuratorModel):
    """Exact content-level membership supplied through CuratorInput associations."""

    primary_id: ContentDigest
    artifact_type: str = Field(min_length=1, strict=True)
    artifact_ids: tuple[ContentDigest, ...]

    @model_validator(mode="after")
    def _validate_members(self) -> CuratorInputAssociation:
        if len(self.artifact_ids) != len(set(self.artifact_ids)):
            msg = "Curator associated inputs must not repeat a content ID"
            raise ValueError(msg)
        return self


class CuratorAdmission(_CuratorModel):
    """Freeze a store-bound invocation without inventing a creator recipe.

    The original step record and input role order remain authoritative. Producer
    sets describe supplied content membership, not scientific derivation. The
    fixed store view permits the curator's documented content-level queries.
    """

    domain: Literal["artisan.curator-admission"] = "artisan.curator-admission"
    record: StepStartRecord
    operation_name: str = Field(min_length=1, strict=True)
    operation_version: str = Field(min_length=1, strict=True)
    definition: CuratorDefinitionContract | None = None
    software_digest: SoftwareDigest | None = None
    software_cacheable: bool = Field(default=True, strict=True)
    cacheable: bool = Field(default=True, strict=True)
    inputs: tuple[CuratorInputRole, ...]
    associations: tuple[CuratorInputAssociation, ...] = ()
    view: CuratorReadSnapshot

    @model_validator(mode="after")
    def _validate_owner(self) -> CuratorAdmission:
        if (self.definition is None) != (self.software_digest is None):
            msg = "Curator qualification requires both definition and software identity"
            raise ValueError(msg)
        self._validate_definition()
        roles = [item.role for item in self.inputs]
        if len(roles) != len(set(roles)):
            msg = "Curator admission repeats an input role"
            raise ValueError(msg)
        types: dict[str, str] = {}
        for role in self.inputs:
            for member in role.members:
                if (
                    types.setdefault(member.artifact_id, member.artifact_type)
                    != member.artifact_type
                ):
                    msg = "Curator inputs disagree on one content ID's registered type"
                    raise ValueError(msg)
        pairs = [(item.primary_id, item.artifact_type) for item in self.associations]
        if pairs != sorted(set(pairs)):
            msg = (
                "Curator associated input pairs must be unique and canonically ordered"
            )
            raise ValueError(msg)
        primary_ids = set(types)
        for association in self.associations:
            if association.primary_id not in primary_ids:
                msg = "Curator associated input has no supplied primary content"
                raise ValueError(msg)
            for artifact_id in association.artifact_ids:
                if (
                    types.setdefault(artifact_id, association.artifact_type)
                    != association.artifact_type
                ):
                    msg = "Curator associated inputs disagree on a content ID's type"
                    raise ValueError(msg)
        if (
            self.record.step_number < 0
            or len(self.record.step_run_id) != 32
            or any(char not in "0123456789abcdef" for char in self.record.step_run_id)
        ):
            msg = "Curator admission requires a real outer step owner"
            raise ValueError(msg)
        return self

    def _validate_definition(self) -> None:
        if self.definition is None:
            return
        definition = self.definition
        if (
            (self.operation_name, self.operation_version, self.cacheable)
            != (definition.name, definition.version, definition.cacheable)
            or self.record.operation_class
            != f"{definition.module}.{definition.qualname}"
        ):
            msg = "Curator admission metadata differs from its closed definition"
            raise ValueError(msg)
        roles = json.loads(self.record.output_roles_json)
        expected = {item.role: item.artifact_type for item in definition.outputs}
        if (
            len(roles) != len(expected)
            or set(roles) != expected.keys()
            or json.loads(self.record.output_types_json) != expected
        ):
            msg = (
                "Curator admitted output declarations differ from its closed definition"
            )
            raise ValueError(msg)
        options = json.loads(self.record.compute_options_json)
        dynamic = (
            options.get("dynamic_output_roles", False)
            if isinstance(options, dict)
            else None
        )
        if type(dynamic) is not bool or dynamic != (not definition.outputs):
            msg = "Curator dynamic output behavior differs from its closed definition"
            raise ValueError(msg)

    @property
    def definition_digest(self) -> str | None:
        """Derive scientific identity from the typed captured contract."""
        return self.definition.digest if self.definition is not None else None

    @property
    def digest(self) -> str:
        return compute_content_digest(
            b"artisan.curator-admission-v1\0"
            + canonical_json_bytes(self.model_dump(mode="json"))
        )


class CuratorAdmissionReference(_CuratorModel):
    step_run_id: ContentDigest
    admission_digest: ContentDigest
