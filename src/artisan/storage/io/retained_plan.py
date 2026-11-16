"""Closed exact effects for a controller-claimed retained delivery."""

from __future__ import annotations

from functools import cached_property
from types import MappingProxyType
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.admission import AdmissionReference, ManagedCarrier
from artisan.schemas.execution.assignment import ContentDigest, SealedDelivery
from artisan.storage.core.projection_schemas import PROJECTION_TABLES
from artisan.storage.core.table_schemas import (
    get_natural_key,
    get_physical_schema_for_path,
)
from artisan.storage.io.plan_evidence import PlannedFile
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest

_KeyValue = Annotated[str, Field(strict=True)] | Annotated[int, Field(strict=True)]
_Origin = Annotated[int, Field(ge=0, strict=True)] | None


class _PlanModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )


class RetainedPlannedFile(PlannedFile):
    """Strict controller-owned Parquet evidence using the existing file contract."""

    size_bytes: int = Field(gt=0, strict=True)
    row_count: int = Field(ge=0, strict=True)
    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )


class RetainedPlannedTable(_PlanModel):
    """One exact table effect, including the protected origin for every owned key."""

    table_path: str = Field(min_length=1, strict=True)
    natural_key: tuple[str, ...]
    row_keys: tuple[tuple[_KeyValue, ...], ...]
    row_count: int = Field(ge=0, strict=True)
    table_plan_key: ContentDigest
    files: tuple[RetainedPlannedFile, ...]
    row_origins: tuple[_Origin, ...]

    @model_validator(mode="after")
    def _validate_effect(self) -> RetainedPlannedTable:
        if self.table_path not in retained_table_paths():
            msg = "Retained plan names an unregistered table"
            raise ValueError(msg)
        if self.natural_key != get_natural_key(self.table_path):
            msg = "Retained plan has the wrong natural key"
            raise ValueError(msg)
        if (
            not self.files
            or len({item.relative_path for item in self.files}) != len(self.files)
            or sum(item.row_count for item in self.files) != self.row_count
            or len(self.row_keys) != self.row_count
            or len(set(self.row_keys)) != self.row_count
            or any(len(key) != len(self.natural_key) for key in self.row_keys)
        ):
            msg = "Retained plan has incomplete or duplicate table evidence"
            raise ValueError(msg)
        origin_count = (
            self.row_count
            if "origin_step_number" in get_physical_schema_for_path(self.table_path)
            else 0
        )
        if len(self.row_origins) != origin_count:
            msg = "Retained plan must bind each protected origin in natural-key order"
            raise ValueError(msg)
        return self

    @cached_property
    def origins(self) -> MappingProxyType[tuple[_KeyValue, ...], int | None]:
        """Expose a read-only derived index; tuples remain the durable authority."""
        return MappingProxyType(dict(zip(self.row_keys, self.row_origins, strict=True)))


class RetainedCommitPlan(_PlanModel):
    """One immutable retained result plan; the journal still grants winner authority.

    Legacy plans retain their existing wire representation. This separate domain
    binds mixed original origins and managed-file publications without changing
    which evidence qualifies a delivery for acceptance or recovery.
    """

    domain: Literal["artisan.retained-commit"] = "artisan.retained-commit"
    schema_version: Literal[1] = 1
    receipt: SealedDelivery
    decision_sequence: int = Field(ge=0, strict=True)
    admission: AdmissionReference
    step_number: int = Field(ge=0, strict=True)
    tables: tuple[RetainedPlannedTable, ...]
    carriers: tuple[ManagedCarrier, ...]
    plan_digest: ContentDigest

    @field_validator("schema_version", mode="before")
    @classmethod
    def _strict_version(cls, value: object) -> object:
        if type(value) is not int:
            msg = "Retained plan version must be an integer"
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def _validate_plan(self) -> RetainedCommitPlan:
        if self.admission.attempt != self.receipt.delivery.attempt:
            msg = "Retained plan admission and delivery must belong to the same attempt"
            raise ValueError(msg)
        paths = [item.table_path for item in self.tables]
        order = retained_table_paths()
        if paths != sorted(set(paths), key=order.index):
            msg = "Retained plan table effects must be unique and dependency ordered"
            raise ValueError(msg)
        if not {item.table_path for item in PROJECTION_TABLES} <= set(paths):
            msg = "Retained plan requires the complete occurrence projection"
            raise ValueError(msg)
        files = [item.relative_path for table in self.tables for item in table.files]
        if len(files) != len(set(files)):
            msg = "Retained plan repeats a staged file"
            raise ValueError(msg)
        carriers = [item.digest for item in self.carriers]
        if carriers != sorted(set(carriers)):
            msg = "Retained plan carriers must be unique and canonically ordered"
            raise ValueError(msg)
        if self.plan_digest != retained_plan_digest(
            self.model_dump(mode="json", exclude={"plan_digest"})
        ):
            msg = "Retained plan digest does not match its exact effects"
            raise ValueError(msg)
        return self

    @property
    def logical_commit_id(self) -> str:
        """Own exactly one logical assignment, independent of retry delivery."""
        return (
            f"retained_result:{self.receipt.delivery.attempt.assignment.assignment_id}"
        )

    @property
    def commit_kind(self) -> str:
        return "retained_result"

    @property
    def step_run_id(self) -> str:
        return self.receipt.delivery.attempt.assignment.step_run_id

    @property
    def recovery_batch_id(self) -> None:
        return None

    def table(self, table_path: str) -> RetainedPlannedTable | None:
        return next(
            (item for item in self.tables if item.table_path == table_path), None
        )

    def expected_origin(self, table_path: str, key: tuple[Any, ...]) -> int | None:
        table = self.table(table_path)
        if table is None:
            msg = "Origin evidence requested for an unplanned table"
            raise ValueError(msg)
        return table.origins[key]


def retained_table_paths() -> tuple[str, ...]:
    """Return private dependency order without activating the public table domain."""
    return (
        *sorted(item.table_path for item in ArtifactTypeDef.get_all().values()),
        TablePath.ARTIFACT_INDEX.value,
        TablePath.ARTIFACT_LOCATIONS.value,
        *(item.table_path for item in PROJECTION_TABLES),
    )


def retained_plan_digest(payload: dict[str, Any]) -> str:
    """Bind all normalized plan fields except the digest itself."""
    return compute_content_digest(
        b"artisan.retained-commit-v1\0" + canonical_json_bytes(payload)
    )
