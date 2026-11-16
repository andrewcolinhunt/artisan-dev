"""Immutable raw-file acquisition and exact managed registration effects."""

from __future__ import annotations

from typing import Any, Literal

from fsspec import AbstractFileSystem
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.external import validate_persistable_uri
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.admission import ManagedArtifactBinding, ManagedCarrier
from artisan.schemas.execution.assignment import ContentDigest
from artisan.storage.io.retained_plan import RetainedPlannedTable
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest
from artisan.utils.path import uri_join

REGISTRATION_TABLES = (
    "artifacts/file_refs",
    TablePath.ARTIFACT_INDEX.value,
    TablePath.ARTIFACT_LOCATIONS.value,
)


class RegisteredInput(BaseModel):
    """One ordered raw input, including duplicate acquisitions of the same ID."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )

    uri: str = Field(min_length=1, strict=True)
    artifact_id: ContentDigest

    @field_validator("uri")
    @classmethod
    def _validate_uri(cls, value: str) -> str:
        validate_persistable_uri(value)
        return value


class InputRegistrationPlan(BaseModel):
    """Controller-selected raw imports without scientific execution authority."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )

    domain: Literal["artisan.input-registration"] = "artisan.input-registration"
    schema_version: Literal[1] = 1
    step_run_id: ContentDigest
    step_number: int = Field(ge=0, strict=True)
    operation_name: str = Field(min_length=1, strict=True)
    inputs: tuple[RegisteredInput, ...] = Field(min_length=1)
    tables: tuple[RetainedPlannedTable, ...]
    carriers: tuple[ManagedCarrier, ...]
    external_bindings: tuple[ManagedArtifactBinding, ...]
    plan_digest: ContentDigest

    @field_validator("schema_version", mode="before")
    @classmethod
    def _strict_version(cls, value: object) -> object:
        if type(value) is not int:
            msg = "Registration version must be an integer"
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def _validate_membership(self) -> InputRegistrationPlan:
        if tuple(table.table_path for table in self.tables) != REGISTRATION_TABLES:
            msg = "Registration requires exactly content, index and locations"
            raise ValueError(msg)
        ids = {item.artifact_id for item in self.inputs}
        for table in self.tables:
            if {key[0] for key in table.row_keys} != ids or table.row_count != len(ids):
                msg = "Registration tables differ from ordered input membership"
                raise ValueError(msg)
        digests = [item.digest for item in self.carriers]
        bound = [item.artifact_id for item in self.external_bindings]
        if digests != sorted(set(digests)) or bound != sorted(ids):
            msg = "Registration carrier inventories must be exact and canonical"
            raise ValueError(msg)
        if {item.carrier_digest for item in self.external_bindings} != set(digests):
            msg = "Registration has missing or orphaned carriers"
            raise ValueError(msg)
        files = [item.relative_path for table in self.tables for item in table.files]
        if len(files) != len(set(files)):
            msg = "Registration repeats a table evidence file"
            raise ValueError(msg)
        if self.tables[0].origins != self.tables[1].origins:
            msg = "Registration content and index have different protected origins"
            raise ValueError(msg)
        if self.plan_digest != input_registration_digest(
            self.model_dump(mode="json", exclude={"plan_digest"})
        ):
            msg = "Registration plan digest differs from its exact acquisition"
            raise ValueError(msg)
        return self

    @property
    def logical_commit_id(self) -> str:
        return f"input_registration:{self.step_run_id}"

    @property
    def commit_kind(self) -> str:
        return "input_registration"

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
            msg = "Origin requested for an unplanned registration table"
            raise ValueError(msg)
        return table.origins[key]


def input_registration_digest(payload: dict[str, Any]) -> str:
    """Bind acquisition order, complete carriers, protected rows and exact effects."""
    return compute_content_digest(
        b"artisan.input-registration-v1\0" + canonical_json_bytes(payload)
    )


def registration_directory(delta_root: str, step_run_id: str) -> str:
    """Return the selected controller plan directory without accepting path input."""
    if len(step_run_id) != 32 or any(
        char not in "0123456789abcdef" for char in step_run_id
    ):
        msg = "Invalid registration owner"
        raise ValueError(msg)
    return uri_join(delta_root, "_artisan", "input-registrations", step_run_id)


def read_input_registration(
    delta_root: str, step_run_id: str, fs: AbstractFileSystem
) -> InputRegistrationPlan | None:
    """Read canonical selected evidence, independently of logical completion."""
    path = uri_join(registration_directory(delta_root, step_run_id), "plan.json")
    if not fs.exists(path):
        return None
    with fs.open(path, "rb", cache_type="none") as stream:
        encoded = stream.read()
    plan = InputRegistrationPlan.model_validate_json(encoded)
    if (
        plan.step_run_id != step_run_id
        or canonical_json_bytes(plan.model_dump(mode="json")) != encoded
    ):
        msg = "Registration plan has another owner or noncanonical bytes"
        raise StoreIntegrityError(msg)
    return plan
