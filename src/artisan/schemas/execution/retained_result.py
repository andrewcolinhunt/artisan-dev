"""Closed inventory for one completed retained-result delivery."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from artisan.schemas.execution.assignment import ContentDigest, SuccessfulDelivery


class _SealModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )


class RetainedTableFile(_SealModel):
    """Exact Parquet bytes with a registered logical table destination."""

    table_path: str = Field(min_length=1, strict=True)
    filename: str = Field(pattern=r"^[a-zA-Z_][a-zA-Z0-9_-]*\.parquet$", strict=True)
    digest: ContentDigest
    size_bytes: int = Field(gt=0, strict=True)
    row_count: int = Field(ge=0, strict=True)


class ExternalCarrier(_SealModel):
    """Whole carrier bytes, independent of artifact or record identity."""

    filename: str = Field(pattern=r"^carrier-[0-9a-f]{32}\.bin$", strict=True)
    digest: ContentDigest
    size_bytes: int = Field(ge=0, strict=True)

    @model_validator(mode="after")
    def _validate_name(self) -> ExternalCarrier:
        if self.filename != f"carrier-{self.digest}.bin":
            msg = "External carrier name must match its complete byte digest"
            raise ValueError(msg)
        return self


class ExternalBinding(_SealModel):
    """One externally backed retained artifact's sealed carrier membership."""

    artifact_id: ContentDigest
    filename: str = Field(pattern=r"^carrier-[0-9a-f]{32}\.bin$", strict=True)


class RetainedResultSeal(_SealModel):
    """Complete immutable worker evidence; only a controller can accept it."""

    domain: Literal["artisan.retained-delivery"] = "artisan.retained-delivery"
    schema_version: Literal[1] = 1
    completion: SuccessfulDelivery
    projection_digest: ContentDigest
    tables: tuple[RetainedTableFile, ...]
    carriers: tuple[ExternalCarrier, ...] = ()
    external_bindings: tuple[ExternalBinding, ...] = ()

    @field_validator("schema_version", mode="before")
    @classmethod
    def _strict_version(cls, value: object) -> object:
        if type(value) is not int:
            msg = "Retained seal version must be an integer"
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def _validate_inventory(self) -> RetainedResultSeal:
        names = [item.filename for item in self.tables]
        paths = [item.table_path for item in self.tables]
        carriers = [item.filename for item in self.carriers]
        bindings = [item.artifact_id for item in self.external_bindings]
        if any(
            values != sorted(set(values)) for values in (names, carriers, bindings)
        ) or len(paths) != len(set(paths)):
            msg = "Retained seal inventories must be unique and canonically ordered"
            raise ValueError(msg)
        if {item.filename for item in self.external_bindings} != set(carriers):
            msg = "External bindings must name every sealed carrier without orphans"
            raise ValueError(msg)
        return self
