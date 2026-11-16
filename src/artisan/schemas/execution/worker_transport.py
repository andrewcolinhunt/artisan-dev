"""Closed compatibility evidence for trusted live-object worker transport."""

from __future__ import annotations

import hashlib
import re
from pathlib import PurePosixPath
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from artisan.utils.hashing import canonical_json_bytes

TRANSPORT_PACKAGES = (
    "cloudpickle",
    "dexterity-artisan",
    "pydantic",
    "pydantic-core",
)


class _TransportModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )


class PayloadFile(_TransportModel):
    """Actual import payload bytes at a distribution-relative installed path."""

    path: str = Field(min_length=1, strict=True)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$", strict=True)

    @field_validator("path")
    @classmethod
    def _validate_path(cls, value: str) -> str:
        path = PurePosixPath(value)
        if (
            path.is_absolute()
            or ".." in path.parts
            or str(path) != value
            or "\\" in value
            or ":" in value
            or value == "."
        ):
            msg = "Package payload paths must be canonical relative POSIX paths"
            raise ValueError(msg)
        return value


class DistributionMetadata(_TransportModel):
    """Installed package name and version used for Python compatibility."""

    name: str = Field(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$", strict=True)
    version: str = Field(min_length=1, strict=True)

    @field_validator("name", mode="before")
    @classmethod
    def _normalize_name(cls, value: object) -> object:
        return (
            re.sub(r"[-_.]+", "-", value).lower() if isinstance(value, str) else value
        )

    @property
    def digest(self) -> str:
        """Hash compatibility metadata independently of authored software inputs."""
        return hashlib.sha256(
            b"artisan.distribution-metadata-v1\0"
            + canonical_json_bytes(self.model_dump(mode="json"))
        ).hexdigest()


class RuntimeCompatibility(_TransportModel):
    """Prepared interpreter and selected package compatibility metadata."""

    python_implementation: str = Field(min_length=1, strict=True)
    python_version: str = Field(min_length=1, strict=True)
    cache_tag: str = Field(min_length=1, strict=True)
    packages: tuple[DistributionMetadata, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_packages(self) -> RuntimeCompatibility:
        names = [package.name for package in self.packages]
        if names != sorted(set(names)):
            msg = "Runtime packages must be unique and sorted by normalized name"
            raise ValueError(msg)
        if missing := set(TRANSPORT_PACKAGES) - set(names):
            msg = f"Live worker transport requires packages: {sorted(missing)}"
            raise ValueError(msg)
        return self


class WorkerTransportHeader(_TransportModel):
    """Inert admission evidence checked before loading a trusted live payload.

    This binds a payload to a qualified runtime; it does not authenticate an
    untrusted sender or turn pickle into a durable data format.
    """

    domain: Literal["artisan.worker-transport"] = "artisan.worker-transport"
    schema_version: Literal[2] = 2
    compatibility: RuntimeCompatibility
    payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$", strict=True)

    @field_validator("schema_version", mode="before")
    @classmethod
    def _strict_version(cls, value: object) -> object:
        if type(value) is not int:
            msg = "Worker transport schema version must be an integer"
            raise ValueError(msg)
        return value
