"""Location-independent descriptors and placement for externally owned files.

Importing this module does not register artifact types or read installations.
"""

from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from artisan.utils.hashing import canonical_json_bytes, compute_content_digest

EXTERNAL_FILES_RECEIPT = ".artisan-external-files.json"


class ExternalFile(BaseModel):
    """One regular file identified by its declared content-hash algorithm.

    SHA256 manifests can be used directly without reading the installation.
    The algorithm is part of descriptor identity and explicit preparation
    verifies the corresponding digest.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    path: str
    content_hash: str = Field(pattern=r"^[0-9a-f]+$")
    hash_algorithm: Literal["xxh3_128", "sha256"] = "xxh3_128"
    size_bytes: int = Field(ge=0, strict=True)

    @model_validator(mode="after")
    def _hash_length(self) -> Self:
        expected_length = 32 if self.hash_algorithm == "xxh3_128" else 64
        if len(self.content_hash) != expected_length:
            msg = f"External content hash requires {expected_length} hexadecimal characters"
            raise ValueError(msg)
        return self

    @field_validator("path")
    @classmethod
    def _portable_path(cls, value: str) -> str:
        parts = value.split("/")
        if (
            not value
            or any(part in {"", ".", ".."} for part in parts)
            or any(character in value for character in "\\\x00\n\r")
            or ":" in parts[0]
            or EXTERNAL_FILES_RECEIPT in parts
        ):
            msg = "External file paths must be portable relative paths"
            raise ValueError(msg)
        return value


class ExternalFilesDescriptor(BaseModel):
    """Immutable, sorted inventory independent of where its files reside."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal[1] = 1
    files: tuple[ExternalFile, ...] = Field(min_length=1)

    @field_validator("schema_version", mode="before")
    @classmethod
    def _version(cls, value: object) -> object:
        if type(value) is not int:
            msg = "External descriptor version must be an integer"
            raise ValueError(msg)
        return value

    @field_validator("files")
    @classmethod
    def _closed_inventory(
        cls, values: tuple[ExternalFile, ...]
    ) -> tuple[ExternalFile, ...]:
        paths = {item.path for item in values}
        if len(paths) != len(values):
            msg = "External descriptor contains duplicate file paths"
            raise ValueError(msg)
        for path in paths:
            if any(str(parent) in paths for parent in PurePosixPath(path).parents):
                msg = "External descriptor file paths overlap"
                raise ValueError(msg)
        return tuple(sorted(values, key=lambda item: item.path))

    def canonical_bytes(self) -> bytes:
        """Return the canonical descriptor stored in preparation evidence."""
        return canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def digest(self) -> str:
        """Return the content digest of this complete descriptor."""
        return compute_content_digest(self.canonical_bytes())


def expected_receipt_bytes(descriptor: ExternalFilesDescriptor) -> bytes:
    """Return completion evidence without reading any external files."""
    return canonical_json_bytes(
        {
            "domain": "artisan.external-files-preparation",
            "schema_version": 1,
            "descriptor": descriptor.model_dump(mode="json"),
        }
    )


def expected_receipt_digest(descriptor: ExternalFilesDescriptor) -> str:
    """Return the exact receipt identity for a selected descriptor."""
    return compute_content_digest(expected_receipt_bytes(descriptor))


class PreparedExternalFiles(BaseModel):
    """Explicit placement and preparation evidence for an existing installation.

    Construction checks metadata only, including for a remote worker's path.
    Actual access and installation layout require execution-time verification.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    root: Path
    descriptor: ExternalFilesDescriptor
    receipt_digest: str = Field(pattern=r"^[0-9a-f]{32}$")

    @field_validator("root")
    @classmethod
    def _absolute_root(cls, value: Path) -> Path:
        if not value.is_absolute() or ".." in value.parts or "\x00" in str(value):
            msg = "Prepared external roots require an explicit absolute path"
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def _receipt_identity(self) -> Self:
        if self.receipt_digest != expected_receipt_digest(self.descriptor):
            msg = "Prepared external receipt differs from its descriptor"
            raise ValueError(msg)
        return self
