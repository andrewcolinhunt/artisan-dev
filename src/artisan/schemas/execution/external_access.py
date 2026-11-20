"""Frozen access evidence for ordinary artifacts with externally owned files."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from artisan.schemas.artifact.external_descriptor import PreparedExternalFiles
from artisan.schemas.execution.assignment import AssignmentAttempt, ContentDigest
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest


class _ExternalAccessModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )


class ExternalAccessBinding(_ExternalAccessModel):
    """Connect one selected artifact to an explicitly prepared installation."""

    artifact_id: ContentDigest
    prepared: PreparedExternalFiles


class ExternalAccessMount(_ExternalAccessModel):
    """Identify one provider-mounted source without importing its SDK."""

    source_id: str = Field(min_length=1, strict=True)
    target: Path
    readonly: Literal[True] = True

    @field_validator("source_id")
    @classmethod
    def _source_id(cls, value: str) -> str:
        if value != value.strip() or any(
            character in value for character in "\x00\n\r"
        ):
            msg = "External mount source identity must be nonempty and unambiguous"
            raise ValueError(msg)
        return value

    @field_validator("target", mode="before")
    @classmethod
    def _target(cls, value: object) -> str | Path:
        if not isinstance(value, (str, Path)):
            msg = "External mount target must be an absolute filesystem path"
            raise ValueError(msg)
        path = Path(value)
        if (
            not path.is_absolute()
            or path == Path("/")
            or str(path) != str(value)
            or ".." in path.parts
            or any(character in str(path) for character in ":,\x00\n\r\\")
        ):
            msg = "External mount target must be a canonical absolute directory"
            raise ValueError(msg)
        return value

    @field_validator("readonly", mode="before")
    @classmethod
    def _readonly(cls, value: object) -> object:
        if value is not True:
            msg = "External access mounts must be read-only"
            raise ValueError(msg)
        return value


class ExternalAccessRecord(_ExternalAccessModel):
    """Authenticate placement separately from scientific artifact identity."""

    domain: Literal["artisan.external-artifact-access"] = (
        "artisan.external-artifact-access"
    )
    schema_version: Literal[1] = 1
    attempt: AssignmentAttempt
    provider: Literal["local", "modal"]
    bindings: tuple[ExternalAccessBinding, ...] = Field(min_length=1)
    mounts: tuple[ExternalAccessMount, ...] = ()

    @field_validator("schema_version", mode="before")
    @classmethod
    def _version(cls, value: object) -> object:
        if type(value) is not int:
            msg = "External access version must be an integer"
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def _validate_inventory(self) -> Self:
        ids = [binding.artifact_id for binding in self.bindings]
        if ids != sorted(set(ids)):
            msg = "External access bindings must have unique sorted artifact IDs"
            raise ValueError(msg)
        roots: dict[Path, str] = {}
        for binding in self.bindings:
            root, digest = binding.prepared.root, binding.prepared.descriptor.digest
            if root in roots and roots[root] != digest:
                msg = "One external root cannot name conflicting file inventories"
                raise ValueError(msg)
            roots[root] = digest
        _validate_disjoint_paths(tuple(roots))
        targets = [mount.target for mount in self.mounts]
        if targets != sorted(set(targets), key=str) or len(
            {mount.source_id for mount in self.mounts}
        ) != len(self.mounts):
            msg = "External mount sources and sorted targets must be unique"
            raise ValueError(msg)
        _validate_disjoint_paths(tuple(targets))
        self._validate_provider_roots(tuple(roots))
        return self

    def _validate_provider_roots(self, roots: tuple[Path, ...]) -> None:
        if self.provider == "local":
            if self.mounts:
                msg = "Local external access does not declare remote volume mounts"
                raise ValueError(msg)
            return
        if any(
            sum(root.is_relative_to(mount.target) for mount in self.mounts) != 1
            for root in roots
        ):
            msg = "Each remote external root requires exactly one admitted volume mount"
            raise ValueError(msg)

    @property
    def digest(self) -> str:
        """Return the canonical placement-evidence digest."""
        return compute_content_digest(
            canonical_json_bytes(self.model_dump(mode="json"))
        )


def _validate_disjoint_paths(paths: tuple[Path, ...]) -> None:
    for index, path in enumerate(paths):
        if any(
            path.is_relative_to(other) or other.is_relative_to(path)
            for other in paths[index + 1 :]
        ):
            msg = "External installation and mount roots must not overlap"
            raise ValueError(msg)
