"""Exact occurrence and required-reference contracts independent of storage."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field

from artisan.schemas.artifact.base import Artifact


class OccurrenceAddress(BaseModel):
    """Address one ordered artifact occurrence without merging equal content."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )

    invocation_id: str = Field(min_length=1, strict=True)
    role: str = Field(min_length=1, strict=True)
    index: int = Field(ge=0, strict=True)

    @property
    def key(self) -> str:
        """Return an unambiguous stable encoding of the address."""
        return json.dumps(
            [self.invocation_id, self.role, self.index], separators=(",", ":")
        )


class InputReference(OccurrenceAddress):
    """A consuming invocation's input position and expected artifact identity."""

    artifact_id: str = Field(pattern=r"^[0-9a-fA-F]{32}$", strict=True)

    @property
    def address(self) -> OccurrenceAddress:
        """Return the consuming input position, not its producer attribution."""
        return OccurrenceAddress(
            invocation_id=self.invocation_id, role=self.role, index=self.index
        )


@dataclass(frozen=True)
class ArtifactOccurrence:
    """One admitted address holding a finalized artifact."""

    address: OccurrenceAddress
    artifact: Artifact

    def __post_init__(self) -> None:
        if type(self.address) is not OccurrenceAddress:
            msg = "Producer occurrence requires an OccurrenceAddress, not an input reference"
            raise ValueError(msg)
        OccurrenceAddress.model_validate(self.address)
        if self.artifact.artifact_id is None:
            msg = "Occurrence artifacts must be finalized"
            raise ValueError(msg)
        self.artifact.finalize()


class RequiredReference(BaseModel):
    """An exact semantic referent with its expected content identity and type."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    address: OccurrenceAddress
    artifact_id: str = Field(pattern=r"^[0-9a-fA-F]{32}$", strict=True)
    artifact_type: str = Field(min_length=1, strict=True)


@dataclass(frozen=True)
class ReferenceContext:
    """Admitted occurrences and validated direct parents available to a policy."""

    artifacts: Mapping[OccurrenceAddress, Artifact]
    direct_parents: tuple[OccurrenceAddress, ...] = ()

    def __post_init__(self) -> None:
        artifacts = dict(self.artifacts)
        for address, artifact in artifacts.items():
            ArtifactOccurrence(address, artifact)
        parents = tuple(self.direct_parents)
        if len(set(parents)) != len(parents) or any(
            parent not in artifacts for parent in parents
        ):
            msg = "Direct parents must be unique admitted occurrences"
            raise ValueError(msg)
        object.__setattr__(self, "artifacts", MappingProxyType(artifacts))
        object.__setattr__(self, "direct_parents", parents)

    def validate(self, reference: RequiredReference) -> None:
        """Reject missing, changed, or incorrectly typed policy targets."""
        artifact = self.artifacts.get(reference.address)
        if artifact is None:
            msg = "Required referent is not an admitted occurrence"
            raise ValueError(msg)
        artifact.finalize()
        if (artifact.artifact_id, artifact.artifact_type) != (
            reference.artifact_id,
            reference.artifact_type,
        ):
            msg = "Required referent identity or type does not match"
            raise ValueError(msg)


class RequiredReferencePolicy(Protocol):
    """An artifact type's store-free semantic-reference resolver."""

    def __call__(
        self, artifact: Artifact, context: ReferenceContext
    ) -> Sequence[RequiredReference]:
        """Return exact required referents from artifact content or relationships."""
        ...
