"""Closed evidence for conservative reuse of observed curator reads."""

from __future__ import annotations

from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from artisan.schemas.artifact.external import validate_persistable_uri
from artisan.schemas.execution.admission import ManagedCarrier
from artisan.schemas.execution.assignment import ContentDigest, SoftwareDigest

CURATOR_CACHE_PROOF_KEY = "artisan_curator_cache_proof"

type CuratorCacheExclusion = Literal[
    "untracked_filesystem_access",
    "untracked_external_content",
    "unsupported_view_schema",
]


class CuratorViewFingerprint(BaseModel):
    """One complete registered logical view, including an absent table read."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )

    table_path: str = Field(pattern=r"^[a-zA-Z_][a-zA-Z0-9_/-]*$", strict=True)
    direct: bool = Field(default=False, strict=True)
    present: bool = Field(strict=True)
    schema_signature: tuple[tuple[str, str], ...]
    content_digest: ContentDigest | None

    @model_validator(mode="after")
    def _validate_view(self) -> Self:
        if self.direct and self.table_path != "provenance/artifact_edges":
            msg = "Only artifact-edge views distinguish original direct relationships"
            raise ValueError(msg)
        names = [name for name, _ in self.schema_signature]
        if names != sorted(set(names)):
            msg = "Curator view schema must have unique canonical column names"
            raise ValueError(msg)
        return self


class CuratorCarrierRead(BaseModel):
    """One supported read of a complete managed carrier at its original URI."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )

    artifact_id: ContentDigest
    uri: str = Field(min_length=1, strict=True)
    carrier: ManagedCarrier

    @model_validator(mode="after")
    def _validate_uri(self) -> Self:
        validate_persistable_uri(self.uri)
        return self


class CuratorCacheProof(BaseModel):
    """Observed reads bound to trusted science/software and the admitted view.

    Definition identity includes effective parameters and the complete resolved
    curator contract. Software identity comes from qualified runtime observation.
    Neither stamp is an author-supplied version or dependency list.
    """

    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )

    domain: Literal["artisan.curator-cache-proof"] = "artisan.curator-cache-proof"
    definition_digest: ContentDigest
    software_digest: SoftwareDigest
    cache_key: ContentDigest
    snapshot_digest: ContentDigest
    views: tuple[CuratorViewFingerprint, ...]
    carrier_reads: tuple[CuratorCarrierRead, ...] = ()
    ineligible_reasons: tuple[CuratorCacheExclusion, ...] = ()

    @model_validator(mode="after")
    def _validate_membership(self) -> Self:
        views = [(item.table_path, item.direct) for item in self.views]
        if views != sorted(set(views)):
            msg = "Curator read views must be unique and canonically ordered"
            raise ValueError(msg)
        carriers = [(item.artifact_id, item.uri) for item in self.carrier_reads]
        if carriers != sorted(set(carriers)):
            msg = "Curator carrier reads must be unique and canonically ordered"
            raise ValueError(msg)
        if self.ineligible_reasons != tuple(sorted(set(self.ineligible_reasons))):
            msg = "Curator cache exclusions must be unique and canonically ordered"
            raise ValueError(msg)
        if (
            any(item.content_digest is None for item in self.views)
            and "unsupported_view_schema" not in self.ineligible_reasons
        ):
            msg = "Unfingerprinted views must explicitly exclude cache reuse"
            raise ValueError(msg)
        return self
