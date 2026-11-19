"""Closed curator configuration without serialized parameter secrets or code."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from artisan.schemas.composites.plan import (
    InputContract,
    InvocationPolicy,
    OutputRoleContract,
)
from artisan.schemas.execution.assignment import ContentDigest
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest


class CuratorDefinitionContract(BaseModel):
    """Bind one effective curator configuration; recorded names authorize no imports."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )

    kind: Literal["curator"] = "curator"
    name: str = Field(min_length=1, strict=True)
    version: str = Field(min_length=1, strict=True)
    module: str = Field(min_length=1, strict=True)
    qualname: str = Field(min_length=1, strict=True)
    params_digest: ContentDigest
    inputs: tuple[InputContract, ...]
    outputs: tuple[OutputRoleContract, ...]
    policy: InvocationPolicy
    runtime_defined_inputs: bool = Field(strict=True)
    hydrate_inputs: bool = Field(strict=True)
    cacheable: bool = Field(strict=True)

    @model_validator(mode="after")
    def _validate_roles(self) -> CuratorDefinitionContract:
        for contracts in (self.inputs, self.outputs):
            roles = [item.role for item in contracts]
            if len(roles) != len(set(roles)):
                msg = "Curator definition repeats a declared role"
                raise ValueError(msg)
        return self

    @property
    def digest(self) -> str:
        """Bind configuration independently of store/step or software ownership."""
        return compute_content_digest(
            b"artisan.curator-definition-v1\0"
            + canonical_json_bytes(self.model_dump(mode="json"))
        )
