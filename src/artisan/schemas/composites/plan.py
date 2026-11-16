"""Immutable composition recipes, without execution or import authority."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import GroupByStrategy
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest


class _RecipeModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class DefinitionIdentity(_RecipeModel):
    """Describe required registered code; these names never authorize imports."""

    kind: Literal["creator", "composite"]
    name: str = Field(min_length=1)
    version: str = Field(min_length=1)
    module: str = Field(min_length=1)
    qualname: str = Field(min_length=1)


class InputContract(_RecipeModel):
    """Capture scientific input behavior without mutable specification objects."""

    role: str = Field(min_length=1)
    artifact_type: str = Field(min_length=1)
    required: bool
    materialize: bool
    hydrate: bool
    materialize_as: str | None
    resolve_references: bool
    with_associated: tuple[str, ...]


class OutputContract(_RecipeModel):
    """Capture an output's presence, type, and declared parent namespace."""

    role: str = Field(min_length=1)
    artifact_type: str = Field(min_length=1)
    required: bool
    parent_kind: Literal["inputs", "outputs"] | None
    parent_roles: tuple[str, ...]


class InvocationPolicy(_RecipeModel):
    """Scientific partitioning, independent of transport packing or placement."""

    group_by: GroupByStrategy | None = None
    independent_input_streams: bool = False
    per_artifact_dispatch: bool = True
    artifacts_per_unit: int = Field(default=1, ge=1, strict=True)
    max_artifacts_per_unit: int | None = Field(default=None, ge=1, strict=True)


class ToolContract(_RecipeModel):
    """Freeze the command prefix independently of its prepared software."""

    executable: str = Field(min_length=1)
    interpreter: str | None = None
    subcommand: str | None = None


class ExecutionContract(_RecipeModel):
    """Capture scientific execution mode without placement or environment state."""

    mode: Literal["native", "command", "python_adapter", "composite"]
    tool: ToolContract | None = None


class RecipeReference(_RecipeModel):
    """An input role or a preceding call's output role in one recipe."""

    call: str | None = None
    role: str = Field(min_length=1)


class RoleBinding(_RecipeModel):
    """An ordered concatenation, including explicit empty streams and repeats."""

    role: str = Field(min_length=1)
    sources: tuple[RecipeReference, ...]


class RecipeCall(_RecipeModel):
    """One named invocation with its complete nested recipe and input wiring."""

    name: str = Field(min_length=1)
    inputs: tuple[RoleBinding, ...]
    recipe: ExecutionRecipe


class DefinitionContract(_RecipeModel):
    """A definition's immutable parameter, role and invocation contracts."""

    definition: DefinitionIdentity
    params_json: str
    inputs: tuple[InputContract, ...]
    outputs: tuple[OutputContract, ...]
    policy: InvocationPolicy
    execution: ExecutionContract
    cacheable: bool

    @model_validator(mode="after")
    def _validate_parameter_snapshot(self) -> DefinitionContract:
        if (self.definition.kind == "composite") != (
            self.execution.mode == "composite"
        ):
            msg = "Definition kind and execution mode disagree"
            raise ValueError(msg)
        params = json.loads(self.params_json)
        if not isinstance(params, dict):
            msg = "Recipe parameters must be a JSON object"
            raise ValueError(msg)
        json.dumps(params, allow_nan=False)
        for contracts in (self.inputs, self.outputs):
            roles = [item.role for item in contracts]
            if len(roles) != len(set(roles)):
                msg = "Definition contracts contain duplicate roles"
                raise ValueError(msg)
        return self


class ExecutionRecipe(DefinitionContract):
    """Closed scientific recipe; prepared software is bound separately."""

    schema_version: Literal[1] = 1
    calls: tuple[RecipeCall, ...] = ()
    exports: tuple[RoleBinding, ...] = ()

    @model_validator(mode="after")
    def _validate_wiring(self) -> ExecutionRecipe:
        if self.definition.kind == "creator":
            if self.calls or self.exports:
                msg = "A creator recipe cannot contain composition wiring"
                raise ValueError(msg)
            return self
        if any(spec.parent_kind is not None for spec in self.outputs):
            msg = "Composite outputs cannot declare derives_from"
            raise ValueError(msg)
        available: dict[str | None, dict[str, str]] = {
            None: {spec.role: spec.artifact_type for spec in self.inputs}
        }
        for call in self.calls:
            if call.name in available:
                msg = f"Duplicate recipe call {call.name!r}"
                raise ValueError(msg)
            if call.recipe.policy.group_by in (
                GroupByStrategy.NAME,
                GroupByStrategy.LINEAGE,
            ):
                msg = "Internal calls do not support NAME or LINEAGE grouping"
                raise ValueError(msg)
            expected_inputs = {spec.role: spec for spec in call.recipe.inputs}
            _validate_bindings(
                call.inputs, expected_inputs, available, require_all=False
            )
            available[call.name] = {
                spec.role: spec.artifact_type for spec in call.recipe.outputs
            }
        expected_outputs = {spec.role: spec for spec in self.outputs}
        _validate_bindings(self.exports, expected_outputs, available, require_all=True)
        if self.cacheable and any(not call.recipe.cacheable for call in self.calls):
            msg = "A composition cannot cache a noncacheable child"
            raise ValueError(msg)
        return self

    @property
    def digest(self) -> str:
        """Identify parameters, nested definitions, contracts and ordered wiring."""
        return compute_content_digest(
            b"execution-recipe-v1\0"
            + canonical_json_bytes(self.model_dump(mode="json"))
        )


def _validate_bindings(
    bindings: tuple[RoleBinding, ...],
    expected: Mapping[str, InputContract | OutputContract],
    available: dict[str | None, dict[str, str]],
    *,
    require_all: bool,
) -> None:
    roles = [binding.role for binding in bindings]
    required = {role for role, spec in expected.items() if require_all or spec.required}
    if (
        len(roles) != len(set(roles))
        or set(roles) - expected.keys()
        or required - set(roles)
    ):
        msg = "Recipe bindings have duplicate, unknown or missing roles"
        raise ValueError(msg)
    for binding in bindings:
        spec = expected[binding.role]
        if require_all and spec.required and not binding.sources:
            msg = "Required public outputs cannot be explicitly empty"
            raise ValueError(msg)
        for source in binding.sources:
            actual = available.get(source.call, {}).get(source.role)
            if actual is None:
                msg = "Recipe references must name inputs or preceding call outputs"
                raise ValueError(msg)
            if not ArtifactTypes.matches(spec.artifact_type, actual):
                msg = "Recipe reference has an incompatible artifact type"
                raise ValueError(msg)
