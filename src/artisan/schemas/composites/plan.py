"""Immutable composition recipes, without execution or import authority."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.composites.grouping import AnchoredNameGrouping
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
    optional_parent_roles: tuple[str, ...] = Field(
        default=(), exclude_if=lambda roles: not roles
    )

    @model_validator(mode="after")
    def _validate_optional_parents(self) -> OutputContract:
        roles = self.optional_parent_roles
        if roles and (
            self.parent_kind is None
            or len(roles) != len(set(roles))
            or any(not role or role not in self.parent_roles for role in roles)
        ):
            msg = "Optional parent roles must be unique declared parent roles"
            raise ValueError(msg)
        return self


class MixedOutputContract(_RecipeModel):
    """Capture input and sibling-output parent contracts without merging roles."""

    role: str = Field(min_length=1)
    artifact_type: str = Field(min_length=1)
    required: bool
    input_parent_roles: tuple[str, ...]
    output_parent_roles: tuple[str, ...] = Field(min_length=1)
    optional_parent_roles: tuple[str, ...] = Field(
        default=(), exclude_if=lambda roles: not roles
    )

    @model_validator(mode="after")
    def _validate_parent_roles(self) -> MixedOutputContract:
        for roles in (self.input_parent_roles, self.output_parent_roles):
            if any(not role for role in roles) or len(roles) != len(set(roles)):
                msg = "Mixed parent namespaces require nonempty unique role names"
                raise ValueError(msg)
        optional = self.optional_parent_roles
        allowed = {*self.input_parent_roles, *self.output_parent_roles}
        if len(optional) != len(set(optional)) or any(
            not role or role not in allowed for role in optional
        ):
            msg = "Optional parent roles must be unique declared parent roles"
            raise ValueError(msg)
        return self


type OutputRoleContract = OutputContract | MixedOutputContract


class InvocationPolicy(_RecipeModel):
    """Scientific partitioning, independent of transport packing or placement."""

    group_by: GroupByStrategy | AnchoredNameGrouping | None = None
    independent_input_streams: bool = False
    per_artifact_dispatch: bool = True
    artifacts_per_unit: int = Field(default=1, ge=1, strict=True)
    max_artifacts_per_unit: int | None = Field(default=None, ge=1, strict=True)
    shared_input_roles: tuple[str, ...] = Field(
        default=(), exclude_if=lambda roles: not roles
    )

    required_nonempty_inputs: tuple[str, ...] = Field(
        default=(), exclude_if=lambda roles: not roles
    )

    @model_validator(mode="after")
    def _validate_shared_roles(self) -> InvocationPolicy:
        roles = self.shared_input_roles
        if len(roles) != len(set(roles)) or any(not role for role in roles):
            msg = "Shared input roles must be unique nonempty names"
            raise ValueError(msg)
        if roles and self.independent_input_streams:
            msg = "Shared input roles cannot be combined with independent streams"
            raise ValueError(msg)
        required = self.required_nonempty_inputs
        if len(required) != len(set(required)) or any(not role for role in required):
            msg = "Required nonempty inputs must be unique nonempty names"
            raise ValueError(msg)
        anchored = isinstance(self.group_by, AnchoredNameGrouping)
        if required and not anchored:
            msg = "Required nonempty inputs require anchored grouping"
            raise ValueError(msg)
        if anchored and (self.independent_input_streams or self.per_artifact_dispatch):
            msg = "Anchored grouping requires intact group dispatch"
            raise ValueError(msg)
        return self


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
    outputs: tuple[OutputRoleContract, ...]
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
        shared = set(self.policy.shared_input_roles)
        required = {spec.role for spec in self.inputs if spec.required}
        if shared and (
            self.definition.kind != "creator"
            or not shared <= required
            or not ({spec.role for spec in self.inputs} - shared)
        ):
            msg = "Shared roles require declared required creator inputs and a nonshared role"
            raise ValueError(msg)
        names = {spec.role for spec in self.inputs}
        if not set(self.policy.required_nonempty_inputs) <= names:
            msg = "Required nonempty inputs must name declared roles"
            raise ValueError(msg)
        grouping = self.policy.group_by
        if isinstance(grouping, AnchoredNameGrouping) and (
            self.definition.kind != "creator"
            or grouping.primary_role not in required
            or {grouping.primary_role, *grouping.matched_roles} != names - shared
        ):
            msg = "Anchored grouping must cover all nonshared creator roles with a required primary"
            raise ValueError(msg)
        return self

    @property
    def recipe_version(self) -> Literal[1, 2]:
        """Select the extended format only when a new contract is declared."""
        return (
            2
            if self.policy.shared_input_roles
            or isinstance(self.policy.group_by, AnchoredNameGrouping)
            or self.policy.required_nonempty_inputs
            or any(spec.optional_parent_roles for spec in self.outputs)
            else 1
        )


class ExecutionRecipe(DefinitionContract):
    """Closed scientific recipe; prepared software is bound separately."""

    schema_version: Literal[1, 2] = 1
    calls: tuple[RecipeCall, ...] = ()
    exports: tuple[RoleBinding, ...] = ()

    @model_validator(mode="after")
    def _validate_wiring(self) -> ExecutionRecipe:
        if self.schema_version == 1 and (
            self.recipe_version == 2
            or any(call.recipe.schema_version == 2 for call in self.calls)
        ):
            msg = "Extended input or parent contracts require recipe schema version 2"
            raise ValueError(msg)
        if self.definition.kind == "creator":
            if self.calls or self.exports:
                msg = "A creator recipe cannot contain composition wiring"
                raise ValueError(msg)
            return self
        if any(
            isinstance(spec, MixedOutputContract) or spec.parent_kind is not None
            for spec in self.outputs
        ):
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
            f"execution-recipe-v{self.schema_version}\0".encode()
            + canonical_json_bytes(self.model_dump(mode="json"))
        )


def _validate_bindings(
    bindings: tuple[RoleBinding, ...],
    expected: Mapping[str, InputContract | OutputRoleContract],
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
