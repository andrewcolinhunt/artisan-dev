"""Declare creator and nested-composite calls under one scientific boundary."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, cast
from uuid import uuid4

from artisan.composites.base.compiled import CompiledCall, CompiledPlan, ConstantInput
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.composites.composite_ref import CompositeCall, CompositeRef
from artisan.schemas.composites.plan import (
    ConstantArtifact,
    DefinitionContract,
    ExecutionRecipe,
    InputContract,
    OutputRoleContract,
    RecipeCall,
    RecipeReference,
    RoleBinding,
)

if TYPE_CHECKING:
    from artisan.composites.base.compiler import Definition, Parameters


class CompositeContext:
    """Build one immutable scientific composition without dispatch or storage."""

    def __init__(
        self,
        recipe: DefinitionContract,
        stack: tuple[Definition, ...],
        *,
        captured: ExecutionRecipe | None = None,
    ) -> None:
        self._owner = uuid4().hex
        self._recipe = recipe
        self._stack = stack
        self._calls: dict[str, CompiledPlan] = {}
        self._wiring: list[RecipeCall] = []
        self._exports: dict[str, RoleBinding] = {}
        self._constants: list[ConstantInput] = []
        self._captured = captured

    def input(self, role: str) -> CompositeRef:
        """Reference a declared boundary role without resolving any artifacts."""
        if role not in {item.role for item in self._recipe.inputs}:
            msg = f"Unknown composite input role {role!r}"
            raise ValueError(msg)
        return CompositeRef(self._owner, None, role)

    def call(
        self,
        definition: Definition,
        *,
        inputs: Mapping[str, CompositeRef | Sequence[CompositeRef]] | None = None,
        params: Parameters = None,
        name: str | None = None,
    ) -> CompositeCall:
        """Declare one creator or nested composition with ordered input wiring."""
        call_name = name if name is not None else f"call_{len(self._calls) + 1}"
        if not isinstance(cast(object, call_name), str) or not call_name:
            msg = "Call names must be nonempty strings"
            raise ValueError(msg)
        if call_name in self._calls:
            msg = f"Duplicate composition call name {call_name!r}"
            raise ValueError(msg)
        from artisan.composites.base.compiler import _compile

        recorded = self._captured_call(call_name)
        child = _compile(
            definition,
            params,
            self._stack,
            captured=None if recorded is None else recorded.recipe,
        )
        bindings, constants = self._bind_inputs(
            child, {} if inputs is None else inputs, call_name, recorded
        )
        recipe_call = RecipeCall(name=call_name, inputs=bindings, recipe=child.recipe)
        self._calls[call_name] = child
        self._wiring.append(recipe_call)
        self._constants.extend(constants)
        self._constants.extend(
            ConstantInput((call_name, *item.call_path), item.role, item.artifacts)
            for item in child.constants
        )
        return CompositeCall(
            self._owner, call_name, tuple(item.role for item in child.recipe.outputs)
        )

    def output(
        self, role: str, reference: CompositeRef | Sequence[CompositeRef]
    ) -> None:
        """Bind one public role to an ordered concatenation of streams."""
        contracts = {item.role: item for item in self._recipe.outputs}
        if role not in contracts:
            msg = f"Unknown composite output role {role!r}"
            raise ValueError(msg)
        if role in self._exports:
            msg = f"Composite output role {role!r} is already bound"
            raise ValueError(msg)
        sources = self._references(reference, contracts[role].artifact_type)
        if not sources and contracts[role].required:
            msg = f"Required composite output {role!r} cannot be explicitly empty"
            raise ValueError(msg)
        self._exports[role] = RoleBinding(role=role, sources=sources)

    def finish(self) -> CompiledPlan:
        """Freeze all calls and output bindings under one scientific recipe."""
        missing = {item.role for item in self._recipe.outputs} - self._exports.keys()
        if missing:
            msg = f"Composite output roles were not bound: {sorted(missing)}"
            raise ValueError(msg)
        recipe = ExecutionRecipe.model_validate(
            {
                **self._recipe.model_dump(),
                "schema_version": max(
                    self._recipe.recipe_version,
                    *(call.recipe.schema_version for call in self._wiring),
                    3
                    if any(
                        binding.constants is not None
                        for call in self._wiring
                        for binding in call.inputs
                    )
                    else 1,
                )
                if self._wiring
                else self._recipe.recipe_version,
                "calls": tuple(self._wiring),
                "exports": tuple(
                    self._exports[item.role] for item in self._recipe.outputs
                ),
                "cacheable": self._recipe.cacheable
                and all(child.recipe.cacheable for child in self._calls.values()),
            }
        )
        return CompiledPlan(
            self._stack[-1],
            recipe,
            tuple(CompiledCall(name, child) for name, child in self._calls.items()),
            constants=tuple(self._constants),
        )

    def _captured_call(self, name: str) -> RecipeCall | None:
        if self._captured is None:
            return None
        for call in self._captured.calls:
            if call.name == name:
                return call
        msg = "Composition call is absent from the captured recipe"
        raise ValueError(msg)

    def _bind_inputs(
        self,
        child: CompiledPlan,
        inputs: object,
        call_name: str,
        recorded: RecipeCall | None,
    ) -> tuple[tuple[RoleBinding, ...], list[ConstantInput]]:
        if not isinstance(inputs, Mapping):
            msg = "Composition inputs must map roles to plan references"
            raise TypeError(msg)
        if any(not isinstance(role, str) for role in inputs):
            msg = "Input role names must be strings"
            raise TypeError(msg)
        contracts = {item.role: item for item in child.recipe.inputs}
        unknown = inputs.keys() - contracts.keys()
        bindings = []
        constants: list[ConstantInput] = []
        for spec in contracts.values():
            binding = self._input_binding(
                child, spec, inputs, call_name, recorded, constants
            )
            if binding is not None:
                bindings.append(binding)
        bound_roles = {item.role for item in bindings}
        missing = {
            role
            for role, spec in contracts.items()
            if spec.required and role not in bound_roles
        }
        if unknown or missing:
            msg = f"Invalid call inputs: unknown={sorted(unknown)}, missing={sorted(missing)}"
            raise ValueError(msg)
        return tuple(bindings), constants

    def _input_binding(
        self,
        child: CompiledPlan,
        spec: InputContract,
        inputs: Mapping[str, object],
        call_name: str,
        recorded: RecipeCall | None,
        constants: list[ConstantInput],
    ) -> RoleBinding | None:
        if spec.role in inputs:
            return RoleBinding(
                role=spec.role,
                sources=self._references(inputs[spec.role], spec.artifact_type),
            )
        if recorded is not None:
            return next(
                (
                    item
                    for item in recorded.inputs
                    if item.role == spec.role and item.constants is not None
                ),
                None,
            )
        artifacts = child.definition.inputs[spec.role].snapshot_default()
        if artifacts is None:
            return None
        constants.append(ConstantInput((call_name,), spec.role, artifacts))
        return RoleBinding(
            role=spec.role,
            sources=(),
            constants=tuple(
                ConstantArtifact(
                    artifact_id=cast(str, artifact.artifact_id),
                    artifact_type=artifact.artifact_type,
                )
                for artifact in artifacts
            ),
        )

    def _references(
        self,
        reference: object,
        expected_type: str,
    ) -> tuple[RecipeReference, ...]:
        values = (reference,) if isinstance(reference, CompositeRef) else reference
        if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
            msg = "Wiring requires a plan reference or an ordered reference sequence"
            raise TypeError(msg)
        result = []
        for value in values:
            if not isinstance(value, CompositeRef) or value.owner != self._owner:
                msg = "Wiring reference belongs to another composition"
                raise ValueError(msg)
            artifact_type = self._reference_type(value)
            if not ArtifactTypes.matches(expected_type, artifact_type):
                msg = f"Cannot wire {artifact_type!r} into {expected_type!r}"
                raise ValueError(msg)
            result.append(RecipeReference(call=value.call, role=value.role))
        return tuple(result)

    def _reference_type(self, reference: CompositeRef) -> str:
        contracts: tuple[InputContract | OutputRoleContract, ...]
        if reference.call is None:
            contracts = self._recipe.inputs
        elif reference.call in self._calls:
            contracts = self._calls[reference.call].recipe.outputs
        else:
            msg = f"Reference names an undeclared call {reference.call!r}"
            raise ValueError(msg)
        for contract in contracts:
            if contract.role == reference.role:
                return contract.artifact_type
        msg = f"Reference names an undeclared role {reference.role!r}"
        raise ValueError(msg)
