"""Declare creator and nested-composite calls under one scientific boundary."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, cast
from uuid import uuid4

from artisan.composites.base.compiled import CompiledCall, CompiledPlan
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.composites.composite_ref import CompositeCall, CompositeRef
from artisan.schemas.composites.plan import (
    DefinitionContract,
    ExecutionRecipe,
    InputContract,
    OutputContract,
    RecipeCall,
    RecipeReference,
    RoleBinding,
)

if TYPE_CHECKING:
    from artisan.composites.base.compiler import Definition, Parameters


class CompositeContext:
    """Build one immutable scientific composition without dispatch or storage."""

    def __init__(
        self, recipe: DefinitionContract, stack: tuple[Definition, ...]
    ) -> None:
        self._owner = uuid4().hex
        self._recipe = recipe
        self._stack = stack
        self._calls: dict[str, CompiledPlan] = {}
        self._wiring: list[RecipeCall] = []
        self._exports: dict[str, RoleBinding] = {}

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

        child = _compile(definition, params, self._stack)
        bindings = self._bind_inputs(child.recipe, {} if inputs is None else inputs)
        self._calls[call_name] = child
        self._wiring.append(
            RecipeCall(name=call_name, inputs=bindings, recipe=child.recipe)
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
        )

    def _bind_inputs(
        self,
        recipe: ExecutionRecipe,
        inputs: object,
    ) -> tuple[RoleBinding, ...]:
        if not isinstance(inputs, Mapping):
            msg = "Composition inputs must map roles to plan references"
            raise TypeError(msg)
        if any(not isinstance(role, str) for role in inputs):
            msg = "Input role names must be strings"
            raise TypeError(msg)
        contracts = {item.role: item for item in recipe.inputs}
        unknown = inputs.keys() - contracts.keys()
        missing = {
            role
            for role, spec in contracts.items()
            if spec.required and role not in inputs
        }
        if unknown or missing:
            msg = f"Invalid call inputs: unknown={sorted(unknown)}, missing={sorted(missing)}"
            raise ValueError(msg)
        return tuple(
            RoleBinding(
                role=role,
                sources=self._references(inputs[role], spec.artifact_type),
            )
            for role, spec in contracts.items()
            if role in inputs
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
        contracts: tuple[InputContract | OutputContract, ...]
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
