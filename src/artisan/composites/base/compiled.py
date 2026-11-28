"""Live registered definitions paired with immutable scientific recipes."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from artisan.composites.base.called_operations import CapturedCalledOperation
from artisan.composites.base.root_selection import RootSelection
from artisan.composites.base.snapshots import parameter_class
from artisan.schemas.composites.plan import ExecutionRecipe

if TYPE_CHECKING:
    from artisan.composites.base.composite_definition import CompositeDefinition
    from artisan.operations.base.operation_definition import OperationDefinition
    from artisan.schemas.artifact.base import Artifact


@dataclass(frozen=True)
class ConstantInput:
    """Detached artifacts at one nested call's omitted input role."""

    call_path: tuple[str, ...]
    role: str
    artifacts: tuple[Artifact, ...]


@dataclass(frozen=True)
class CompiledCall:
    """A named call whose live code was explicitly supplied by the caller."""

    name: str
    plan: CompiledPlan


@dataclass(frozen=True)
class CompiledPlan:
    """Executable code bindings kept separate from durable recipe data."""

    definition: type[OperationDefinition] | type[CompositeDefinition]
    recipe: ExecutionRecipe
    calls: tuple[CompiledCall, ...] = ()
    root_selection: RootSelection = field(default_factory=RootSelection)
    constants: tuple[ConstantInput, ...] = ()
    called_operations: tuple[CapturedCalledOperation, ...] = ()

    def instantiate(self) -> OperationDefinition | CompositeDefinition:
        """Construct fresh scientific state without importing recorded names."""
        params_class = parameter_class(self.definition)
        if params_class is None:
            instance = self.definition()
        else:
            validated = params_class.model_validate_json(
                self.recipe.params_json, extra="forbid", by_name=True
            )
            instance = self.definition.model_validate({"params": validated})
        current = self.root_selection.snapshot(instance).model_dump()
        current["cacheable"] = (
            current["cacheable"]
            and all(call.plan.recipe.cacheable for call in self.calls)
            and all(item.contract.cacheable for item in self.called_operations)
        )
        if current != self.recipe.model_dump(include=set(current)):
            msg = "Definition execution configuration changed; compile the plan again"
            raise ValueError(msg)
        return instance
