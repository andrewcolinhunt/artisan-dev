"""Validate fixed recipe inputs against ordinary captured artifact occurrences."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

from artisan.schemas.composites.plan import (
    ConstantArtifact,
    ExecutionRecipe,
    InputContract,
)
from artisan.schemas.provenance.projection import BoundaryProjection


@dataclass(frozen=True)
class ConstantBinding:
    """One used fixed input, identified independently of scientific grouping."""

    call_path: tuple[str, ...]
    role: str
    artifacts: tuple[ConstantArtifact, ...]
    spec: InputContract


def constant_bindings(
    recipe: ExecutionRecipe, path: tuple[str, ...] = ()
) -> Iterator[ConstantBinding]:
    """Visit the ordered fixed inputs of this recipe and its nested calls."""
    for call in recipe.calls:
        child_path = (*path, call.name)
        specs = {spec.role: spec for spec in call.recipe.inputs}
        for binding in call.inputs:
            if binding.constants is not None:
                yield ConstantBinding(
                    child_path, binding.role, binding.constants, specs[binding.role]
                )
        yield from constant_bindings(call.recipe, child_path)


def validate_constant_context(
    recipe: ExecutionRecipe, boundary: BoundaryProjection
) -> None:
    """Require exact recipe membership and unscoped constant artifact identity."""
    expected = tuple(constant_bindings(recipe))
    actual = boundary.frozen_context.constants
    if len(expected) != len(actual):
        msg = "Captured constant inputs do not match the compiled recipe"
        raise ValueError(msg)
    occurrences = {item.address: item for item in boundary.occurrences}
    producers = {item.boundary: item.producer for item in boundary.input_attribution}
    roots = {
        address for group in boundary.boundary_inputs for address in group.occurrences
    }
    for declared, captured in zip(expected, actual, strict=True):
        if (
            (declared.call_path, declared.role) != (captured.call_path, captured.role)
            or len(declared.artifacts) != len(captured.occurrences)
            or roots.intersection(captured.occurrences)
        ):
            msg = "Constant input capture changed its binding or occurrence positions"
            raise ValueError(msg)
        if declared.artifacts and declared.spec.with_associated:
            msg = (
                "Fixed artifacts have no accepted producer for associated input queries"
            )
            raise ValueError(msg)
        for artifact, address in zip(
            declared.artifacts, captured.occurrences, strict=True
        ):
            occurrence = occurrences.get(address)
            if (
                occurrence is None
                or (occurrence.artifact_id, occurrence.artifact_type)
                != (artifact.artifact_id, artifact.artifact_type)
                or address not in producers
                or producers[address] is not None
            ):
                msg = "Captured constant differs from its unscoped recipe artifact"
                raise ValueError(msg)
