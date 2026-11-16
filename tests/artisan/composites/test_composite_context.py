"""Public composition references describe one immutable scientific boundary."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from inspect import signature

import pytest
from fixtures.composition_operations import PredictMeasure, Transform

from artisan.composites import CompositeCall, CompositeContext, CompositeRef
from artisan.composites.base.compiler import compile_definition


def test_public_context_exposes_only_scientific_call_controls() -> None:
    assert list(signature(CompositeContext.call).parameters) == [
        "self",
        "definition",
        "inputs",
        "params",
        "name",
    ]
    assert not hasattr(CompositeContext, "run")


def test_public_references_are_scoped_frozen_and_not_futures() -> None:
    plan = compile_definition(PredictMeasure)
    context = CompositeContext(plan.recipe, (PredictMeasure,))
    source = context.input("structure")
    child = context.call(Transform, inputs={"structure": source})
    assert isinstance(source, CompositeRef)
    assert isinstance(child, CompositeCall)
    assert child.output("structure").owner == source.owner
    assert not hasattr(child, "result")
    with pytest.raises(FrozenInstanceError):
        source.role = "different"
    with pytest.raises(ValueError, match="Unknown output"):
        child.output("missing")
    other = CompositeContext(plan.recipe, (PredictMeasure,))
    with pytest.raises(ValueError, match="another composition"):
        other.call(Transform, inputs={"structure": source})
