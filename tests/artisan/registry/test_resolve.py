"""Test resolution of explicitly declared operation classes."""

from __future__ import annotations

import pytest

from artisan.operations.examples import WaitTool
from artisan.registry.resolve import resolve_operation


class TestResolveOperation:
    def test_round_trip(self) -> None:
        assert (
            resolve_operation(f"{WaitTool.__module__}:{WaitTool.__qualname__}")
            is WaitTool
        )

    def test_non_operation_raises(self) -> None:
        with pytest.raises(TypeError, match="OperationDefinition subclass"):
            resolve_operation("artisan.schemas.operation_config.tool_spec:ToolSpec")


def test_resolve_definition_accepts_explicit_composite() -> None:
    from artisan.composites.base.composite_definition import CompositeDefinition
    from artisan.registry.resolve import resolve_definition

    assert (
        resolve_definition(
            "artisan.composites.base.composite_definition:CompositeDefinition"
        )
        is CompositeDefinition
    )
    assert (
        resolve_definition(f"{WaitTool.__module__}:{WaitTool.__qualname__}") is WaitTool
    )
    with pytest.raises(TypeError, match="operation or composite"):
        resolve_definition("artisan.schemas.operation_config.tool_spec:ToolSpec")
