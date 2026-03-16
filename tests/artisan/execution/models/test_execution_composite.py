"""Tests for ExecutionComposite transport model and CompositeIntermediates enum."""

from __future__ import annotations

import pickle
from enum import StrEnum
from typing import ClassVar

from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.execution.models.execution_composite import (
    CompositeIntermediates,
    ExecutionComposite,
)
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.operation_config.resource_config import ResourceConfig
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

# ---------------------------------------------------------------------------
# Test composite for transport model tests
# ---------------------------------------------------------------------------


class SimpleComposite(CompositeDefinition):
    """Simple composite for testing."""

    name = "test_transport_simple"
    description = "Test composite"

    class InputRole(StrEnum):
        DATA = "data"

    class OutputRole(StrEnum):
        RESULT = "result"

    inputs: ClassVar[dict[str, InputSpec]] = {
        "data": InputSpec(artifact_type="data", required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "result": OutputSpec(artifact_type="data"),
    }

    def compose(self, ctx) -> None:
        pass


# ---------------------------------------------------------------------------
# Tests: CompositeIntermediates
# ---------------------------------------------------------------------------


class TestCompositeIntermediates:
    def test_enum_values(self):
        assert CompositeIntermediates.DISCARD == "discard"
        assert CompositeIntermediates.PERSIST == "persist"
        assert CompositeIntermediates.EXPOSE == "expose"

    def test_is_str_enum(self):
        assert isinstance(CompositeIntermediates.DISCARD, str)

    def test_all_values(self):
        assert set(CompositeIntermediates) == {"discard", "persist", "expose"}


# ---------------------------------------------------------------------------
# Tests: ExecutionComposite
# ---------------------------------------------------------------------------


class TestExecutionComposite:
    def test_construction(self):
        composite = SimpleComposite()
        ec = ExecutionComposite(
            composite=composite,
            inputs={"data": ["abc" * 10 + "ab"]},
            step_number=0,
            execution_spec_id="spec123",
        )
        assert ec.composite is composite
        assert ec.step_number == 0
        assert ec.intermediates == CompositeIntermediates.DISCARD

    def test_pickle_roundtrip(self):
        composite = SimpleComposite()
        ec = ExecutionComposite(
            composite=composite,
            inputs={"data": ["a" * 32]},
            step_number=1,
            execution_spec_id="spec456",
            resources=ResourceConfig(cpus=4),
            execution=ExecutionConfig(artifacts_per_unit=10),
            intermediates=CompositeIntermediates.PERSIST,
        )
        data = pickle.dumps(ec)
        restored = pickle.loads(data)

        assert restored.composite.name == "test_transport_simple"
        assert restored.inputs == {"data": ["a" * 32]}
        assert restored.step_number == 1
        assert restored.resources.cpus == 4
        assert restored.intermediates == CompositeIntermediates.PERSIST

    def test_defaults(self):
        ec = ExecutionComposite(
            composite=SimpleComposite(),
            inputs={},
            step_number=0,
            execution_spec_id="x",
        )
        assert ec.resources.cpus == 1
        assert ec.execution.artifacts_per_unit == 1
        assert ec.intermediates == CompositeIntermediates.DISCARD
