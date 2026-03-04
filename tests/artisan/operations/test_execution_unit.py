"""Unit tests for the unified ExecutionUnit dataclass.

Reference: design_execution_unit_refactor.md
"""

from __future__ import annotations

from enum import StrEnum, auto
from typing import Any, ClassVar

import pytest
from pydantic import Field

from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.specs.input_models import PreprocessInput
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

# Test artifact IDs (32-char hex strings mimicking xxh3_128 hashes)
ARTIFACT_ID_1 = "a" * 32
ARTIFACT_ID_2 = "b" * 32
ARTIFACT_ID_3 = "c" * 32
ARTIFACT_ID_4 = "d" * 32


# Mock operation definitions (prefixed with Mock to avoid pytest collection)
class MockOperation(OperationDefinition):
    """Mock operation for testing."""

    class InputRole(StrEnum):
        files = auto()

    class OutputRole(StrEnum):
        result = auto()

    name: ClassVar[str] = "mock_op"
    description: ClassVar[str] = "Mock operation for testing"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.files: InputSpec(
            artifact_type=ArtifactTypes.DATA,
            required=False,
            description="Input files",
        ),
    }

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.result: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            include=["*"],
            description="Output files",
            infer_lineage_from={"inputs": ["files"]},
        ),
    }

    count: int = Field(default=1)

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            role: [a.materialized_path for a in artifacts]
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute(self, inputs: dict[str, Any], output_dir):
        return ArtifactResult(success=True, metadata={"count": self.count})


class MultiInputOp(OperationDefinition):
    """Mock operation with multiple inputs for testing."""

    class InputRole(StrEnum):
        data = auto()
        config = auto()

    class OutputRole(StrEnum):
        result = auto()

    name: ClassVar[str] = "multi_input_op"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.data: InputSpec(
            artifact_type=ArtifactTypes.DATA,
            required=False,
            description="Data input",
        ),
        InputRole.config: InputSpec(
            artifact_type=ArtifactTypes.DATA,
            required=False,
            description="Config input",
        ),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.result: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            include=["*"],
            infer_lineage_from={"inputs": ["data", "config"]},
        ),
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            role: [a.materialized_path for a in artifacts]
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute(self, inputs: dict[str, Any], output_dir):
        return ArtifactResult(success=True)


class GenerativeOp(OperationDefinition):
    """Mock generative operation (no inputs) for testing."""

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "generative_op"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            include=["*"],
            infer_lineage_from={"inputs": []},
        ),
    }

    def execute(self, inputs: dict[str, Any], output_dir):
        return ArtifactResult(success=True)


class UnionOp(OperationDefinition):
    """Mock operation that allows unequal input lengths (like MergeOp)."""

    class OutputRole(StrEnum):
        merged = auto()

    name: ClassVar[str] = "union_op"
    runtime_defined_inputs: ClassVar[bool] = True
    independent_input_streams: ClassVar[bool] = True
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.merged: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            include=["*"],
            infer_lineage_from={"inputs": []},
        ),
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            role: [a.materialized_path for a in artifacts]
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute(self, inputs: dict[str, Any], output_dir):
        return ArtifactResult(success=True)


class TestExecutionUnit:
    """Tests for ExecutionUnit with batch inputs."""

    def test_create_with_single_role_batch(self):
        """Test creation with single input role batch."""
        unit = ExecutionUnit(
            operation=MockOperation(),
            inputs={"files": [ARTIFACT_ID_1, ARTIFACT_ID_2]},
        )
        assert unit.inputs == {"files": [ARTIFACT_ID_1, ARTIFACT_ID_2]}
        assert unit.get_batch_size() == 2

    def test_create_with_multi_role_batch(self):
        """Test creation with multiple input roles (same batch size)."""
        unit = ExecutionUnit(
            operation=MultiInputOp(),
            inputs={
                "data": [ARTIFACT_ID_1, ARTIFACT_ID_2],
                "config": [ARTIFACT_ID_3, ARTIFACT_ID_4],
            },
        )
        assert unit.get_batch_size() == 2

    def test_empty_inputs_for_generative_ops(self):
        """Test creation with empty inputs (generative ops)."""
        unit = ExecutionUnit(
            operation=GenerativeOp(),
            inputs={},
        )
        assert unit.get_batch_size() == 0

    def test_mismatched_batch_sizes_rejected(self):
        """Test that different batch sizes across roles are rejected."""
        with pytest.raises(ValueError, match="same batch size"):
            ExecutionUnit(
                operation=MultiInputOp(),
                inputs={
                    "data": [ARTIFACT_ID_1, ARTIFACT_ID_2],
                    "config": [ARTIFACT_ID_3],  # Only 1 item!
                },
            )

    def test_unequal_input_lengths_allowed_when_flagged(self):
        """Test that unequal input lengths are allowed when independent_input_streams=True."""
        # UnionOp has independent_input_streams = True
        unit = ExecutionUnit(
            operation=UnionOp(),
            inputs={
                "branch_a": [ARTIFACT_ID_1, ARTIFACT_ID_2],
                "branch_b": [ARTIFACT_ID_3],  # Different size - should be allowed
            },
        )
        # Should not raise - union semantics allow different sizes
        assert "branch_a" in unit.inputs
        assert "branch_b" in unit.inputs
        assert len(unit.inputs["branch_a"]) == 2
        assert len(unit.inputs["branch_b"]) == 1

    def test_invalid_artifact_id_rejected(self):
        """Test that non-32-char strings are rejected."""
        with pytest.raises(ValueError, match="32-char hex string"):
            ExecutionUnit(
                operation=MockOperation(),
                inputs={"files": ["short"]},
            )

    def test_non_list_value_rejected(self):
        """Test that non-list values are rejected."""
        # Pydantic raises ValidationError when type doesn't match
        with pytest.raises(Exception, match="list"):
            ExecutionUnit(
                operation=MockOperation(),
                inputs={"files": ARTIFACT_ID_1},  # Not a list!
            )

    def test_should_access_operation_metadata(self):
        """Test accessing operation metadata through execution unit."""
        unit = ExecutionUnit(
            operation=MockOperation(),
            inputs={},
        )

        assert unit.operation.name == "mock_op"

    def test_should_validate_inputs_against_inputs(self):
        """Test that inputs are validated against inputs spec."""
        # Valid inputs
        unit = ExecutionUnit(
            operation=MockOperation(),
            inputs={"files": [ARTIFACT_ID_1]},
        )
        assert "files" in unit.inputs

        # Invalid input name should raise
        with pytest.raises(ValueError, match="Unexpected input"):
            ExecutionUnit(
                operation=MockOperation(),
                inputs={"unknown_input": [ARTIFACT_ID_1]},
            )

    def test_should_get_input_artifact_ids(self):
        """Test extracting artifact IDs from inputs."""
        unit = ExecutionUnit(
            operation=MockOperation(),
            inputs={"files": [ARTIFACT_ID_1, ARTIFACT_ID_2]},
        )

        artifact_ids = unit.get_input_artifact_ids()
        assert artifact_ids == {"files": [ARTIFACT_ID_1, ARTIFACT_ID_2]}

    def test_create_with_step_number(self):
        """Test creation with all optional fields."""
        unit = ExecutionUnit(
            operation=MockOperation(count=3),
            inputs={"files": [ARTIFACT_ID_1]},
            execution_spec_id="spec123" + "0" * 25,
            step_number=5,
        )

        assert unit.operation.count == 3
        assert unit.execution_spec_id == "spec123" + "0" * 25
        assert unit.step_number == 5


class TestExecutionUnitGroupIds:
    """Tests for ExecutionUnit group_ids field."""

    def test_group_ids_defaults_to_none(self):
        """Test that group_ids defaults to None when not provided."""
        unit = ExecutionUnit(
            operation=MockOperation(),
            inputs={"files": [ARTIFACT_ID_1]},
        )
        assert unit.group_ids is None

    def test_group_ids_set_on_creation(self):
        """Test that group_ids can be set when creating ExecutionUnit."""
        unit = ExecutionUnit(
            operation=MockOperation(),
            inputs={"files": [ARTIFACT_ID_1, ARTIFACT_ID_2]},
            group_ids=["group_a", "group_b"],
        )
        assert unit.group_ids == ["group_a", "group_b"]

    def test_group_ids_with_multi_role_inputs(self):
        """Test group_ids with multi-role inputs (paired operation)."""
        unit = ExecutionUnit(
            operation=MultiInputOp(),
            inputs={
                "data": [ARTIFACT_ID_1, ARTIFACT_ID_2],
                "config": [ARTIFACT_ID_3, ARTIFACT_ID_4],
            },
            group_ids=["g1", "g2"],
        )
        assert unit.group_ids == ["g1", "g2"]
        assert unit.get_batch_size() == 2

    def test_group_ids_empty_list(self):
        """Test that empty group_ids list is valid (e.g., no matches found)."""
        unit = ExecutionUnit(
            operation=GenerativeOp(),
            inputs={},
            group_ids=[],
        )
        assert unit.group_ids == []


class TestOperationDefinitionValidation:
    """Tests for OperationDefinition validation."""

    def test_should_validate_operation_with_execute_method(self):
        """Operations with execute() should be valid and usable."""
        # MockOperation has execute() defined - class creation should succeed
        assert MockOperation.name == "mock_op"

    def test_should_fail_without_execute_method(self):
        """Operations without execute() or execute_curator() should fail validation."""
        # Since validation happens automatically in __pydantic_init_subclass__,
        # the error is raised during class definition, not when calling validate_execution_spec()
        with pytest.raises(
            TypeError,
            match="must implement either execute\\(\\) \\(creator ops\\) or execute_curator\\(\\)",
        ):

            class BadOp(OperationDefinition):
                name: ClassVar[str] = "bad_op"

                inputs: ClassVar[dict[str, InputSpec]] = {}
                outputs: ClassVar[dict[str, OutputSpec]] = {}
                # No execute() or execute_curator() method!
