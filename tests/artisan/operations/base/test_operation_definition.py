"""Tests for the unified OperationDefinition model."""

from __future__ import annotations

from enum import StrEnum, auto
from typing import Any, ClassVar

import pytest
from pydantic import Field, ValidationError

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.operation_config.resource_config import ResourceConfig
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class SimpleOperation(OperationDefinition):
    """Simple operation for testing."""

    class OutputRole(StrEnum):
        result = auto()

    name: ClassVar[str] = "simple_op"
    description: ClassVar[str] = "A simple test operation"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.result: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            infer_lineage_from={"inputs": []},
        ),
    }

    count: int = Field(default=1, ge=1)
    label: str = Field(default="default")
    verbose: bool = Field(default=False)

    def execute(self, inputs: dict[str, Any], output_dir):
        return ArtifactResult(success=True, metadata={"count": self.count})


class PositionalOperation(OperationDefinition):
    """Operation with required parameters."""

    class OutputRole(StrEnum):
        result = auto()

    name: ClassVar[str] = "positional_op"
    description: ClassVar[str] = "Operation with required params"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.result: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            infer_lineage_from={"inputs": []},
        ),
    }

    input_file: str = Field(...)
    output_file: str = Field(...)
    verbose: bool = Field(default=False)

    def execute(self, inputs: dict[str, Any], output_dir):
        return ArtifactResult(success=True)


class TestOperationDefinitionValidation:
    """Tests for OperationDefinition validation."""

    def test_should_create_with_defaults(self):
        """Should create instance with default values."""
        op = SimpleOperation()

        assert op.count == 1
        assert op.label == "default"
        assert op.verbose is False

    def test_should_accept_overrides(self):
        """Should accept override values."""
        op = SimpleOperation(count=5, label="custom", verbose=True)

        assert op.count == 5
        assert op.label == "custom"
        assert op.verbose is True

    def test_should_validate_constraints(self):
        """Should validate Pydantic constraints."""
        with pytest.raises(ValidationError) as exc_info:
            SimpleOperation(count=0)  # Must be >= 1

        assert "greater than or equal to 1" in str(exc_info.value)

    def test_should_reject_unknown_params(self):
        """Should reject unknown parameters."""
        with pytest.raises(ValidationError) as exc_info:
            SimpleOperation(unknown=True)

        assert "extra" in str(exc_info.value).lower()

    def test_should_require_positional_params(self):
        """Should require positional parameters."""
        with pytest.raises(ValidationError):
            PositionalOperation()  # Missing required input_file and output_file

    def test_should_accept_required_params(self):
        """Should accept required parameters."""
        op = PositionalOperation(input_file="in.txt", output_file="out.txt")

        assert op.input_file == "in.txt"
        assert op.output_file == "out.txt"


class TestOperationDefinitionExecute:
    """Tests for execute method."""

    def test_should_execute_successfully(self, tmp_path):
        """Should execute and return ArtifactResult."""
        op = SimpleOperation(count=5)
        result = op.execute(inputs={}, output_dir=tmp_path)

        assert result.success is True
        assert result.metadata["count"] == 5

    def test_should_access_params_via_self(self, tmp_path):
        """Should be able to access params via self in execute."""
        op = SimpleOperation(count=10, label="test")
        result = op.execute(inputs={}, output_dir=tmp_path)

        # The implementation accesses self.count
        assert result.metadata["count"] == 10


class TestOperationDefinitionModelDump:
    """Tests for model_dump method (Pydantic native)."""

    def test_should_convert_to_dict(self):
        """Should convert to dict via model_dump."""
        op = SimpleOperation(count=5, verbose=True)
        kwargs = op.model_dump()

        assert kwargs["count"] == 5
        assert kwargs["label"] == "default"
        assert kwargs["verbose"] is True
        # Base class sub-model fields are also present
        assert "resources" in kwargs
        assert "execution" in kwargs

    def test_should_roundtrip_via_model_dump(self):
        """Should be able to reconstruct operation from model_dump."""
        op = SimpleOperation(count=3)
        kwargs = op.model_dump()

        reconstructed = SimpleOperation(**kwargs)
        assert reconstructed.count == 3
        assert reconstructed.label == "default"


class TestOperationDefinitionMetadata:
    """Tests for operation metadata access."""

    def test_should_access_class_attributes(self):
        """Should be able to access operation metadata via class attributes."""
        assert SimpleOperation.name == "simple_op"
        assert SimpleOperation.description == "A simple test operation"


class TestDataTransformerIntegration:
    """Integration tests with DataTransformer from examples."""

    def test_should_import_data_transformer_operation(self):
        """Should be able to import DataTransformer."""
        from artisan.operations.examples.data_transformer import DataTransformer

        assert DataTransformer.name == "data_transformer"

    def test_should_validate_data_transformer_params(self):
        """Should validate DataTransformer parameters."""
        from artisan.operations.examples.data_transformer import DataTransformer

        # Valid
        op = DataTransformer(
            params=DataTransformer.Params(scale_factor=2.0, variants=3)
        )
        assert op.params.scale_factor == 2.0
        assert op.params.variants == 3

        # Invalid scale_factor (negative)
        with pytest.raises(ValidationError):
            DataTransformer(params=DataTransformer.Params(scale_factor=-1.0))

        # Invalid variants (zero)
        with pytest.raises(ValidationError):
            DataTransformer(params=DataTransformer.Params(variants=0))

    def test_data_transformer_default_params(self):
        """Test that DataTransformer has correct default params."""
        from artisan.operations.examples.data_transformer import DataTransformer

        op = DataTransformer()
        assert op.params.scale_factor == 1.5
        assert op.params.variants == 1
        assert op.params.seed is None


class TestResourcesAndExecutionDefaults:
    """Tests for resources and execution instance fields on the base class."""

    def test_default_resources(self):
        """Operations get default ResourceConfig from base class."""
        op = SimpleOperation()
        assert isinstance(op.resources, ResourceConfig)
        assert op.resources.cpus == 1
        assert op.resources.memory_gb == 4
        assert op.resources.gpus == 0

    def test_default_execution(self):
        """Operations get default ExecutionConfig from base class."""
        op = SimpleOperation()
        assert isinstance(op.execution, ExecutionConfig)
        assert op.execution.artifacts_per_unit == 1
        assert op.execution.units_per_worker == 1
        assert op.execution.job_name is None

    def test_resources_not_in_model_dump_exclude(self):
        """resources and execution are instance fields on the base class."""
        op = SimpleOperation()
        dump = op.model_dump()
        # resources and execution are present in model_dump (they are instance fields)
        assert "resources" in dump
        assert "execution" in dump

    def test_data_transformer_script_has_instance_first_fields(self):
        """DataTransformerScript uses instance-first tool/environments/resources/execution."""
        from artisan.operations.examples.data_transformer_script import (
            DataTransformerScript,
        )
        from artisan.schemas.operation_config.tool_spec import ToolSpec

        op = DataTransformerScript()
        assert isinstance(op.tool, ToolSpec)
        assert op.tool.interpreter == "python"
        assert op.environments.available() == ["local", "docker"]
        assert op.resources.cpus == 1
        assert op.execution.job_name == "data_transformer_script"


class TestRoleEnumValidation:
    """Tests for InputRole/OutputRole StrEnum requirements."""

    def test_missing_output_role_raises_type_error(self):
        """Op with outputs but no OutputRole raises TypeError."""
        with pytest.raises(TypeError, match="must define OutputRole"):

            class NoOutputRole(OperationDefinition):
                name: ClassVar[str] = "no_output_role"
                inputs: ClassVar[dict[str, InputSpec]] = {}
                outputs: ClassVar[dict[str, OutputSpec]] = {
                    "result": OutputSpec(
                        artifact_type=ArtifactTypes.DATA,
                        infer_lineage_from={"inputs": []},
                    ),
                }

                def execute(self, inputs, output_dir):
                    pass

    def test_missing_input_role_raises_type_error(self):
        """Op with inputs but no InputRole raises TypeError."""
        with pytest.raises(TypeError, match="must define InputRole"):

            class NoInputRole(OperationDefinition):
                class OutputRole(StrEnum):
                    result = auto()

                name: ClassVar[str] = "no_input_role"
                inputs: ClassVar[dict[str, InputSpec]] = {
                    "data": InputSpec(artifact_type=ArtifactTypes.DATA),
                }
                outputs: ClassVar[dict[str, OutputSpec]] = {
                    "result": OutputSpec(
                        artifact_type=ArtifactTypes.DATA,
                        infer_lineage_from={"inputs": ["data"]},
                    ),
                }

                def preprocess(self, inputs):
                    return {}

                def execute(self, inputs, output_dir):
                    pass

    def test_mismatched_output_role_raises_type_error(self):
        """OutputRole members != outputs keys raises TypeError."""
        with pytest.raises(TypeError, match="don't match outputs keys"):

            class MismatchedOutput(OperationDefinition):
                class OutputRole(StrEnum):
                    wrong_name = auto()

                name: ClassVar[str] = "mismatched_output"
                inputs: ClassVar[dict[str, InputSpec]] = {}
                outputs: ClassVar[dict[str, OutputSpec]] = {
                    "result": OutputSpec(
                        artifact_type=ArtifactTypes.DATA,
                        infer_lineage_from={"inputs": []},
                    ),
                }

                def execute(self, inputs, output_dir):
                    pass

    def test_mismatched_input_role_raises_type_error(self):
        """InputRole members != inputs keys raises TypeError."""
        with pytest.raises(TypeError, match="don't match inputs keys"):

            class MismatchedInput(OperationDefinition):
                class InputRole(StrEnum):
                    wrong_name = auto()

                class OutputRole(StrEnum):
                    result = auto()

                name: ClassVar[str] = "mismatched_input"
                inputs: ClassVar[dict[str, InputSpec]] = {
                    "data": InputSpec(artifact_type=ArtifactTypes.DATA),
                }
                outputs: ClassVar[dict[str, OutputSpec]] = {
                    "result": OutputSpec(
                        artifact_type=ArtifactTypes.DATA,
                        infer_lineage_from={"inputs": ["data"]},
                    ),
                }

                def preprocess(self, inputs):
                    return {}

                def execute(self, inputs, output_dir):
                    pass

    def test_inherited_roles_pass_validation(self):
        """Subclass inheriting parent's enums passes validation."""
        from artisan.operations.examples.data_transformer import DataTransformer

        # Create a subclass that inherits DataTransformer's InputRole/OutputRole
        class CustomTransformer(DataTransformer):
            name = "custom_transformer"
            description = "Subclass for testing role inheritance"

        assert hasattr(CustomTransformer, "InputRole")
        assert hasattr(CustomTransformer, "OutputRole")
        assert set(CustomTransformer.OutputRole) == set(DataTransformer.OutputRole)

    def test_generative_op_no_input_role_ok(self):
        """Empty inputs + no InputRole is valid."""
        # SimpleOperation has no inputs and no InputRole — should not raise
        assert SimpleOperation.inputs == {}
        assert not hasattr(SimpleOperation, "InputRole")

    def test_runtime_defined_inputs_no_input_role_ok(self):
        """runtime_defined_inputs=True + no InputRole is valid."""
        from artisan.operations.curator.merge import Merge

        assert Merge.runtime_defined_inputs is True
        # Merge only has OutputRole, no InputRole — should not raise
        assert hasattr(Merge, "OutputRole")

    def test_runtime_defined_inputs_with_input_role_ok(self):
        """runtime_defined_inputs=True + InputRole is valid (Filter pattern)."""
        from artisan.operations.curator.filter import Filter

        assert Filter.runtime_defined_inputs is True
        assert hasattr(Filter, "InputRole")
        assert hasattr(Filter, "OutputRole")
        assert set(Filter.InputRole) == set(Filter.inputs)
        assert Filter.InputRole.passthrough == "passthrough"

    def test_enum_values_are_strings(self):
        """StrEnum values equal their string names."""
        from artisan.operations.examples.data_transformer import DataTransformer

        assert DataTransformer.OutputRole.DATASET == "dataset"
        assert DataTransformer.InputRole.DATASET == "dataset"

    def test_enum_members_iterable(self):
        """Enum members can be iterated."""
        from artisan.operations.examples.data_transformer import DataTransformer

        members = list(DataTransformer.OutputRole)
        assert len(members) == 1
        assert "dataset" in members

    def test_runtime_docstring_includes_roles(self):
        """Operation docstrings include Input/Output Roles sections."""
        from artisan.operations.examples.data_transformer import DataTransformer

        doc = DataTransformer.__doc__
        assert "Input Roles:" in doc
        assert "Output Roles:" in doc
        assert "dataset" in doc

    def test_runtime_docstring_replaces_static_sections(self):
        """Static Input Roles section replaced, not duplicated."""
        from artisan.operations.examples.data_transformer import DataTransformer

        doc = DataTransformer.__doc__
        # Should appear exactly once
        assert doc.count("Input Roles:") == 1
        assert doc.count("Output Roles:") == 1
