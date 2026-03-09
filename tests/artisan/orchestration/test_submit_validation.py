"""Tests for submit-time validation of overrides.

Verifies that unknown keys in params, resources, execution, command,
and input roles are caught immediately at submit() time with helpful
error messages.
"""

from __future__ import annotations

from enum import StrEnum, auto
from typing import Any, ClassVar
from unittest.mock import MagicMock

import pytest

# =============================================================================
# Mock Operations
# =============================================================================
from pydantic import BaseModel

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.pipeline_manager import (
    _validate_environment,
    _validate_execution,
    _validate_input_roles,
    _validate_input_types,
    _validate_params,
    _validate_required_inputs,
    _validate_resources,
    _validate_tool,
)
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.operation_config.environment_spec import (
    ApptainerEnvironmentSpec,
    DockerEnvironmentSpec,
)
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class MockParams(BaseModel):
    count: int = 1
    seed: int = 42


class MockOpWithParams(OperationDefinition):
    """Operation with a params sub-model."""

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "mock_with_params"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type=ArtifactTypes.DATA, is_memory_output=True
        ),
    }
    params: MockParams = MockParams()

    def execute_curator(self, execute_input: Any) -> Any:
        from artisan.schemas.execution.curator_result import ArtifactResult

        return ArtifactResult(success=True)


class MockFlatFieldsOp(OperationDefinition):
    """Operation with flat fields (no params sub-model)."""

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "mock_flat_fields"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type=ArtifactTypes.DATA, is_memory_output=True
        ),
    }

    flavor: str = "vanilla"

    def execute_curator(self, execute_input: Any) -> Any:
        from artisan.schemas.execution.curator_result import ArtifactResult

        return ArtifactResult(success=True)


class MockCreatorOp(OperationDefinition):
    """Creator op with tool and environments (for override tests)."""

    class InputRole(StrEnum):
        data = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "mock_creator_cmd"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.data: InputSpec(artifact_type=ArtifactTypes.DATA, required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            infer_lineage_from={"inputs": ["data"]},
        ),
    }

    tool: ToolSpec = ToolSpec(executable="/opt/run.sh")
    environments: Environments = Environments(
        apptainer=ApptainerEnvironmentSpec(image="/opt/image.sif"),
        docker=DockerEnvironmentSpec(image="img:latest"),
    )

    def preprocess(self, inputs: Any) -> dict:
        return {}

    def execute(self, inputs: Any) -> Any:
        return None


class MockCuratorOp(OperationDefinition):
    """Curator op (no command field)."""

    class InputRole(StrEnum):
        data = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "mock_curator_no_cmd"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.data: InputSpec(artifact_type=ArtifactTypes.DATA, required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type=ArtifactTypes.DATA, is_memory_output=True
        ),
    }

    def execute_curator(self, execute_input: Any) -> Any:
        from artisan.schemas.execution.curator_result import ArtifactResult

        return ArtifactResult(success=True)


class MockRuntimeInputsOp(OperationDefinition):
    """Curator op with runtime_defined_inputs=True."""

    class OutputRole(StrEnum):
        merged = auto()

    name: ClassVar[str] = "mock_runtime_inputs"
    runtime_defined_inputs: ClassVar[bool] = True
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.merged: OutputSpec(
            artifact_type=ArtifactTypes.DATA, is_memory_output=True
        ),
    }

    def execute_curator(self, execute_input: Any) -> Any:
        from artisan.schemas.execution.curator_result import ArtifactResult

        return ArtifactResult(success=True)


class MockOpWithOptionalInput(OperationDefinition):
    """Operation with both required and optional inputs."""

    class InputRole(StrEnum):
        dataset = auto()
        reference = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "mock_with_optional"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.dataset: InputSpec(artifact_type=ArtifactTypes.DATA, required=True),
        InputRole.reference: InputSpec(
            artifact_type=ArtifactTypes.DATA, required=False
        ),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type=ArtifactTypes.DATA, is_memory_output=True
        ),
    }

    def execute_curator(self, execute_input: Any) -> Any:
        from artisan.schemas.execution.curator_result import ArtifactResult

        return ArtifactResult(success=True)


class MockOpWithAnyInput(OperationDefinition):
    """Operation that accepts ANY artifact type."""

    class InputRole(StrEnum):
        data = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "mock_any_input"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.data: InputSpec(artifact_type=ArtifactTypes.ANY, required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type=ArtifactTypes.DATA, is_memory_output=True
        ),
    }

    def execute_curator(self, execute_input: Any) -> Any:
        from artisan.schemas.execution.curator_result import ArtifactResult

        return ArtifactResult(success=True)


# =============================================================================
# Tests for _validate_params
# =============================================================================


class TestValidateParams:
    """Tests for param validation."""

    def test_valid_params_with_sub_model(self):
        """Valid keys for a params sub-model should not raise."""
        _validate_params(MockOpWithParams, {"count": 5, "seed": 99})

    def test_unknown_param_with_sub_model_raises(self):
        """Unknown key in params sub-model should raise ValueError."""
        with pytest.raises(ValueError, match="Unknown params.*bogus"):
            _validate_params(MockOpWithParams, {"count": 5, "bogus": True})

    def test_valid_flat_fields(self):
        """Valid flat field keys should not raise."""
        _validate_params(MockFlatFieldsOp, {"flavor": "chocolate"})

    def test_unknown_flat_field_raises(self):
        """Unknown flat field key should raise ValueError."""
        with pytest.raises(ValueError, match="Unknown params.*bogus"):
            _validate_params(MockFlatFieldsOp, {"bogus": True})


# =============================================================================
# Tests for _validate_resources
# =============================================================================


class TestValidateResources:
    """Tests for resource validation."""

    def test_valid_resources(self):
        """Valid resource keys should not raise."""
        _validate_resources({"memory_gb": 32, "gpus": 1})

    def test_unknown_resource_raises(self):
        """Unknown resource key should raise with valid keys listed."""
        with pytest.raises(ValueError, match="Unknown resource keys.*mem_Gb"):
            _validate_resources({"mem_Gb": 32})

    def test_error_lists_valid_keys(self):
        """Error message should list valid keys."""
        with pytest.raises(ValueError, match="Valid keys:.*memory_gb"):
            _validate_resources({"bogus": True})


# =============================================================================
# Tests for _validate_execution
# =============================================================================


class TestValidateExecution:
    """Tests for execution validation."""

    def test_valid_execution(self):
        """Valid execution keys should not raise."""
        _validate_execution({"artifacts_per_unit": 10, "max_workers": 8})

    def test_unknown_execution_raises(self):
        """Unknown execution key should raise ValueError."""
        with pytest.raises(ValueError, match="Unknown execution keys.*batch_size"):
            _validate_execution({"batch_size": 10})


# =============================================================================
# Tests for _validate_environment
# =============================================================================


class TestValidateEnvironment:
    """Tests for environment validation."""

    def test_valid_string_environment(self):
        """Valid environment string should not raise."""
        _validate_environment(MockCreatorOp, "local")

    def test_unknown_string_environment_raises(self):
        """Unknown environment string should raise ValueError."""
        with pytest.raises(ValueError, match="not configured"):
            _validate_environment(MockCreatorOp, "pixi")

    def test_valid_dict_environment(self):
        """Valid environment dict should not raise."""
        _validate_environment(MockCreatorOp, {"active": "docker"})

    def test_unknown_environment_key_raises(self):
        """Unknown environment key should raise ValueError."""
        with pytest.raises(ValueError, match="Unknown environment keys.*bogus"):
            _validate_environment(MockCreatorOp, {"bogus": True})

    def test_unknown_nested_key_raises(self):
        """Unknown key in nested environment spec should raise."""
        with pytest.raises(ValueError, match="Unknown keys for docker.*bogus"):
            _validate_environment(MockCreatorOp, {"docker": {"bogus": True}})

    def test_empty_dict_accepted(self):
        """Empty dict should not raise."""
        _validate_environment(MockCreatorOp, {})


# =============================================================================
# Tests for _validate_tool
# =============================================================================


class TestValidateTool:
    """Tests for tool validation."""

    def test_valid_tool_override(self):
        """Valid tool keys should not raise."""
        _validate_tool(MockCreatorOp, {"executable": "/new/path"})

    def test_unknown_tool_key_raises(self):
        """Unknown tool key should raise ValueError."""
        with pytest.raises(ValueError, match="Unknown tool keys.*bogus"):
            _validate_tool(MockCreatorOp, {"bogus": True})

    def test_no_tool_raises(self):
        """Operation without tool should raise on tool override."""
        with pytest.raises(ValueError, match="has no tool to override"):
            _validate_tool(MockCuratorOp, {"executable": "/new"})

    def test_empty_tool_accepted(self):
        """Empty tool dict should not raise."""
        _validate_tool(MockCreatorOp, {})


# =============================================================================
# Tests for _validate_input_roles
# =============================================================================


class TestValidateInputRoles:
    """Tests for input role validation."""

    def test_valid_input_roles(self):
        """Valid input role names should not raise."""
        ref = MagicMock()
        _validate_input_roles(MockCuratorOp, {"data": ref})

    def test_unknown_input_role_raises(self):
        """Unknown input role should raise ValueError."""
        ref = MagicMock()
        with pytest.raises(ValueError, match="Unknown input roles.*bogus"):
            _validate_input_roles(MockCuratorOp, {"bogus": ref})

    def test_runtime_defined_inputs_skips_validation(self):
        """runtime_defined_inputs=True should skip input role validation."""
        ref = MagicMock()
        # This should NOT raise even though "anything" is not a declared role
        _validate_input_roles(MockRuntimeInputsOp, {"anything": ref})

    def test_non_dict_inputs_skips_validation(self):
        """Non-dict inputs (list, None) should skip role validation."""
        _validate_input_roles(MockCuratorOp, None)
        _validate_input_roles(MockCuratorOp, [MagicMock()])


# =============================================================================
# Tests for no-overrides case
# =============================================================================


class TestNoOverrides:
    """Tests for the no-overrides case."""

    def test_all_none_overrides_uses_defaults(self):
        """All None overrides should use operation defaults without error."""
        from artisan.orchestration.engine.step_executor import (
            instantiate_operation,
        )

        instance = instantiate_operation(
            MockOpWithParams,
            params=None,
            resources=None,
            execution=None,
            environment=None,
            tool=None,
        )
        assert instance.params.count == 1
        assert instance.resources.cpus == 1
        assert instance.execution.artifacts_per_unit == 1


# =============================================================================
# Tests for _validate_required_inputs
# =============================================================================


class TestValidateRequiredInputs:
    """Tests for required input validation."""

    def test_missing_required_role_raises(self):
        """Missing a required role should raise ValueError."""
        with pytest.raises(ValueError, match="Missing required input.*data"):
            _validate_required_inputs(MockCuratorOp, {})

    def test_all_required_provided(self):
        """All required roles provided should not raise."""
        ref = MagicMock()
        _validate_required_inputs(MockCuratorOp, {"data": ref})

    def test_optional_missing_no_error(self):
        """Missing optional role should not raise."""
        ref = MagicMock()
        _validate_required_inputs(MockOpWithOptionalInput, {"dataset": ref})

    def test_runtime_defined_inputs_skips(self):
        """runtime_defined_inputs=True should skip validation."""
        _validate_required_inputs(MockRuntimeInputsOp, {})

    def test_generative_op_no_error(self):
        """Generative op (empty inputs ClassVar) should not raise."""
        _validate_required_inputs(MockOpWithParams, None)

    def test_non_dict_inputs_skips(self):
        """Non-dict inputs (list, None) should skip validation."""
        _validate_required_inputs(MockCuratorOp, None)
        _validate_required_inputs(MockCuratorOp, [MagicMock()])


# =============================================================================
# Tests for _validate_input_types
# =============================================================================


class TestValidateInputTypes:
    """Tests for input type validation."""

    def test_type_mismatch_raises(self):
        """Wiring metric output into data input should raise ValueError."""
        ref = OutputReference(
            source_step=0, role="metrics", artifact_type=ArtifactTypes.METRIC
        )
        with pytest.raises(ValueError, match="Type mismatch.*data"):
            _validate_input_types(MockCuratorOp, {"data": ref})

    def test_type_match_no_error(self):
        """Matching types should not raise."""
        ref = OutputReference(
            source_step=0, role="output", artifact_type=ArtifactTypes.DATA
        )
        _validate_input_types(MockCuratorOp, {"data": ref})

    def test_any_downstream_spec_no_error(self):
        """ANY downstream spec should accept any upstream type."""
        ref = OutputReference(
            source_step=0, role="output", artifact_type=ArtifactTypes.DATA
        )
        _validate_input_types(MockOpWithAnyInput, {"data": ref})

    def test_any_upstream_ref_skips(self):
        """ANY upstream ref should skip type check."""
        ref = OutputReference(
            source_step=0, role="output", artifact_type=ArtifactTypes.ANY
        )
        _validate_input_types(MockCuratorOp, {"data": ref})

    def test_non_output_reference_skips(self):
        """Non-OutputReference values (raw lists) should skip type check."""
        _validate_input_types(MockCuratorOp, {"data": ["artifact-id-1"]})

    def test_non_dict_inputs_skips(self):
        """Non-dict inputs should skip type check."""
        _validate_input_types(MockCuratorOp, None)
        _validate_input_types(MockCuratorOp, [MagicMock()])

    def test_runtime_defined_role_skips(self):
        """Role not in operation.inputs (runtime-defined) should skip."""
        ref = OutputReference(
            source_step=0, role="output", artifact_type=ArtifactTypes.METRIC
        )
        _validate_input_types(MockRuntimeInputsOp, {"custom_role": ref})
