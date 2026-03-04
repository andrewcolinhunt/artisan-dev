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
    _validate_command,
    _validate_execution,
    _validate_input_roles,
    _validate_params,
    _validate_resources,
)
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.operation_config.command_spec import ApptainerCommandSpec
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
    """Creator op with a command field (for command override tests)."""

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

    command: ApptainerCommandSpec = ApptainerCommandSpec(
        script="/opt/run.sh",
        arg_style="hydra",
        image="/opt/image.sif",
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
        _validate_resources({"partition": "gpu", "mem_gb": 32})

    def test_unknown_resource_raises(self):
        """Unknown resource key should raise with valid keys listed."""
        with pytest.raises(ValueError, match="Unknown resource keys.*mem_Gb"):
            _validate_resources({"mem_Gb": 32})

    def test_error_lists_valid_keys(self):
        """Error message should list valid keys."""
        with pytest.raises(ValueError, match="Valid keys:.*partition"):
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
# Tests for _validate_command
# =============================================================================


class TestValidateCommand:
    """Tests for command validation."""

    def test_valid_command_override(self):
        """Valid command keys should not raise."""
        _validate_command(MockCreatorOp, {"gpu": True})

    def test_unknown_command_key_raises(self):
        """Unknown command key should raise ValueError."""
        with pytest.raises(ValueError, match="Unknown command keys.*bogus"):
            _validate_command(MockCreatorOp, {"bogus": True})

    def test_curator_op_raises_on_command(self):
        """Curator ops without command field should raise on any command overrides."""
        with pytest.raises(ValueError, match="does not support command overrides"):
            _validate_command(MockCuratorOp, {"gpu": True})

    def test_empty_command_accepted(self):
        """Empty command dict should not raise (treated as no overrides)."""
        _validate_command(MockCreatorOp, {})


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
            MockOpWithParams, params=None, resources=None, execution=None, command=None
        )
        assert instance.params.count == 1
        assert instance.resources.partition == "cpu"
        assert instance.execution.artifacts_per_unit == 1
