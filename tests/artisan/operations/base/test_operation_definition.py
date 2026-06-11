"""Tests for the unified OperationDefinition model."""

from __future__ import annotations

import json
from enum import StrEnum, auto
from typing import Any, ClassVar

import pytest
from pydantic import BaseModel, Field, ValidationError

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.operation_config.compute import (
    ComputeProvider,
    ModalComputeConfig,
)
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_models import ExecuteInput
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

    def execute_function(self, inputs: dict[str, Any], output_dir):
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

    def execute_function(self, inputs: dict[str, Any], output_dir):
        return ArtifactResult(success=True)


class ShellTool(OperationDefinition):
    """Tool op for testing the framework execute_function()."""

    class OutputRole(StrEnum):
        result = auto()

    name: ClassVar[str] = "shell_tool_test"
    description: ClassVar[str] = "Writes a marker file via bash"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.result: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            infer_lineage_from={"inputs": []},
        ),
    }

    tool: ToolSpec = ToolSpec(executable="bash", interpreter=None)
    message: str = Field(default="hello")

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        return [*self.tool.parts(), "-c", f'echo "{self.message}" > marker.txt']


class FlagOp(OperationDefinition):
    """execute_as_tool fixture: a Python body shipped as a framework command."""

    class OutputRole(StrEnum):
        result = auto()

    name: ClassVar[str] = "flag_op_def_test"
    description: ClassVar[str] = "Writes a marker file via the op-run shim"
    execute_as_tool: ClassVar[bool] = True
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.result: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            infer_lineage_from={"inputs": []},
        ),
    }

    class Params(BaseModel):
        suffix: str = Field(default="out", description="Output filename suffix.")

    params: Params = Params()

    def execute_function(self, inputs: ExecuteInput) -> None:
        return None


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
        result = op.execute_function(inputs={}, output_dir=tmp_path)

        assert result.success is True
        assert result.metadata["count"] == 5

    def test_should_access_params_via_self(self, tmp_path):
        """Should be able to access params via self in execute."""
        op = SimpleOperation(count=10, label="test")
        result = op.execute_function(inputs={}, output_dir=tmp_path)

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
        assert "runner_resources" in kwargs
        assert "batch_strategy" in kwargs

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
        """Operations get default RunnerResources from base class."""
        op = SimpleOperation()
        assert isinstance(op.runner_resources, RunnerResources)
        assert op.runner_resources.cpus == 1
        assert op.runner_resources.memory_gb == 4
        assert op.runner_resources.gpus == 0

    def test_default_execution(self):
        """Operations get default BatchStrategy from base class."""
        op = SimpleOperation()
        assert isinstance(op.batch_strategy, BatchStrategy)
        assert op.batch_strategy.artifacts_per_unit == 1
        assert op.batch_strategy.units_per_worker == 1
        assert op.batch_strategy.job_name is None

    def test_resources_not_in_model_dump_exclude(self):
        """resources and execution are instance fields on the base class."""
        op = SimpleOperation()
        dump = op.model_dump()
        # resources and execution are present in model_dump (they are instance fields)
        assert "runner_resources" in dump
        assert "batch_strategy" in dump

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
        assert op.runner_resources.cpus == 1
        assert op.batch_strategy.job_name == "data_transformer_script"


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

                def execute_function(self, inputs, output_dir):
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

                def execute_function(self, inputs, output_dir):
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

                def execute_function(self, inputs, output_dir):
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

                def execute_function(self, inputs, output_dir):
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

    def test_fixed_inputs_with_input_role_ok(self):
        """Fixed inputs + InputRole is valid (Filter pattern)."""
        from artisan.operations.curator.filter import Filter

        assert Filter.runtime_defined_inputs is False
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


class TestToolOps:
    """Tool ops: ToolSpec + execute_command() in place of execute_function()."""

    def test_tool_op_passes_subclass_validation(self):
        """A ToolSpec + execute_command() satisfies the must-implement check."""
        assert "shell_tool_test" in OperationDefinition.get_all()
        assert ShellTool._kind() == "creator"

    def test_build_command_without_tool_raises(self):
        """execute_command() without a ToolSpec fails at class definition."""
        with pytest.raises(TypeError, match="declares no ToolSpec"):

            class NoToolSpec(OperationDefinition):
                name: ClassVar[str] = "no_tool_spec_test"
                inputs: ClassVar[dict[str, InputSpec]] = {}
                outputs: ClassVar[dict[str, OutputSpec]] = {}

                def execute_command(self, inputs: dict[str, Any]) -> list[str]:
                    return ["true"]

    def test_neither_execute_nor_tool_raises(self):
        """No execute, no execute_curator, no tool command fails."""
        with pytest.raises(TypeError, match="must implement execute"):

            class Neither(OperationDefinition):
                name: ClassVar[str] = "neither_test"
                inputs: ClassVar[dict[str, InputSpec]] = {}
                outputs: ClassVar[dict[str, OutputSpec]] = {}

    def test_modal_default_requires_tool_op(self):
        """A class defaulting to modal must declare a command op."""
        with pytest.raises(TypeError, match="modal requires a command op"):

            class ModalPurePython(OperationDefinition):
                name: ClassVar[str] = "modal_pure_python_test"
                inputs: ClassVar[dict[str, InputSpec]] = {}
                outputs: ClassVar[dict[str, OutputSpec]] = {}
                compute_provider: ComputeProvider = ComputeProvider(
                    active="modal", modal=ModalComputeConfig()
                )

                def execute_function(self, inputs):
                    return None

    def test_execute_function_stub_raises_for_command_op(self):
        """Slots are empty override points — command ops don't inherit a body."""
        op = ShellTool(message="hi")
        with pytest.raises(NotImplementedError, match="execute_function"):
            op.execute_function(ExecuteInput(execute_dir="/tmp"))

    def test_unnamed_non_tool_base_execute_raises(self):
        """Base execute_function() still raises for abstract non-tool subclasses."""

        class AbstractOp(OperationDefinition):
            pass  # no name — skips registration and validation

        with pytest.raises(NotImplementedError, match="execute_function"):
            AbstractOp().execute_function(ExecuteInput(execute_dir="/tmp"))

    def test_build_command_stub_raises(self):
        """Base execute_command() raises for non-tool subclasses."""

        class AbstractOp2(OperationDefinition):
            pass

        with pytest.raises(NotImplementedError, match="execute_command"):
            AbstractOp2().execute_command({})


class TestExactlyOneSlot:
    """A concrete op must fill exactly one execute slot."""

    def test_function_plus_command_raises(self):
        """A function body plus a command builder is dead code — TypeError."""
        with pytest.raises(TypeError, match="exactly one"):

            class FunctionAndCommand(OperationDefinition):
                name: ClassVar[str] = "function_and_command_test"
                inputs: ClassVar[dict[str, InputSpec]] = {}
                outputs: ClassVar[dict[str, OutputSpec]] = {}
                tool: ToolSpec = ToolSpec(executable="bash", interpreter=None)

                def execute_function(self, inputs):
                    return None

                def execute_command(self, inputs: dict[str, Any]) -> list[str]:
                    return ["true"]

    def test_function_plus_curator_raises(self):
        """A creator body plus a curator body is two slots — TypeError."""
        with pytest.raises(TypeError, match="exactly one"):

            class FunctionAndCurator(OperationDefinition):
                name: ClassVar[str] = "function_and_curator_test"
                inputs: ClassVar[dict[str, InputSpec]] = {}
                outputs: ClassVar[dict[str, OutputSpec]] = {}

                def execute_function(self, inputs):
                    return None

                def execute_curator(self, inputs, step_number, artifact_store):
                    return None

    def test_each_single_slot_is_valid(self):
        """One slot per op passes: function (SimpleOperation), command
        (ShellTool), curator (framework Filter op)."""
        from artisan.operations.curator.filter import Filter

        assert SimpleOperation._kind() == "creator"
        assert ShellTool._kind() == "creator"
        assert Filter._kind() == "curator"


class TestKindDerivation:
    """``_kind`` classifies ops by whether ``execute_curator`` is overridden."""

    def test_creator_when_only_execute_overridden(self) -> None:
        from artisan.operations.examples.data_generator import DataGenerator
        from artisan.operations.examples.data_transformer import DataTransformer

        assert DataTransformer._kind() == "creator"
        assert DataGenerator._kind() == "creator"

    def test_curator_when_execute_curator_overridden(self) -> None:
        from artisan.operations.curator.filter import Filter
        from artisan.operations.curator.merge import Merge

        assert Filter._kind() == "curator"
        assert Merge._kind() == "curator"


class TestIntrospectionPayloads:
    """``to_summary`` and ``to_metadata`` shape the agent-facing payloads."""

    def test_to_summary_carries_roles_and_kind(self) -> None:
        from artisan.operations.examples.data_transformer import DataTransformer

        summary = DataTransformer.to_summary()
        assert summary.name == "data_transformer"
        assert summary.kind == "creator"
        assert summary.input_roles == ["dataset"]
        assert summary.output_roles == ["dataset"]
        assert summary.schema_version == "1"

    def test_to_metadata_includes_params_schema_and_source_module(self) -> None:
        from artisan.operations.examples.data_transformer import DataTransformer

        meta = DataTransformer.to_metadata()
        assert meta.source_module.startswith("artisan.operations.examples")
        assert "scale_factor" in meta.params_schema["properties"]
        assert meta.inputs["dataset"].artifact_type == "data"
        assert meta.inputs["dataset"].materialize is True
        assert meta.outputs["dataset"].artifact_type == "data"

    def test_to_metadata_parameter_less_op_yields_empty_params_schema(self) -> None:
        from artisan.operations.curator.merge import Merge

        meta = Merge.to_metadata()
        assert meta.kind == "curator"
        assert meta.params_schema == {
            "type": "object",
            "title": "Params",
            "properties": {},
        }


class TestExecuteAsTool:
    """execute_as_tool: a function op shipped as a framework command."""

    def test_flag_op_passes_subclass_validation(self):
        """The flag satisfies the must-implement check and the predicates."""
        assert "flag_op_def_test" in OperationDefinition.get_all()
        assert FlagOp._kind() == "creator"
        assert FlagOp().is_command_op()

    def test_flag_without_execute_function_raises(self):
        with pytest.raises(TypeError, match="does not implement execute_function"):

            class NoBody(OperationDefinition):
                name: ClassVar[str] = "flag_no_body_test"
                execute_as_tool: ClassVar[bool] = True
                inputs: ClassVar[dict[str, InputSpec]] = {}
                outputs: ClassVar[dict[str, OutputSpec]] = {}

    def test_flag_with_execute_command_raises(self):
        with pytest.raises(TypeError, match="framework supplies the command"):

            class FlagAndCommand(OperationDefinition):
                name: ClassVar[str] = "flag_and_command_test"
                execute_as_tool: ClassVar[bool] = True
                inputs: ClassVar[dict[str, InputSpec]] = {}
                outputs: ClassVar[dict[str, OutputSpec]] = {}

                def execute_function(self, inputs):
                    return None

                def execute_command(self, inputs: dict[str, Any]) -> list[str]:
                    return ["true"]

    def test_flag_with_tool_raises(self):
        with pytest.raises(TypeError, match="needs no ToolSpec"):

            class FlagAndTool(OperationDefinition):
                name: ClassVar[str] = "flag_and_tool_test"
                execute_as_tool: ClassVar[bool] = True
                inputs: ClassVar[dict[str, InputSpec]] = {}
                outputs: ClassVar[dict[str, OutputSpec]] = {}
                tool: ToolSpec = ToolSpec(executable="bash", interpreter=None)

                def execute_function(self, inputs):
                    return None

    def test_flag_with_top_level_field_raises(self):
        """Top-level config never crosses the wire — params-only rule."""
        with pytest.raises(TypeError, match="move per-run config"):

            class FlagTopLevel(OperationDefinition):
                name: ClassVar[str] = "flag_top_level_test"
                execute_as_tool: ClassVar[bool] = True
                inputs: ClassVar[dict[str, InputSpec]] = {}
                outputs: ClassVar[dict[str, OutputSpec]] = {}
                rate: float = 1.0

                def execute_function(self, inputs):
                    return None

    def test_flag_params_not_nested_class_raises(self):
        """The worker rebuilds via getattr(op_cls, 'Params') — enforce it."""

        class LooseConfig(BaseModel):
            rate: float = Field(default=1.0, description="Rate.")

        with pytest.raises(TypeError, match="nested Params class"):

            class FlagLooseParams(OperationDefinition):
                name: ClassVar[str] = "flag_loose_params_test"
                execute_as_tool: ClassVar[bool] = True
                inputs: ClassVar[dict[str, InputSpec]] = {}
                outputs: ClassVar[dict[str, OutputSpec]] = {}
                params: LooseConfig = LooseConfig()

                def execute_function(self, inputs):
                    return None

    def test_flag_defined_in_main_raises(self):
        """__main__ classes can't be resolved by the op-run subprocess."""
        with pytest.raises(TypeError, match="__main__"):
            type(
                "MainFlagOp",
                (OperationDefinition,),
                {
                    "__module__": "__main__",
                    "name": "flag_main_module_test",
                    "execute_as_tool": True,
                    "inputs": {},
                    "outputs": {},
                    "execute_function": lambda self, inputs: None,
                },
            )

    def test_flag_modal_default_accepted(self):
        """A flag-op may default to modal — it is a command op."""

        class FlagModal(OperationDefinition):
            class OutputRole(StrEnum):
                result = auto()

            name: ClassVar[str] = "flag_modal_default_test"
            execute_as_tool: ClassVar[bool] = True
            inputs: ClassVar[dict[str, InputSpec]] = {}
            outputs: ClassVar[dict[str, OutputSpec]] = {
                OutputRole.result: OutputSpec(
                    artifact_type=ArtifactTypes.DATA,
                    infer_lineage_from={"inputs": []},
                ),
            }
            compute_provider: ComputeProvider = ComputeProvider(
                active="modal", modal=ModalComputeConfig()
            )

            def execute_function(self, inputs):
                return None

        assert FlagModal.declares_command_execute()

    def test_declares_command_execute_truth_table(self):
        """Flag op and external tool op are command ops; function op is not."""
        assert FlagOp.declares_command_execute()
        assert ShellTool.declares_command_execute()
        assert not SimpleOperation.declares_command_execute()

    def test_params_json_round_trips_nested_params(self):
        op = FlagOp(params=FlagOp.Params(suffix="embedded"))
        assert json.loads(op.params_json()) == {"suffix": "embedded"}

    def test_params_json_without_params_model(self):
        assert ShellTool().params_json() == "{}"

    def test_shim_argv_carries_target_params_and_inputs(self):
        """The base execute_command returns the generic op-run argv."""
        op = FlagOp(params=FlagOp.Params(suffix="x"))
        argv = op.execute_command({"source": ["/tmp/a.csv"]})

        assert argv[:3] == ["artisan", "op", "run"]
        assert argv[3] == f"{FlagOp.__module__}:FlagOp"
        assert argv[4] == "--params"
        assert json.loads(argv[5]) == {"suffix": "x"}
        assert argv[6] == "--inputs"
        assert json.loads(argv[7]) == {"source": ["/tmp/a.csv"]}

    def test_shim_argv_non_serializable_input_names_role(self):
        with pytest.raises(TypeError, match="source"):
            FlagOp().execute_command({"source": object()})
