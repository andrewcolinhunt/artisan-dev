"""Tests for the shared invocation primitive ``invoke_op_work``."""

from __future__ import annotations

from enum import StrEnum, auto
from typing import Any, ClassVar
from unittest.mock import MagicMock, patch

import pytest

from artisan.execution.compute.invoke import invoke_op_work, tool_command_inputs
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_models import ExecuteInput
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class _EchoTool(OperationDefinition):
    """Command op writing its message to a marker file via bash."""

    class OutputRole(StrEnum):
        result = auto()

    name: ClassVar[str] = "invoke_echo_tool_test"
    description: ClassVar[str] = "Writes a marker file via bash"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.result: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            infer_lineage_from={"inputs": []},
        ),
    }

    tool: ToolSpec = ToolSpec(executable="bash", interpreter=None)
    message: str = "hello"

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        return [*self.tool.parts(), "-c", f'echo "{self.message}" | tee marker.txt']


class _FunctionOp(OperationDefinition):
    """Function op returning a value derived from its inputs."""

    class OutputRole(StrEnum):
        result = auto()

    name: ClassVar[str] = "invoke_function_op_test"
    description: ClassVar[str] = "Returns its prepared inputs"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.result: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            infer_lineage_from={"inputs": []},
        ),
    }

    def execute_function(self, inputs: ExecuteInput) -> Any:
        return {"echo": inputs.inputs}


class TestToolCommandInputs:
    """Single-element-list unwrapping for ``execute_command``."""

    def test_unwraps_single_element_lists(self):
        assert tool_command_inputs({"dataset": ["/a.csv"]}) == {"dataset": "/a.csv"}

    def test_preserves_multi_element_lists(self):
        assert tool_command_inputs({"many": ["/a", "/b"]}) == {"many": ["/a", "/b"]}

    def test_passes_plain_values_through(self):
        assert tool_command_inputs({"plain": "/a"}) == {"plain": "/a"}


class TestInvokeCommandOp:
    """Command form: argv built by the op, performed by the primitive."""

    def test_runs_tool_and_returns_none(self, tmp_path):
        """The tool runs in execute_dir with the unit log captured."""
        op = _EchoTool(message="hi")
        result = invoke_op_work(
            op,
            ExecuteInput(
                execute_dir=str(tmp_path),
                log_path=str(tmp_path / "tool_output.log"),
            ),
        )
        assert result is None
        assert (tmp_path / "marker.txt").read_text().strip() == "hi"
        assert "hi" in (tmp_path / "tool_output.log").read_text()

    def test_execute_command_receives_unwrapped_inputs(self, tmp_path):
        """Per-artifact one-element lists are unwrapped before the slot."""
        seen: list[dict[str, Any]] = []

        class _Spy(_EchoTool):
            def execute_command(self, inputs: dict[str, Any]) -> list[str]:
                seen.append(inputs)
                return [*self.tool.parts(), "-c", "true"]

        invoke_op_work(
            _Spy(),
            ExecuteInput(inputs={"dataset": ["/one.csv"]}, execute_dir=str(tmp_path)),
        )
        assert seen == [{"dataset": "/one.csv"}]

    def test_sequential_invocations_accumulate_in_one_log(self, tmp_path):
        """log_mode='a': a unit's per-artifact runs share one unit log."""
        log_path = str(tmp_path / "tool_output.log")
        for message in ("first", "second"):
            invoke_op_work(
                _EchoTool(message=message),
                ExecuteInput(execute_dir=str(tmp_path), log_path=log_path),
            )
        log = (tmp_path / "tool_output.log").read_text()
        assert "first" in log
        assert "second" in log

    def test_run_command_called_with_env_cwd_log_stream(self, tmp_path):
        """The primitive threads environment, cwd, log, and streaming through."""
        op = _EchoTool()
        execute_input = ExecuteInput(
            execute_dir=str(tmp_path),
            log_path=str(tmp_path / "tool_output.log"),
        )
        environment = LocalEnvironmentSpec()
        with patch("artisan.execution.compute.invoke.run_command") as mock_run:
            invoke_op_work(
                op, execute_input, environment=environment, stream_output=True
            )
        mock_run.assert_called_once_with(
            environment,
            op.execute_command({}),
            cwd=str(tmp_path),
            log_path=execute_input.log_path,
            log_mode="a",
            stream_output=True,
        )

    def test_defaults_to_active_environment(self, tmp_path):
        """Without an override, the op's active environment wraps the tool."""
        op = _EchoTool()
        with patch("artisan.execution.compute.invoke.run_command") as mock_run:
            invoke_op_work(op, ExecuteInput(execute_dir=str(tmp_path)))
        assert mock_run.call_args.args[0] == op.environments.current()


class TestInvokeFunctionOp:
    """Function form: the slot body runs in-process."""

    def test_passthrough_return_value(self, tmp_path):
        op = _FunctionOp()
        execute_input = ExecuteInput(inputs={"key": "value"}, execute_dir=str(tmp_path))
        assert invoke_op_work(op, execute_input) == {"echo": {"key": "value"}}

    def test_exception_propagates(self, tmp_path):
        operation = MagicMock()
        operation.is_command_op.return_value = False
        operation.execute_function.side_effect = RuntimeError("boom")
        with pytest.raises(RuntimeError, match="boom"):
            invoke_op_work(operation, ExecuteInput(execute_dir=str(tmp_path)))
