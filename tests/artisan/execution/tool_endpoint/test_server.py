"""Tests for worker-side tool-request execution."""

from __future__ import annotations

import json
import shutil
import tarfile
from enum import StrEnum, auto
from io import BytesIO
from typing import Any, ClassVar
from unittest.mock import patch

import pytest

from artisan.execution.tool_endpoint.protocol import InputRef, ToolRequest
from artisan.execution.tool_endpoint.server import (
    resolve_op,
    run_tool_request,
)
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.examples import WaitTool
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

# The worker streams tool output (stream_output=True); the filters swallow
# a pre-existing pipe-cleanup quirk in _run_with_streaming (Popen.stdout
# closed by GC, not explicitly) — benign, same as test_streaming_echo.
pytestmark = [
    pytest.mark.skipif(shutil.which("bash") is None, reason="bash not on PATH"),
    pytest.mark.filterwarnings("ignore::ResourceWarning"),
    pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning"),
]


class FailTool(OperationDefinition):
    """Tool op whose command always exits non-zero."""

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "fail_tool_test"
    description: ClassVar[str] = "Always fails"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type="data",
            infer_lineage_from={"inputs": []},
        ),
    }

    tool: ToolSpec = ToolSpec(executable="bash", interpreter=None)

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        return [*self.tool.parts(), "-c", "echo boom >&2; exit 3"]


class CatTool(OperationDefinition):
    """Tool op that copies its input file to an output file."""

    class InputRole(StrEnum):
        source = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "cat_tool_test"
    description: ClassVar[str] = "Copies its input to copied.txt"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.source: InputSpec(artifact_type="data", required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type="data",
            infer_lineage_from={"inputs": ["source"]},
        ),
    }

    tool: ToolSpec = ToolSpec(executable="bash", interpreter=None)

    def preprocess(self, inputs):  # pragma: no cover - not exercised here
        return {}

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        return [*self.tool.parts(), "-c", f'cat "{inputs["source"]}" > copied.txt']


def _tar_names(payload: bytes) -> list[str]:
    with tarfile.open(fileobj=BytesIO(payload), mode="r") as tar:
        return sorted(tar.getnames())


class TestRunToolRequest:
    def test_success_returns_manifest_and_tar(self):
        result = run_tool_request(
            WaitTool,
            ToolRequest(
                params={"seconds": 1},
                inputs=[
                    InputRef(name="dataset", filename="in.csv", data=b"a,b\n1,2\n")
                ],
            ),
        )
        assert result.manifest.error is None
        assert result.manifest.output_names == ["in_waited.csv"]
        # the log travels as log_tail, never on the data plane — locally
        # it lives outside execute_dir, so the tar must not leak it in
        assert result.manifest.log_tail is not None
        assert "tick 1 / 1" in result.manifest.log_tail
        assert result.output_tar is not None
        assert _tar_names(result.output_tar) == ["in_waited.csv"]

    def test_inputs_resolved_outside_outputs(self, tmp_path):
        src = tmp_path / "in.txt"
        src.write_text("payload")
        result = run_tool_request(
            CatTool,
            ToolRequest(inputs=[InputRef(name="source", data=src.read_bytes())]),
        )
        assert result.manifest.error is None
        # the input file must not be swept into the output tar
        assert "source" not in result.manifest.output_names
        assert "copied.txt" in result.manifest.output_names

    def test_tool_failure_returns_envelope(self):
        result = run_tool_request(FailTool, ToolRequest())
        assert result.output_tar is None
        error = result.manifest.error
        assert error is not None
        assert error.code == "op_execute_failed"
        assert error.error_type == "compute"
        assert error.operation_name == "fail_tool_test"
        assert "exit code 3" in error.message
        assert "boom" in error.message  # stderr tail rides the envelope
        assert result.manifest.log_tail is not None

    def test_invalid_params_raise(self):
        with pytest.raises(Exception, match="(?i)extra"):
            run_tool_request(WaitTool, ToolRequest(params={"no_such_param": 1}))

    def test_flag_op_spawns_op_run_with_wire_params_and_inputs(self):
        """The worker path composes for execute_as_tool ops: instantiate
        from wire params, then spawn the generated op-run argv whose
        --inputs carry the protocol's bare-str paths (the runner re-wraps
        them — the str→[str] leg of the local/remote symmetry)."""
        from fixtures.endpoint_ops import FlagTool

        with patch("artisan.execution.compute.invoke.run_command") as mock_run:
            result = run_tool_request(
                FlagTool,
                ToolRequest(
                    params={"batch_size": 2},
                    inputs=[InputRef(name="source", data=b"x")],
                ),
            )

        assert result.manifest.error is None
        argv = mock_run.call_args.args[1]
        assert argv[:3] == ["artisan", "op", "run"]
        assert argv[3] == "fixtures.endpoint_ops:FlagTool"
        assert json.loads(argv[argv.index("--params") + 1]) == {"batch_size": 2}
        inputs = json.loads(argv[argv.index("--inputs") + 1])
        assert isinstance(inputs["source"], str)  # wire shape: one file per role
        assert inputs["source"].endswith("source")


class TestResolveOp:
    def test_round_trip(self):
        assert resolve_op(WaitTool.__module__, WaitTool.__qualname__) is WaitTool

    def test_non_operation_raises(self):
        with pytest.raises(TypeError, match="not an OperationDefinition"):
            resolve_op("artisan.schemas.operation_config.tool_spec", "ToolSpec")
