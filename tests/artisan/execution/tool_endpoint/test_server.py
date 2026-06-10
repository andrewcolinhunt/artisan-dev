"""Tests for worker-side tool-request execution."""

from __future__ import annotations

import shutil
import tarfile
from enum import StrEnum, auto
from io import BytesIO
from typing import Any, ClassVar

import pytest

from artisan.execution.tool_endpoint.protocol import InputRef, ToolRequest
from artisan.execution.tool_endpoint.server import (
    resolve_op,
    run_tool_request,
)
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.examples import EchoTool
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="bash not on PATH")


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

    def build_command(self, inputs: dict[str, Any]) -> list[str]:
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

    def build_command(self, inputs: dict[str, Any]) -> list[str]:
        return [*self.tool.parts(), "-c", f'cat "{inputs["source"]}" > copied.txt']


def _tar_names(payload: bytes) -> list[str]:
    with tarfile.open(fileobj=BytesIO(payload), mode="r") as tar:
        return sorted(tar.getnames())


class TestRunToolRequest:
    def test_success_returns_manifest_and_tar(self):
        result = run_tool_request(
            EchoTool, ToolRequest(params={"text": "yo", "filename": "f.txt"})
        )
        assert result.manifest.error is None
        assert "f.txt" in result.manifest.output_names
        assert "tool_output.log" in result.manifest.output_names
        assert result.output_tar is not None
        assert _tar_names(result.output_tar) == sorted(result.manifest.output_names)

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
            run_tool_request(EchoTool, ToolRequest(params={"no_such_param": 1}))


class TestResolveOp:
    def test_round_trip(self):
        assert resolve_op(EchoTool.__module__, EchoTool.__qualname__) is EchoTool

    def test_non_operation_raises(self):
        with pytest.raises(TypeError, match="not an OperationDefinition"):
            resolve_op("artisan.schemas.operation_config.tool_spec", "ToolSpec")
