"""Tests for worker-side tool-request execution."""

from __future__ import annotations

import json
import os
import shutil
import tarfile
import tempfile
from enum import StrEnum, auto
from io import BytesIO
from typing import Any, ClassVar
from unittest.mock import patch

import httpx
import pytest

from artisan.execution.tool_endpoint import server as server_mod
from artisan.execution.tool_endpoint import transport as transport_mod
from artisan.execution.tool_endpoint.protocol import (
    InputRef,
    StoredOutputs,
    ToolRequest,
)
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


class NoopTool(OperationDefinition):
    """Tool op that succeeds without writing any output files."""

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "noop_tool_test"
    description: ClassVar[str] = "Succeeds, writes nothing"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type="data",
            infer_lineage_from={"inputs": []},
        ),
    }

    tool: ToolSpec = ToolSpec(executable="bash", interpreter=None)

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        return [*self.tool.parts(), "-c", "true"]


def _tar_names(payload: bytes) -> list[str]:
    with tarfile.open(fileobj=BytesIO(payload), mode="r") as tar:
        return sorted(tar.getnames())


def _http_error() -> httpx.HTTPStatusError:
    """A refused presigned PUT, as upload_outputs would raise it."""
    request = httpx.Request("PUT", "https://store.example/obj")
    response = httpx.Response(403, request=request)
    return httpx.HTTPStatusError("403 Forbidden", request=request, response=response)


def _capture_tempdirs(monkeypatch) -> list[str]:
    """Record every ``mkdtemp`` path a request allocates, calling through."""
    created: list[str] = []
    real = tempfile.mkdtemp

    def spy(*args, **kwargs):
        path = real(*args, **kwargs)
        created.append(path)
        return path

    monkeypatch.setattr(tempfile, "mkdtemp", spy)
    return created


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
        assert result.manifest.stored is None  # no output_store → inline
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

    def test_invalid_params_returns_envelope(self):
        # bad params that the /submit JSON-schema gate cannot express reach
        # instantiate_op and raise pydantic ValidationError; the worker now
        # returns a structured envelope instead of propagating (500).
        result = run_tool_request(WaitTool, ToolRequest(params={"no_such_param": 1}))
        assert result.output_tar is None
        error = result.manifest.error
        assert error is not None
        assert error.code == "param_type_mismatch"
        assert error.error_type == "validation"
        assert error.recovery_hint == "CHECK_INPUT"
        assert error.operation_name == "wait_tool"

    def test_bad_input_ref_returns_envelope(self):
        # a ref carrying neither uri nor data raises ValueError in
        # unpack_inputs — the agent supplied the ref and can correct it
        result = run_tool_request(
            WaitTool, ToolRequest(inputs=[InputRef(name="dataset")])
        )
        assert result.output_tar is None
        error = result.manifest.error
        assert error is not None
        assert error.code == "input_resolution_failed"
        assert error.error_type == "io"
        assert error.recovery_hint == "CHECK_INPUT"

    def test_input_fetch_failure_returns_envelope(self, monkeypatch):
        # an unreachable input URI surfaces as OSError from ref_fs.get; the
        # (ValueError, OSError) guard maps it to the same CHECK_INPUT envelope
        def boom(self, refs, dest, fs=None):
            msg = "s3://bucket/missing.pdb"
            raise FileNotFoundError(msg)

        monkeypatch.setattr(
            server_mod.InlineTransport, "unpack_inputs", boom, raising=True
        )
        result = run_tool_request(
            WaitTool,
            ToolRequest(
                inputs=[InputRef(name="dataset", uri="s3://bucket/missing.pdb")]
            ),
        )
        error = result.manifest.error
        assert error is not None
        assert error.code == "input_resolution_failed"
        assert error.recovery_hint == "CHECK_INPUT"

    def test_input_failure_removes_job_dir(self, monkeypatch):
        # warm-container reuse: an input-resolution failure still cleans up
        created = _capture_tempdirs(monkeypatch)
        result = run_tool_request(
            WaitTool, ToolRequest(inputs=[InputRef(name="dataset")])
        )
        assert result.manifest.error is not None
        assert created  # the job dir was allocated before unpack_inputs
        assert all(not os.path.exists(p) for p in created)

    def test_success_removes_job_dir(self, monkeypatch):
        # warm-container reuse: the per-request job tree must not survive
        created = _capture_tempdirs(monkeypatch)
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
        assert created  # the worker did allocate a job dir
        assert all(not os.path.exists(p) for p in created)

    def test_tool_failure_removes_job_dir(self, monkeypatch):
        created = _capture_tempdirs(monkeypatch)
        result = run_tool_request(FailTool, ToolRequest())
        assert result.manifest.error is not None
        assert created  # the worker did allocate a job dir
        assert all(not os.path.exists(p) for p in created)

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


class TestRunToolRequestStoredOutputs:
    _REQUEST = ToolRequest(
        params={"seconds": 1},
        inputs=[InputRef(name="dataset", filename="in.csv", data=b"a,b\n1,2\n")],
        output_store="s3://bucket/prefix",
    )

    def test_output_store_uploads_and_omits_tar(self, monkeypatch):
        stored = StoredOutputs(uri="s3://bucket/p/wait_tool/x.tar.gz")
        calls: dict = {}

        def fake_upload(src: str, names: list, store: str, op_name: str):
            calls.update(names=names, store=store, op_name=op_name)
            return stored

        monkeypatch.setattr(server_mod, "upload_outputs", fake_upload)
        result = run_tool_request(WaitTool, self._REQUEST)
        assert result.manifest.error is None
        assert result.manifest.stored == stored
        assert result.output_tar is None
        assert result.manifest.output_names == ["in_waited.csv"]
        assert calls == {
            "names": ["in_waited.csv"],
            "store": "s3://bucket/prefix",
            "op_name": "wait_tool",
        }

    def test_stored_path_never_reaches_inline_cap(self, monkeypatch):
        # with the cap below any tar, pack_outputs would raise — proving
        # the stored path never invokes it (the any-size criterion)
        monkeypatch.setattr(transport_mod, "MAX_INLINE_BYTES", 1)
        monkeypatch.setattr(
            server_mod,
            "upload_outputs",
            lambda *args: StoredOutputs(uri="s3://b/x.tar.gz"),
        )
        result = run_tool_request(WaitTool, self._REQUEST)
        assert result.manifest.error is None
        assert result.output_tar is None

    def test_tool_failure_uploads_nothing(self, monkeypatch):
        def explode(*args):
            pytest.fail("upload_outputs must not run on tool failure")

        monkeypatch.setattr(server_mod, "upload_outputs", explode)
        result = run_tool_request(FailTool, ToolRequest(output_store="s3://b/p"))
        assert result.manifest.error is not None
        assert result.manifest.stored is None
        assert result.output_tar is None

    def test_no_outputs_falls_back_to_empty_inline_tar(self, monkeypatch):
        def explode(*args):
            pytest.fail("upload_outputs must not run with no outputs")

        monkeypatch.setattr(server_mod, "upload_outputs", explode)
        result = run_tool_request(NoopTool, ToolRequest(output_store="s3://b/p"))
        assert result.manifest.error is None
        assert result.manifest.output_names == []
        assert result.manifest.stored is None
        assert result.output_tar is not None
        assert _tar_names(result.output_tar) == []

    @pytest.mark.parametrize("exc", [OSError("disk full"), _http_error()])
    def test_delivery_failure_returns_envelope_with_outputs(self, monkeypatch, exc):
        # the tool ran; only delivery to the store failed — the envelope
        # surfaces the outputs it produced and asks the agent to retry
        # delivery (RETRY_LATER), not re-run the (possibly GPU) compute
        def failing_upload(*args):
            raise exc

        monkeypatch.setattr(server_mod, "upload_outputs", failing_upload)
        result = run_tool_request(WaitTool, self._REQUEST)
        error = result.manifest.error
        assert error is not None
        assert error.code == "output_delivery_failed"
        assert error.error_type == "io"
        assert error.recovery_hint == "RETRY_LATER"
        assert result.manifest.output_names == ["in_waited.csv"]  # produced
        assert result.manifest.log_tail is not None  # the tool's log survives
        assert result.manifest.stored is None
        assert result.output_tar is None

    def test_non_signing_store_returns_misconfigured(self, monkeypatch):
        # a store that cannot presign fails 100% of requests — a deployment
        # misconfiguration to report, not a transient to retry
        def cannot_presign(*args):
            msg = "filesystem cannot sign"
            raise NotImplementedError(msg)

        monkeypatch.setattr(server_mod, "upload_outputs", cannot_presign)
        result = run_tool_request(WaitTool, self._REQUEST)
        error = result.manifest.error
        assert error is not None
        assert error.code == "tool_endpoint_misconfigured"
        assert error.error_type == "config"
        assert error.recovery_hint == "REPORT_TO_USER"
        assert result.manifest.output_names == ["in_waited.csv"]
        assert result.manifest.log_tail is not None
        assert result.output_tar is None

    def test_delivery_failure_removes_job_dir(self, monkeypatch):
        created = _capture_tempdirs(monkeypatch)

        def failing_upload(*args):
            msg = "disk full"
            raise OSError(msg)

        monkeypatch.setattr(server_mod, "upload_outputs", failing_upload)
        result = run_tool_request(WaitTool, self._REQUEST)
        assert result.manifest.error is not None
        assert created  # the job dir was allocated
        assert all(not os.path.exists(p) for p in created)


class TestResolveOp:
    def test_round_trip(self):
        assert resolve_op(WaitTool.__module__, WaitTool.__qualname__) is WaitTool

    def test_non_operation_raises(self):
        with pytest.raises(TypeError, match="not an OperationDefinition"):
            resolve_op("artisan.schemas.operation_config.tool_spec", "ToolSpec")
