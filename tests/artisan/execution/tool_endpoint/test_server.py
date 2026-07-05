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
from botocore.exceptions import EndpointConnectionError, NoCredentialsError

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
from artisan.operations.base.per_artifact import PerArtifact
from artisan.operations.examples import WaitTool
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.operation_config.compute import (
    ComputeProvider,
    ModalComputeConfig,
)
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_models import PostprocessInput, PreprocessInput
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


class _CloudInputTool(OperationDefinition):
    """Endpoint-routed command op with a cloud ``large_file`` input.

    Names its output from the input file stem (``<stem>_waited.csv``) and
    declares ``infer_lineage_from`` so lineage capture ties the output back
    to the input — the shape the endpoint-routing skip must preserve.
    """

    class InputRole(StrEnum):
        source = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "cloud_input_tool_test"
    description: ClassVar[str] = "Echo a cloud input into <stem>_waited.csv"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.source: InputSpec(artifact_type="large_file", required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type="data",
            infer_lineage_from={"inputs": ["source"]},
        ),
    }

    tool: ToolSpec = ToolSpec(executable="bash", interpreter=None)
    compute_provider: ComputeProvider = ComputeProvider(
        active="modal", modal=ModalComputeConfig()
    )

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            "source": PerArtifact(
                [a.materialized_path for a in inputs.input_artifacts["source"]]
            )
        }

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        src = inputs["source"]
        return [
            *self.tool.parts(),
            "-c",
            (
                f'src="{src}"; stem="$(basename "$src")"; stem="${{stem%.*}}"; '
                f'printf "ok\\n" > "${{stem}}_waited.csv"'
            ),
        ]

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        drafts: list[Any] = []
        for file_path in inputs.file_outputs:
            if file_path.endswith(".csv"):
                with open(file_path, "rb") as f:
                    content = f.read()
                drafts.append(
                    DataArtifact.draft(
                        content=content,
                        original_name=os.path.basename(file_path),
                        step_number=inputs.step_number,
                    )
                )
        return ArtifactResult(success=True, artifacts={"output": drafts})


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


def _set_ambient_creds(storage, monkeypatch) -> None:
    """Put the MinIO creds + endpoint on the env, exactly as the Modal Secret
    would hand them to the worker, then clear the s3fs instance cache so a
    filesystem built before the env was patched is not reused."""
    import s3fs as s3fs_mod

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", storage.options["key"])
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", storage.options["secret"])
    monkeypatch.setenv(
        "AWS_ENDPOINT_URL", storage.options["client_kwargs"]["endpoint_url"]
    )
    s3fs_mod.S3FileSystem.clear_instance_cache()


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

    @pytest.mark.parametrize(
        "exc",
        [
            NoCredentialsError(),
            EndpointConnectionError(endpoint_url="https://typo.example"),
        ],
        ids=["no_credentials", "bad_endpoint"],
    )
    def test_botocore_root_fetch_failure_returns_envelope(self, monkeypatch, exc):
        # The load-bearing guard-tuple regression: the two R2-shaped
        # misconfigurations raise botocore roots s3fs returns untranslated
        # (NoCredentialsError, EndpointConnectionError — both BotoCoreError,
        # neither an OSError). They must land on INPUT_RESOLUTION_FAILED,
        # NOT escape as a worker crash → OP_EXECUTE_FAILED. This test fails
        # under the draft's (ValueError, OSError, RuntimeError) tuple.
        def boom(self, refs, dest, fs=None):
            raise exc

        monkeypatch.setattr(
            server_mod.InlineTransport, "unpack_inputs", boom, raising=True
        )
        result = run_tool_request(
            WaitTool,
            ToolRequest(inputs=[InputRef(name="dataset", uri="s3://bucket/x.csv")]),
        )
        assert result.output_tar is None
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


class TestRunToolRequestUriInputMinIO:
    """Worker resolves a cloud URI input against MinIO — the URI path the
    endpoint-routing skip newly feeds (s3 marker via ``s3_fs``)."""

    def test_uri_input_resolves_and_runs_below_inline_cap(self, s3_fs, monkeypatch):
        fs, storage, uri_prefix = s3_fs
        bucket = uri_prefix.removeprefix("s3://")
        # a 50 KB input; the tiny WaitTool output tars to one 10 KB record
        fs.pipe_file(f"{bucket}/inputs/dataset_00001.csv", b"x" * 50_000)
        _set_ambient_creds(storage, monkeypatch)
        # cap between the output tar (~10 KB) and the input (50 KB): the
        # worker never checks the inline cap on the URI input path
        # (pack_inputs is client-side) — a 50 KB input past a 20 KB cap
        # still resolves and runs
        monkeypatch.setattr(transport_mod, "MAX_INLINE_BYTES", 20_000)

        result = run_tool_request(
            WaitTool,
            ToolRequest(
                params={"seconds": 1},
                inputs=[
                    InputRef(
                        name="dataset",
                        filename="dataset_00001.csv",
                        uri=f"{uri_prefix}/inputs/dataset_00001.csv",
                    )
                ],
            ),
        )

        assert result.manifest.error is None
        assert result.manifest.output_names == ["dataset_00001_waited.csv"]
        assert result.output_tar is not None


class _WorkerLoopbackRouter:
    """Drive the endpoint hop in-process: the client-side transport of
    ``call_endpoint`` (``pack_inputs``) → the worker (``run_tool_request``,
    which fetches ``s3://`` refs) → ``unpack_outputs`` into ``execute_dir``.

    The HTTP/Modal hop is the only thing elided — the byte route (cloud
    input by reference, tar output) is the real production code.
    """

    def __init__(self, op_cls: type[OperationDefinition]) -> None:
        self._op_cls = op_cls

    def route_execute(self, operation, execute_inputs, sandbox_root):
        from artisan.execution.tool_endpoint.client import _file_inputs
        from artisan.execution.tool_endpoint.transport import InlineTransport

        params = json.loads(operation.params_json())
        results: list[Any] = []
        for execute_input in execute_inputs:
            files = _file_inputs(operation.name, execute_input.inputs)
            refs = InlineTransport().pack_inputs(files)
            result = run_tool_request(
                self._op_cls, ToolRequest(params=params, inputs=refs)
            )
            if result.manifest.error is not None:
                results.append(RuntimeError(result.manifest.error.message))
                continue
            InlineTransport().unpack_outputs(
                result.output_tar, execute_input.execute_dir
            )
            results.append(None)
        return results


class TestEndpointRoutedLineageMinIO:
    """Lineage ship gate: a real cloud ``LargeFileArtifact`` crosses by
    reference into an endpoint op; the input→output edge and the output's
    human-readable name must survive the natural-basename route (s3 marker
    via ``s3_fs``)."""

    def test_edge_and_name_preserved_under_skip(self, s3_fs, tmp_path, monkeypatch):
        import polars as pl

        from artisan.execution.executors.creator import run_creator_lifecycle
        from artisan.execution.models.execution_unit import ExecutionUnit
        from artisan.schemas.artifact.large_file import LargeFileArtifact
        from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
        from artisan.storage.core.table_schemas import ARTIFACT_INDEX_SCHEMA

        fs, storage, uri_prefix = s3_fs
        bucket = uri_prefix.removeprefix("s3://")
        # the cloud input: bytes in MinIO, metadata in a local Delta store.
        # The files_root object name preserves original_name, so the worker
        # materializes under a basename whose stem is the artifact's name.
        external_path = f"{uri_prefix}/files/dataset_00001.bin"
        fs.pipe_file(f"{bucket}/files/dataset_00001.bin", b"weights")
        art = LargeFileArtifact.draft(
            content_hash="c" * 32,
            size_bytes=7,
            step_number=0,
            external_path=external_path,
            original_name="dataset_00001",
            extension=".bin",
        ).finalize()

        base = tmp_path / "delta"
        pl.DataFrame(
            [art.to_row()], schema=LargeFileArtifact.POLARS_SCHEMA
        ).write_delta(str(base / "artifacts/large_files"))
        pl.DataFrame(
            [
                {
                    "artifact_id": art.artifact_id,
                    "artifact_type": "large_file",
                    "origin_step_number": 0,
                    "metadata": "{}",
                }
            ],
            schema=ARTIFACT_INDEX_SCHEMA,
        ).write_delta(str(base / "artifacts/index"))

        working = tmp_path / "working"
        working.mkdir()
        staging = tmp_path / "staging"
        staging.mkdir()
        runtime_env = RuntimeEnvironment(
            delta_root=str(base),
            working_root=str(working),
            staging_root=str(staging),
        )
        # ambient creds so the worker fetches external_path from MinIO
        _set_ambient_creds(storage, monkeypatch)

        unit = ExecutionUnit(
            operation=_CloudInputTool(),
            inputs={"source": [art.artifact_id]},
            execution_spec_id="spec_lg" + "0" * 26,
            step_number=1,
        )
        result = run_creator_lifecycle(
            unit, runtime_env, execute_router=_WorkerLoopbackRouter(_CloudInputTool)
        )

        outputs = result.artifacts["output"]
        assert len(outputs) == 1
        out = outputs[0]
        # human-readable name from the natural basename — no artifact_id
        # prefix to strip (derive_human_names is a no-op with the empty map)
        assert out.original_name == "dataset_00001_waited"
        assert not out.original_name.startswith(art.artifact_id)
        # input→output edge captured via the original_name stem fallback,
        # even though the filesystem match map is empty under the skip
        edges = {(e.source_artifact_id, e.target_artifact_id) for e in result.edges}
        assert (art.artifact_id, out.artifact_id) in edges


class TestResolveOp:
    def test_round_trip(self):
        assert resolve_op(WaitTool.__module__, WaitTool.__qualname__) is WaitTool

    def test_non_operation_raises(self):
        with pytest.raises(TypeError, match="not an OperationDefinition"):
            resolve_op("artisan.schemas.operation_config.tool_spec", "ToolSpec")
