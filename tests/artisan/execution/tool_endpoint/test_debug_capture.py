"""Remote replay evidence survives compute, transport, and archive failures."""

from __future__ import annotations

import io
import os
import tarfile
from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import MagicMock

import httpx
import pytest
from fixtures.endpoint_ops import PlainTool
from pydantic import BaseModel, Field, ValidationError

from artisan.errors import ArtisanError, ErrorCode
from artisan.execution.compute.endpoint import EndpointExecuteRouter
from artisan.execution.recording.replay_snapshot import ReplayBuilder
from artisan.execution.tool_endpoint import client as client_mod
from artisan.execution.tool_endpoint import server as server_mod
from artisan.execution.tool_endpoint import transport as transport_mod
from artisan.execution.tool_endpoint.client import call_endpoint, endpoint_dispatch
from artisan.execution.tool_endpoint.protocol import (
    DebugCaptureManifest,
    InputRef,
    SchemaResponse,
    StoredOutputs,
    ToolManifest,
    ToolRequest,
    WorkerResult,
)
from artisan.execution.tool_endpoint.server import run_tool_request
from artisan.execution.tool_endpoint.transport import InlineTransport
from artisan.registry.resolve import operation_identity
from artisan.schemas.execution.command_record import CommandRecording
from artisan.schemas.execution.replay import (
    RemoteObservation,
    ReplayDiagnostic,
    ReplaySnapshot,
)
from artisan.schemas.operation_config.compute import ComputeProvider, ModalComputeConfig
from artisan.schemas.operation_config.endpoint_policy import ToolEndpointDataPolicy
from artisan.schemas.specs.input_models import ExecuteInput


class DiagnosticTool(PlainTool):
    """Produce intermediate output and a full log before optionally failing."""

    name: ClassVar[str] = "diagnostic_test"

    class Params(BaseModel):
        fail: bool = Field(
            default=False, description="Fail after writing intermediates."
        )

    params: Params = Params()

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        return [
            "bash",
            "-c",
            "printf partial > partial.txt; printf 'full job log\\n'; "
            + ("exit 9" if self.params.fail else "true"),
        ]


def _archive_files(payload: bytes) -> dict[str, bytes]:
    with tarfile.open(fileobj=io.BytesIO(payload)) as archive:
        return {
            item.name: archive.extractfile(item).read()
            for item in archive.getmembers()
            if item.isfile()
        }


@pytest.mark.parametrize("fail", [False, True])
def test_capture_keeps_inputs_outputs_and_full_log_before_cleanup(fail, monkeypatch):
    roots = []
    original = server_mod.tempfile.mkdtemp

    def remember(*args, **kwargs):
        root = original(*args, **kwargs)
        roots.append(root)
        return root

    monkeypatch.setattr(server_mod.tempfile, "mkdtemp", remember)
    result = run_tool_request(
        DiagnosticTool,
        ToolRequest(
            params={"fail": fail},
            debug_capture=True,
            inputs=[InputRef(name="source", filename="source.txt", data=b"source")],
        ),
    )
    assert (result.manifest.error is not None) is fail
    assert result.manifest.operation_identity == operation_identity(DiagnosticTool)
    capture = result.manifest.debug_capture
    assert capture.status == "complete"
    files = _archive_files(result.debug_tar)
    assert files["inputs/source/source.txt"] == b"source"
    assert files["outputs/partial.txt"] == b"partial"
    assert b"full job log" in files["outputs/tool_output.log"]
    assert sorted(files) == capture.entries
    assert all(not os.path.exists(root) for root in roots)
    if not fail:
        assert set(_archive_files(result.output_tar)) == {"partial.txt"}
    WorkerResult.model_validate(result.model_dump())


def test_ordinary_request_does_not_build_diagnostic_archive(monkeypatch):
    monkeypatch.setattr(
        server_mod, "_diagnostic_names", MagicMock(side_effect=AssertionError)
    )
    result = run_tool_request(DiagnosticTool, ToolRequest())
    assert result.manifest.error is None
    assert result.manifest.debug_capture is None
    assert result.debug_tar is None


def test_prework_failure_has_unavailable_capture():
    result = run_tool_request(
        DiagnosticTool, ToolRequest(params={"fail": []}, debug_capture=True)
    )
    assert result.manifest.error.code == ErrorCode.PARAM_TYPE_MISMATCH
    assert result.manifest.debug_capture.status == "unavailable"
    assert result.debug_tar is None


def test_partial_input_materialization_is_captured(monkeypatch):
    def partial(self, refs, dest, policy=None):
        Path(dest).mkdir()
        (Path(dest) / "partial.txt").write_text("first input")
        msg = "second input failed"
        raise OSError(msg)

    monkeypatch.setattr(InlineTransport, "unpack_inputs", partial)
    result = run_tool_request(DiagnosticTool, ToolRequest(debug_capture=True))
    assert result.manifest.error.code == ErrorCode.INPUT_RESOLUTION_FAILED
    assert _archive_files(result.debug_tar) == {"inputs/partial.txt": b"first input"}


def test_output_delivery_failure_still_captures_completed_work(monkeypatch):
    original = InlineTransport.pack_outputs

    def fail_outputs(self, src, names, *, max_bytes=None):
        if src.endswith("/outputs"):
            msg = "output packing failed"
            raise OSError(msg)
        return original(self, src, names, max_bytes=max_bytes)

    monkeypatch.setattr(InlineTransport, "pack_outputs", fail_outputs)
    result = run_tool_request(DiagnosticTool, ToolRequest(debug_capture=True))
    assert result.manifest.error.code == ErrorCode.OUTPUT_DELIVERY_FAILED
    assert _archive_files(result.debug_tar)["outputs/partial.txt"] == b"partial"


@pytest.mark.parametrize("fail", [False, True])
def test_capture_failure_preserves_compute_outcome(fail, monkeypatch):
    monkeypatch.setattr(
        server_mod, "_diagnostic_names", MagicMock(side_effect=OSError("secret detail"))
    )
    result = run_tool_request(
        DiagnosticTool, ToolRequest(params={"fail": fail}, debug_capture=True)
    )
    assert (result.manifest.error is not None) is fail
    assert result.manifest.debug_capture.status == "failed"
    assert "secret detail" not in result.manifest.model_dump_json()
    assert result.debug_tar is None
    assert result.manifest.debug_capture.stored is None


@pytest.mark.parametrize("target", ["inside", "outside", "directory"])
def test_capture_rejects_every_symlink(tmp_path, monkeypatch, target):
    original = server_mod.invoke_op_work

    def with_link(op, inputs, **kwargs):
        original(op, inputs, **kwargs)
        link_target = "partial.txt" if target == "inside" else str(tmp_path)
        Path(inputs.execute_dir, "link").symlink_to(
            link_target, target_is_directory=target == "directory"
        )

    monkeypatch.setattr(server_mod, "invoke_op_work", with_link)
    result = run_tool_request(DiagnosticTool, ToolRequest(debug_capture=True))
    assert result.manifest.debug_capture.status == "failed"
    assert result.debug_tar is None


def test_combined_result_budget_counts_both_archives_and_manifest(monkeypatch):
    monkeypatch.setattr(transport_mod, "MAX_INLINE_BYTES", 17_000)
    result = run_tool_request(DiagnosticTool, ToolRequest(debug_capture=True))
    assert result.manifest.error is None
    assert result.manifest.debug_capture.status == "failed"
    total = len(result.output_tar or b"") + len(result.debug_tar or b"")
    total += len(result.manifest.model_dump_json().encode())
    assert total <= 17_000


def test_http_put_destination_is_used_only_for_regular_outputs(monkeypatch):
    calls = []

    def upload(src, names, store, op_name, policy=None):
        calls.append(src)
        return StoredOutputs(uri="https://store.example/output.tar")

    monkeypatch.setattr(server_mod, "upload_outputs", upload)
    result = run_tool_request(
        DiagnosticTool,
        ToolRequest(
            debug_capture=True,
            output_store="https://store.example/output.tar?sig=capability",
        ),
        ToolEndpointDataPolicy(output_allowlist=("https://store.example",)),
    )
    assert len(calls) == 1
    assert calls[0].endswith("/outputs")
    assert result.manifest.debug_capture.status == "complete"
    assert result.debug_tar is not None
    assert result.manifest.debug_capture.stored is None


def test_object_archives_use_distinct_fresh_keys(monkeypatch):
    uploads = []

    class Filesystem:
        def sign(self, path, expiration):
            return f"https://store.example/{path}?sig=capability"

        def put(self, local, remote):
            uploads.append((remote, Path(local).read_bytes()))

    monkeypatch.setattr(
        transport_mod,
        "_resolve_s3",
        lambda target, **kwargs: (Filesystem(), transport_mod._s3_remote_path(target)),
    )
    result = run_tool_request(
        DiagnosticTool,
        ToolRequest(
            debug_capture=True,
            output_store="s3://bucket/prefix",
        ),
        ToolEndpointDataPolicy(
            output_allowlist=("s3://bucket/prefix", "https://store.example")
        ),
    )
    assert result.manifest.error is None
    assert len(uploads) == 2
    assert uploads[0][0] != uploads[1][0]
    assert set(_archive_files(uploads[0][1])) == {"partial.txt"}
    assert "outputs/partial.txt" in _archive_files(uploads[1][1])
    assert result.debug_tar is None
    assert result.manifest.debug_capture.stored is not None
    WorkerResult.model_validate(result.model_dump())


@pytest.mark.parametrize(
    ("debug", "status", "stored", "valid"),
    [
        (b"tar", "complete", None, True),
        (None, "complete", "s3://b/k", True),
        (b"tar", "complete", "s3://b/k", False),
        (None, "complete", None, False),
        (b"tar", "failed", None, False),
        (None, "failed", None, True),
        (b"tar", None, None, False),
    ],
)
def test_diagnostic_plane_exclusivity(debug, status, stored, valid):
    manifest = ToolManifest(
        command_recording=CommandRecording.empty(),
        operation_identity=operation_identity(DiagnosticTool),
        debug_capture=None
        if status is None
        else DebugCaptureManifest(
            status=status,
            stored=StoredOutputs(uri=stored) if stored else None,
        ),
    )
    if valid:
        WorkerResult(manifest=manifest, debug_tar=debug)
    else:
        with pytest.raises(ValidationError):
            WorkerResult(manifest=manifest, debug_tar=debug)


def _builder(monkeypatch, *, source_status="observed", allow=False):
    identity = operation_identity(DiagnosticTool)
    diagnostic = ReplayDiagnostic(
        source_execution_run_id="source",
        source_execution_spec_id="spec",
        source_identity=identity,
        selected_identity=identity,
        source_remote_identity=[
            RemoteObservation(
                dispatch_index=0,
                status=source_status,
                identity=identity if source_status == "observed" else None,
            )
        ],
        allow_code_change=allow,
        runner="local",
        roots={},
    )
    builder = ReplayBuilder(
        ReplaySnapshot.unavailable("test").model_copy(update={"diagnostic": diagnostic})
    )
    monkeypatch.setattr(client_mod, "current_replay_builder", lambda: builder)
    return builder


def _client(monkeypatch, result, *, download_error=False, schema=None):
    client = MagicMock()
    schema = (
        schema
        or SchemaResponse(
            operation=DiagnosticTool.name,
            operation_identity=operation_identity(DiagnosticTool),
            debug_capture_supported=True,
        ).model_dump()
    )

    def get(path, params=None):
        if path == "/schema":
            return httpx.Response(200, json=schema)
        if path == "/result":
            return httpx.Response(
                200,
                json={
                    "status": "done",
                    "manifest": result.manifest.model_dump(mode="json"),
                },
            )
        if params.get("plane") == "diagnostics":
            if download_error:
                msg = "diagnostic download failed secret"
                raise OSError(msg)
            return httpx.Response(200, content=result.debug_tar)
        return httpx.Response(200, content=result.output_tar)

    client.get.side_effect = get
    client.post.return_value = httpx.Response(200, json={"call_id": "call"})
    monkeypatch.setattr(
        client_mod.httpx,
        "Client",
        MagicMock(return_value=MagicMock(__enter__=MagicMock(return_value=client))),
    )
    return client


def _operation():
    return DiagnosticTool(
        compute_provider=ComputeProvider(
            active="modal",
            modal=ModalComputeConfig(endpoint_url="https://tool.example"),
        )
    )


@pytest.mark.parametrize("failure", ["none", "compute", "outputs", "identity"])
def test_client_preserves_diagnostics_before_every_primary_error(
    tmp_path, monkeypatch, failure
):
    result = run_tool_request(
        DiagnosticTool,
        ToolRequest(params={"fail": failure == "compute"}, debug_capture=True),
    )
    if failure == "outputs":
        result.output_tar = b"not an archive"
    if failure == "identity":
        result.manifest.operation_identity = (
            result.manifest.operation_identity.model_copy(update={"version": "changed"})
        )
    builder = _builder(monkeypatch)
    _client(monkeypatch, result)
    execute = tmp_path / "execute"
    execute.mkdir()
    with endpoint_dispatch(0, str(tmp_path)):
        if failure == "none":
            call_endpoint(
                _operation(), ExecuteInput(inputs={}, execute_dir=str(execute))
            )
        else:
            with pytest.raises((ArtisanError, tarfile.TarError)):
                call_endpoint(
                    _operation(), ExecuteInput(inputs={}, execute_dir=str(execute))
                )
    assert (
        tmp_path / "remote-debug/artifact_0/outputs/partial.txt"
    ).read_text() == "partial"
    assert not (execute / "inputs").exists()
    assert builder.snapshot.remote_identity[0].diagnostic_status == "complete"


@pytest.mark.parametrize("fail", [False, True])
def test_client_delivery_failure_does_not_replace_compute_outcome(
    tmp_path, monkeypatch, fail
):
    result = run_tool_request(
        DiagnosticTool, ToolRequest(params={"fail": fail}, debug_capture=True)
    )
    builder = _builder(monkeypatch)
    _client(monkeypatch, result, download_error=True)
    with endpoint_dispatch(0, str(tmp_path)):
        if fail:
            with pytest.raises(
                ArtisanError, match="exit status 9|non-zero|return code 9|code 9"
            ):
                call_endpoint(
                    _operation(),
                    ExecuteInput(inputs={}, execute_dir=str(tmp_path / "execute")),
                )
        else:
            call_endpoint(
                _operation(),
                ExecuteInput(inputs={}, execute_dir=str(tmp_path / "execute")),
            )
    assert builder.snapshot.diagnostic.status == "incomplete"
    assert "secret" not in builder.snapshot.model_dump_json()
    if not fail:
        assert (tmp_path / "execute/partial.txt").exists()


@pytest.mark.parametrize("problem", ["old", "unsupported", "changed", "unavailable"])
def test_replay_preflight_stops_before_remote_submission(
    tmp_path, monkeypatch, problem
):
    result = run_tool_request(DiagnosticTool, ToolRequest())
    _builder(
        monkeypatch,
        source_status="unavailable" if problem == "unavailable" else "observed",
    )
    schema = SchemaResponse(
        operation=DiagnosticTool.name,
        operation_identity=operation_identity(DiagnosticTool),
        debug_capture_supported=True,
    ).model_dump()
    if problem == "old":
        del schema["debug_capture_supported"]
    elif problem == "unsupported":
        schema["debug_capture_supported"] = False
    elif problem == "changed":
        schema["operation_identity"]["version"] = "new"
    client = _client(monkeypatch, result, schema=schema)
    with pytest.raises(ArtisanError):
        call_endpoint(_operation(), ExecuteInput(inputs={}, execute_dir=str(tmp_path)))
    client.post.assert_not_called()


@pytest.mark.parametrize(
    ("source_status", "allow"), [("not_started", False), ("unavailable", True)]
)
def test_unstarted_or_explicitly_allowed_history_can_replay(
    tmp_path, monkeypatch, source_status, allow
):
    result = run_tool_request(DiagnosticTool, ToolRequest(debug_capture=True))
    builder = _builder(monkeypatch, source_status=source_status, allow=allow)
    _client(monkeypatch, result)
    call_endpoint(
        _operation(), ExecuteInput(inputs={}, execute_dir=str(tmp_path / "execute"))
    )
    assert builder.snapshot.remote_identity[0].status == "observed"


def test_ordinary_endpoint_capture_records_actual_worker_identity(
    tmp_path, monkeypatch
):
    builder = ReplayBuilder(ReplaySnapshot.unavailable("test"))
    monkeypatch.setattr(client_mod, "current_replay_builder", lambda: builder)
    result = run_tool_request(DiagnosticTool, ToolRequest())
    client = _client(monkeypatch, result)
    call_endpoint(_operation(), ExecuteInput(inputs={}, execute_dir=str(tmp_path)))
    assert builder.snapshot.remote_identity[0].identity == operation_identity(
        DiagnosticTool
    )
    assert all(call.args[0] != "/schema" for call in client.get.call_args_list)
    assert "debug_capture" not in client.post.call_args.kwargs["data"]


def test_parallel_artifact_calls_keep_separate_diagnostic_ownership(
    tmp_path, monkeypatch
):
    result = run_tool_request(DiagnosticTool, ToolRequest(debug_capture=True))
    builder = _builder(monkeypatch, allow=True)
    _client(monkeypatch, result)
    router = EndpointExecuteRouter(max_concurrent_calls=3)
    results = router.route_execute(
        _operation(),
        [
            ExecuteInput(inputs={}, execute_dir=str(tmp_path / f"execute/{index}"))
            for index in range(3)
        ],
        str(tmp_path),
    )
    assert results == [None, None, None]
    assert {item.dispatch_index for item in builder.snapshot.remote_identity} == {
        0,
        1,
        2,
    }
    for index in range(3):
        assert (
            tmp_path / f"remote-debug/artifact_{index}/outputs/partial.txt"
        ).exists()


def test_diagnostic_roundtrip_through_minio(s3_fs, tmp_path, monkeypatch):
    import s3fs

    _fs, storage, prefix = s3_fs
    endpoint = storage.options["client_kwargs"]["endpoint_url"]
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", storage.options["key"])
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", storage.options["secret"])
    monkeypatch.setenv("AWS_ENDPOINT_URL", endpoint)
    s3fs.S3FileSystem.clear_instance_cache()
    policy = ToolEndpointDataPolicy(output_allowlist=(prefix, endpoint))
    result = run_tool_request(
        DiagnosticTool,
        ToolRequest(
            params={"fail": True},
            debug_capture=True,
            output_store=f"{prefix}/diagnostics",
        ),
        policy,
    )
    assert result.manifest.error is not None
    assert result.manifest.debug_capture.status == "complete"
    capture = result.manifest.debug_capture
    InlineTransport().download_outputs(
        capture.stored.presigned_url,
        str(tmp_path / "restored"),
        policy=policy,
        prefixes=("inputs/", "outputs/"),
    )
    assert (tmp_path / "restored/outputs/partial.txt").read_text() == "partial"
    assert "full job log" in (tmp_path / "restored/outputs/tool_output.log").read_text()


@pytest.mark.parametrize(
    "member_type", [tarfile.SYMTYPE, tarfile.LNKTYPE, tarfile.FIFOTYPE]
)
def test_diagnostic_download_rejects_links_and_special_files_before_extract(
    tmp_path, member_type
):
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w") as archive:
        member = tarfile.TarInfo("outputs/link")
        member.type = member_type
        member.linkname = "outputs/ordinary.txt"
        archive.addfile(member)
    with pytest.raises(ValueError, match="link or special"):
        InlineTransport().unpack_outputs(
            payload.getvalue(), str(tmp_path / "dest"), prefixes=("inputs/", "outputs/")
        )
    assert not list((tmp_path / "dest").iterdir())


def test_diagnostic_download_rejects_unowned_prefix_before_extract(tmp_path):
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w") as archive:
        member = tarfile.TarInfo("mounted-volume/data.txt")
        member.size = 1
        archive.addfile(member, io.BytesIO(b"x"))
    with pytest.raises(ValueError, match="unexpected prefix"):
        InlineTransport().unpack_outputs(
            payload.getvalue(), str(tmp_path / "dest"), prefixes=("inputs/", "outputs/")
        )
    assert not list((tmp_path / "dest").iterdir())


@pytest.mark.parametrize("debug", [False, True])
def test_oversized_primary_error_is_bounded_without_replacing_its_identity(
    debug, monkeypatch
):
    from artisan.execution.recording.commands import capture_commands

    monkeypatch.setattr(transport_mod, "MAX_INLINE_BYTES", 2000)
    error = ArtisanError(
        code=ErrorCode.OP_EXECUTE_FAILED, message='🧪\\"' * 3000, error_type="compute"
    ).envelope
    result = WorkerResult(
        manifest=ToolManifest(
            command_recording=CommandRecording.empty(),
            operation_identity=operation_identity(DiagnosticTool),
            debug_capture=DebugCaptureManifest(status="failed", error="delivery failed")
            if debug
            else None,
            error=error,
        )
    )
    with capture_commands():
        bounded = server_mod._bound_result(result, DiagnosticTool)
    assert len(bounded.manifest.model_dump_json().encode()) <= 2000
    assert bounded.manifest.error.code == error.code
    assert bounded.manifest.error.error_type == error.error_type
    assert bounded.manifest.error.message.endswith(" [truncated]")
    if debug:
        assert bounded.manifest.debug_capture.status == "failed"
    assert bounded.output_tar is None
    assert bounded.debug_tar is None


@pytest.mark.parametrize("fail", [False, True])
def test_execute_as_tool_python_intermediates_survive_subprocess_failure(
    fail, monkeypatch
):
    import sys

    from fixtures.replay_endpoint_ops import PythonDiagnosticTool

    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(sys.path))
    result = run_tool_request(
        PythonDiagnosticTool, ToolRequest(params={"fail": fail}, debug_capture=True)
    )
    assert (result.manifest.error is not None) is fail
    assert result.manifest.debug_capture.status == "complete"
    files = _archive_files(result.debug_tar)
    assert files["outputs/partial.txt"] == b"Python intermediate"
    assert b"Python diagnostic log" in files["outputs/tool_output.log"]
    assert len(result.manifest.command_recording.commands) == 1
    if fail:
        assert result.manifest.command_recording.commands[0].outcome == "failed"


@pytest.mark.parametrize("started", [False, True])
def test_source_remote_observation_distinguishes_preflight_from_transport_gap(
    tmp_path, monkeypatch, started
):
    builder = ReplayBuilder(ReplaySnapshot.unavailable("test"))
    monkeypatch.setattr(client_mod, "current_replay_builder", lambda: builder)
    result = run_tool_request(DiagnosticTool, ToolRequest())
    client = _client(monkeypatch, result)
    if started:
        client.post.side_effect = OSError("transport interrupted")
    inputs = {} if started else {"unexpected": 42}
    with pytest.raises((OSError, ArtisanError)):
        call_endpoint(
            _operation(), ExecuteInput(inputs=inputs, execute_dir=str(tmp_path))
        )
    observation = builder.snapshot.remote_identity[0]
    assert observation.status == ("unavailable" if started else "not_started")
    assert observation.identity is None


def test_incomplete_archive_manifest_keeps_compute_success_but_marks_diagnostics(
    tmp_path, monkeypatch
):
    result = run_tool_request(DiagnosticTool, ToolRequest(debug_capture=True))
    result.manifest.debug_capture.entries.append("outputs/missing.txt")
    builder = _builder(monkeypatch)
    _client(monkeypatch, result)
    call_endpoint(
        _operation(), ExecuteInput(inputs={}, execute_dir=str(tmp_path / "execute"))
    )
    assert (tmp_path / "execute/partial.txt").exists()
    assert builder.snapshot.diagnostic.status == "incomplete"


def test_duplicate_diagnostic_member_rejects_capture_without_failing_compute(
    tmp_path, monkeypatch
):
    result = run_tool_request(DiagnosticTool, ToolRequest(debug_capture=True))
    files = _archive_files(result.debug_tar)
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w") as archive:
        entries = [*files.items(), ("outputs/partial.txt", b"overwritten evidence")]
        for name, content in entries:
            member = tarfile.TarInfo(name)
            member.size = len(content)
            archive.addfile(member, io.BytesIO(content))
    result.debug_tar = payload.getvalue()
    builder = _builder(monkeypatch)
    _client(monkeypatch, result)

    with endpoint_dispatch(0, str(tmp_path)):
        call_endpoint(
            _operation(), ExecuteInput(inputs={}, execute_dir=str(tmp_path / "execute"))
        )

    assert (tmp_path / "execute/partial.txt").read_bytes() == b"partial"
    assert list((tmp_path / "remote-debug/artifact_0").iterdir()) == []
    assert builder.snapshot.diagnostic.status == "incomplete"
    assert builder.snapshot.remote_identity[0].diagnostic_status == "incomplete"
