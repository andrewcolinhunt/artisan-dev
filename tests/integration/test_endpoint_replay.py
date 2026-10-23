"""Real HTTP endpoint replay through subprocess execution and logical commits."""

from __future__ import annotations

import json
import socket
import threading
import time
import uuid
from pathlib import Path
from typing import Literal

import polars as pl
import pytest
import uvicorn
from fastapi import FastAPI, File, Form, UploadFile
from fastapi.responses import Response

from artisan.execution.tool_endpoint import client as client_module
from artisan.execution.tool_endpoint import server as server_module
from artisan.execution.tool_endpoint.protocol import (
    InputRef,
    SchemaResponse,
    ToolRequest,
)
from artisan.execution.tool_endpoint.server import run_tool_request
from artisan.execution.tool_endpoint.spec import command_contract
from artisan.execution.tool_endpoint.transport import InlineTransport
from artisan.operations.examples import DataGenerator, WaitTool
from artisan.orchestration import PipelineManager, StepStatus, replay_execution
from artisan.registry.resolve import operation_identity
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.storage.core.committed_scan import read_committed

pytestmark = pytest.mark.integration


@pytest.fixture
def endpoint_requests() -> dict[str, list[str]]:
    return {"submit": [], "result": [], "download": []}


@pytest.fixture
def endpoint_url(monkeypatch, request, endpoint_requests):
    mode = request.param
    if mode == "compute_failure":
        original = server_module.invoke_op_work

        def fail_after_outputs(*args, **kwargs):
            original(*args, **kwargs)
            msg = "remote compute failed after writing intermediates"
            raise RuntimeError(msg)

        monkeypatch.setattr(server_module, "invoke_op_work", fail_after_outputs)
    elif mode == "diagnostic_failure":
        pack = InlineTransport.pack_outputs

        def fail_diagnostics(self, src, names, *, max_bytes=None):
            if not src.endswith("/outputs"):
                msg = "diagnostic archive cannot be delivered"
                raise OSError(msg)
            return pack(self, src, names, max_bytes=max_bytes)

        monkeypatch.setattr(InlineTransport, "pack_outputs", fail_diagnostics)
    app = FastAPI()
    results = {}

    @app.get("/schema")
    def schema():
        return SchemaResponse(
            command_contract=command_contract(WaitTool),
            operation=WaitTool.name,
            operation_identity=operation_identity(WaitTool),
            debug_capture_supported=True,
        ).model_dump()

    @app.post("/submit")
    async def submit(
        expected_command_contract: str = Form(...),
        params: str = Form("{}"),
        input_filenames: str = Form("{}"),
        debug_capture: bool = Form(False),
        files: list[UploadFile] = File(default=[]),  # noqa: B008
    ):
        filenames = json.loads(input_filenames)
        refs = [
            InputRef(
                name=file.filename,
                filename=filenames[file.filename],
                data=await file.read(),
            )
            for file in files
        ]
        result = run_tool_request(
            WaitTool,
            ToolRequest(
                expected_command_contract=json.loads(expected_command_contract),
                params=json.loads(params),
                inputs=refs,
                debug_capture=debug_capture,
            ),
        )
        if mode == "identity_change" and debug_capture:
            result.manifest.operation_identity = (
                result.manifest.operation_identity.model_copy(
                    update={"version": "changed-worker"}
                )
            )
            result.manifest.command_contract = (
                result.manifest.command_contract.model_copy(
                    update={"operation_identity": result.manifest.operation_identity}
                )
            )
        call_id = uuid.uuid4().hex
        results[call_id] = result
        endpoint_requests["submit"].append(call_id)
        return {"call_id": call_id}

    @app.get("/result")
    def result(call_id: str):
        endpoint_requests["result"].append(call_id)
        if mode == "poll_recovery":
            polls = len(endpoint_requests["result"])
            if polls == 1:
                return Response(status_code=500)
            if polls == 2:
                return {"status": "pending", "manifest": None}
        manifest = results[call_id].manifest
        return {
            "status": "failed" if manifest.error else "done",
            "manifest": manifest.model_dump(mode="json"),
        }

    @app.get("/download")
    def download(call_id: str, plane: Literal["outputs", "diagnostics"] = "outputs"):
        endpoint_requests["download"].append(call_id)
        outcome = results[call_id]
        return Response(
            outcome.debug_tar if plane == "diagnostics" else outcome.output_tar,
            media_type="application/x-tar",
        )

    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        server = uvicorn.Server(uvicorn.Config(app, log_level="warning", ws="none"))
        thread = threading.Thread(
            target=server.run, kwargs={"sockets": [listener]}, daemon=True
        )
        thread.start()
        deadline = time.monotonic() + 10
        try:
            while not server.started:
                if not thread.is_alive() or time.monotonic() >= deadline:
                    pytest.fail("Local endpoint did not start")
                time.sleep(0.01)
            yield f"http://127.0.0.1:{listener.getsockname()[1]}", mode
        finally:
            server.should_exit = True
            thread.join(timeout=10)
            assert not thread.is_alive()


@pytest.mark.parametrize("endpoint_url", ["poll_recovery"], indirect=True)
def test_result_poll_recovery_commits_original_call_with_lineage(
    tmp_path: Path,
    endpoint_url: tuple[str, str],
    endpoint_requests: dict[str, list[str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(client_module, "_RESULT_BACKOFF_SECONDS", 0.001)
    url, _ = endpoint_url
    manager = PipelineManager.create(
        "poll-recovery",
        str(tmp_path / "delta"),
        str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
    )
    source = manager.run(DataGenerator, params={"count": 1, "seed": 17}, compact=False)
    recovered = manager.run(
        WaitTool,
        inputs={"dataset": source.output("datasets")},
        params={"seconds": 1},
        compute_provider={
            "active": "modal",
            "modal": {"endpoint_url": url, "poll_interval": 0.001},
        },
        compact=False,
    )
    assert manager.finalize()["overall_success"]
    assert recovered.status == StepStatus.SUCCEEDED
    assert len(endpoint_requests["submit"]) == 1
    call_id = endpoint_requests["submit"][0]
    assert endpoint_requests["result"] == [call_id] * 3
    assert endpoint_requests["download"] == [call_id]

    delta_root = manager.config.delta_root
    fs = manager.config.storage.filesystem()
    executions = read_committed(delta_root, TablePath.EXECUTIONS, fs=fs).filter(
        pl.col("step_run_id") == recovered.step_run_id
    )
    assert executions.height == 1
    execution = executions.row(0, named=True)
    assert execution["success"] is True
    assert execution["error"] is None
    recording = json.loads(execution["command_recording"])
    assert recording["status"] == "complete"
    assert recording["missing_invocations"] == []
    assert len(recording["commands"]) == 1
    assert recording["commands"][0]["location"] == "endpoint"
    assert recording["commands"][0]["outcome"] == "succeeded"
    identity = json.loads(execution["replay_snapshot"])["remote_identity"]
    assert len(identity) == 1
    assert identity[0]["status"] == "observed"
    assert identity[0]["identity"] == operation_identity(WaitTool).model_dump(
        mode="json"
    )

    artifacts = read_committed(delta_root, "artifacts/data", fs=fs)
    inputs = artifacts.filter(pl.col("origin_step_number") == source.step_number)
    outputs = artifacts.filter(pl.col("origin_step_number") == recovered.step_number)
    assert inputs.height == outputs.height == 1
    parent = inputs.row(0, named=True)
    output = outputs.row(0, named=True)
    assert output["original_name"] == f"{parent['original_name']}_waited"
    assert output["row_count"] == 1
    assert json.loads(output["columns"]) == ["seconds", "host", "source"]
    marker = output["content"].decode()
    assert marker.startswith("seconds,host,source\n1,")
    assert marker.rstrip().endswith(f",{parent['artifact_id']}.csv")
    edges = read_committed(delta_root, TablePath.ARTIFACT_EDGES, fs=fs).filter(
        pl.col("execution_run_id") == execution["execution_run_id"]
    )
    assert edges.to_dicts() == [
        {
            "execution_run_id": execution["execution_run_id"],
            "source_artifact_id": parent["artifact_id"],
            "target_artifact_id": output["artifact_id"],
            "source_artifact_type": "data",
            "target_artifact_type": "data",
            "source_role": "dataset",
            "target_role": "output",
            "group_id": None,
            "step_boundary": True,
        }
    ]


@pytest.mark.parametrize(
    "endpoint_url",
    ["success", "compute_failure", "diagnostic_failure", "identity_change"],
    indirect=True,
)
def test_endpoint_replay_records_actual_outcome_and_retains_remote_work(
    tmp_path, endpoint_url
):
    url, mode = endpoint_url
    manager = PipelineManager.create(
        "source",
        str(tmp_path / "delta"),
        str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
    )
    source = manager.run(DataGenerator, params={"count": 1, "seed": 17}, compact=False)
    original = manager.run(
        WaitTool,
        inputs={"dataset": source.output("datasets")},
        params={"seconds": 1},
        compute_provider={
            "active": "modal",
            "modal": {"endpoint_url": url, "poll_interval": 0.01},
        },
        compact=False,
    )
    manager.finalize()
    assert original.status == (
        StepStatus.FAILED if mode == "compute_failure" else StepStatus.SUCCEEDED
    )
    runtime = RuntimeEnvironment(
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "debug/staging"),
        working_root=str(tmp_path / "debug/work"),
        files_root=str(tmp_path / "debug/files"),
        failure_logs_root=str(tmp_path / "debug/logs"),
    )
    before = read_committed(
        runtime.delta_root, TablePath.EXECUTIONS, fs=runtime.storage.filesystem()
    )
    row = before.filter(pl.col("step_run_id") == original.step_run_id).row(
        0, named=True
    )
    old_snapshot = json.loads(row["replay_snapshot"])
    assert old_snapshot["remote_identity"][0]["status"] == "observed"
    result = replay_execution(row["execution_run_id"], runtime=runtime)
    assert result.execution_run_id is not None
    expected = (
        StepStatus.FAILED
        if mode in {"compute_failure", "identity_change"}
        else StepStatus.SUCCEEDED
    )
    assert result.step_result.status == expected
    assert result.diagnostic_status == (
        "incomplete" if mode == "diagnostic_failure" else "complete"
    )
    after = read_committed(
        runtime.delta_root, TablePath.EXECUTIONS, fs=runtime.storage.filesystem()
    )
    assert after.filter(
        pl.col("execution_run_id").is_in(before["execution_run_id"].to_list())
    ).equals(before)
    replayed = after.filter(pl.col("execution_run_id") == result.execution_run_id).row(
        0, named=True
    )
    snapshot = json.loads(replayed["replay_snapshot"])
    assert snapshot["diagnostic"]["status"] == result.diagnostic_status
    assert (
        snapshot["remote_identity"][0]["diagnostic_status"] == result.diagnostic_status
    )
    if mode != "diagnostic_failure":
        roots = list(
            Path(result.diagnostic_roots["working_root"]).rglob(
                "remote-debug/artifact_0"
            )
        )
        assert len(roots) == 1
        assert list((roots[0] / "inputs/dataset").glob("*.csv"))
        assert list((roots[0] / "outputs").glob("*_waited.csv"))
        assert "tick" in (roots[0] / "outputs/tool_output.log").read_text()
