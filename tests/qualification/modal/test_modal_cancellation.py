"""Opt-in qualification of an explicitly deployed, owned synthetic Modal app."""

from __future__ import annotations

import asyncio
import json
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx
import modal
import pytest
from managed_cancellation_probe import (
    APP_NAME,
    MARKER,
    ManagedCancellationProbe,
    source_digest,
)
from modal._logs import LogsFilters, _fetch_interval
from modal._utils.async_utils import synchronizer
from modal.client import _Client
from modal_proto import api_pb2

from artisan.execution.tool_endpoint import client as endpoint_client
from artisan.execution.tool_endpoint.client import EndpointCancellationError
from artisan.execution.tool_endpoint.protocol import SchemaResponse
from artisan.execution.tool_endpoint.spec import command_contract
from artisan.schemas.operation_config.compute import ComputeProvider, ModalComputeConfig
from artisan.schemas.orchestration.step_lifecycle import CancellationStatus
from artisan.schemas.specs.input_models import ExecuteInput

pytestmark = pytest.mark.modal


@synchronizer.create_blocking
async def provider_observation(
    call_id: str, task_id: str | None = None
) -> dict[str, Any]:
    """Read exact provider records without interpreting generic exception text."""
    client = await _Client.from_env()
    response = await asyncio.wait_for(
        client.stub.FunctionGetOutputs(
            api_pb2.FunctionGetOutputsRequest(
                function_call_id=call_id,
                timeout=0,
                max_values=1,
                clear_on_success=False,
            )
        ),
        timeout=10,
    )
    result: dict[str, Any] = {
        "observed_at": time.time(),
        "call_statuses": [item.result.status for item in response.outputs],
    }
    if task_id:
        response = await asyncio.wait_for(
            client.stub.TaskGetInfo(api_pb2.TaskGetInfoRequest(task_id=task_id)),
            timeout=10,
        )
        assert response.info.id == task_id
        result["task"] = {
            "id": response.info.id,
            "started_at": response.info.started_at,
            "finished_at": response.info.finished_at,
            "result_status": response.info.result.status,
        }
    return result


@synchronizer.create_blocking
async def probe_logs(app_id: str, token: str, since: float) -> list[dict[str, Any]]:
    """Read only this harness's synthetic markers, never general app logs."""
    client = await _Client.from_env()
    batches = await asyncio.wait_for(
        _fetch_interval(
            client,
            app_id,
            since,
            time.time(),
            200,
            LogsFilters(search_text=token),
        ),
        timeout=10,
    )
    markers = []
    for batch in batches:
        for item in batch.items:
            for line in item.data.splitlines():
                if line.startswith(MARKER):
                    marker = json.loads(line[len(MARKER) :])
                    if marker.get("token") == token:
                        markers.append(marker)
    return markers


class Harness:
    """Keep exact ownership and bounded evidence for one temporary deployment."""

    def __init__(self, manifest: dict[str, Any], evidence_path: Path) -> None:
        self.manifest = manifest
        self.evidence_path = evidence_path
        self.owned: dict[str, dict[str, Any]] = {}
        self.submissions = 0
        self.started = time.time()
        self.deadline = time.monotonic() + 900
        app = modal.App.lookup(APP_NAME, create_if_missing=False)
        assert app.app_id == manifest["app_id"]
        assert source_digest() == manifest["source_digest"]
        url = modal.Function.from_name(APP_NAME, "endpoint").get_web_url()
        assert url
        self.url = url
        headers = endpoint_client._auth_headers(
            "MODAL_PROXY", "managed_cancellation_probe"
        )
        self.client = httpx.Client(base_url=url, headers=headers, timeout=20)
        try:
            schema = SchemaResponse.model_validate(self.client.get("/schema").json())
            assert schema.command_contract == command_contract(ManagedCancellationProbe)
            self.contract = schema.command_contract.model_dump_json()
        except BaseException:
            self.client.close()
            raise

    def save(self) -> None:
        self.evidence_path.write_text(
            json.dumps(
                {
                    "deployment": self.manifest,
                    "started": self.started,
                    "calls": self.owned,
                    "submissions": self.submissions,
                },
                indent=2,
            )
            + "\n"
        )

    def submit(self, *, mode: str = "normal", seconds: int = 60) -> str:
        assert time.monotonic() < self.deadline
        assert self.submissions < self.manifest["max_owned_calls"]
        self.submissions += 1
        self.save()
        token = uuid.uuid4().hex
        response = self.client.post(
            "/submit",
            data={
                "expected_command_contract": self.contract,
                "params": json.dumps(
                    {"mode": mode, "seconds": seconds, "token": token}
                ),
                "input_filenames": json.dumps({"dataset": "synthetic.csv"}),
            },
            files=[("files", ("dataset", b"a,b\n1,2\n"))],
        )
        response.raise_for_status()
        call_id = response.json()["call_id"]
        self.owned[call_id] = {
            "token": token,
            "mode": mode,
            "submitted_at": time.time(),
        }
        self.save()
        return call_id

    def ready(self, call_id: str, event: str = "child_ready") -> dict[str, Any]:
        deadline = min(self.deadline, time.monotonic() + 120)
        state = self.owned[call_id]
        while time.monotonic() < deadline:
            markers = probe_logs(self.manifest["app_id"], state["token"], self.started)
            worker = next((m for m in markers if m["event"] == "worker_ready"), None)
            if worker:
                assert worker["call_id"] == call_id
                assert worker["source_digest"] == self.manifest["source_digest"]
                state["task_id"] = worker["task_id"]
            marker = next((m for m in markers if m["event"] == event), None)
            if marker and worker:
                state["markers"] = markers
                self.save()
                return marker
            time.sleep(0.5)
        msg = f"missing {event} for owned call {call_id}"
        raise AssertionError(msg)

    def cancel(self, call_id: str) -> dict[str, Any]:
        state = self.owned[call_id]
        started = time.monotonic()
        response = self.client.post("/cancel", params={"call_id": call_id})
        received_at = time.time()
        response.raise_for_status()
        body = response.json()
        assert body["call_id"] == call_id
        assert time.monotonic() - started < 20
        observation = provider_observation(call_id, state.get("task_id"))
        state.setdefault("cancellations", []).append(
            {
                "response": body,
                "received_at": received_at,
                "latency_seconds": time.monotonic() - started,
                "immediate_provider_observation": observation,
            }
        )
        self.save()
        return body

    def settled(self, call_id: str, *, require_task: bool = True) -> dict[str, Any]:
        state = self.owned[call_id]
        deadline = min(self.deadline, time.monotonic() + 45)
        while time.monotonic() < deadline:
            observed = provider_observation(call_id, state.get("task_id"))
            statuses = observed["call_statuses"]
            task = observed.get("task")
            if statuses == [api_pb2.GenericResult.GENERIC_STATUS_TERMINATED] and (
                not require_task
                or (task and task["finished_at"] > 0 and task["result_status"] > 0)
            ):
                state["terminal_provider_observation"] = observed
                self.save()
                assert (
                    self.client.get(
                        "/download", params={"call_id": call_id}
                    ).status_code
                    == 404
                )
                return observed
            time.sleep(0.5)
        state["last_provider_observation"] = observed
        self.save()
        msg = f"no positive termination evidence for owned call {call_id}"
        raise AssertionError(msg)

    def completed(self, call_id: str) -> dict[str, Any]:
        deadline = min(self.deadline, time.monotonic() + 120)
        while time.monotonic() < deadline:
            response = self.client.get("/result", params={"call_id": call_id})
            response.raise_for_status()
            result = response.json()
            if result["status"] != "pending":
                assert result["status"] == "done", result
                assert result["manifest"]["output_names"] == ["synthetic_waited.csv"]
                self.owned[call_id]["natural_completion"] = True
                self.save()
                return result
            time.sleep(0.5)
        msg = f"owned call {call_id} did not complete"
        raise AssertionError(msg)

    def cleanup(self) -> None:
        errors = []
        for call_id, state in self.owned.items():
            if state.get("terminal_provider_observation") or state.get(
                "natural_completion"
            ):
                continue
            try:
                modal.FunctionCall.from_id(call_id).cancel(terminate_containers=True)
            except Exception as exc:
                errors.append({"call_id": call_id, "error_type": type(exc).__name__})
        self.client.close()
        self.manifest["call_cleanup"] = {"cancel_errors": errors}
        self.save()


@pytest.fixture(scope="module")
def harness():
    path = os.environ.get("ARTISAN_REMOTE_QUALIFICATION_MANIFEST")
    if not path:
        pytest.skip(
            "explicit owned deployment manifest is required; this test never deploys"
        )
    manifest = json.loads(Path(path).read_text())
    assert manifest["app_name"] == APP_NAME
    assert manifest["app_id"].startswith("ap-")
    evidence = Path(path).with_name("qualification-evidence.json")
    owned = None
    try:
        owned = Harness(manifest, evidence)
        yield owned
    finally:
        if owned is not None:
            owned.cleanup()


def test_running_child_closure_and_sibling_isolation(harness: Harness) -> None:
    cancelled = harness.submit(mode="stubborn", seconds=90)
    sibling = harness.submit(seconds=12)
    harness.ready(cancelled)
    harness.ready(sibling)
    assert harness.owned[cancelled]["task_id"] != harness.owned[sibling]["task_id"]
    response = harness.cancel(cancelled)
    assert response["status"] in {"requested", "confirmed"}
    observed = harness.settled(cancelled)
    harness.completed(sibling)
    assert harness.cancel(sibling)["status"] == "rejected"
    repeated = harness.cancel(cancelled)
    assert repeated["status"] in {"confirmed", "rejected"}
    if response["status"] == "confirmed":
        acknowledgement = harness.owned[cancelled]["cancellations"][0]
        immediate = acknowledgement["immediate_provider_observation"]["task"]
        assert immediate["finished_at"] > 0, (
            "confirmed before positive provider task closure"
        )
        acknowledgement["finish_minus_ack_seconds"] = (
            observed["task"]["finished_at"] - acknowledgement["received_at"]
        )
        acknowledgement["interpretation"] = (
            "Empirical timing only; no synchronous closure guarantee established."
        )
        harness.save()


def test_cancel_before_command_launch(harness: Harness) -> None:
    call_id = harness.submit(mode="startup", seconds=30)
    harness.ready(call_id, "worker_ready")
    harness.cancel(call_id)
    harness.settled(call_id)
    markers = probe_logs(
        harness.manifest["app_id"], harness.owned[call_id]["token"], harness.started
    )
    assert not any(m["event"] == "command_ready" for m in markers)


def test_cancel_queued_call_does_not_run_later(harness: Harness) -> None:
    occupying = [harness.submit(mode="stubborn", seconds=90) for _ in range(3)]
    for call_id in occupying:
        harness.ready(call_id)
    queued = harness.submit(seconds=2)
    assert (
        probe_logs(
            harness.manifest["app_id"], harness.owned[queued]["token"], harness.started
        )
        == []
    )
    harness.cancel(queued)
    harness.settled(queued, require_task=False)
    for call_id in occupying:
        harness.cancel(call_id)
        harness.settled(call_id)
    fresh = harness.submit(seconds=2)
    harness.completed(fresh)
    assert (
        probe_logs(
            harness.manifest["app_id"], harness.owned[queued]["token"], harness.started
        )
        == []
    )


def test_lost_cancellation_response_stays_unknown(harness: Harness) -> None:
    call_id = harness.submit(mode="stubborn", seconds=90)
    harness.ready(call_id)
    event = threading.Event()
    event.set()
    real_post = harness.client.post

    def lose_response(url: str, **kwargs: Any) -> httpx.Response:
        response = real_post(url, **kwargs)
        assert response.is_success
        msg = "synthetic dropped acknowledgement"
        raise httpx.ReadTimeout(msg)

    with (
        patch.object(harness.client, "post", side_effect=lose_response),
        endpoint_client.cancel_scope(event),
        pytest.raises(EndpointCancellationError) as exc,
    ):
        endpoint_client._poll(
            harness.client, call_id, 0.1, "managed_cancellation_probe"
        )
    assert exc.value.acknowledgement.status == CancellationStatus.UNKNOWN
    harness.settled(call_id)


def test_real_client_cancel_does_not_deliver_output(
    harness: Harness, tmp_path: Path
) -> None:
    token = uuid.uuid4().hex
    operation = ManagedCancellationProbe(
        params={"token": token, "mode": "stubborn", "seconds": 90},
        compute_provider=ComputeProvider(
            active="modal", modal=ModalComputeConfig(poll_interval=0.1)
        ),
    )
    source = tmp_path / "synthetic.csv"
    source.write_text("a,b\n1,2\n")
    output = tmp_path / "outputs"
    output.mkdir()
    event = threading.Event()
    poll = endpoint_client._poll
    captured = []

    def cancel_after_ready(
        client: httpx.Client, call_id: str, interval: float, name: str
    ):
        harness.owned[call_id] = {
            "token": token,
            "mode": "stubborn",
            "submitted_at": time.time(),
        }
        captured.append(call_id)
        harness.save()
        harness.ready(call_id)
        event.set()
        return poll(client, call_id, interval, name)

    assert harness.submissions < harness.manifest["max_owned_calls"]
    harness.submissions += 1
    harness.save()
    with (
        patch.object(endpoint_client, "_poll", side_effect=cancel_after_ready),
        endpoint_client.cancel_scope(event),
        pytest.raises(EndpointCancellationError) as exc,
    ):
        endpoint_client.call_endpoint(
            operation,
            ExecuteInput(inputs={"dataset": str(source)}, execute_dir=str(output)),
        )
    assert exc.value.acknowledgement.status in {
        CancellationStatus.CONFIRMED,
        CancellationStatus.UNKNOWN,
    }
    assert len(captured) == 1
    harness.settled(captured[0])
    assert list(output.iterdir()) == []


def test_pipeline_cancel_preserves_completed_artifacts(
    harness: Harness, tmp_path: Path
) -> None:
    from artisan.operations.examples import DataGenerator
    from artisan.orchestration import PipelineManager
    from artisan.schemas.enums import TablePath
    from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
    from artisan.schemas.orchestration.step_lifecycle import StepStatus
    from artisan.storage.core.committed_scan import read_committed

    token = uuid.uuid4().hex
    assert harness.submissions < harness.manifest["max_owned_calls"]
    harness.submissions += 1
    harness.save()
    runtime = RuntimeEnvironment(
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path / "working"),
    )
    with PipelineManager.create(
        "remote-cancellation-qualification",
        delta_root=runtime.delta_root,
        staging_root=runtime.staging_root,
        working_root=runtime.working_root,
        application_modules=("managed_cancellation_probe",),
    ) as pipeline:
        source = pipeline.run(
            DataGenerator, params={"count": 1, "rows_per_file": 1}, compact=False
        )
        before = read_committed(
            runtime.delta_root, TablePath.EXECUTIONS, fs=runtime.storage.filesystem()
        )
        future = pipeline.submit(
            ManagedCancellationProbe,
            inputs={"dataset": source.output("datasets")},
            params={"token": token, "mode": "stubborn", "seconds": 90},
            compute_provider={"active": "modal", "modal": {"poll_interval": 0.1}},
            compact=False,
        )
        call_id = None
        deadline = min(harness.deadline, time.monotonic() + 120)
        try:
            while time.monotonic() < deadline:
                markers = probe_logs(harness.manifest["app_id"], token, harness.started)
                worker = next(
                    (m for m in markers if m["event"] == "worker_ready"), None
                )
                if worker:
                    call_id = worker["call_id"]
                    harness.owned[call_id] = {
                        "token": token,
                        "mode": "stubborn",
                        "submitted_at": time.time(),
                        "task_id": worker["task_id"],
                    }
                    harness.save()
                    harness.ready(call_id)
                    break
                time.sleep(0.5)
        finally:
            pipeline.cancel()
        assert call_id is not None, "pipeline did not start its remote invocation"
        started = time.monotonic()
        result = future.result(timeout=20)
        assert time.monotonic() - started < 15
        assert result.status in {StepStatus.CANCELLED, StepStatus.FAILED}
        assert result.cancellation_status in {
            CancellationStatus.CONFIRMED,
            CancellationStatus.UNKNOWN,
        }
        harness.owned[call_id]["pipeline_status"] = {
            "status": result.status.value,
            "cancellation_status": result.cancellation_status.value,
        }
        harness.settled(call_id)
    after = read_committed(
        runtime.delta_root, TablePath.EXECUTIONS, fs=runtime.storage.filesystem()
    )
    assert after["execution_run_id"].to_list() == before["execution_run_id"].to_list()


def test_normal_completion_then_fresh_execution(harness: Harness) -> None:
    for _ in range(2):
        call_id = harness.submit(seconds=2)
        harness.completed(call_id)
        assert harness.cancel(call_id)["status"] == "rejected"
        response = harness.client.get("/download", params={"call_id": call_id})
        assert response.is_success
        assert response.content


def test_natural_completion_cancellation_race(harness: Harness) -> None:
    for delay in (1.5, 2.5, 3.5):
        call_id = harness.submit(seconds=3)
        harness.ready(call_id)
        # Timing selects competing outcomes; final provider records prove them.
        time.sleep(delay)
        response = harness.cancel(call_id)
        if response["status"] == "rejected":
            harness.completed(call_id)
        else:
            assert response["status"] in {"confirmed", "requested"}
            harness.settled(call_id)
