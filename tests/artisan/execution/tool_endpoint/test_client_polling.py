"""Result retrieval recovers without repeating remote computation."""

from __future__ import annotations

import ssl
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from artisan.errors import ArtisanError, ErrorCode
from artisan.execution.recording.commands import capture_commands
from artisan.execution.tool_endpoint import client as client_mod
from artisan.execution.tool_endpoint.client import (
    EndpointCancellationError,
    cancel_scope,
)
from artisan.execution.tool_endpoint.protocol import SchemaResponse, ToolManifest
from artisan.execution.tool_endpoint.spec import command_contract
from artisan.operations.examples import WaitTool
from artisan.schemas.execution.command_record import CommandRecording
from artisan.schemas.orchestration.step_lifecycle import CancellationStatus
from artisan.schemas.specs.input_models import ExecuteInput

_URL = "https://endpoint.example"
_CALL_ID = "fc-existing"


@dataclass
class _Clock:
    now: float = 0.0
    waits: list[float] = field(default_factory=list)

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        assert seconds >= 0
        self.waits.append(seconds)
        self.now += seconds


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _Clock:
    clock = _Clock()
    monkeypatch.setattr(
        client_mod,
        "time",
        SimpleNamespace(monotonic=clock.monotonic, sleep=clock.sleep),
    )
    return clock


def _done() -> httpx.Response:
    contract = command_contract(WaitTool)
    manifest = ToolManifest(
        command_contract=contract,
        command_recording=CommandRecording.empty(),
        operation_identity=contract.operation_identity,
        debug_capture=None,
    )
    return httpx.Response(
        200, json={"status": "done", "manifest": manifest.model_dump(mode="json")}
    )


def _client(
    responses: list[httpx.Response | Exception], requests: list[httpx.Request]
) -> httpx.Client:
    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        response = responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    return httpx.Client(base_url=_URL, transport=httpx.MockTransport(handle))


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_poll_transient_failure_pending_done_retains_call(
    status: int, clock: _Clock
) -> None:
    requests: list[httpx.Request] = []
    responses = [
        httpx.Response(status),
        httpx.Response(200, json={"status": "pending", "manifest": None}),
        _done(),
    ]
    with _client(responses, requests) as client:
        manifest, acknowledgement = client_mod._poll(client, _CALL_ID, 2.0, "wait_tool")

    assert manifest.operation_identity == command_contract(WaitTool).operation_identity
    assert acknowledgement is None
    assert clock.waits == [1.0, 2.0]
    assert len(requests) == 3
    assert all(request.method == "GET" for request in requests)
    assert all(request.url.path == "/result" for request in requests)
    assert all(request.url.params["call_id"] == _CALL_ID for request in requests)


@pytest.mark.parametrize(
    "exception_type",
    [
        httpx.ConnectTimeout,
        httpx.ReadTimeout,
        httpx.WriteTimeout,
        httpx.PoolTimeout,
        httpx.ConnectError,
        httpx.ReadError,
        httpx.WriteError,
        httpx.RemoteProtocolError,
    ],
)
def test_poll_recovers_selected_transport_errors(
    exception_type: type[httpx.TransportError], clock: _Clock
) -> None:
    requests: list[httpx.Request] = []
    with _client([exception_type("temporary failure"), _done()], requests) as client:
        client_mod._poll(client, _CALL_ID, 2.0, "wait_tool")

    assert len(requests) == 2
    assert clock.waits == [1.0]


def test_poll_recovery_backoff_is_capped(clock: _Clock) -> None:
    requests: list[httpx.Request] = []
    with _client(
        [*[httpx.Response(503) for _ in range(7)], _done()], requests
    ) as client:
        client_mod._poll(client, _CALL_ID, 2.0, "wait_tool")

    assert clock.waits == [1.0, 2.0, 4.0, 8.0, 10.0, 10.0, 10.0]


def test_poll_recovery_budget_includes_first_failed_request(
    monkeypatch: pytest.MonkeyPatch, clock: _Clock
) -> None:
    monkeypatch.setattr(client_mod, "_RESULT_RECOVERY_SECONDS", 5.0)
    starts: list[float] = []

    def handle(request: httpx.Request) -> httpx.Response:
        starts.append(clock.now)
        clock.now += 2.0
        return httpx.Response(503)

    with (
        httpx.Client(base_url=_URL, transport=httpx.MockTransport(handle)) as client,
        pytest.raises(ArtisanError) as caught,
    ):
        client_mod._poll(client, _CALL_ID, 2.0, "wait_tool")

    assert starts == [0.0, 3.0]
    assert clock.now == 5.0
    assert clock.waits == [1.0]
    assert caught.value.code == ErrorCode.OP_EXECUTE_FAILED
    assert all(part in str(caught.value) for part in ("GET /result", _CALL_ID, "503"))


def test_poll_valid_pending_resets_recovery_window(
    monkeypatch: pytest.MonkeyPatch, clock: _Clock
) -> None:
    monkeypatch.setattr(client_mod, "_RESULT_RECOVERY_SECONDS", 3.0)
    requests: list[httpx.Request] = []
    responses = [
        httpx.Response(500),
        httpx.Response(200, json={"status": "pending", "manifest": None}),
        httpx.Response(500),
        _done(),
    ]
    with _client(responses, requests) as client:
        client_mod._poll(client, _CALL_ID, 50.0, "wait_tool")

    assert clock.waits == [1.0, 50.0, 1.0]


def test_poll_recovery_request_timeout_uses_remaining_budget(
    monkeypatch: pytest.MonkeyPatch, clock: _Clock
) -> None:
    monkeypatch.setattr(client_mod, "_RESULT_RECOVERY_SECONDS", 3.0)
    requests: list[httpx.Request] = []
    with (
        _client([httpx.Response(503), httpx.Response(503)], requests) as client,
        pytest.raises(ArtisanError),
    ):
        client_mod._poll(client, _CALL_ID, 2.0, "wait_tool")

    assert len(requests) == 2
    assert set(requests[0].extensions["timeout"].values()) == {120.0}
    assert set(requests[1].extensions["timeout"].values()) == {2.0}
    assert clock.waits == [1.0, 2.0]


def test_poll_transport_exhaustion_retains_last_http_status(
    monkeypatch: pytest.MonkeyPatch, clock: _Clock
) -> None:
    monkeypatch.setattr(client_mod, "_RESULT_RECOVERY_SECONDS", 3.0)
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(503)
        clock.now += 2.0
        message = "private transport detail"
        raise httpx.ReadTimeout(message)

    with (
        httpx.Client(base_url=_URL, transport=httpx.MockTransport(handle)) as client,
        pytest.raises(ArtisanError) as caught,
    ):
        client_mod._poll(client, _CALL_ID, 2.0, "wait_tool")

    assert len(requests) == 2
    assert clock.now == 3.0
    assert all(part in str(caught.value) for part in ("503", "ReadTimeout", _CALL_ID))
    assert "private transport detail" not in str(caught.value.to_dict())
    assert caught.value.__cause__ is None


@pytest.mark.parametrize(
    "status", [301, 307, 400, 401, 403, 404, 407, 408, 422, 429, 501, 505]
)
def test_poll_terminal_http_error_fails_immediately(status: int, clock: _Clock) -> None:
    requests: list[httpx.Request] = []
    with (
        _client(
            [httpx.Response(status, text="private response body")], requests
        ) as client,
        pytest.raises(ArtisanError) as caught,
    ):
        client_mod._poll(client, _CALL_ID, 2.0, "wait_tool")

    assert len(requests) == 1
    assert clock.waits == []
    expected = (
        ErrorCode.TOOL_ENDPOINT_MISCONFIGURED
        if status in (301, 307, 401, 407)
        else ErrorCode.OP_EXECUTE_FAILED
    )
    assert caught.value.code == expected
    assert all(
        part in str(caught.value) for part in ("GET /result", _CALL_ID, str(status))
    )
    assert "private response body" not in str(caught.value)


@pytest.mark.parametrize(
    "exception_type",
    [
        httpx.LocalProtocolError,
        httpx.UnsupportedProtocol,
        httpx.ProxyError,
        httpx.DecodingError,
    ],
)
def test_poll_other_transport_failures_are_terminal(
    exception_type: type[httpx.HTTPError], clock: _Clock
) -> None:
    requests: list[httpx.Request] = []
    with (
        _client([exception_type("private failure detail")], requests) as client,
        pytest.raises(ArtisanError) as caught,
    ):
        client_mod._poll(client, _CALL_ID, 2.0, "wait_tool")

    assert len(requests) == 1
    assert clock.waits == []
    assert "private failure detail" not in str(caught.value.to_dict())
    assert caught.value.__cause__ is None


@pytest.mark.parametrize("chain", ["__cause__", "__context__"])
def test_poll_ssl_failure_is_not_retried(chain: str, clock: _Clock) -> None:
    error = httpx.ConnectError("private certificate diagnostics")
    setattr(error, chain, ssl.SSLCertVerificationError("certificate rejected"))
    requests: list[httpx.Request] = []
    with _client([error], requests) as client, pytest.raises(ArtisanError) as caught:
        client_mod._poll(client, _CALL_ID, 2.0, "wait_tool")

    assert len(requests) == 1
    assert clock.waits == []
    assert caught.value.__cause__ is None
    assert "private certificate diagnostics" not in str(caught.value.to_dict())


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200, text="private invalid JSON"),
        httpx.Response(200, json={"status": "invalid", "manifest": None}),
        httpx.Response(200, json={"status": "done", "manifest": None}),
        httpx.Response(200, json={"status": "expired", "manifest": None}),
    ],
)
def test_poll_invalid_or_expired_result_is_terminal(
    response: httpx.Response, clock: _Clock
) -> None:
    requests: list[httpx.Request] = []
    with _client([response], requests) as client, pytest.raises(ArtisanError) as caught:
        client_mod._poll(client, _CALL_ID, 2.0, "wait_tool")

    assert len(requests) == 1
    assert clock.waits == []
    assert "private invalid JSON" not in str(caught.value.to_dict())
    assert all(
        part in str(caught.value) for part in ("GET /result", _CALL_ID, "HTTP 200")
    )


@pytest.mark.parametrize("status", ["confirmed", "unknown", "requested", "rejected"])
def test_poll_cancellation_interrupts_recovery_with_existing_acknowledgements(
    monkeypatch: pytest.MonkeyPatch, clock: _Clock, status: str
) -> None:
    event = threading.Event()
    waits: list[float] = []
    requests: list[httpx.Request] = []

    def wait(timeout: float | None = None) -> bool:
        assert timeout is not None
        waits.append(timeout)
        assert len(waits) == 1, "an acknowledged cancellation must not spin the backoff"
        event.set()
        return True

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.params["call_id"] == _CALL_ID
        if len(requests) == 1:
            return httpx.Response(500)
        if request.url.path == "/cancel":
            outcome = "confirmed" if len(requests) == 3 else status
            return httpx.Response(200, json={"call_id": _CALL_ID, "status": outcome})
        return httpx.Response(503) if len(requests) == 3 else _done()

    monkeypatch.setattr(event, "wait", wait)
    with (
        httpx.Client(base_url=_URL, transport=httpx.MockTransport(handle)) as client,
        cancel_scope(event),
    ):
        if status == "rejected":
            _, acknowledgement = client_mod._poll(client, _CALL_ID, 2.0, "wait_tool")
            assert acknowledgement is not None
            assert acknowledgement.status is CancellationStatus.REJECTED
            assert clock.waits == [2.0]
        else:
            with pytest.raises(EndpointCancellationError) as caught:
                client_mod._poll(client, _CALL_ID, 2.0, "wait_tool")
            expected = "confirmed" if status == "requested" else status
            assert caught.value.acknowledgement.status.value == expected

    posts = [request for request in requests if request.method == "POST"]
    assert len(posts) == (2 if status == "requested" else 1)
    assert waits == [1.0]


def test_poll_cancellation_transport_failure_is_not_retried(
    clock: _Clock,
) -> None:
    event = threading.Event()
    event.set()
    requests: list[httpx.Request] = []
    with (
        _client([httpx.ReadTimeout("cancel unavailable")], requests) as client,
        cancel_scope(event),
        pytest.raises(EndpointCancellationError) as caught,
    ):
        client_mod._poll(client, _CALL_ID, 2.0, "wait_tool")

    assert [(request.method, request.url.path) for request in requests] == [
        ("POST", "/cancel")
    ]
    assert caught.value.acknowledgement.status is CancellationStatus.UNKNOWN
    assert clock.waits == []


@pytest.mark.parametrize("status", ["confirmed", "unknown"])
def test_poll_cancellation_during_failed_request_precedes_budget_exhaustion(
    monkeypatch: pytest.MonkeyPatch, clock: _Clock, status: str
) -> None:
    monkeypatch.setattr(client_mod, "_RESULT_RECOVERY_SECONDS", 3.0)
    event = threading.Event()
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/result":
            clock.now += 3.0
            event.set()
            return httpx.Response(503)
        return httpx.Response(200, json={"call_id": _CALL_ID, "status": status})

    with (
        httpx.Client(base_url=_URL, transport=httpx.MockTransport(handle)) as client,
        cancel_scope(event),
        pytest.raises(EndpointCancellationError) as caught,
    ):
        client_mod._poll(client, _CALL_ID, 2.0, "wait_tool")

    assert [request.url.path for request in requests] == ["/result", "/cancel"]
    assert caught.value.acknowledgement.status.value == status
    assert clock.waits == []


@pytest.mark.parametrize(
    "failure", [httpx.Response(503), httpx.ReadError("review-secret")]
)
@pytest.mark.parametrize(
    "secret", ["review-secret", "review\nsecret", "review\\secret"]
)
def test_poll_exhaustion_diagnostics_redact_credentials_and_exception_chain(
    monkeypatch: pytest.MonkeyPatch,
    clock: _Clock,
    failure: httpx.Response | Exception,
    secret: str,
) -> None:
    monkeypatch.setattr(client_mod, "_RESULT_RECOVERY_SECONDS", 0.5)
    requests: list[httpx.Request] = []
    with (
        capture_commands() as recorder,
        _client([failure, failure], requests) as client,
        pytest.raises(ArtisanError) as caught,
    ):
        recorder.add_sensitive_data(secret)
        client_mod._poll(client, f"fc-{secret}", 2.0, "wait_tool")

    diagnostic = str(caught.value.to_dict())
    assert "review-secret" not in diagnostic
    assert secret not in diagnostic
    assert "fc-<redacted>" in diagnostic
    assert "GET /result" in diagnostic
    assert caught.value.__cause__ is None
    assert clock.now == 0.5


def _endpoint(
    monkeypatch: pytest.MonkeyPatch,
    result: Callable[[httpx.Request], httpx.Response],
    requests: list[httpx.Request],
) -> WaitTool:
    operation = WaitTool()
    operation.compute_provider = operation.compute_provider.model_copy(
        update={
            "active": "modal",
            "modal": operation.compute_provider.modal.model_copy(
                update={"endpoint_url": _URL}
            ),
        }
    )
    contract = command_contract(operation)
    schema = SchemaResponse(
        command_contract=contract,
        operation=operation.name,
        operation_identity=contract.operation_identity,
        debug_capture_supported=True,
    )

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/schema":
            return httpx.Response(200, json=schema.model_dump(mode="json"))
        return result(request)

    client_class = httpx.Client
    monkeypatch.setattr(
        client_mod.httpx,
        "Client",
        lambda **kwargs: client_class(**kwargs, transport=httpx.MockTransport(handle)),
    )
    return operation


@pytest.mark.parametrize(
    "failure", [httpx.Response(503), httpx.ReadTimeout("submit lost")]
)
def test_call_endpoint_does_not_retry_submission(
    monkeypatch: pytest.MonkeyPatch,
    clock: _Clock,
    tmp_path: Path,
    failure: httpx.Response | Exception,
) -> None:
    requests: list[httpx.Request] = []

    def result(request: httpx.Request) -> httpx.Response:
        if isinstance(failure, Exception):
            raise failure
        return failure

    operation = _endpoint(monkeypatch, result, requests)
    with pytest.raises((ArtisanError, httpx.ReadTimeout)):
        client_mod.call_endpoint(
            operation, ExecuteInput(inputs={}, execute_dir=str(tmp_path))
        )

    assert [request.url.path for request in requests] == ["/schema", "/submit"]
    assert clock.waits == []


@pytest.mark.parametrize("invalid", ["recording", "identity", "contract", "worker"])
def test_call_endpoint_recovery_preserves_terminal_evidence_validation(
    monkeypatch: pytest.MonkeyPatch, clock: _Clock, tmp_path: Path, invalid: str
) -> None:
    body: dict[str, Any] = _done().json()
    manifest = body["manifest"]
    if invalid == "recording":
        del manifest["command_recording"]
    elif invalid == "identity":
        manifest["operation_identity"]["name"] = "different-operation"
    elif invalid == "contract":
        manifest["command_contract"]["tool_parts"] = ["other-tool"]
    else:
        manifest["error"] = {
            "code": ErrorCode.OP_EXECUTE_FAILED,
            "error_type": "compute",
            "message": "worker computation failed",
        }
        body["status"] = "failed"
    requests: list[httpx.Request] = []
    responses = [httpx.Response(503), httpx.Response(200, json=body)]

    def result(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/submit":
            return httpx.Response(200, json={"call_id": _CALL_ID})
        return responses.pop(0)

    operation = _endpoint(monkeypatch, result, requests)
    with pytest.raises(ArtisanError):
        client_mod.call_endpoint(
            operation, ExecuteInput(inputs={}, execute_dir=str(tmp_path))
        )

    assert [request.url.path for request in requests] == [
        "/schema",
        "/submit",
        "/result",
        "/result",
    ]
    assert clock.waits == [1.0]
