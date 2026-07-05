"""Tests for the tool-endpoint HTTP client (httpx mocked)."""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from artisan.errors import ArtisanError, ErrorCode
from artisan.execution.tool_endpoint import client as client_mod
from artisan.execution.tool_endpoint.client import (
    call_endpoint,
    cancel_scope,
)
from artisan.execution.tool_endpoint.protocol import StoredOutputs, ToolManifest
from artisan.execution.tool_endpoint.transport import InlineTransport
from artisan.operations.examples import WaitTool
from artisan.schemas.operation_config.compute import (
    ComputeProvider,
    ModalComputeConfig,
)
from artisan.schemas.specs.input_models import ExecuteInput

_URL = "https://tool.example"


def _op(**modal_kwargs: Any) -> WaitTool:
    modal_kwargs.setdefault("endpoint_url", _URL)
    modal_kwargs.setdefault("poll_interval", 0.001)
    return WaitTool(
        params=WaitTool.Params(seconds=1),
        compute_provider=ComputeProvider(
            active="modal", modal=ModalComputeConfig(**modal_kwargs)
        ),
    )


def _response(json_data: Any = None, status: int = 200, content: bytes = b""):
    response = MagicMock()
    response.status_code = status
    response.json.return_value = json_data
    response.content = content
    response.text = str(json_data)
    return response


def _tar_payload(tmp_path: Path) -> bytes:
    src = tmp_path / "worker_outputs"
    src.mkdir()
    (src / "out.txt").write_text("hi\n")
    return InlineTransport().pack_outputs(str(src), ["out.txt"])


@pytest.fixture
def mock_http(monkeypatch) -> MagicMock:
    """Patch httpx; returns the client mock entered by the context manager."""
    mock_httpx = MagicMock()
    monkeypatch.setattr(client_mod, "httpx", mock_httpx)
    return mock_httpx


def _client_of(mock_httpx: MagicMock) -> MagicMock:
    return mock_httpx.Client.return_value.__enter__.return_value


class TestCallEndpointHappyPath:
    def test_submit_poll_download(self, mock_http, tmp_path):
        client = _client_of(mock_http)
        client.post.return_value = _response({"call_id": "fc-1"})
        manifest = ToolManifest(output_names=["out.txt"], log_tail="ran fine\n")
        client.get.side_effect = [
            _response({"status": "pending", "manifest": None}),
            _response({"status": "done", "manifest": manifest.model_dump()}),
            _response(content=_tar_payload(tmp_path)),
        ]
        execute_dir = tmp_path / "execute"
        execute_dir.mkdir()
        log_path = tmp_path / "tool_output.log"

        result = call_endpoint(
            _op(),
            ExecuteInput(
                inputs={},
                execute_dir=str(execute_dir),
                log_path=str(log_path),
            ),
        )

        assert result is None
        assert (execute_dir / "out.txt").read_text() == "hi\n"
        assert "ran fine" in log_path.read_text()
        submit_kwargs = client.post.call_args_list[0].kwargs
        assert json.loads(submit_kwargs["data"]["params"])["seconds"] == 1
        mock_http.Client.assert_called_once()
        assert mock_http.Client.call_args.kwargs["base_url"] == _URL

    def test_input_files_packed_inline(self, mock_http, tmp_path):
        client = _client_of(mock_http)
        client.post.return_value = _response({"call_id": "fc-1"})
        client.get.return_value = _response(
            {"status": "done", "manifest": ToolManifest().model_dump()}
        )
        source = tmp_path / "input.pdb"
        source.write_bytes(b"ATOM")

        call_endpoint(
            _op(),
            ExecuteInput(
                inputs={"pdb": str(source), "ref": "s3://bucket/key"},
                execute_dir=str(tmp_path),
            ),
        )

        submit_kwargs = client.post.call_args_list[0].kwargs
        assert submit_kwargs["files"] == [("files", ("pdb", b"ATOM"))]
        assert json.loads(submit_kwargs["data"]["input_uris"]) == {
            "ref": "s3://bucket/key"
        }
        # original filenames ride along so the worker preserves basenames
        assert json.loads(submit_kwargs["data"]["input_filenames"]) == {
            "pdb": "input.pdb",
            "ref": "key",
        }

    def test_per_artifact_list_inputs_unwrap(self, mock_http, tmp_path):
        """Per-artifact dispatch hands roles as one-element lists — accepted."""
        client = _client_of(mock_http)
        client.post.return_value = _response({"call_id": "fc-1"})
        client.get.return_value = _response(
            {"status": "done", "manifest": ToolManifest().model_dump()}
        )
        source = tmp_path / "input.pdb"
        source.write_bytes(b"ATOM")

        call_endpoint(
            _op(),
            ExecuteInput(
                inputs={"pdb": [str(source)]},  # the _split_prepared_inputs shape
                execute_dir=str(tmp_path),
            ),
        )

        submit_kwargs = client.post.call_args_list[0].kwargs
        assert submit_kwargs["files"] == [("files", ("pdb", b"ATOM"))]


class TestStoredOutputs:
    _MANIFEST = ToolManifest(
        output_names=["out.txt"],
        stored=StoredOutputs(
            uri="s3://bucket/prefix/wait_tool/abc.tar.gz",
            presigned_url="https://signed.example/get?sig=x",
        ),
    )

    def test_output_store_sent_when_configured(self, mock_http, tmp_path):
        client = _client_of(mock_http)
        client.post.return_value = _response({"call_id": "fc-1"})
        client.get.return_value = _response(
            {"status": "done", "manifest": ToolManifest().model_dump()}
        )
        call_endpoint(
            _op(output_store="s3://bucket/prefix"),
            ExecuteInput(inputs={}, execute_dir=str(tmp_path)),
        )
        data = client.post.call_args_list[0].kwargs["data"]
        assert data["output_store"] == "s3://bucket/prefix"

    def test_output_store_absent_when_unset(self, mock_http, tmp_path):
        client = _client_of(mock_http)
        client.post.return_value = _response({"call_id": "fc-1"})
        client.get.return_value = _response(
            {"status": "done", "manifest": ToolManifest().model_dump()}
        )
        call_endpoint(_op(), ExecuteInput(inputs={}, execute_dir=str(tmp_path)))
        assert "output_store" not in client.post.call_args_list[0].kwargs["data"]

    def test_stored_manifest_fetched_via_bare_presigned_get(self, mock_http, tmp_path):
        client = _client_of(mock_http)
        client.post.return_value = _response({"call_id": "fc-1"})
        client.get.return_value = _response(
            {"status": "done", "manifest": self._MANIFEST.model_dump()}
        )
        mock_http.get.return_value = _response(content=_tar_payload(tmp_path))
        execute_dir = tmp_path / "execute"
        execute_dir.mkdir()

        call_endpoint(
            _op(output_store="s3://bucket/prefix"),
            ExecuteInput(inputs={}, execute_dir=str(execute_dir)),
        )

        # bare one-shot GET on the module, not the proxy-authenticated
        # client: positional URL, timeout only — no headers ride along
        assert mock_http.get.call_args.args == ("https://signed.example/get?sig=x",)
        assert "headers" not in mock_http.get.call_args.kwargs
        assert (execute_dir / "out.txt").read_text() == "hi\n"
        # /download is never hit — every client.get was a /result poll
        assert all(call.args[0] == "/result" for call in client.get.call_args_list)

    def test_stored_without_presigned_url_fails_fast(self, mock_http, tmp_path):
        manifest = ToolManifest(
            output_names=["out.txt"],
            stored=StoredOutputs(uri="https://their-bucket/run.tar.gz"),
        )
        client = _client_of(mock_http)
        client.post.return_value = _response({"call_id": "fc-1"})
        client.get.return_value = _response(
            {"status": "done", "manifest": manifest.model_dump()}
        )
        with pytest.raises(ArtisanError, match="no presigned URL"):
            call_endpoint(_op(), ExecuteInput(inputs={}, execute_dir=str(tmp_path)))
        mock_http.get.assert_not_called()


class TestCallEndpointFailures:
    def test_worker_envelope_reraised(self, mock_http, tmp_path):
        client = _client_of(mock_http)
        client.post.return_value = _response({"call_id": "fc-1"})
        envelope = ArtisanError(
            code=ErrorCode.OP_EXECUTE_FAILED,
            message="tool exploded",
            error_type="compute",
            operation_name="wait_tool",
        ).envelope
        manifest = ToolManifest(error=envelope, log_tail="boom\n")
        client.get.return_value = _response(
            {"status": "failed", "manifest": manifest.model_dump()}
        )
        log_path = tmp_path / "tool_output.log"

        with pytest.raises(ArtisanError, match="tool exploded") as exc_info:
            call_endpoint(
                _op(),
                ExecuteInput(
                    inputs={},
                    execute_dir=str(tmp_path),
                    log_path=str(log_path),
                ),
            )
        assert exc_info.value.code == "op_execute_failed"
        assert "boom" in log_path.read_text()  # tail lands before the raise

    def test_field_and_suggestions_survive_reraise(self, mock_http, tmp_path):
        # a worker envelope carrying field + suggestions must reach the
        # persisted client-side ArtisanError intact (inspect_failures reads them)
        client = _client_of(mock_http)
        client.post.return_value = _response({"call_id": "fc-1"})
        envelope = ArtisanError(
            code=ErrorCode.PARAM_TYPE_MISMATCH,
            message="bad param",
            error_type="validation",
            operation_name="wait_tool",
            field="params.seconds",
            suggestions=["second", "secs"],
            recovery_hint="CHECK_INPUT",
        ).envelope
        manifest = ToolManifest(error=envelope)
        client.get.return_value = _response(
            {"status": "failed", "manifest": manifest.model_dump()}
        )

        with pytest.raises(ArtisanError) as exc_info:
            call_endpoint(_op(), ExecuteInput(inputs={}, execute_dir=str(tmp_path)))

        assert exc_info.value.envelope.field == "params.seconds"
        assert exc_info.value.envelope.suggestions == ["second", "secs"]

    @pytest.mark.parametrize(
        ("code", "error_type"),
        [
            (ErrorCode.PARAM_TYPE_MISMATCH, "validation"),
            (ErrorCode.INPUT_RESOLUTION_FAILED, "io"),
        ],
    )
    def test_check_input_envelope_propagates(
        self, mock_http, tmp_path, code, error_type
    ):
        # the worker's param/input failures reach the client as CHECK_INPUT
        # envelopes — the code and recovery hint survive the re-raise
        client = _client_of(mock_http)
        client.post.return_value = _response({"call_id": "fc-1"})
        envelope = ArtisanError(
            code=code,
            message="bad call",
            error_type=error_type,
            operation_name="wait_tool",
            recovery_hint="CHECK_INPUT",
        ).envelope
        manifest = ToolManifest(error=envelope)
        client.get.return_value = _response(
            {"status": "failed", "manifest": manifest.model_dump()}
        )

        with pytest.raises(ArtisanError) as exc_info:
            call_endpoint(_op(), ExecuteInput(inputs={}, execute_dir=str(tmp_path)))

        assert exc_info.value.code == code
        assert exc_info.value.envelope.recovery_hint == "CHECK_INPUT"

    def test_delivery_failure_appends_log_and_raises_retry_later(
        self, mock_http, tmp_path
    ):
        # a delivery failure: the tool ran (log_tail + output_names present)
        # but its outputs were not delivered — the client appends the log,
        # raises RETRY_LATER, and never attempts a download
        client = _client_of(mock_http)
        client.post.return_value = _response({"call_id": "fc-1"})
        envelope = ArtisanError(
            code=ErrorCode.OUTPUT_DELIVERY_FAILED,
            message="delivery to s3://bucket/prefix failed",
            error_type="io",
            operation_name="wait_tool",
            recovery_hint="RETRY_LATER",
        ).envelope
        manifest = ToolManifest(
            error=envelope, output_names=["out.txt"], log_tail="ran fine\n"
        )
        client.get.return_value = _response(
            {"status": "failed", "manifest": manifest.model_dump()}
        )
        log_path = tmp_path / "tool_output.log"

        with pytest.raises(ArtisanError) as exc_info:
            call_endpoint(
                _op(),
                ExecuteInput(
                    inputs={},
                    execute_dir=str(tmp_path),
                    log_path=str(log_path),
                ),
            )

        assert exc_info.value.code == "output_delivery_failed"
        assert exc_info.value.envelope.recovery_hint == "RETRY_LATER"
        # the tool's log survives even though its outputs were not delivered
        assert "ran fine" in log_path.read_text()
        # the error short-circuits before the output_names download branch —
        # output_names on an error manifest is purely informational
        mock_http.get.assert_not_called()  # no presigned GET
        assert all(call.args[0] == "/result" for call in client.get.call_args_list)

    def test_expired_result_raises(self, mock_http, tmp_path):
        client = _client_of(mock_http)
        client.post.return_value = _response({"call_id": "fc-1"})
        client.get.return_value = _response({"status": "expired", "manifest": None})

        with pytest.raises(ArtisanError, match="expired"):
            call_endpoint(_op(), ExecuteInput(inputs={}, execute_dir=str(tmp_path)))

    def test_http_error_raises(self, mock_http, tmp_path):
        client = _client_of(mock_http)
        client.post.return_value = _response({"detail": "bad params"}, status=422)

        with pytest.raises(ArtisanError, match="422"):
            call_endpoint(_op(), ExecuteInput(inputs={}, execute_dir=str(tmp_path)))

    def test_non_file_input_raises(self, mock_http, tmp_path):
        with pytest.raises(ArtisanError, match="not a\\s+file path"):
            call_endpoint(
                _op(),
                ExecuteInput(inputs={"n": 3}, execute_dir=str(tmp_path)),
            )
        _client_of(mock_http).post.assert_not_called()

    def test_missing_modal_config_raises(self, mock_http, tmp_path):
        op = WaitTool(
            compute_provider=ComputeProvider(modal=None),
        )
        with pytest.raises(ArtisanError, match="no compute_provider.modal"):
            call_endpoint(op, ExecuteInput(inputs={}, execute_dir=str(tmp_path)))


class TestCancellation:
    def test_cancel_event_posts_cancel_and_raises(self, mock_http, tmp_path):
        client = _client_of(mock_http)
        client.post.return_value = _response({"call_id": "fc-1"})
        event = threading.Event()
        event.set()

        with cancel_scope(event), pytest.raises(RuntimeError, match="cancelled"):
            call_endpoint(_op(), ExecuteInput(inputs={}, execute_dir=str(tmp_path)))

        cancel_calls = [
            c for c in client.post.call_args_list if c.args and c.args[0] == "/cancel"
        ]
        assert len(cancel_calls) == 1
        assert cancel_calls[0].kwargs["params"] == {"call_id": "fc-1"}


class TestTokenDiscovery:
    def test_auth_headers_fall_back_to_dotenv_file(self, tmp_path, monkeypatch):
        from artisan.execution.tool_endpoint.client import _auth_headers

        (tmp_path / ".env").write_text(
            "MODAL_PROXY_TOKEN_ID=wk-file\nMODAL_PROXY_TOKEN_SECRET=ws-file\n"
        )
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("MODAL_PROXY_TOKEN_ID", raising=False)
        monkeypatch.delenv("MODAL_PROXY_TOKEN_SECRET", raising=False)

        assert _auth_headers(None) == {
            "Modal-Key": "wk-file",
            "Modal-Secret": "ws-file",
        }

    def test_no_tokens_artisan_endpoint_raises_locally(
        self, mock_http, monkeypatch, tmp_path
    ):
        """endpoint_url unset + no tokens anywhere → fail before any request."""
        monkeypatch.setattr(client_mod, "env_or_dotenv", lambda _name: None)

        with pytest.raises(ArtisanError, match="no proxy-auth tokens") as exc_info:
            call_endpoint(
                _op(endpoint_url=None),
                ExecuteInput(inputs={}, execute_dir=str(tmp_path)),
            )

        assert exc_info.value.code == "tool_endpoint_misconfigured"
        assert ".env" in (exc_info.value.envelope.hint or "")
        mock_http.Client.assert_not_called()

    def test_no_tokens_external_endpoint_proceeds(
        self, mock_http, monkeypatch, tmp_path
    ):
        """endpoint_url set → missing proxy tokens are not an error."""
        monkeypatch.setattr(client_mod, "env_or_dotenv", lambda _name: None)
        client = _client_of(mock_http)
        client.post.return_value = _response({"call_id": "fc-1"})
        client.get.return_value = _response(
            {"status": "done", "manifest": ToolManifest().model_dump()}
        )

        call_endpoint(_op(), ExecuteInput(inputs={}, execute_dir=str(tmp_path)))

        assert mock_http.Client.call_args.kwargs["headers"] == {}
        client.post.assert_called_once()

    def test_401_raises_actionable_config_error(self, mock_http, tmp_path, monkeypatch):
        monkeypatch.setenv("MODAL_PROXY_TOKEN_ID", "wk-bad")
        monkeypatch.setenv("MODAL_PROXY_TOKEN_SECRET", "ws-bad")
        client = _client_of(mock_http)
        client.post.return_value = _response({"detail": "unauthorized"}, status=401)

        with pytest.raises(ArtisanError, match="rejected authentication") as exc_info:
            call_endpoint(_op(), ExecuteInput(inputs={}, execute_dir=str(tmp_path)))

        assert exc_info.value.code == "tool_endpoint_misconfigured"
        assert exc_info.value.error_type == "config"
        assert "Proxy Auth Tokens" in (exc_info.value.envelope.hint or "")


class TestAuthAndUrl:
    def test_default_proxy_auth_headers_from_env(
        self, mock_http, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("MODAL_PROXY_TOKEN_ID", "wk-id")
        monkeypatch.setenv("MODAL_PROXY_TOKEN_SECRET", "ws-secret")
        client = _client_of(mock_http)
        client.post.return_value = _response({"call_id": "fc-1"})
        client.get.return_value = _response(
            {"status": "done", "manifest": ToolManifest().model_dump()}
        )

        call_endpoint(_op(), ExecuteInput(inputs={}, execute_dir=str(tmp_path)))

        headers = mock_http.Client.call_args.kwargs["headers"]
        assert headers == {"Modal-Key": "wk-id", "Modal-Secret": "ws-secret"}

    def test_auth_secret_prefix_override(self, mock_http, tmp_path, monkeypatch):
        monkeypatch.setenv("MY_AUTH_TOKEN_ID", "id2")
        monkeypatch.setenv("MY_AUTH_TOKEN_SECRET", "secret2")
        client = _client_of(mock_http)
        client.post.return_value = _response({"call_id": "fc-1"})
        client.get.return_value = _response(
            {"status": "done", "manifest": ToolManifest().model_dump()}
        )

        call_endpoint(
            _op(auth_secret="MY_AUTH"),
            ExecuteInput(inputs={}, execute_dir=str(tmp_path)),
        )

        headers = mock_http.Client.call_args.kwargs["headers"]
        assert headers["Modal-Key"] == "id2"

    @patch("modal.Function.from_name")
    def test_url_resolved_from_modal_when_unset(
        self, mock_from_name, mock_http, tmp_path, monkeypatch
    ):
        # tokens present so the local no-tokens guard lets resolution run
        monkeypatch.setenv("MODAL_PROXY_TOKEN_ID", "wk-x")
        monkeypatch.setenv("MODAL_PROXY_TOKEN_SECRET", "ws-x")
        mock_from_name.return_value.get_web_url.return_value = (
            "https://ws--artisan-tool-wait-tool.modal.run"
        )
        client = _client_of(mock_http)
        client.post.return_value = _response({"call_id": "fc-1"})
        client.get.return_value = _response(
            {"status": "done", "manifest": ToolManifest().model_dump()}
        )

        call_endpoint(
            _op(endpoint_url=None),
            ExecuteInput(inputs={}, execute_dir=str(tmp_path)),
        )

        mock_from_name.assert_called_once_with("artisan-tool-wait_tool", "endpoint")
        base_url = mock_http.Client.call_args.kwargs["base_url"]
        assert base_url == "https://ws--artisan-tool-wait-tool.modal.run"
