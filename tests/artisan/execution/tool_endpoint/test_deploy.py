"""Tests for build_app (modal mocked)."""

from __future__ import annotations

import asyncio
import inspect
import json
import subprocess
import sys
from types import SimpleNamespace
from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, call, patch

import jsonschema
import modal
import pytest
from fastapi.testclient import TestClient
from fixtures.endpoint_ops import GpuTool, PlainTool

from artisan.execution.tool_endpoint import deploy as deploy_mod
from artisan.execution.tool_endpoint._optional import MODAL_EXTRA_MESSAGE
from artisan.execution.tool_endpoint.deploy import build_app
from artisan.execution.tool_endpoint.protocol import CancelResponse, SchemaResponse
from artisan.execution.tool_endpoint.spec import endpoint_spec
from artisan.operations.examples import DataGenerator, WaitTool
from artisan.registry.schemas import params_schema_for
from artisan.schemas.operation_config.endpoint_policy import ToolEndpointDataPolicy
from artisan.schemas.orchestration.step_lifecycle import CancellationStatus

_PARAMS = json.dumps({"contigs": "10-20"})
"""Minimal valid GpuTool params — /submit schema-validates before anything else."""


@pytest.fixture
def mock_modal(monkeypatch) -> MagicMock:
    mock = MagicMock()
    monkeypatch.setattr(deploy_mod, "import_modal", lambda: mock)
    return mock


class TestOptionalModalDependency:
    def test_tool_endpoint_modules_import_without_modal(self) -> None:
        code = """
import sys

class BlockModal:
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'modal' or fullname.startswith('modal.'):
            raise AssertionError(f'Modal import attempted: {fullname}')
        return None

sys.meta_path.insert(0, BlockModal())
import artisan.execution.tool_endpoint
import artisan.execution.tool_endpoint.client
import artisan.execution.tool_endpoint.deploy
assert 'modal' not in sys.modules
"""
        subprocess.run([sys.executable, "-c", code], check=True)

    def test_build_app_translates_missing_modal(self) -> None:
        missing = ModuleNotFoundError("No module named 'modal'", name="modal")
        original_import = __import__

        def fail_modal_import(name, *args, **kwargs):
            if name == "modal":
                raise missing
            return original_import(name, *args, **kwargs)

        with (
            patch("builtins.__import__", side_effect=fail_modal_import),
            pytest.raises(ImportError) as exc_info,
        ):
            build_app(GpuTool)

        assert str(exc_info.value) == MODAL_EXTRA_MESSAGE
        assert exc_info.value.__cause__ is missing

    def test_build_app_propagates_unrelated_nested_import_failure(self) -> None:
        missing = ModuleNotFoundError(
            "No module named 'modal_dependency'", name="modal_dependency"
        )
        original_import = __import__

        def fail_modal_import(name, *args, **kwargs):
            if name == "modal":
                raise missing
            return original_import(name, *args, **kwargs)

        with (
            patch("builtins.__import__", side_effect=fail_modal_import),
            pytest.raises(ModuleNotFoundError) as exc_info,
        ):
            build_app(GpuTool)

        assert exc_info.value is missing


class TestBuildApp:
    def test_app_name_and_worker_wiring(self, mock_modal: MagicMock):
        build_app(GpuTool)

        mock_modal.App.assert_called_once_with("artisan-tool-gpu_tool_test")
        app = mock_modal.App.return_value
        assert app.function.call_count == 2  # worker + endpoint

        worker_kwargs = app.function.call_args_list[0].kwargs
        assert worker_kwargs["name"] == "worker"
        assert app.function.call_args_list[1].kwargs["name"] == "endpoint"
        assert worker_kwargs["serialized"] is True
        assert worker_kwargs["gpu"] == "A100"
        assert worker_kwargs["memory"] == 8192
        assert worker_kwargs["timeout"] == 600
        assert worker_kwargs["min_containers"] == 1
        assert worker_kwargs["retries"] == 3
        assert "max_containers" not in worker_kwargs  # None → omitted

        mock_modal.concurrent.assert_called_once_with(max_inputs=1)
        # webhook labels allow only [a-z0-9-] — underscores sanitized
        mock_modal.asgi_app.assert_called_once_with(
            label="artisan-tool-gpu-tool-test", requires_proxy_auth=True
        )

    def test_worker_mounts_sources_endpoint_stays_artisan_free(
        self, mock_modal: MagicMock
    ):
        build_app(GpuTool)

        mock_modal.Image.from_registry.assert_called_once()
        worker_chain = mock_modal.Image.from_registry.return_value.env
        worker_chain.assert_called_once_with({})
        worker_chain.return_value.add_local_python_source.assert_called_once_with(
            "artisan"
        )
        # the endpoint container must never import artisan — slim image only
        endpoint_chain = mock_modal.Image.debian_slim.return_value.uv_pip_install
        endpoint_chain.assert_called_once_with("fastapi[standard]", "jsonschema")
        endpoint_chain.return_value.add_local_python_source.assert_not_called()

    def test_empty_default_adds_no_overlay(self, mock_modal: MagicMock):
        """Baked-by-default: a bare config ships no deploy-machine source."""
        build_app(PlainTool)

        env_chain = mock_modal.Image.from_registry.return_value.env
        env_chain.return_value.add_local_python_source.assert_not_called()

    def test_overlay_appends_to_configured_sources(self, mock_modal: MagicMock):
        """--overlay packages append after config sources, deduplicated."""
        build_app(GpuTool, overlay=["mypkg", "artisan"])

        env_chain = mock_modal.Image.from_registry.return_value.env
        env_chain.return_value.add_local_python_source.assert_called_once_with(
            "artisan", "mypkg"
        )

    def test_volumes_and_secrets_resolved_by_name(self, mock_modal: MagicMock):
        build_app(GpuTool)

        mock_modal.Volume.from_name.assert_called_once_with(
            "weights-vol", create_if_missing=True, version=2
        )
        mock_modal.Secret.from_name.assert_called_once_with("hf-read")

    def test_non_tool_op_raises_before_modal(self, mock_modal: MagicMock):
        with pytest.raises(ValueError, match="not a command op"):
            build_app(DataGenerator)
        mock_modal.App.assert_not_called()


class TestBakedPolicyBoundary:
    def test_worker_reconstructs_only_baked_policy(
        self, mock_modal: MagicMock, monkeypatch
    ):
        spec = endpoint_spec(GpuTool).model_copy(
            update={
                "data_policy": {
                    "input_allowlist": ["s3://trusted/inputs"],
                    "output_allowlist": ["https://uploads.example"],
                }
            }
        )
        monkeypatch.setattr(deploy_mod, "endpoint_spec", lambda _op: spec)
        build_app(GpuTool)
        worker_fn = mock_modal.concurrent.return_value.call_args.args[0]
        worker_result = MagicMock()
        worker_result.model_dump.return_value = {"manifest": {}}

        with (
            patch(
                "artisan.registry.resolve.resolve_operation",
                return_value=GpuTool,
            ),
            patch(
                "artisan.execution.tool_endpoint.server.run_tool_request",
                return_value=worker_result,
            ) as run,
        ):
            result = worker_fn(
                {
                    "params": {"contigs": "10-20"},
                    "inputs": [],
                    "output_store": None,
                }
            )

        request = run.call_args.args[1]
        baked = run.call_args.kwargs["data_policy"]
        assert result == {"manifest": {}}
        assert not hasattr(request, "data_policy")
        assert baked == ToolEndpointDataPolicy(
            input_allowlist=("s3://trusted/inputs",),
            output_allowlist=("https://uploads.example",),
        )

    def test_worker_rejects_caller_submitted_policy(
        self, mock_modal: MagicMock, monkeypatch
    ):
        spec = endpoint_spec(GpuTool).model_copy(
            update={
                "data_policy": {
                    "input_allowlist": ["s3://trusted/inputs"],
                    "output_allowlist": [],
                }
            }
        )
        monkeypatch.setattr(deploy_mod, "endpoint_spec", lambda _op: spec)
        build_app(GpuTool)
        worker_fn = mock_modal.concurrent.return_value.call_args.args[0]

        with (
            patch("artisan.execution.tool_endpoint.server.run_tool_request") as run,
            pytest.raises(Exception, match="Extra inputs"),
        ):
            worker_fn(
                {
                    "params": {"contigs": "10-20"},
                    "inputs": [],
                    "output_store": None,
                    "data_policy": {
                        "input_allowlist": ["s3://attacker"],
                        "output_allowlist": ["s3://attacker"],
                    },
                }
            )

        run.assert_not_called()

    def test_submit_route_has_no_policy_field(self, mock_modal: MagicMock):
        build_app(GpuTool)
        endpoint_fn = mock_modal.asgi_app.return_value.call_args.args[0]
        web = endpoint_fn()
        submit = next(route.endpoint for route in web.routes if route.path == "/submit")

        assert "data_policy" not in inspect.signature(submit).parameters
        assert "input_allowlist" not in inspect.signature(submit).parameters
        assert "output_allowlist" not in inspect.signature(submit).parameters

    @pytest.mark.parametrize("size", [-1, True, "1"])
    def test_submit_direct_route_rejects_invalid_integrity_size(
        self, mock_modal: MagicMock, size
    ):
        build_app(GpuTool)
        endpoint_fn = mock_modal.asgi_app.return_value.call_args.args[0]
        web = endpoint_fn()
        submit = next(route.endpoint for route in web.routes if route.path == "/submit")
        integrity = json.dumps(
            {
                "reference": {
                    "content_digest": "a" * 32,
                    "size_bytes": size,
                }
            }
        )

        with pytest.raises(Exception) as exc_info:
            asyncio.run(
                submit(
                    params=_PARAMS,
                    input_uris='{"reference": "s3://bucket/input"}',
                    input_filenames="{}",
                    input_integrity=integrity,
                    output_store="",
                    files=[],
                )
            )

        assert getattr(exc_info.value, "status_code", None) == 422

    def test_submit_direct_route_forwards_landed_integrity_shape(
        self, mock_modal: MagicMock
    ):
        build_app(GpuTool)
        endpoint_fn = mock_modal.asgi_app.return_value.call_args.args[0]
        web = endpoint_fn()
        submit = next(route.endpoint for route in web.routes if route.path == "/submit")
        worker = mock_modal.App.return_value.function.return_value.return_value
        worker.spawn.aio = AsyncMock(return_value=SimpleNamespace(object_id="fc-1"))

        response = asyncio.run(
            submit(
                params=_PARAMS,
                input_uris='{"reference": "s3://bucket/input"}',
                input_filenames='{"reference": "source.bin"}',
                input_integrity=json.dumps(
                    {
                        "reference": {
                            "content_digest": "a" * 32,
                            "size_bytes": 1,
                        }
                    }
                ),
                output_store="",
                files=[],
            )
        )

        assert response == {"call_id": "fc-1"}
        assert worker.spawn.aio.call_args.args[0]["inputs"] == [
            {
                "name": "reference",
                "filename": "source.bin",
                "uri": "s3://bucket/input",
                "data": None,
                "content_digest": "a" * 32,
                "size_bytes": 1,
            }
        ]

    @pytest.mark.parametrize(
        ("input_uris", "input_integrity"),
        [
            ('{"reference": "s3://bucket/input"}', "{}"),
            (
                "{}",
                '{"reference": {"content_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "size_bytes": 1}}',
            ),
            (
                '{"reference": "s3://bucket/input"}',
                '{"reference": {"content_digest": "short", "size_bytes": 1}}',
            ),
            (
                '{"reference": "s3://bucket/input"}',
                '{"reference": {"content_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}',
            ),
        ],
    )
    def test_submit_direct_route_rejects_missing_or_malformed_integrity_map(
        self,
        mock_modal: MagicMock,
        input_uris: str,
        input_integrity: str,
    ):
        build_app(GpuTool)
        endpoint_fn = mock_modal.asgi_app.return_value.call_args.args[0]
        web = endpoint_fn()
        submit = next(route.endpoint for route in web.routes if route.path == "/submit")
        worker = mock_modal.App.return_value.function.return_value.return_value

        with pytest.raises(Exception) as exc_info:
            asyncio.run(
                submit(
                    params=_PARAMS,
                    input_uris=input_uris,
                    input_filenames="{}",
                    input_integrity=input_integrity,
                    output_store="",
                    files=[],
                )
            )

        assert getattr(exc_info.value, "status_code", None) == 422
        worker.spawn.aio.assert_not_called()


class TestEndpointRoutes:
    """Exercise the FastAPI app the endpoint function builds (modal mocked)."""

    @pytest.fixture
    def client(self, mock_modal: MagicMock) -> TestClient:
        build_app(GpuTool)
        # the undecorated endpoint fn is what asgi_app's decorator received;
        # calling it builds the real FastAPI app
        endpoint_fn = mock_modal.asgi_app.return_value.call_args.args[0]
        return TestClient(endpoint_fn())

    @pytest.fixture
    def worker(self, mock_modal: MagicMock) -> MagicMock:
        # the closure's worker handle: the result of app.function(...)(fn)
        return mock_modal.App.return_value.function.return_value.return_value

    def test_schema_serves_baked_contract(self, client: TestClient):
        response = client.get("/schema")
        assert response.status_code == 200
        body = response.json()
        assert body["operation"] == "gpu_tool_test"
        assert body["description"] == "Tool op with hardware spec"
        # the served schema is the registry's canonical one — both agent
        # planes build it through params_schema_for
        assert body["params_schema"] == params_schema_for(GpuTool)
        assert body["inputs"] == {
            "reference": {
                "required": False,
                "description": "Optional reference structure",
            }
        }
        SchemaResponse(**body)  # served dict matches the documented wire shape

    def test_conforming_params_pass_submit(self, client: TestClient, worker: MagicMock):
        """The served schema and the /submit-enforced schema are the same dict."""
        worker.spawn.aio = AsyncMock(return_value=SimpleNamespace(object_id="fc-1"))
        served = client.get("/schema").json()["params_schema"]
        payload = {"contigs": "10-20"}
        jsonschema.validate(payload, served)  # conforms to what /schema served
        response = client.post("/submit", data={"params": json.dumps(payload)})
        assert response.status_code == 200
        assert response.json() == {"call_id": "fc-1"}

    def test_nonconforming_params_rejected(self, client: TestClient):
        response = client.post("/submit", data={"params": json.dumps({"contigs": 1})})
        assert response.status_code == 422

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("input_uris", "[]"),
            ("input_uris", '{"reference": 1}'),
            ("input_filenames", "[]"),
            ("input_filenames", '{"reference": 1}'),
        ],
    )
    def test_submit_rejects_non_string_maps(
        self, client: TestClient, worker: MagicMock, field: str, value: str
    ):
        response = client.post("/submit", data={"params": _PARAMS, field: value})
        assert response.status_code == 422
        assert "mapping strings to strings" in response.json()["detail"]
        worker.spawn.aio.assert_not_called()

    def test_submit_rejects_unknown_role(self, client: TestClient, worker: MagicMock):
        response = client.post(
            "/submit",
            data={"params": _PARAMS, "input_uris": '{"unknown": "s3://b/k"}'},
        )
        assert response.status_code == 422
        assert "unknown input roles" in response.json()["detail"]
        worker.spawn.aio.assert_not_called()

    def test_submit_rejects_duplicate_role_across_planes(
        self, client: TestClient, worker: MagicMock
    ):
        response = client.post(
            "/submit",
            data={
                "params": _PARAMS,
                "input_uris": '{"reference": "s3://b/k"}',
            },
            files=[("files", ("reference", b"inline"))],
        )
        assert response.status_code == 422
        assert "duplicate input roles" in response.json()["detail"]
        worker.spawn.aio.assert_not_called()

    def test_submit_rejects_duplicate_json_role(
        self, client: TestClient, worker: MagicMock
    ):
        response = client.post(
            "/submit",
            data={
                "params": _PARAMS,
                "input_uris": (
                    '{"reference": "s3://b/one", "reference": "s3://b/two"}'
                ),
            },
        )
        assert response.status_code == 422
        assert "duplicate role" in response.json()["detail"]
        worker.spawn.aio.assert_not_called()

    def test_submit_forwards_valid_inline_input(
        self, client: TestClient, worker: MagicMock
    ):
        worker.spawn.aio = AsyncMock(return_value=SimpleNamespace(object_id="fc-1"))
        response = client.post(
            "/submit",
            data={
                "params": _PARAMS,
                "input_filenames": '{"reference": "source.pdb"}',
            },
            files=[("files", ("reference", b"ATOM"))],
        )
        assert response.status_code == 200
        assert worker.spawn.aio.call_args.args[0]["inputs"] == [
            {
                "name": "reference",
                "filename": "source.pdb",
                "uri": None,
                "data": b"ATOM",
            }
        ]

    def test_submit_forwards_output_store(self, client: TestClient, worker: MagicMock):
        worker.spawn.aio = AsyncMock(return_value=SimpleNamespace(object_id="fc-1"))
        response = client.post(
            "/submit",
            data={"params": _PARAMS, "output_store": "s3://bucket/prefix"},
        )
        assert response.status_code == 200
        payload = worker.spawn.aio.call_args.args[0]
        assert payload["output_store"] == "s3://bucket/prefix"

    def test_submit_without_output_store_sends_none(
        self, client: TestClient, worker: MagicMock
    ):
        worker.spawn.aio = AsyncMock(return_value=SimpleNamespace(object_id="fc-1"))
        assert client.post("/submit", data={"params": _PARAMS}).status_code == 200
        assert worker.spawn.aio.call_args.args[0]["output_store"] is None

    def test_schemeless_output_store_rejected(self, client: TestClient):
        response = client.post(
            "/submit", data={"params": _PARAMS, "output_store": "not-a-uri"}
        )
        assert response.status_code == 422
        assert "output_store" in response.json()["detail"]

    def test_concurrent_callers_reach_worker_with_their_own_stores(
        self, client: TestClient, worker: MagicMock
    ):
        """One deployment, per-request destinations — the caller-data criterion."""
        worker.spawn.aio = AsyncMock(return_value=SimpleNamespace(object_id="fc-1"))
        client.post(
            "/submit", data={"params": _PARAMS, "output_store": "s3://team-a/runs"}
        )
        client.post(
            "/submit", data={"params": _PARAMS, "output_store": "s3://team-b/other"}
        )
        stores = [
            call.args[0]["output_store"] for call in worker.spawn.aio.call_args_list
        ]
        assert stores == ["s3://team-a/runs", "s3://team-b/other"]

    def test_missing_required_role_rejected_before_dispatch(
        self, mock_modal: MagicMock
    ):
        build_app(WaitTool)
        endpoint_fn = mock_modal.asgi_app.return_value.call_args.args[0]
        client = TestClient(endpoint_fn())
        worker = mock_modal.App.return_value.function.return_value.return_value

        response = client.post("/submit", data={"params": json.dumps({"seconds": 1})})

        assert response.status_code == 422
        assert "missing required input roles" in response.json()["detail"]
        worker.spawn.aio.assert_not_called()

    def test_inline_uploads_enforce_aggregate_limit(
        self, mock_modal: MagicMock, monkeypatch
    ):
        spec = endpoint_spec(GpuTool).model_copy(
            update={
                "input_roles": {
                    "left": {"required": True, "description": ""},
                    "right": {"required": True, "description": ""},
                }
            }
        )
        monkeypatch.setattr(deploy_mod, "endpoint_spec", lambda _op: spec)
        monkeypatch.setattr(deploy_mod, "MAX_INLINE_BYTES", 4)
        build_app(GpuTool)
        endpoint_fn = mock_modal.asgi_app.return_value.call_args.args[0]
        client = TestClient(endpoint_fn())
        worker = mock_modal.App.return_value.function.return_value.return_value

        response = client.post(
            "/submit",
            data={"params": _PARAMS},
            files=[
                ("files", ("left", b"abc")),
                ("files", ("right", b"de")),
            ],
        )

        assert response.status_code == 413
        assert "4-byte aggregate limit" in response.json()["detail"]
        worker.spawn.aio.assert_not_called()


class TestRetainedResultRoutes:
    """/result and /download against a mocked retained FunctionCall result."""

    STORED: ClassVar[dict[str, str]] = {
        "uri": "s3://bucket/prefix/my_op/abc.tar.gz",
        "presigned_url": "https://signed.example/get?sig=x",
    }

    @pytest.fixture
    def client(self, mock_modal: MagicMock) -> TestClient:
        build_app(GpuTool)
        endpoint_fn = mock_modal.asgi_app.return_value.call_args.args[0]
        return TestClient(endpoint_fn())

    def _retain(self, monkeypatch, raw: dict) -> None:
        # the endpoint body imports the real `modal` at app build —
        # patch its FunctionCall lookup, not the deploy-module mock
        fc = MagicMock()
        fc.get.return_value = raw
        monkeypatch.setattr("modal.FunctionCall.from_id", lambda call_id: fc)

    def _raise(self, monkeypatch, exc: Exception) -> None:
        fc = MagicMock()
        fc.get.side_effect = exc
        monkeypatch.setattr("modal.FunctionCall.from_id", lambda call_id: fc)

    def test_pending_modal_timeout_returns_pending(self, client, monkeypatch):
        self._raise(monkeypatch, modal.exception.TimeoutError())
        body = client.get("/result", params={"call_id": "fc-1"}).json()
        assert body == {"status": "pending", "manifest": None}

    def test_pending_builtin_timeout_returns_pending(self, client, monkeypatch):
        self._raise(monkeypatch, TimeoutError())
        body = client.get("/result", params={"call_id": "fc-1"}).json()
        assert body == {"status": "pending", "manifest": None}

    def test_expired_modal_timeout_returns_expired(self, client, monkeypatch):
        self._raise(monkeypatch, modal.exception.OutputExpiredError())
        body = client.get("/result", params={"call_id": "fc-1"}).json()
        assert body == {"status": "expired", "manifest": None}

    @pytest.mark.parametrize(
        "exc",
        [
            modal.exception.FunctionTimeoutError("worker timed out"),
            modal.exception.RemoteError("worker failed"),
        ],
    )
    def test_terminal_modal_error_returns_failed(self, client, monkeypatch, exc):
        self._raise(monkeypatch, exc)
        body = client.get("/result", params={"call_id": "fc-1"}).json()
        assert body == {"status": "failed", "manifest": None}

    def test_result_carries_stored_pointer_untouched(self, client, monkeypatch):
        manifest = {"output_names": ["a.txt"], "stored": self.STORED, "error": None}
        self._retain(monkeypatch, {"manifest": manifest, "output_tar": None})
        body = client.get("/result", params={"call_id": "fc-1"}).json()
        assert body["status"] == "done"
        assert body["manifest"]["stored"] == self.STORED

    def test_download_redirects_to_presigned_url(self, client, monkeypatch):
        manifest = {"output_names": ["a.txt"], "stored": self.STORED}
        self._retain(monkeypatch, {"manifest": manifest, "output_tar": None})
        response = client.get(
            "/download", params={"call_id": "fc-1"}, follow_redirects=False
        )
        assert response.status_code == 307
        assert response.headers["location"] == self.STORED["presigned_url"]

    def test_download_409_when_caller_owns_destination(self, client, monkeypatch):
        stored = {"uri": "https://their-bucket/run.tar.gz", "presigned_url": None}
        self._retain(
            monkeypatch,
            {"manifest": {"output_names": ["a.txt"], "stored": stored}},
        )
        response = client.get("/download", params={"call_id": "fc-1"})
        assert response.status_code == 409
        assert "caller-supplied" in response.json()["detail"]

    def test_download_still_streams_inline_tar(self, client, monkeypatch):
        manifest = {"output_names": ["a.txt"], "stored": None}
        self._retain(monkeypatch, {"manifest": manifest, "output_tar": b"tarbytes"})
        response = client.get("/download", params={"call_id": "fc-1"})
        assert response.status_code == 200
        assert response.content == b"tarbytes"

    def test_diagnostic_download_streams_separate_plane(self, client, monkeypatch):
        self._retain(
            monkeypatch,
            {
                "manifest": {
                    "stored": None,
                    "debug_capture": {"status": "complete", "stored": None},
                },
                "output_tar": b"outputs",
                "debug_tar": b"diagnostics",
            },
        )
        response = client.get(
            "/download", params={"call_id": "fc-1", "plane": "diagnostics"}
        )
        assert response.status_code == 200
        assert response.content == b"diagnostics"
        assert client.get("/download", params={"call_id": "fc-1"}).content == b"outputs"

    def test_diagnostic_download_redirects_its_own_pointer(self, client, monkeypatch):
        self._retain(
            monkeypatch,
            {
                "manifest": {
                    "stored": None,
                    "debug_capture": {"status": "complete", "stored": self.STORED},
                },
                "output_tar": b"outputs",
                "debug_tar": None,
            },
        )
        response = client.get(
            "/download",
            params={"call_id": "fc-1", "plane": "diagnostics"},
            follow_redirects=False,
        )
        assert response.status_code == 307
        assert response.headers["location"] == self.STORED["presigned_url"]

    @pytest.mark.parametrize("status", ["failed", "unavailable"])
    def test_incomplete_capture_is_not_downloadable(self, client, monkeypatch, status):
        self._retain(
            monkeypatch,
            {"manifest": {"debug_capture": {"status": status}}, "debug_tar": None},
        )
        assert (
            client.get(
                "/download", params={"call_id": "fc-1", "plane": "diagnostics"}
            ).status_code
            == 404
        )

    def test_download_rejects_unknown_plane(self, client):
        assert (
            client.get(
                "/download", params={"call_id": "fc-1", "plane": "anything"}
            ).status_code
            == 422
        )


class TestCancellationRoute:
    """Exercise cancellation without the environment's blocked TestClient."""

    @pytest.fixture
    def cancel_endpoint(self, mock_modal: MagicMock):
        build_app(GpuTool)
        endpoint_fn = mock_modal.asgi_app.return_value.call_args.args[0]
        web = endpoint_fn()
        return next(route.endpoint for route in web.routes if route.path == "/cancel")

    def test_pending_call_returns_confirmed_for_same_call_id(
        self, cancel_endpoint, monkeypatch
    ):
        function_call = MagicMock()
        function_call.get.side_effect = modal.exception.TimeoutError()
        lookup = MagicMock(return_value=function_call)
        monkeypatch.setattr(modal.FunctionCall, "from_id", lookup)

        response = CancelResponse(**cancel_endpoint(call_id="fc-1"))

        assert response.call_id == "fc-1"
        assert response.status is CancellationStatus.CONFIRMED
        assert lookup.call_args_list == [call("fc-1"), call("fc-1")]
        function_call.cancel.assert_called_once_with(terminate_containers=True)

    def test_completed_call_returns_rejected_without_cancelling(
        self, cancel_endpoint, monkeypatch
    ):
        function_call = MagicMock()
        function_call.get.return_value = {"manifest": {}, "output_tar": None}
        monkeypatch.setattr(
            modal.FunctionCall, "from_id", lambda _call_id: function_call
        )

        response = CancelResponse(**cancel_endpoint(call_id="fc-finished"))

        assert response.call_id == "fc-finished"
        assert response.status is CancellationStatus.REJECTED
        function_call.cancel.assert_not_called()

    def test_cancellation_error_returns_unknown_for_same_call_id(
        self, cancel_endpoint, monkeypatch
    ):
        function_call = MagicMock()
        function_call.get.side_effect = modal.exception.TimeoutError()
        function_call.cancel.side_effect = RuntimeError("lost acknowledgement")
        monkeypatch.setattr(
            modal.FunctionCall, "from_id", lambda _call_id: function_call
        )

        response = CancelResponse(**cancel_endpoint(call_id="fc-unknown"))

        assert response.call_id == "fc-unknown"
        assert response.status is CancellationStatus.UNKNOWN
        assert response.message == "Cancellation failed: RuntimeError"


class TestParameterlessSubmit:
    """A parameter-less op bakes and enforces the closed empty-object schema.

    Both non-object bodies and nonempty objects fail before worker dispatch.
    """

    @pytest.fixture
    def routes(self, mock_modal: MagicMock) -> tuple[Any, Any, MagicMock]:
        build_app(PlainTool)
        endpoint_fn = mock_modal.asgi_app.return_value.call_args.args[0]
        web = endpoint_fn()
        schema = next(route.endpoint for route in web.routes if route.path == "/schema")
        submit = next(route.endpoint for route in web.routes if route.path == "/submit")
        worker = mock_modal.App.return_value.function.return_value.return_value
        return schema, submit, worker

    def test_schema_serves_closed_empty_object_shape(self, routes) -> None:
        schema, _, _ = routes

        assert schema()["params_schema"] == {
            "type": "object",
            "title": "Params",
            "properties": {},
            "additionalProperties": False,
        }

    def test_empty_params_still_validates(self, routes) -> None:
        _, submit, worker = routes
        worker.spawn.aio = AsyncMock(return_value=SimpleNamespace(object_id="fc-1"))

        result = asyncio.run(
            submit(
                params="{}",
                input_uris="{}",
                input_filenames="{}",
                input_integrity="{}",
                output_store="",
                files=[],
            )
        )

        assert result == {"call_id": "fc-1"}

    @pytest.mark.parametrize("params", ['{"unexpected": 1}', "[]"])
    def test_nonempty_or_nonobject_params_rejected_before_dispatch(
        self, routes, params: str
    ) -> None:
        _, submit, worker = routes

        with pytest.raises(Exception) as exc_info:
            asyncio.run(
                submit(
                    params=params,
                    input_uris="{}",
                    input_filenames="{}",
                    input_integrity="{}",
                    output_store="",
                    files=[],
                )
            )

        assert getattr(exc_info.value, "status_code", None) == 422
        worker.spawn.aio.assert_not_called()

    def test_non_object_body_rejected_without_schema(
        self, mock_modal: MagicMock, monkeypatch
    ):
        spec = endpoint_spec(PlainTool).model_copy(update={"params_schema": {}})
        monkeypatch.setattr(deploy_mod, "endpoint_spec", lambda _op: spec)
        build_app(PlainTool)
        endpoint_fn = mock_modal.asgi_app.return_value.call_args.args[0]
        web = endpoint_fn()
        submit = next(route.endpoint for route in web.routes if route.path == "/submit")
        worker = mock_modal.App.return_value.function.return_value.return_value

        with pytest.raises(Exception) as exc_info:
            asyncio.run(
                submit(
                    params="[]",
                    input_uris="{}",
                    input_filenames="{}",
                    input_integrity="{}",
                    output_store="",
                    files=[],
                )
            )

        assert getattr(exc_info.value, "status_code", None) == 422
        assert getattr(exc_info.value, "detail", None) == "params must be a JSON object"
        worker.spawn.aio.assert_not_called()
