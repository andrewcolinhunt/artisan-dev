"""Tests for endpoint_spec / build_app (modal mocked)."""

from __future__ import annotations

import json
from enum import StrEnum, auto
from types import SimpleNamespace
from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock

import jsonschema
import pytest
from fastapi.testclient import TestClient
from pydantic import BaseModel, Field, ValidationError

from artisan.execution.tool_endpoint import deploy as deploy_mod
from artisan.execution.tool_endpoint.deploy import build_app, endpoint_spec
from artisan.execution.tool_endpoint.protocol import SchemaResponse
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.examples import DataGenerator, WaitTool
from artisan.schemas.operation_config.compute import (
    ARTISAN_WORKER_IMAGE,
    ComputeProvider,
    ModalComputeConfig,
)
from artisan.schemas.operation_config.compute_resources import ComputeResources
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_models import PreprocessInput
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

_OUTPUTS: dict[str, OutputSpec] = {
    "output": OutputSpec(
        artifact_type="data",
        infer_lineage_from={"inputs": []},
    ),
}


class GpuTool(OperationDefinition):
    """Tool op with a hardware spec and a required param."""

    class InputRole(StrEnum):
        reference = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "gpu_tool_test"
    description: ClassVar[str] = "Tool op with hardware spec"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.reference: InputSpec(
            artifact_type="data",
            required=False,
            description="Optional reference structure",
        ),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = _OUTPUTS

    class Params(BaseModel):
        """Parameters for GpuTool."""

        contigs: str = Field(description="Required — proves deploy never instantiates.")

    params: Params

    tool: ToolSpec = ToolSpec(executable="bash", interpreter=None)
    compute_provider: ComputeProvider = ComputeProvider(
        modal=ModalComputeConfig(
            volumes={"/weights": "weights-vol"},
            secrets=["hf-read"],
            min_containers=1,
        )
    )
    compute_resources: ComputeResources = ComputeResources(
        gpu="A100", memory_gb=8, timeout=600
    )

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {}

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        return [*self.tool.parts(), "-c", "true"]


class NoModalTool(OperationDefinition):
    """Tool op without a modal config — not deployable."""

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "no_modal_tool_test"
    description: ClassVar[str] = "Tool op without modal config"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = _OUTPUTS

    tool: ToolSpec = ToolSpec(executable="bash", interpreter=None)

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        return [*self.tool.parts(), "-c", "true"]


@pytest.fixture
def mock_modal(monkeypatch) -> MagicMock:
    mock = MagicMock()
    monkeypatch.setattr(deploy_mod, "modal", mock)
    return mock


class TestEndpointSpec:
    def test_flattens_class_level_config(self):
        spec = endpoint_spec(WaitTool)
        assert spec.name == "wait_tool"
        assert spec.op_module == WaitTool.__module__
        assert spec.op_qualname == "WaitTool"
        assert spec.image == ARTISAN_WORKER_IMAGE
        assert spec.local_python_sources == ["artisan"]

    def test_bakes_params_json_schema(self):
        """Boundary validation uses the baked schema — no artisan on the endpoint."""
        spec = endpoint_spec(WaitTool)
        assert set(spec.params_schema["properties"]) == {"seconds"}
        assert spec.params_schema["additionalProperties"] is False  # extra="forbid"

    def test_bakes_description_and_input_roles(self):
        spec = endpoint_spec(WaitTool)
        assert spec.description == WaitTool.description
        assert spec.input_roles == {
            "dataset": {
                "required": True,
                "description": "Artifacts to fan out over — one tool run per artifact",
            }
        }
        # plain str keys — StrEnum roles must not leak into the baked closure
        assert all(type(role) is str for role in spec.input_roles)

    def test_optional_input_role_baked_as_not_required(self):
        spec = endpoint_spec(GpuTool)
        assert spec.input_roles == {
            "reference": {
                "required": False,
                "description": "Optional reference structure",
            }
        }

    def test_hardware_from_compute_resources(self):
        spec = endpoint_spec(GpuTool)
        assert spec.gpu == "A100"
        assert spec.memory_mb == 8192
        assert spec.timeout == 600
        assert spec.volumes == {"/weights": "weights-vol"}
        assert spec.secrets == ["hf-read"]
        assert spec.min_containers == 1

    def test_never_instantiates_op(self):
        with pytest.raises(ValidationError):
            GpuTool()  # required param — bare instantiation raises
        endpoint_spec(GpuTool)  # but the spec reads class-level defaults

    def test_non_tool_op_raises(self):
        with pytest.raises(ValueError, match="not a tool op"):
            endpoint_spec(DataGenerator)

    def test_missing_modal_config_raises(self):
        with pytest.raises(ValueError, match="no compute_provider.modal"):
            endpoint_spec(NoModalTool)


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

    def test_volumes_and_secrets_resolved_by_name(self, mock_modal: MagicMock):
        build_app(GpuTool)

        mock_modal.Volume.from_name.assert_called_once_with(
            "weights-vol", create_if_missing=True, version=2
        )
        mock_modal.Secret.from_name.assert_called_once_with("hf-read")

    def test_non_tool_op_raises_before_modal(self, mock_modal: MagicMock):
        with pytest.raises(ValueError, match="not a tool op"):
            build_app(DataGenerator)
        mock_modal.App.assert_not_called()


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
        assert body["params_schema"] == GpuTool.Params.model_json_schema()
        assert body["inputs"] == {
            "reference": {
                "required": False,
                "description": "Optional reference structure",
            }
        }
        SchemaResponse(**body)  # served dict matches the documented wire shape

    def test_conforming_params_pass_submit(
        self, client: TestClient, worker: MagicMock
    ):
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
