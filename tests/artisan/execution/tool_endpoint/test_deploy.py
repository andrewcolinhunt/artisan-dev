"""Tests for endpoint_spec / build_app (modal mocked)."""

from __future__ import annotations

from enum import StrEnum, auto
from typing import Any, ClassVar
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel, Field, ValidationError

from artisan.execution.tool_endpoint import deploy as deploy_mod
from artisan.execution.tool_endpoint.deploy import build_app, endpoint_spec
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.examples import DataGenerator, WaitTool
from artisan.schemas.operation_config.compute import (
    ARTISAN_WORKER_IMAGE,
    ComputeProvider,
    ModalComputeConfig,
)
from artisan.schemas.operation_config.compute_resources import ComputeResources
from artisan.schemas.operation_config.tool_spec import ToolSpec
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

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "gpu_tool_test"
    description: ClassVar[str] = "Tool op with hardware spec"
    inputs: ClassVar[dict[str, InputSpec]] = {}
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
    @pytest.fixture
    def mock_modal(self, monkeypatch) -> MagicMock:
        mock = MagicMock()
        monkeypatch.setattr(deploy_mod, "modal", mock)
        return mock

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
