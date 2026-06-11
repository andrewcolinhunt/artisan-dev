"""Tests for endpoint_spec / EndpointSpec (modal-free by design)."""

from __future__ import annotations

import subprocess
import sys

import pytest
from fixtures.endpoint_ops import GpuTool, NoModalTool
from pydantic import ValidationError

from artisan.execution.tool_endpoint.spec import endpoint_spec
from artisan.operations.examples import DataGenerator, WaitTool
from artisan.schemas.operation_config.compute import ARTISAN_WORKER_IMAGE


class TestEndpointSpec:
    def test_flattens_class_level_config(self):
        spec = endpoint_spec(WaitTool)
        assert spec.name == "wait_tool"
        assert spec.op_module == WaitTool.__module__
        assert spec.op_qualname == "WaitTool"
        assert spec.image == ARTISAN_WORKER_IMAGE
        # WaitTool pins the overlay explicitly — the field default is []
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


def test_spec_module_imports_without_modal():
    """The extraction's point: resolving a spec must not require the modal SDK.

    Run in a subprocess — an in-process check is order-dependent on whatever
    earlier tests already imported.
    """
    code = (
        "import sys; "
        "import artisan.execution.tool_endpoint.spec; "
        "assert 'modal' not in sys.modules, 'spec import pulled in modal'"
    )
    subprocess.run([sys.executable, "-c", code], check=True)
