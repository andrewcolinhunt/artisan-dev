"""Tests for endpoint_spec / EndpointSpec (modal-free by design)."""

from __future__ import annotations

import subprocess
import sys

import pytest
from fixtures.endpoint_ops import (
    DocstringParamsTool,
    FlagTool,
    GpuTool,
    NoModalTool,
    PlainTool,
)
from pydantic import ValidationError

from artisan.execution.tool_endpoint.spec import endpoint_spec
from artisan.operations.examples import CsvHead, DataGenerator, WaitTool
from artisan.registry.schemas import params_schema_for
from artisan.schemas.operation_config.compute import ARTISAN_WORKER_IMAGE
from artisan.schemas.operation_config.endpoint_policy import ToolEndpointDataPolicy


class TestEndpointSpec:
    def test_flattens_class_level_config(self):
        spec = endpoint_spec(WaitTool)
        assert spec.name == "wait_tool"
        assert spec.op_module == WaitTool.__module__
        assert spec.op_qualname == "WaitTool"
        assert spec.image == ARTISAN_WORKER_IMAGE
        # WaitTool pins the overlay explicitly — the field default is []
        assert spec.local_python_sources == ["artisan"]
        assert spec.data_policy == {"input_allowlist": [], "output_allowlist": []}

    def test_bakes_only_normalized_plain_policy_data(self, monkeypatch):
        provider = WaitTool.model_fields["compute_provider"].default.model_copy(
            update={
                "modal": WaitTool.model_fields[
                    "compute_provider"
                ].default.modal.model_copy(
                    update={
                        "data_policy": ToolEndpointDataPolicy(
                            input_allowlist=("S3://BUCKET/read/",),
                            output_allowlist=("HTTPS://RESULTS.EXAMPLE:443/",),
                        )
                    }
                )
            }
        )
        monkeypatch.setattr(
            WaitTool.model_fields["compute_provider"], "default", provider
        )

        assert endpoint_spec(WaitTool).data_policy == {
            "input_allowlist": ["s3://bucket/read"],
            "output_allowlist": ["https://results.example"],
        }

    def test_rejects_default_output_store_outside_baked_policy(self, monkeypatch):
        modal = WaitTool.model_fields["compute_provider"].default.modal.model_copy(
            update={"output_store": "s3://bucket/private"}
        )
        provider = WaitTool.model_fields["compute_provider"].default.model_copy(
            update={"modal": modal}
        )
        monkeypatch.setattr(
            WaitTool.model_fields["compute_provider"], "default", provider
        )

        with pytest.raises(ValueError, match="outside the endpoint data policy"):
            endpoint_spec(WaitTool)

    def test_revalidates_copied_class_default_policy(self, monkeypatch):
        modal = WaitTool.model_fields["compute_provider"].default.modal.model_copy(
            update={"data_policy": {"input_allowlist": ["file:///tmp"]}}
        )
        provider = WaitTool.model_fields["compute_provider"].default.model_copy(
            update={"modal": modal}
        )
        monkeypatch.setattr(
            WaitTool.model_fields["compute_provider"], "default", provider
        )

        with pytest.raises(ValueError, match="class-default.*invalid"):
            endpoint_spec(WaitTool)

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
        with pytest.raises(ValueError, match="not a command op"):
            endpoint_spec(DataGenerator)

    def test_missing_modal_config_raises(self):
        with pytest.raises(ValueError, match="no compute_provider.modal"):
            endpoint_spec(NoModalTool)

    def test_flag_op_accepted_without_toolspec(self):
        """execute_as_tool satisfies the command-op predicate; the nested
        Params schema bakes exactly as for external tool ops."""
        spec = endpoint_spec(FlagTool)
        assert spec.name == "flag_tool_test"
        assert spec.op_qualname == "FlagTool"
        assert set(spec.params_schema["properties"]) == {"batch_size"}
        assert spec.params_schema["additionalProperties"] is False


_DEPLOYABLE = [WaitTool, CsvHead, GpuTool, PlainTool, FlagTool, DocstringParamsTool]


class TestParamsSchemaSingleSource:
    """The wire plane serves the registry's canonical schema — no drift."""

    @pytest.mark.parametrize("op_cls", _DEPLOYABLE, ids=lambda op: op.name)
    def test_matches_canonical_builder(self, op_cls):
        assert endpoint_spec(op_cls).params_schema == params_schema_for(op_cls)

    def test_docstring_only_descriptions_are_served(self):
        # Canonical schema generation merges docstring-only descriptions
        # that Pydantic's model_json_schema would omit.
        schema = endpoint_spec(DocstringParamsTool).params_schema
        assert (
            schema["properties"]["threshold"]["description"]
            == "Minimum score to keep — documented only here."
        )

    def test_parameter_less_op_serves_empty_params_shape(self):
        # Parameterless endpoints publish the same closed empty-object schema
        # enforced by core construction.
        assert endpoint_spec(PlainTool).params_schema == {
            "type": "object",
            "title": "Params",
            "properties": {},
            "additionalProperties": False,
        }


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
