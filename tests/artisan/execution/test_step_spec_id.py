"""Tests for compute_step_spec_id hashing function."""

from __future__ import annotations

from enum import StrEnum, auto
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.engine.step_executor import instantiate_operation
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.utils.hashing import compute_step_spec_id


class TestComputeStepSpecId:
    """Tests for step_spec_id computation."""

    def test_deterministic(self):
        """Same inputs produce same spec_id."""
        spec1 = compute_step_spec_id(
            operation_name="ToolC",
            step_number=1,
            params={"model": "v2"},
            input_spec={"data": ("abc123", "data")},
        )
        spec2 = compute_step_spec_id(
            operation_name="ToolC",
            step_number=1,
            params={"model": "v2"},
            input_spec={"data": ("abc123", "data")},
        )
        assert spec1 == spec2
        assert len(spec1) == 32

    def test_upstream_change_cascades(self):
        """Different upstream spec_id produces different downstream."""
        spec1 = compute_step_spec_id(
            operation_name="ToolB",
            step_number=2,
            params=None,
            input_spec={"data": ("upstream_v1", "data")},
        )
        spec2 = compute_step_spec_id(
            operation_name="ToolB",
            step_number=2,
            params=None,
            input_spec={"data": ("upstream_v2", "data")},
        )
        assert spec1 != spec2

    def test_param_change(self):
        """Different params produce different spec_id."""
        spec1 = compute_step_spec_id(
            operation_name="ToolC",
            step_number=1,
            params={"model": "v1"},
            input_spec={"data": ("abc123", "data")},
        )
        spec2 = compute_step_spec_id(
            operation_name="ToolC",
            step_number=1,
            params={"model": "v2"},
            input_spec={"data": ("abc123", "data")},
        )
        assert spec1 != spec2

    def test_role_matters(self):
        """Same upstream but different role produces different spec_id."""
        spec1 = compute_step_spec_id(
            operation_name="Score",
            step_number=2,
            params=None,
            input_spec={"data": ("abc123", "data")},
        )
        spec2 = compute_step_spec_id(
            operation_name="Score",
            step_number=2,
            params=None,
            input_spec={"data": ("abc123", "scored")},
        )
        assert spec1 != spec2

    def test_step_number_matters(self):
        """Same operation at different positions produces different spec_id."""
        spec1 = compute_step_spec_id(
            operation_name="Score",
            step_number=1,
            params=None,
            input_spec={"data": ("abc123", "data")},
        )
        spec2 = compute_step_spec_id(
            operation_name="Score",
            step_number=3,
            params=None,
            input_spec={"data": ("abc123", "data")},
        )
        assert spec1 != spec2

    def test_empty_inputs(self):
        """Generative ops with no inputs produce valid spec_id."""
        spec = compute_step_spec_id(
            operation_name="Generate",
            step_number=0,
            params={"count": 10},
            input_spec={},
        )
        assert len(spec) == 32

    def test_none_params(self):
        """None params produces same spec_id as empty dict."""
        spec_none = compute_step_spec_id(
            operation_name="Op",
            step_number=0,
            params=None,
            input_spec={},
        )
        spec_empty = compute_step_spec_id(
            operation_name="Op",
            step_number=0,
            params={},
            input_spec={},
        )
        assert spec_none == spec_empty

    def test_config_overrides_none_same_as_empty(self):
        """config_overrides=None produces same spec_id as empty dict."""
        spec_none = compute_step_spec_id(
            operation_name="Op",
            step_number=0,
            params=None,
            input_spec={},
            config_overrides=None,
        )
        spec_empty = compute_step_spec_id(
            operation_name="Op",
            step_number=0,
            params=None,
            input_spec={},
            config_overrides={},
        )
        assert spec_none == spec_empty

    def test_config_overrides_changes_hash(self):
        """Different config_overrides produce different spec_id."""
        spec1 = compute_step_spec_id(
            operation_name="ToolC",
            step_number=1,
            params=None,
            input_spec={"data": ("abc123", "data")},
            config_overrides=None,
        )
        spec2 = compute_step_spec_id(
            operation_name="ToolC",
            step_number=1,
            params=None,
            input_spec={"data": ("abc123", "data")},
            config_overrides={"image": "/path/to/image.sif"},
        )
        assert spec1 != spec2

    def test_config_overrides_deterministic(self):
        """Same config_overrides produce same spec_id."""
        kwargs = {
            "operation_name": "ToolC",
            "step_number": 1,
            "params": None,
            "input_spec": {"data": ("abc123", "data")},
            "config_overrides": {"image": "/path/to/image.sif", "gpu": True},
        }
        assert compute_step_spec_id(**kwargs) == compute_step_spec_id(**kwargs)

    def test_config_overrides_with_path_objects(self):
        """Path objects in config_overrides serialize correctly."""
        from pathlib import Path

        spec1 = compute_step_spec_id(
            operation_name="ToolC",
            step_number=1,
            params=None,
            input_spec={},
            config_overrides={"image": Path("/opt/containers/tool_c.sif")},
        )
        spec2 = compute_step_spec_id(
            operation_name="ToolC",
            step_number=1,
            params=None,
            input_spec={},
            config_overrides={"image": Path("/opt/containers/tool_c.sif")},
        )
        assert spec1 == spec2
        assert len(spec1) == 32


class _ParamsV1(BaseModel):
    temperature: float = Field(default=0.5)
    max_steps: int = Field(default=100)


class _ParamsV2(BaseModel):
    temperature: float = Field(default=0.8)
    max_steps: int = Field(default=100)


class _OpV1(OperationDefinition):
    name = "test_op"
    description = "Test operation v1"
    inputs: ClassVar[dict[str, InputSpec]] = {}

    class OutputRole(StrEnum):
        result = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        "result": OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            infer_lineage_from={"inputs": []},
        ),
    }
    params: _ParamsV1 = _ParamsV1()

    def execute(self, inputs: Any) -> dict[str, Any]:
        return {}

    def postprocess(self, inputs: Any) -> Any:
        return None


class _OpV2(OperationDefinition):
    name = "test_op"
    description = "Test operation v2"
    inputs: ClassVar[dict[str, InputSpec]] = {}

    class OutputRole(StrEnum):
        result = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        "result": OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            infer_lineage_from={"inputs": []},
        ),
    }
    params: _ParamsV2 = _ParamsV2()

    def execute(self, inputs: Any) -> dict[str, Any]:
        return {}

    def postprocess(self, inputs: Any) -> Any:
        return None


class TestStepSpecIdWithDefaults:
    """Tests that step_spec_id correctly reflects operation defaults."""

    def test_different_defaults_produce_different_spec_id(self):
        """step_spec_id changes when operation defaults change, even with params=None.

        This validates the Phase 3 fix: step_spec_id uses full instantiated
        params (defaults + overrides), not just user overrides.
        """
        instance_v1 = instantiate_operation(_OpV1, params=None)
        instance_v2 = instantiate_operation(_OpV2, params=None)

        full_params_v1 = instance_v1.params.model_dump(mode="json")
        full_params_v2 = instance_v2.params.model_dump(mode="json")

        assert full_params_v1 != full_params_v2

        spec1 = compute_step_spec_id(
            operation_name="test_op",
            step_number=0,
            params=full_params_v1,
            input_spec={},
        )
        spec2 = compute_step_spec_id(
            operation_name="test_op",
            step_number=0,
            params=full_params_v2,
            input_spec={},
        )
        assert spec1 != spec2

    def test_same_defaults_produce_same_spec_id(self):
        """step_spec_id is stable when defaults are unchanged."""
        instance1 = instantiate_operation(_OpV1, params=None)
        instance2 = instantiate_operation(_OpV1, params=None)

        full_params1 = instance1.params.model_dump(mode="json")
        full_params2 = instance2.params.model_dump(mode="json")

        spec1 = compute_step_spec_id(
            operation_name="test_op",
            step_number=0,
            params=full_params1,
            input_spec={},
        )
        spec2 = compute_step_spec_id(
            operation_name="test_op",
            step_number=0,
            params=full_params2,
            input_spec={},
        )
        assert spec1 == spec2

    def test_user_override_matches_same_default(self):
        """Explicit user override equal to default produces same spec_id as no override."""
        instance_no_override = instantiate_operation(_OpV1, params=None)
        instance_with_override = instantiate_operation(
            _OpV1, params={"temperature": 0.5}
        )

        params_no = instance_no_override.params.model_dump(mode="json")
        params_with = instance_with_override.params.model_dump(mode="json")

        assert params_no == params_with

        spec_no = compute_step_spec_id(
            operation_name="test_op",
            step_number=0,
            params=params_no,
            input_spec={},
        )
        spec_with = compute_step_spec_id(
            operation_name="test_op",
            step_number=0,
            params=params_with,
            input_spec={},
        )
        assert spec_no == spec_with
