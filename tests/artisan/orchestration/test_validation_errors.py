"""Envelope-level coverage for pipeline_manager._validate_* helpers.

Each migrated raise site exposes a structured ``ArtisanError`` envelope
(``code``, ``error_type``, ``field``, ``recovery_hint``, and optionally
``suggestions`` / ``fix_example``). These tests exercise the helpers
directly so we don't depend on the integration test fixtures for
unit-level coverage of every code.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from artisan.errors import ArtisanError, ErrorCode
from artisan.operations.examples import (
    DataGenerator,
    DataTransformer,
    MetricCalculator,
)
from artisan.orchestration.pipeline_manager import (
    _reject_inactive_provider_config,
    _validate_compute_provider,
    _validate_compute_resources,
    _validate_environment,
    _validate_execution,
    _validate_input_roles,
    _validate_input_types,
    _validate_params,
    _validate_required_inputs,
    _validate_resources,
    _validate_tool,
)
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.orchestration.output_reference import OutputReference


def _assert_validation_envelope(
    exc: ArtisanError,
    *,
    code: str,
    field: str | None = None,
    recovery_hint: str = "CHECK_INPUT",
) -> None:
    """Shared assertion helper — every validation raise site should agree."""
    assert exc.envelope.code == code
    assert exc.envelope.error_type == "validation"
    assert exc.envelope.recovery_hint == recovery_hint
    if field is not None:
        assert exc.envelope.field == field


class TestValidateParams:
    def test_unknown_param_produces_envelope(self) -> None:
        with pytest.raises(ArtisanError) as excinfo:
            _validate_params(DataGenerator, {"count": 1, "nonexistent_param": 42})
        _assert_validation_envelope(
            excinfo.value,
            code=ErrorCode.UNKNOWN_PARAM,
            field="params.nonexistent_param",
        )
        assert excinfo.value.envelope.operation_name == DataGenerator.name
        assert excinfo.value.envelope.fix_example is not None

    def test_typo_yields_suggestion(self) -> None:
        # DataGenerator.Params has "count" — typoing it should suggest "count".
        with pytest.raises(ArtisanError) as excinfo:
            _validate_params(DataGenerator, {"cont": 1})
        assert "count" in excinfo.value.envelope.suggestions


class TestValidateResources:
    def test_unknown_resource_key(self) -> None:
        with pytest.raises(ArtisanError) as excinfo:
            _validate_resources({"bogus_resource": 99})
        _assert_validation_envelope(
            excinfo.value,
            code=ErrorCode.UNKNOWN_RESOURCE_KEY,
            field="bogus_resource",
        )


class TestValidateExecution:
    def test_unknown_execution_key(self) -> None:
        with pytest.raises(ArtisanError) as excinfo:
            _validate_execution({"nonexistent_key": True})
        _assert_validation_envelope(
            excinfo.value,
            code=ErrorCode.UNKNOWN_EXECUTION_KEY,
            field="nonexistent_key",
        )


class TestValidateEnvironment:
    def test_environment_not_configured_from_string(self) -> None:
        with pytest.raises(ArtisanError) as excinfo:
            _validate_environment(DataGenerator, "not_a_real_env")
        _assert_validation_envelope(
            excinfo.value,
            code=ErrorCode.ENVIRONMENT_NOT_CONFIGURED,
            field="environment",
        )

    def test_unknown_environment_key_at_top_level(self) -> None:
        with pytest.raises(ArtisanError) as excinfo:
            _validate_environment(DataGenerator, {"bogus_provider": {}})
        _assert_validation_envelope(
            excinfo.value,
            code=ErrorCode.UNKNOWN_ENVIRONMENT_KEY,
        )
        # Field is dotted with the bad key.
        assert excinfo.value.envelope.field is not None
        assert excinfo.value.envelope.field.startswith("environment.")

    def test_unknown_environment_key_in_nested_spec(self) -> None:
        with pytest.raises(ArtisanError) as excinfo:
            _validate_environment(
                DataGenerator,
                {"active": "local", "local": {"bogus_local_field": True}},
            )
        _assert_validation_envelope(
            excinfo.value,
            code=ErrorCode.UNKNOWN_ENVIRONMENT_KEY,
        )
        assert excinfo.value.envelope.field is not None
        assert excinfo.value.envelope.field.startswith("environment.local.")


class TestValidateComputeProvider:
    def test_invalid_compute_provider_value_wraps_validation_error(self) -> None:
        # 'active' must be a string; passing a list trips Pydantic type
        # validation, which the helper re-wraps as ArtisanError.
        with pytest.raises(ArtisanError) as excinfo:
            _validate_compute_provider({"active": ["not", "a", "string"]})
        _assert_validation_envelope(
            excinfo.value,
            code=ErrorCode.UNKNOWN_COMPUTE_PROVIDER_KEY,
            field="compute_provider",
        )
        assert excinfo.value.__cause__ is not None


class TestValidateComputeResources:
    def test_invalid_compute_resources_value_wraps_validation_error(self) -> None:
        # gpu_count must be an int (or None); a string trips Pydantic.
        with pytest.raises(ArtisanError) as excinfo:
            _validate_compute_resources({"gpu_count": "not-an-int"})
        _assert_validation_envelope(
            excinfo.value,
            code=ErrorCode.UNKNOWN_COMPUTE_RESOURCES_KEY,
            field="compute_resources",
        )
        assert excinfo.value.__cause__ is not None


class TestRejectInactiveProviderConfig:
    def test_inactive_provider_configured(self) -> None:
        with pytest.raises(ArtisanError) as excinfo:
            _reject_inactive_provider_config(
                {"active": "local", "docker": {"image": "foo"}},
                kwarg="environment",
            )
        _assert_validation_envelope(
            excinfo.value,
            code=ErrorCode.INACTIVE_PROVIDER_CONFIGURED,
            field="environment",
        )


class _OpWithDummyTool(BaseModel):
    """Synthetic stand-in to drive _validate_tool without rigging up a full op."""


class TestValidateTool:
    def test_no_tool_to_override(self) -> None:
        # DataGenerator has no .tool — passing any tool override should raise.
        with pytest.raises(ArtisanError) as excinfo:
            _validate_tool(DataGenerator, {"any": "value"})
        _assert_validation_envelope(
            excinfo.value,
            code=ErrorCode.NO_TOOL_TO_OVERRIDE,
            field="tool",
        )

    def test_unknown_tool_key(self) -> None:
        # Pick an example op that DOES declare a tool. The bundled
        # DataTransformerScript declares one — but to avoid coupling to a
        # specific op, we simulate via a minimal op-like instance.
        from artisan.operations.examples import DataTransformerScript

        with pytest.raises(ArtisanError) as excinfo:
            _validate_tool(DataTransformerScript, {"bogus_tool_key": 1})
        _assert_validation_envelope(
            excinfo.value,
            code=ErrorCode.UNKNOWN_TOOL_KEY,
        )
        assert excinfo.value.envelope.field is not None
        assert excinfo.value.envelope.field.startswith("tool.")


class TestValidateInputRoles:
    def test_unknown_role(self) -> None:
        ref = OutputReference(
            source_step=0,
            role="datasets",
            artifact_type=ArtifactTypes.DATA,
        )
        with pytest.raises(ArtisanError) as excinfo:
            _validate_input_roles(DataTransformer, {"unknown_role": ref})
        _assert_validation_envelope(
            excinfo.value,
            code=ErrorCode.UNKNOWN_ROLE,
            field="inputs.unknown_role",
        )


class TestValidateRequiredInputs:
    def test_missing_required_input(self) -> None:
        with pytest.raises(ArtisanError) as excinfo:
            _validate_required_inputs(DataTransformer, {})
        _assert_validation_envelope(
            excinfo.value,
            code=ErrorCode.MISSING_REQUIRED_INPUT,
        )
        assert excinfo.value.envelope.field is not None
        assert excinfo.value.envelope.field.startswith("inputs.")


class TestValidateInputTypes:
    def test_type_mismatch_yields_try_alternative(self) -> None:
        # A METRIC output piped into a DATA input is a type mismatch.
        metric_ref = OutputReference(
            source_step=0,
            role="metrics",
            artifact_type=ArtifactTypes.METRIC,
        )
        with pytest.raises(ArtisanError) as excinfo:
            _validate_input_types(DataTransformer, {"dataset": metric_ref})
        _assert_validation_envelope(
            excinfo.value,
            code=ErrorCode.INPUT_TYPE_MISMATCH,
            field="inputs.dataset",
            recovery_hint="TRY_ALTERNATIVE",
        )


def test_metric_calculator_accepts_data_input() -> None:
    """Regression: MetricCalculator should accept DATA on 'dataset' role.

    This is not a failure case — it asserts the validators stay silent on
    well-typed wiring, so we don't drift to over-eager rejection.
    """
    data_ref = OutputReference(
        source_step=0,
        role="datasets",
        artifact_type=ArtifactTypes.DATA,
    )
    # No exception expected.
    _validate_input_types(MetricCalculator, {"dataset": data_ref})
    _validate_input_roles(MetricCalculator, {"dataset": data_ref})
    _validate_required_inputs(MetricCalculator, {"dataset": data_ref})
