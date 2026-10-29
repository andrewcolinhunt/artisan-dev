"""Pin cache identity domain v2 digests against silent semantic changes.

Unlike determinism checks, recorded values detect changes shared by both sides
of a comparison. Update these constants only for intentional cache invalidation,
and explain the identity change in the PR.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from artisan.operations import examples
from artisan.operations.examples.data_transformer import DataTransformer
from artisan.orchestration.engine.step_executor import instantiate_operation
from artisan.schemas.orchestration.step_overrides import StepOverrides
from artisan.utils.hashing import (
    CacheInputIdentity,
    compute_execution_spec_id,
    compute_step_spec_id,
    effective_config_payload,
)


def _cache_inputs(
    inputs: dict[str, tuple[str, str]],
) -> dict[str, list[CacheInputIdentity]]:
    """Build one concrete typed cache occurrence per input role."""
    return {
        role: [CacheInputIdentity(role, None, 0, artifact_type, artifact_id)]
        for role, (artifact_id, artifact_type) in inputs.items()
    }


@pytest.mark.parametrize(
    ("operation_name", "version"),
    [
        ("DataGenerator", "2"),
        ("DataGeneratorWithMetrics", "2"),
        ("DataTransformer", "2"),
        ("SequentialDataTransformer", "2"),
        ("DataTransformerScript", "2"),
        ("SlowTransformer", "2"),
        ("SequentialSlowTransformer", "2"),
        ("CsvHead", "2"),
        ("Wait", "2"),
        ("WaitTool", "2"),
        ("StreamingEcho", "2"),
        ("MetricCalculator", "1"),
        ("DataTransformerConfig", "1"),
    ],
)
def test_csv_producer_behavior_versions(operation_name: str, version: str) -> None:
    assert getattr(examples, operation_name).version == version


def test_csv_generator_invalidates_cache_without_input_artifacts() -> None:
    operation = examples.DataGenerator()
    current = effective_config_payload(operation)
    previous = {**current, "version": "1"}
    assert current["version"] == "2"
    assert compute_step_spec_id(operation.name, 0, None, {}, current) != (
        compute_step_spec_id(operation.name, 0, None, {}, previous)
    )
    assert compute_execution_spec_id(operation.name, {}, None, current) != (
        compute_execution_spec_id(operation.name, {}, None, previous)
    )


# ---------------------------------------------------------------------------
# Step spec hashes
# ---------------------------------------------------------------------------

RECORDED_STEP_HASHES = [
    pytest.param(
        {
            "operation_name": "data_transformer",
            "step_number": 1,
            "params": {"scale_factor": 0.5, "variants": 1, "seed": 100},
            "inputs": _cache_inputs({"dataset": ("upstream_id_aaa", "csv")}),
            "config_overrides": {
                "environment": "docker",
                "tool": None,
                "compute": "local",
            },
        },
        "e98f30b6465b2900261119cf2e96c081",
        id="env=docker,compute=local",
    ),
    pytest.param(
        {
            "operation_name": "data_transformer",
            "step_number": 0,
            "params": None,
            "inputs": {},
            "config_overrides": None,
        },
        "ae808e0163a20f1a2cf4400ea501f5fd",
        id="bare-step",
    ),
    pytest.param(
        {
            "operation_name": "metric_calculator",
            "step_number": 2,
            "params": {"window": 10},
            "inputs": _cache_inputs({"data": ("step1_aaa", "csv")}),
            "config_overrides": None,
        },
        "04534d620543430ee073f25569ebfbf6",
        id="step1-no-config",
    ),
    pytest.param(
        {
            "operation_name": "merge_op",
            "step_number": 3,
            "params": None,
            "inputs": _cache_inputs({"a": ("up_a", "csv"), "b": ("up_b", "csv")}),
            "config_overrides": {
                "environment": "local",
                "tool": None,
                "compute": "modal",
            },
        },
        "673fea2bbe6c69a7e49552a181027c70",
        id="multi-input",
    ),
]


@pytest.mark.parametrize(("inputs", "expected"), RECORDED_STEP_HASHES)
def test_step_spec_id_is_recorded_value(inputs: dict, expected: str) -> None:
    """Pin the step identity for concrete recorded inputs."""
    assert compute_step_spec_id(**inputs) == expected


def test_step_spec_id_input_order_independent() -> None:
    """Keep input dictionary insertion order out of cache identity."""
    spec_a = compute_step_spec_id(
        operation_name="x",
        step_number=0,
        params=None,
        inputs=_cache_inputs({"a": ("u1", "out"), "b": ("u2", "out")}),
        config_overrides=None,
    )
    spec_b = compute_step_spec_id(
        operation_name="x",
        step_number=0,
        params=None,
        inputs=_cache_inputs({"b": ("u2", "out"), "a": ("u1", "out")}),
        config_overrides=None,
    )
    assert spec_a == spec_b


# These fixtures also pin class defaults and the effective-config payload;
# primitive-only fixtures above cannot detect changes to those inputs.

_PINNED_OP = DataTransformer

_MERGE_SPEC_KWARGS = {
    "operation_name": "data_transformer",
    "step_number": 1,
    "params": {"scale_factor": 0.5, "variants": 1, "seed": 100},
    "inputs": _cache_inputs({"dataset": ("upstream_id_aaa", "csv")}),
}
_MERGE_EXEC_KWARGS = {
    "operation_name": "data_transformer",
    "inputs": _cache_inputs({"dataset": ("artifact_aaa", "csv")}),
    "params": {"scale_factor": 0.5, "variants": 1, "seed": 100},
}

# (merge_kwargs, recorded step_spec_id, recorded execution_spec_id)
RECORDED_MERGE_HASHES = [
    pytest.param(
        {
            "environment": None,
            "tool": None,
            "compute_provider": None,
            "compute_resources": None,
        },
        "5475086b0705cda7216adf4e569edad1",
        "47ce433c579bf773efed632036f9534f",
        id="defaults_only",
    ),
    pytest.param(
        {
            "environment": None,
            "tool": None,
            "compute_provider": None,
            "compute_resources": {"gpu": "A100", "memory_gb": 32},
        },
        "9248481f76604cd104e28adfb840ac3d",
        "d9d64797b7c16178e4ac9367d47aa8dd",
        id="compute_resources_split",
    ),
]


def _effective_payload(merge_kwargs: dict) -> dict:
    """Instantiate the pinned op with the overrides and dump its effective config."""
    instance = instantiate_operation(
        _PINNED_OP, StepOverrides.from_user(**merge_kwargs)
    )
    return effective_config_payload(instance)


def test_invalid_selector_fails_before_hashing() -> None:
    """Reject invalid selectors before constructing the cache payload."""
    with pytest.raises(ValidationError, match="Unknown compute provider"):
        _effective_payload(
            {
                "environment": None,
                "tool": None,
                "compute_provider": "slurm",
                "compute_resources": None,
            }
        )


@pytest.mark.parametrize(
    ("merge_kwargs", "step_hash", "exec_hash"), RECORDED_MERGE_HASHES
)
def test_effective_payload_step_spec_id_is_recorded(
    merge_kwargs: dict, step_hash: str, exec_hash: str
) -> None:
    """Pin step identity after applying class defaults and overrides."""
    config_overrides = _effective_payload(merge_kwargs)
    actual = compute_step_spec_id(
        **_MERGE_SPEC_KWARGS, config_overrides=config_overrides
    )
    assert actual == step_hash


@pytest.mark.parametrize(
    ("merge_kwargs", "step_hash", "exec_hash"), RECORDED_MERGE_HASHES
)
def test_effective_payload_execution_spec_id_is_recorded(
    merge_kwargs: dict, step_hash: str, exec_hash: str
) -> None:
    """Pin execution identity using the effective worker configuration."""
    config_overrides = _effective_payload(merge_kwargs)
    actual = compute_execution_spec_id(
        **_MERGE_EXEC_KWARGS, config_overrides=config_overrides
    )
    assert actual == exec_hash
