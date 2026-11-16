"""Pin cache identity domain v3 digests against silent semantic changes.

Unlike determinism checks, recorded values detect changes shared by both sides
of a comparison. Update these constants only for intentional cache invalidation,
and explain the identity change in the PR.
"""

from __future__ import annotations

import pytest

from artisan.operations import examples
from artisan.utils.hashing import (
    CacheInputIdentity,
    compute_execution_spec_id,
    compute_step_spec_id,
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
    current = {"version": operation.version}
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
        "92323e3e0a0db06c865a3cbf5d552433",
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
        "fed71dad85c4855261c6302737f12205",
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
        "f68dd9ac6284548a9fa88e36ac66b133",
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
        "45ea371da42fe561fcec57ef9dd38266",
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
