"""Pin cache identity domain v2 digests against silent semantic changes.

Unlike determinism checks, recorded values detect changes shared by both sides
of a comparison. Update these constants only for intentional cache invalidation,
and explain the identity change in the PR.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

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


# ---------------------------------------------------------------------------
# Step spec hashes
# ---------------------------------------------------------------------------

RECORDED_STEP_HASHES = [
    pytest.param(
        {
            "operation_name": "data_transformer",
            "step_number": 1,
            "params": {"scale_factor": 0.5, "variants": 1, "seed": 100},
            "inputs": _cache_inputs({"dataset": ("upstream_id_aaa", "data")}),
            "config_overrides": {
                "environment": "docker",
                "tool": None,
                "compute": "local",
            },
        },
        "b9969508c50b7f3c8d6a6a37bce8f0e4",
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
            "inputs": _cache_inputs({"data": ("step1_aaa", "data")}),
            "config_overrides": None,
        },
        "b34554a41102f2a96d099f80e8ff4ba9",
        id="step1-no-config",
    ),
    pytest.param(
        {
            "operation_name": "merge_op",
            "step_number": 3,
            "params": None,
            "inputs": _cache_inputs({"a": ("up_a", "data"), "b": ("up_b", "data")}),
            "config_overrides": {
                "environment": "local",
                "tool": None,
                "compute": "modal",
            },
        },
        "9d0076257a4ef86e2b24922e87bd7f38",
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
    "inputs": _cache_inputs({"dataset": ("upstream_id_aaa", "data")}),
}
_MERGE_EXEC_KWARGS = {
    "operation_name": "data_transformer",
    "inputs": _cache_inputs({"dataset": ("artifact_aaa", "data")}),
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
        "a71c54e23f6b903ccf8396a6ad192b9d",
        "97b9639aa4889a963a12c99035c687c5",
        id="defaults_only",
    ),
    pytest.param(
        {
            "environment": None,
            "tool": None,
            "compute_provider": None,
            "compute_resources": {"gpu": "A100", "memory_gb": 32},
        },
        "948d7c73e739a4a32d394daca41e9172",
        "fe93e1db1df7a182b4c43150dd8aa608",
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
