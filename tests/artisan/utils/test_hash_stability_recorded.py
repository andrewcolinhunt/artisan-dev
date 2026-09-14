"""Recorded-fixture tests for compute_step_spec_id.

These hashes are recorded values from the approved release-format v2 cache
identity (2026-09-14). Any commit that changes
the hashing semantics — adding fields to the payload, changing
canonicalization, reordering concatenation — will flip these digests
and fail CI. That failure is the signal: "this commit invalidates
every cache entry currently in production."

Updating these constants is fine but should be called out in the PR
description so reviewers know to expect a cache flush.

Comparison-only tests (test_step_spec_id.py) verify *determinism* — same
inputs → same hash. They cannot detect a silent semantic change because
both sides of the comparison change together. This file is the brittle,
golden-value safety net.
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
    """compute_step_spec_id must produce the recorded hex digest.

    A failure here means the hashing semantics changed. Every cached
    step in production with these exact inputs will now miss-and-rerun.
    Confirm that's intended before updating the constant.
    """
    assert compute_step_spec_id(**inputs) == expected


def test_step_spec_id_input_order_independent() -> None:
    """Reordering the input_spec dict must not affect the hash.

    Guard against a future change that iterates the dict in insertion
    order rather than sorting keys.
    """
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


# ---------------------------------------------------------------------------
# Step + execution spec hashes via the effective-config payload (end-to-end)
# ---------------------------------------------------------------------------
#
# The fixtures above call the primitives directly with hand-built
# config_overrides dicts, so they lock the hashing primitive but not the
# effective-config payload builder. A change to that payload's shape (adding
# a key, dropping one, renaming one) — or to the config a class default
# carries — would not flip any recorded hash above.
#
# These end-to-end fixtures close that gap. They instantiate a pinned op
# (``DataTransformer`` — matching the fixtures' ``operation_name``) with each
# override set, dump its effective config via ``effective_config_payload``,
# and feed that into ``compute_step_spec_id`` / ``compute_execution_spec_id``.
# The payload now reads the op's class defaults too, so these digests reflect
# what actually runs — not just what the caller typed.
#
# A failure means callers' cached results will miss-and-rerun. Confirm that is
# intended before updating the constants.

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
    """Invalid selectors are no longer accepted as recorded cache inputs."""
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
    """End-to-end: effective_config_payload → compute_step_spec_id.

    Locks the effective-config payload (shape + the class defaults it reads)
    AND the primitive's hashing semantics together. A change to either layer
    flips these digests. A failure means callers' cached step results will
    miss-and-rerun; confirm that is intended before updating the constants.
    """
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
    """Worker-level twin: effective_config_payload → compute_execution_spec_id.

    The creator worker recomputes execution_spec_id per unit rather than
    reusing step_spec_id, so the effective config must reach this level too.
    Previously unguarded by any golden; a failure means the unit cache will
    miss-and-rerun.
    """
    config_overrides = _effective_payload(merge_kwargs)
    actual = compute_execution_spec_id(
        **_MERGE_EXEC_KWARGS, config_overrides=config_overrides
    )
    assert actual == exec_hash
