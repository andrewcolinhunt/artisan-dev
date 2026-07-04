"""Recorded-fixture tests for compute_step_spec_id.

These hashes are recorded values from a known-good `main` snapshot
(post-PipelineManager-refactor, 2026-04-25). Any commit that changes
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

from artisan.operations.examples.data_transformer import DataTransformer
from artisan.orchestration.engine.step_executor import instantiate_operation
from artisan.schemas.orchestration.step_overrides import StepOverrides
from artisan.utils.hashing import (
    compute_execution_spec_id,
    compute_step_spec_id,
    effective_config_payload,
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
            "input_spec": {"dataset": ("upstream_id_aaa", "merged")},
            "config_overrides": {
                "environment": "docker",
                "tool": None,
                "compute": "local",
            },
        },
        "b28c80f0c143c748f3ba6c75734e2e70",
        id="env=docker,compute=local",
    ),
    pytest.param(
        {
            "operation_name": "data_transformer",
            "step_number": 0,
            "params": None,
            "input_spec": {},
            "config_overrides": None,
        },
        "6789d36b4441e0301bcf02cc083b5d8d",
        id="bare-step",
    ),
    pytest.param(
        {
            "operation_name": "metric_calculator",
            "step_number": 2,
            "params": {"window": 10},
            "input_spec": {"data": ("step1_aaa", "out")},
            "config_overrides": None,
        },
        "117a386a46450088322d2efabae23ce3",
        id="step1-no-config",
    ),
    pytest.param(
        {
            "operation_name": "merge_op",
            "step_number": 3,
            "params": None,
            "input_spec": {"a": ("up_a", "out"), "b": ("up_b", "out")},
            "config_overrides": {
                "environment": "local",
                "tool": None,
                "compute": "modal",
            },
        },
        "1b132a36413f810ca164295b317cf482",
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
        input_spec={"a": ("u1", "out"), "b": ("u2", "out")},
        config_overrides=None,
    )
    spec_b = compute_step_spec_id(
        operation_name="x",
        step_number=0,
        params=None,
        input_spec={"b": ("u2", "out"), "a": ("u1", "out")},
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
    "input_spec": {"dataset": ("upstream_id_aaa", "merged")},
}
_MERGE_EXEC_KWARGS = {
    "operation_name": "data_transformer",
    "inputs": {"dataset": ["artifact_aaa"]},
    "params": {"scale_factor": 0.5, "variants": 1, "seed": 100},
}

# (merge_kwargs, recorded step_spec_id, recorded execution_spec_id)
RECORDED_MERGE_HASHES = [
    pytest.param(
        {
            "environment": None,
            "tool": None,
            "compute_provider": "slurm",
            "compute_resources": None,
        },
        "e8ffc89a4c6e3dcbd453a2289636aa4d",
        "1b0b8d8cc28344004835459d4c2cc0cf",
        id="compute_provider_slurm",
    ),
    pytest.param(
        {
            "environment": None,
            "tool": None,
            "compute_provider": None,
            "compute_resources": None,
        },
        "74cd33084726624724a745986e98f9c0",
        "1a2e551df5665411620c078c6fa15af2",
        id="defaults_only",
    ),
    pytest.param(
        {
            "environment": None,
            "tool": None,
            "compute_provider": None,
            "compute_resources": {"gpu": "A100", "memory_gb": 32},
        },
        "88f661bb66400588388ab1f6968ce06d",
        "43a232f4b8d1ffc8c84845b2370c4c4d",
        id="compute_resources_split",
    ),
]


def _effective_payload(merge_kwargs: dict) -> dict:
    """Instantiate the pinned op with the overrides and dump its effective config."""
    instance = instantiate_operation(
        _PINNED_OP, StepOverrides.from_user(**merge_kwargs)
    )
    return effective_config_payload(instance)


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
