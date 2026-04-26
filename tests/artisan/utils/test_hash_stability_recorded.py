"""Recorded-fixture tests for compute_step_spec_id / compute_composite_spec_id.

These hashes are recorded values from a known-good `main` snapshot
(post-PipelineManager-refactor, 2026-04-25). Any commit that changes
the hashing semantics — adding fields to the payload, changing
canonicalization, reordering concatenation — will flip these digests
and fail CI. That failure is the signal: "this commit invalidates
every cache entry currently in production."

Updating these constants is fine but should be called out in the PR
description so reviewers know to expect a cache flush.

Comparison-only tests (test_step_spec_id.py, test_composite_spec_id.py)
verify *determinism* — same inputs → same hash. They cannot detect a
silent semantic change because both sides of the comparison change
together. This file is the brittle, golden-value safety net.
"""

from __future__ import annotations

import pytest

from artisan.orchestration.engine.step_executor import _merge_config_overrides
from artisan.utils.hashing import (
    compute_composite_spec_id,
    compute_step_spec_id,
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
# Step spec hashes via _merge_config_overrides (end-to-end)
# ---------------------------------------------------------------------------
#
# The fixtures above call compute_step_spec_id directly with hand-built
# config_overrides dicts, so they lock the hashing primitive but do not
# exercise _merge_config_overrides. A change to the merge function's
# payload shape (e.g. adding "compute_resources" as a fourth payload
# key, dropping a key, renaming one) would not flip any recorded hash
# above.
#
# These end-to-end fixtures close that gap by flowing inputs through
# _merge_config_overrides before feeding the result into
# compute_step_spec_id. Hashes recorded against PR-A's tip
# (ddf5fc2c2c6bb6ad2becf2ee9848bbc73ce97239), which widened the merge
# payload to include "compute_resources" as the fourth key.

RECORDED_MERGE_HASHES = [
    pytest.param(
        {
            "merge_kwargs": {
                "environment": None,
                "tool": None,
                "compute_provider": "slurm",
                "compute_resources": None,
            },
            "spec_kwargs": {
                "operation_name": "data_transformer",
                "step_number": 1,
                "params": {"scale_factor": 0.5, "variants": 1, "seed": 100},
                "input_spec": {"dataset": ("upstream_id_aaa", "merged")},
            },
        },
        "cc62090baabfcc4ef7b445603960f25d",
        id="step_runner_slurm",
    ),
    pytest.param(
        {
            "merge_kwargs": {
                "environment": None,
                "tool": None,
                "compute_provider": None,
                "compute_resources": None,
            },
            "spec_kwargs": {
                "operation_name": "data_transformer",
                "step_number": 1,
                "params": {"scale_factor": 0.5, "variants": 1, "seed": 100},
                "input_spec": {"dataset": ("upstream_id_aaa", "merged")},
            },
        },
        "b9bbd19393a8fc996a4cd37ee967918b",
        id="runner_resources_cpus_4",
    ),
    pytest.param(
        {
            "merge_kwargs": {
                "environment": None,
                "tool": None,
                "compute_provider": None,
                "compute_resources": {"gpu": "A100", "memory_gb": 32},
            },
            "spec_kwargs": {
                "operation_name": "data_transformer",
                "step_number": 1,
                "params": {"scale_factor": 0.5, "variants": 1, "seed": 100},
                "input_spec": {"dataset": ("upstream_id_aaa", "merged")},
            },
        },
        "ec800f042ef2d895a68d6c0df5a9dfd4",
        id="split_hardware",
    ),
]


@pytest.mark.parametrize(("inputs", "expected"), RECORDED_MERGE_HASHES)
def test_step_spec_id_through_merge_config_overrides_matches_recorded_hash(
    inputs: dict, expected: str
) -> None:
    """End-to-end: _merge_config_overrides → compute_step_spec_id.

    Locks the merge function's payload shape AND the primitive's
    hashing semantics together. A change to either layer (renaming a
    payload key, adding a new one like PR-A's compute_resources,
    reordering canonicalization) flips these digests.

    A failure means callers' cached step results will miss-and-rerun.
    Confirm that's intended before updating the constants.
    """
    config_overrides = _merge_config_overrides(**inputs["merge_kwargs"])
    actual = compute_step_spec_id(
        **inputs["spec_kwargs"], config_overrides=config_overrides
    )
    assert actual == expected


# ---------------------------------------------------------------------------
# Composite spec hashes
# ---------------------------------------------------------------------------

RECORDED_COMPOSITE_HASHES = [
    pytest.param(
        {
            "composite_name": "generate_and_transform",
            "params": {"factor": 2.0},
            "input_spec": {"data": ("upstream_id_bbb", "out")},
        },
        "19c1722566edbd7ce397d33066e93684",
        id="basic-composite",
    ),
    pytest.param(
        {
            "composite_name": "passthrough_composite",
            "params": None,
            "input_spec": {"in": ("upstream_xxx", "data")},
        },
        "7d37930f6056d7d723978b59f0812c23",
        id="composite-no-params",
    ),
    pytest.param(
        {
            "composite_name": "generative_composite",
            "params": {"count": 5, "seed": 42},
            "input_spec": {},
        },
        "7ddf5df018d640ccba52df180a9ca81f",
        id="composite-no-inputs",
    ),
]


@pytest.mark.parametrize(("inputs", "expected"), RECORDED_COMPOSITE_HASHES)
def test_composite_spec_id_is_recorded_value(inputs: dict, expected: str) -> None:
    """compute_composite_spec_id must produce the recorded hex digest."""
    assert compute_composite_spec_id(**inputs) == expected
