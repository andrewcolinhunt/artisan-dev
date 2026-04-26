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
