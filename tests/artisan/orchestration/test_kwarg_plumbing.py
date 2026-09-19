"""Introspection-based tests that the public PipelineManager kwarg
surface is wired through end-to-end.

These tests answer two structural questions that behavior-level
integration tests do not:

1. **Signature drift detection.** Are the kwargs advertised on
   ``submit`` / ``run`` / ``submit_composite`` / ``run_composite`` the
   same set we expect? If a new kwarg is added to ``submit`` but not
   mirrored on ``run``, this fails.

2. **Plumbing-through assertion.** When the user passes a sentinel
   value to a kwarg, does it appear on the prepared operation passed to
   ``execute_step``? Or is it accepted at the boundary and silently dropped?

These tests deliberately do not run real pipelines — they mock the
dispatch path and assert on call args. They are unit-level guards on
the API contract.
"""

from __future__ import annotations

import inspect
from enum import StrEnum, auto
from typing import Any, ClassVar
from unittest.mock import MagicMock, patch

import pytest

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.pipeline_manager import PipelineManager
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import CachePolicy, GroupByStrategy
from artisan.schemas.orchestration.pipeline_config import PipelineConfig
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

# Source of truth for the override-kwarg contract. Drift between this
# constant and inspect.signature(submit) is the failure signal.
SUBMIT_OVERRIDE_KWARGS = frozenset(
    {
        "step_runner",
        "runner_resources",
        "batch_strategy",
        "environment",
        "tool",
        "compute_provider",
        "compute_resources",
        "group_by",
    }
)

# Composite-only kwargs that exist on submit_composite but not submit.
# The current composite API has no additional override kwargs.
COMPOSITE_ONLY_KWARGS: frozenset[str] = frozenset()

# Operation-only kwargs that exist on submit but not on submit_composite.
# ``group_by`` is a pairing strategy specific to multi-input operations
# (composites don't pair their own role inputs — internal ``ctx.run()``
# calls handle per-step pairing).
OPERATION_ONLY_KWARGS = frozenset({"group_by"})


def _kwarg_names(method: Any) -> set[str]:
    """Return the set of parameter names for a method, excluding self / positionals."""
    sig = inspect.signature(method)
    return {
        name
        for name, p in sig.parameters.items()
        if name not in ("self", "operation", "composite")
    }


def test_submit_signature_advertises_all_override_kwargs() -> None:
    """submit() must accept every kwarg we claim is part of the override surface.

    If a new override kwarg is added to submit() but not to
    SUBMIT_OVERRIDE_KWARGS, this fails — and the developer is forced
    to also add it to the plumbing-through test below.
    """
    actual = _kwarg_names(PipelineManager.submit)
    # Strip plumbing-only kwargs to leave only the override set.
    override_kwargs = actual - {
        "inputs",
        "params",
        "name",
        "failure_policy",
        "cache_policy",
        "compact",
        "skip_cache",
    }
    assert override_kwargs == SUBMIT_OVERRIDE_KWARGS, (
        f"submit() override-kwarg surface drifted from contract.\n"
        f"  Added (and not in test contract): "
        f"{override_kwargs - SUBMIT_OVERRIDE_KWARGS}\n"
        f"  Missing (in contract but not signature): "
        f"{SUBMIT_OVERRIDE_KWARGS - override_kwargs}"
    )


def test_run_kwargs_match_submit() -> None:
    """run() is submit().result() — must accept the same kwargs."""
    submit_kwargs = _kwarg_names(PipelineManager.submit)
    run_kwargs = _kwarg_names(PipelineManager.run)
    assert run_kwargs == submit_kwargs, (
        f"run/submit kwarg drift. "
        f"In submit but not run: {submit_kwargs - run_kwargs}. "
        f"In run but not submit: {run_kwargs - submit_kwargs}"
    )


@pytest.mark.parametrize("policy", list(CachePolicy))
def test_cache_policy_reaches_whole_step_lookup(tmp_path, policy: CachePolicy) -> None:
    pipeline = _make_pipeline(tmp_path)
    with (
        patch.object(
            pipeline._step_tracker, "check_cache", return_value=None
        ) as lookup,
        patch(
            "artisan.orchestration.pipeline_manager.execute_step",
            side_effect=RuntimeError("test"),
        ),
    ):
        pipeline.run(_StubOp, cache_policy=policy)
    pipeline.finalize()
    assert lookup.call_args.args[1] is policy


def test_submit_composite_kwargs_match_submit_plus_composite_only() -> None:
    """submit_composite = (submit kwargs - OPERATION_ONLY) + COMPOSITE_ONLY.

    Symmetry guard: every override kwarg on submit must also exist on
    submit_composite, except for kwargs explicitly carved out as
    OPERATION_ONLY_KWARGS (those that have no composite-level analogue).
    """
    submit_kwargs = _kwarg_names(PipelineManager.submit)
    submit_composite_kwargs = _kwarg_names(PipelineManager.submit_composite)
    expected = (submit_kwargs - OPERATION_ONLY_KWARGS) | COMPOSITE_ONLY_KWARGS
    assert submit_composite_kwargs == expected, (
        f"submit_composite kwarg drift.\n"
        f"  Missing from submit_composite: "
        f"{expected - submit_composite_kwargs}\n"
        f"  Extra on submit_composite: "
        f"{submit_composite_kwargs - expected}"
    )


def test_run_composite_kwargs_match_submit_composite() -> None:
    """run_composite must accept the same kwargs as submit_composite."""
    submit_kwargs = _kwarg_names(PipelineManager.submit_composite)
    run_kwargs = _kwarg_names(PipelineManager.run_composite)
    assert run_kwargs == submit_kwargs, (
        f"run_composite/submit_composite kwarg drift. "
        f"Diff: {submit_kwargs ^ run_kwargs}"
    )


class _StubOp(OperationDefinition):
    """Minimal creator op used solely to drive sentinel injection."""

    class InputRole(StrEnum):
        data = auto()

    class OutputRole(StrEnum):
        out = auto()

    name: ClassVar[str] = "stub_op_for_kwarg_plumbing"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.data: InputSpec(artifact_type=ArtifactTypes.DATA, required=False),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.out: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            infer_lineage_from={"inputs": ["data"]},
        ),
    }

    def preprocess(self, inputs: Any) -> dict:
        return {}

    def execute_function(self, inputs: Any, output_dir: Any) -> Any:
        return None


def _make_pipeline(tmp_path) -> PipelineManager:
    """Construct an isolated manager for override tests."""
    config = PipelineConfig(
        name="kwarg_plumbing",
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path / "working"),
    )
    return PipelineManager(config)


# Sentinel values for each kwarg. Each must be a structurally valid
# input the dispatch path accepts — but using a distinguishable value
# we can spot in mock call args.
SENTINELS: dict[str, Any] = {
    "runner_resources": {"cpus": 999},
    "batch_strategy": {"artifacts_per_unit": 999},
    "compute_resources": {"memory_gb": 999},
    "environment": "local",
    "tool": None,  # tool is rarely overridden; tested via a separate path
    "compute_provider": "local",
    "step_runner": "local",
    "group_by": GroupByStrategy.CROSS_PRODUCT,
}


@pytest.mark.parametrize(
    "kwarg",
    sorted(SUBMIT_OVERRIDE_KWARGS - {"step_runner", "tool"}),
)
@patch("artisan.orchestration.pipeline_manager.StepTracker")
@patch("artisan.orchestration.pipeline_manager.execute_step")
def test_kwarg_reaches_execute_step(
    mock_execute: MagicMock,
    mock_tracker_cls: MagicMock,
    kwarg: str,
    tmp_path,
) -> None:
    """Sentinel passed via submit() must affect the prepared operation.

    ``execute_step`` receives the same prepared instance used for step hashing,
    so each operation-valued override must already be applied there.
    """
    from artisan.schemas.orchestration.step_lifecycle import (
        StepDisposition,
        StepStatus,
    )
    from artisan.schemas.orchestration.step_result import StepResult

    # check_cache must return None so the cache-miss path is taken;
    # otherwise execute_step is bypassed.
    mock_tracker = MagicMock()
    mock_tracker.check_cache.return_value = None
    mock_tracker_cls.return_value = mock_tracker

    # execute_step must return a real StepResult so the post-step
    # bookkeeping (duration formatting, etc.) doesn't choke.
    mock_execute.return_value = StepResult(
        step_name=_StubOp.name,
        step_number=0,
        status=StepStatus.SUCCEEDED,
        disposition=StepDisposition.EXECUTED,
        total_count=0,
        succeeded_count=0,
        failed_count=0,
        duration_seconds=0.0,
    )

    sentinel = SENTINELS[kwarg]
    pipeline = _make_pipeline(tmp_path)
    pipeline.submit(_StubOp, inputs=None, **{kwarg: sentinel})

    # _dispatch_step submits a closure to a thread pool — drain it.
    pipeline.finalize()

    assert mock_execute.called, "execute_step was never called"
    call_kwargs = mock_execute.call_args.kwargs
    operation = call_kwargs["operation"]
    instance_field = {
        "runner_resources": "runner_resources",
        "batch_strategy": "batch_strategy",
        "environment": "environments",
        "compute_provider": "compute_provider",
        "compute_resources": "compute_resources",
        "group_by": "group_by",
    }[kwarg]
    value = getattr(operation, instance_field)
    actual = {
        "runner_resources": lambda: value.cpus,
        "batch_strategy": lambda: value.artifacts_per_unit,
        "environment": lambda: value.active,
        "compute_provider": lambda: value.active,
        "compute_resources": lambda: value.memory_gb,
        "group_by": lambda: value,
    }[kwarg]()
    expected = next(iter(sentinel.values())) if isinstance(sentinel, dict) else sentinel
    assert actual == expected, (
        f"submit({kwarg}={sentinel!r}) reached execute_step but the prepared "
        f"operation value "
        f"was {actual!r}"
    )
