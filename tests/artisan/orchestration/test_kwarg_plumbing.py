"""Introspection-based tests that the public PipelineManager kwarg
surface is wired through end-to-end.

These tests answer two structural questions that behavior-level
integration tests do not:

1. **Signature drift detection.** Are the kwargs advertised on
   ``submit`` / ``run`` / ``submit_composite`` / ``run_composite`` the
   same set we expect? If a new kwarg is added to ``submit`` but not
   mirrored on ``run``, this fails.

2. **Plumbing-through assertion.** When the user passes a sentinel
   value to a kwarg, does that sentinel actually reach
   ``instantiate_operation`` (the eventual override site)? Or is it
   accepted at the boundary and silently dropped? The latter was bug
   #3 in the verification script — ``compute_resources=`` was
   accepted by ``submit`` but never threaded through to
   ``instantiate_operation``.

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
    }
)

# Composite-only kwargs that exist on submit_composite but not submit.
COMPOSITE_ONLY_KWARGS = frozenset({"expand", "intermediates"})


# ---------------------------------------------------------------------------
# Signature drift guards
# ---------------------------------------------------------------------------


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


def test_submit_composite_kwargs_match_submit_plus_composite_only() -> None:
    """submit_composite = submit kwargs + {expand, intermediates}.

    Symmetry guard: every override kwarg on submit must also exist on
    submit_composite (the split surface still needs to expose the same
    config knobs to composite users).
    """
    submit_kwargs = _kwarg_names(PipelineManager.submit)
    submit_composite_kwargs = _kwarg_names(PipelineManager.submit_composite)
    expected = submit_kwargs | COMPOSITE_ONLY_KWARGS
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


# ---------------------------------------------------------------------------
# Plumbing-through assertion: each kwarg reaches instantiate_operation
# ---------------------------------------------------------------------------


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

    def execute(self, inputs: Any, output_dir: Any) -> Any:
        return None


def _make_pipeline(tmp_path) -> PipelineManager:
    """Mirrors the helper in test_pipeline_manager.py — minimal manager
    with no Prefect."""
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
    """Sentinel passed via submit() must arrive on the execute_step call args.

    execute_step is the boundary into step_executor — if a kwarg
    doesn't reach here, it has been silently dropped somewhere in
    submit() / _prepare_step_spec / _dispatch_step.
    """
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
        success=True,
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
    assert kwarg in call_kwargs, (
        f"submit({kwarg}=...) did not reach execute_step. "
        f"call_kwargs were: {sorted(call_kwargs)}"
    )
    assert call_kwargs[kwarg] == sentinel, (
        f"submit({kwarg}={sentinel!r}) reached execute_step but the value "
        f"was transformed to {call_kwargs[kwarg]!r}"
    )
