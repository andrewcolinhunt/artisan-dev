"""Known configuration and input-contract errors are rejected before work."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from fixtures.composition_operations import PredictMeasure, Transform
from fixtures.retained_pipeline import NativePipelineRunner

from artisan.composites import CompositeContext, CompositeDefinition
from artisan.operations.curator import Merge
from artisan.operations.examples import WaitTool
from artisan.orchestration import OutputReference, PipelineManager
from artisan.orchestration.engine.pipeline_inputs import (
    normalize_pipeline_inputs,
    validate_pipeline_input_contract,
)
from artisan.orchestration.engine.pipeline_submission import freeze_submission
from artisan.schemas import GroupByStrategy, InputSpec


def _pipeline(tmp_path: Path) -> PipelineManager:
    return PipelineManager.create(
        "validation", str(tmp_path / "validation-store"), str(tmp_path / "staging")
    )


@pytest.mark.parametrize("method", ["run", "submit"])
@pytest.mark.parametrize("policy", ["step_completed", 1, False])
@pytest.mark.parametrize("composite", [False, True])
def test_invalid_cache_policy_allocates_no_work(
    tmp_path: Path, method: str, policy: object, composite: bool
) -> None:
    composed = []

    class EmptyComposite(CompositeDefinition):
        name = "empty_cache_policy_validation"

        def compose(self, ctx: CompositeContext) -> None:
            composed.append(True)

    with _pipeline(tmp_path) as pipeline:
        target = EmptyComposite if composite else Transform
        with pytest.raises(TypeError, match="cache_policy"):
            getattr(pipeline, method)(target, cache_policy=policy)
        assert pipeline.current_step == 0
        assert not pipeline.executions
        assert not pipeline.tracker.load_current_states(pipeline.config.pipeline_run_id)
        assert not composed


@pytest.mark.parametrize(
    "arguments",
    [
        {"params": {"bogus": True}},
        {"params": {"add": "invalid"}},
        {"runner_resources": {"mem_Gb": 3}},
        {"runner_resources": {"cpus": 0}},
        {"batch_strategy": {"batch_size": 2}},
        {"batch_strategy": {"max_workers": 0}},
        {"group_by": "zip"},
        {"tool": {"executable": "unexpected"}},
        {"environment": "local"},
        {"prepared_environment": "receipt"},
        {"compute_resources": {"local": {"cpus": 2}}},
        {"compact": False},
    ],
)
def test_invalid_or_removed_submission_options_create_no_attempt(
    tmp_path: Path, arguments: dict[str, Any]
) -> None:
    with _pipeline(tmp_path) as pipeline:
        with pytest.raises((TypeError, ValueError)):
            pipeline.submit(Transform, **arguments)
        assert pipeline.current_step == 0
        assert not pipeline.executions
        assert not pipeline.tracker.load_current_states(pipeline.config.pipeline_run_id)


def test_valid_scientific_and_operational_patches_remain_separate() -> None:
    submission = freeze_submission(
        Transform,
        params={"add": 5},
        runner_resources={"cpus": 4, "memory_gb": 32},
        batch_strategy={"artifacts_per_unit": 10, "max_workers": 8},
        group_by=GroupByStrategy.ZIP,
    )
    assert submission.instantiate().params.add == 5
    assert submission.resources.cpus == 4
    assert submission.batch_strategy.max_workers == 8
    assert submission.compile().recipe.policy.artifacts_per_unit == 10
    assert "max_workers" not in submission.compile().recipe.policy.model_dump()
    assert freeze_submission(Merge, params={}).is_curator
    with pytest.raises(ValueError):
        freeze_submission(Merge, params={"flavor": "vanilla"})


def test_tool_overrides_validate_against_declared_command_contract() -> None:
    submission = freeze_submission(WaitTool, tool={"executable": "replacement"})
    assert submission.compile().recipe.execution.tool.executable == "replacement"
    with pytest.raises(ValueError, match="bogus"):
        freeze_submission(WaitTool, tool={"bogus": True})
    with pytest.raises(ValueError):
        freeze_submission(Merge, tool={})


@pytest.mark.parametrize("inputs", [{}, {"bogus": []}])
def test_missing_or_unknown_input_roles_fail_before_software_resolution(
    tmp_path: Path, inputs: dict[str, list[str]]
) -> None:
    with _pipeline(tmp_path) as pipeline:
        runner = NativePipelineRunner(pipeline.committer)
        with pytest.raises(ValueError, match="missing required or undeclared roles"):
            pipeline.run(Transform, inputs=inputs, step_runner=runner)
        assert runner.resolutions == 0
        assert pipeline.current_step == 0
        assert not pipeline.executions
        assert not pipeline.tracker.load_current_states(pipeline.config.pipeline_run_id)


@pytest.mark.parametrize("artifact_type", ["csv", "any"])
def test_matching_or_unspecified_reference_type_preserves_declared_wiring(
    artifact_type: str,
) -> None:
    frozen = normalize_pipeline_inputs(
        {
            "data": OutputReference(
                source_step=0, role="value", artifact_type=artifact_type
            )
        },
        step_run_ids={0: "a" * 32},
    )
    validate_pipeline_input_contract(
        frozen, {"data": InputSpec(artifact_type="csv")}, runtime_defined_inputs=False
    )


def test_mismatched_reference_type_rejects_but_any_input_accepts() -> None:
    frozen = normalize_pipeline_inputs(
        {"data": OutputReference(source_step=0, role="value", artifact_type="metric")},
        step_run_ids={0: "a" * 32},
    )
    with pytest.raises(ValueError, match="type"):
        validate_pipeline_input_contract(
            frozen,
            {"data": InputSpec(artifact_type="csv")},
            runtime_defined_inputs=False,
        )
    validate_pipeline_input_contract(
        frozen, {"data": InputSpec(artifact_type="any")}, runtime_defined_inputs=False
    )
    validate_pipeline_input_contract(frozen, {}, runtime_defined_inputs=True)


def test_optional_and_generative_inputs_need_no_manufactured_roles() -> None:
    empty = normalize_pipeline_inputs(None, step_run_ids={})
    validate_pipeline_input_contract(empty, {}, runtime_defined_inputs=False)
    validate_pipeline_input_contract(
        empty,
        {"optional": InputSpec(artifact_type="csv", required=False)},
        runtime_defined_inputs=False,
    )
    assert freeze_submission(PredictMeasure).instantiate().name == PredictMeasure.name


@pytest.mark.parametrize("composite", [False, True])
def test_required_command_parameters_survive_direct_and_composite_submission(
    composite: bool,
) -> None:
    from pydantic import BaseModel

    from artisan.operations.base import OperationDefinition
    from artisan.schemas import ExecuteInput, ToolSpec

    class RequiredCommand(OperationDefinition):
        name = "required_params_command"
        tool: ToolSpec = ToolSpec(executable="tool")

        class Params(BaseModel):
            """Command parameters.

            Attributes:
                count: Number of requested items.
            """

            count: int

        params: Params

        def execute_command(self, inputs: ExecuteInput) -> list[str]:
            return ["tool", str(self.params.count)]

    class RequiredComposite(CompositeDefinition):
        name = "required_params_composite"

        class Params(BaseModel):
            count: int

        params: Params

        def compose(self, ctx: CompositeContext) -> None:
            ctx.call(RequiredCommand, params={"count": self.params.count})

    submission = freeze_submission(
        RequiredComposite if composite else RequiredCommand, params={"count": 3}
    )
    plan = submission.compile()
    command = plan.calls[0].plan if composite else plan
    assert command.recipe.params_json == '{"count":3}'
    assert command.instantiate().params.count == 3
