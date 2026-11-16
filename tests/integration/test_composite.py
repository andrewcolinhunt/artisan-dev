"""Installed compositions execute within one public scientific step."""

from __future__ import annotations

import pytest

from artisan.composites.examples import GenerateAndScore, TransformAndScore
from artisan.operations.examples import DataGenerator, DataTransformer
from artisan.orchestration import PipelineManager
from artisan.orchestration.engine.current_outputs import read_current_step_outputs
from artisan.orchestration.runners import LocalRunner
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.schemas.orchestration.step_lifecycle import StepStatus

pytestmark = pytest.mark.integration


def test_nested_composite_is_one_outer_step(pipeline_env: dict[str, str]) -> None:
    with PipelineManager.create(
        "nested-composite", **pipeline_env, default_step_runner=LocalRunner()
    ) as pipeline:
        result = pipeline.run(GenerateAndScore, params={"count": 2})
        assert result.status == StepStatus.SUCCEEDED, result.error
        assert result.total_count == result.succeeded_count == 1
        assert result.output_types == {"dataset": "csv", "metrics": "metric"}
        outputs = read_current_step_outputs(pipeline.committer, result.step_run_id)
        assert {role.role: len(role.occurrences) for role in outputs} == {
            "dataset": 2,
            "metrics": 2,
        }
        assert len(pipeline) == 1
        assert pipeline[0] == result
    assert pipeline.ownership_state() == OwnershipState.CLOSED


def test_composite_consumes_pending_upstream_and_feeds_next_step(
    pipeline_env: dict[str, str],
) -> None:
    with PipelineManager.create(
        "composite-chain", **pipeline_env, default_step_runner=LocalRunner()
    ) as pipeline:
        generated = pipeline.submit(DataGenerator, params={"count": 2, "seed": 42})
        composite = pipeline.submit(
            TransformAndScore, inputs={"dataset": generated.output("datasets")}
        )
        transformed = pipeline.run(
            DataTransformer,
            inputs={"dataset": composite.output("dataset")},
            params={"variants": 1},
        )
        assert generated.result().status == StepStatus.SUCCEEDED
        assert composite.result().status == StepStatus.SUCCEEDED
        assert transformed.status == StepStatus.SUCCEEDED, transformed.error
        assert len(pipeline) == 3
        outputs = read_current_step_outputs(pipeline.committer, transformed.step_run_id)
        assert len(outputs[0].occurrences) == 2


def test_composite_display_name_does_not_change_scientific_output(
    pipeline_env: dict[str, str],
) -> None:
    with PipelineManager.create(
        "composite-names", **pipeline_env, default_step_runner=LocalRunner()
    ) as pipeline:
        results = [
            pipeline.run(GenerateAndScore, name=name, params={"count": 1})
            for name in ("first label", "second label")
        ]
        assert [result.step_name for result in results] == [
            "first label",
            "second label",
        ]
        selected = [
            read_current_step_outputs(pipeline.committer, result.step_run_id)
            for result in results
        ]
        assert [
            item.artifact.artifact_id
            for role in selected[0]
            for item in role.occurrences
        ] == [
            item.artifact.artifact_id
            for role in selected[1]
            for item in role.occurrences
        ]
        assert pipeline.output("first label", "dataset").source_step == 0
        assert pipeline.output("second label", "dataset").source_step == 1


def test_composite_root_partitioning_preserves_child_output_count(
    pipeline_env: dict[str, str],
) -> None:
    with PipelineManager.create(
        "composite-partitions", **pipeline_env, default_step_runner=LocalRunner()
    ) as pipeline:
        source = pipeline.run(DataGenerator, params={"count": 3})
        result = pipeline.run(
            TransformAndScore,
            inputs={"dataset": source.output("datasets")},
            params={"scale_factor": 3.0},
            batch_strategy={"artifacts_per_unit": 1, "units_per_worker": 2},
        )
        assert result.status == StepStatus.SUCCEEDED, result.error
        assert result.total_count == result.succeeded_count == 3
        outputs = read_current_step_outputs(pipeline.committer, result.step_run_id)
        assert {role.role: len(role.occurrences) for role in outputs} == {
            "dataset": 3,
            "metrics": 3,
        }
