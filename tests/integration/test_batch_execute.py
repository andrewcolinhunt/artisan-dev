"""Scientific partitions and worker packs preserve public outputs and lineage."""

from __future__ import annotations

import pytest

from artisan.operations.examples import DataGenerator, DataTransformer
from artisan.orchestration import PipelineManager
from artisan.orchestration.engine.current_outputs import read_current_step_outputs
from artisan.orchestration.runners import LocalRunner
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.schemas.orchestration.step_lifecycle import StepStatus

pytestmark = pytest.mark.integration


def test_scientific_partitions_preserve_output_membership(
    pipeline_env: dict[str, str],
) -> None:
    with PipelineManager.create(
        "partition-parity", **pipeline_env, default_step_runner=LocalRunner()
    ) as pipeline:
        source = pipeline.run(DataGenerator, params={"count": 3, "seed": 42})
        results = [
            pipeline.run(
                DataTransformer,
                inputs={"dataset": source.output("datasets")},
                params={"scale_factor": 0.5, "noise_amplitude": 0.0, "variants": 1},
                batch_strategy={"artifacts_per_unit": size},
            )
            for size in (1, 3)
        ]
        assert all(result.status == StepStatus.SUCCEEDED for result in results)
        memberships = [
            [
                item.artifact.artifact_id
                for role in read_current_step_outputs(
                    pipeline.committer, result.step_run_id
                )
                for item in role.occurrences
            ]
            for result in results
        ]
        assert len(memberships[0]) == len(memberships[1]) == 3
        assert set(memberships[0]) == set(memberships[1])
        assert [result.total_count for result in results] == [3, 1]
    assert pipeline.ownership_state() == OwnershipState.CLOSED


def test_pipeline_with_batched_transform(pipeline_env: dict[str, str]) -> None:
    with PipelineManager.create(
        "packed-transform", **pipeline_env, default_step_runner=LocalRunner()
    ) as pipeline:
        source = pipeline.run(DataGenerator, params={"count": 4, "seed": 42})
        transformed = pipeline.run(
            DataTransformer,
            inputs={"dataset": source.output("datasets")},
            params={"scale_factor": 0.5, "variants": 1},
            batch_strategy={"artifacts_per_unit": 1, "units_per_worker": 2},
        )
        assert transformed.status == StepStatus.SUCCEEDED, transformed.error
        assert transformed.total_count == transformed.succeeded_count == 4
        outputs = read_current_step_outputs(pipeline.committer, transformed.step_run_id)
        assert sum(len(role.occurrences) for role in outputs) == 4
        execution = pipeline.executions[-1]
        assert len(execution.handles) == 4
        assert len(execution.worker_owners) == 2
        assert all(
            owner.ownership_state() == OwnershipState.CLOSED
            for owner in execution.worker_owners
        )
    assert pipeline.ownership_state() == OwnershipState.CLOSED
