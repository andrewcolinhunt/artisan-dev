"""Diagnostic replay through installed operations, actual workers and acceptance."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from artisan.composites.examples import GenerateAndScore, TransformAndScore
from artisan.operations.curator import IngestCSV, Merge
from artisan.operations.examples import DataGenerator, DataTransformer, MetricCalculator
from artisan.orchestration import PipelineManager, StepStatus, replay_execution
from artisan.orchestration.engine.retained_execution_query import (
    read_retained_execution_membership,
)
from artisan.schemas.provenance.projection import BoundaryProjection
from artisan.storage.cache.retained_lookup import lookup_assignment
from artisan.storage.core.execution_source import (
    CuratorExecutionSource,
    ScientificExecutionSource,
    read_execution_source,
)
from artisan.storage.core.retained_reader import RetainedReader

pytestmark = pytest.mark.integration


def _manager(root: Path) -> PipelineManager:
    return PipelineManager.create(
        "replay-source",
        str(root / "delta"),
        str(root / "staging"),
        working_root=str(root / "work"),
    )


def _scientific_source(
    manager: PipelineManager, step_run_id: str
) -> ScientificExecutionSource:
    selected = [
        item.source
        for item in read_retained_execution_membership(
            manager.committer, pipeline_run_id=manager.config.pipeline_run_id
        )
        if item.state.step_run_id == step_run_id
    ]
    assert len(selected) == 1
    source = selected[0]
    assert isinstance(source, ScientificExecutionSource)
    return source


def _input_ids(boundary: BoundaryProjection, role: str) -> list[str]:
    occurrences = {item.address: item.artifact_id for item in boundary.occurrences}
    selected = next(item for item in boundary.boundary_inputs if item.role == role)
    return [occurrences[address] for address in selected.occurrences]


def test_replay_preserves_duplicate_inputs_with_fresh_diagnostic_ownership(
    tmp_path: Path,
) -> None:
    with _manager(tmp_path) as manager:
        generated = manager.run(DataGenerator, params={"count": 2, "seed": 7})
        assert generated.status is StepStatus.SUCCEEDED, generated.error
        reader = RetainedReader(manager.committer)
        generated_source = _scientific_source(manager, generated.step_run_id)
        ids = [
            item.artifact.artifact_id
            for item in reader.read(generated_source.plan.receipt).output("datasets")
        ]
        inputs = [ids[1], ids[0], ids[1]]
        transformed = manager.run(
            DataTransformer,
            inputs={"dataset": inputs},
            params={"seed": 7},
            batch_strategy={"artifacts_per_unit": 3},
        )
        assert transformed.status is StepStatus.SUCCEEDED, transformed.error
        source = _scientific_source(manager, transformed.step_run_id)
    source_id = source.plan.receipt.delivery.delivery_id
    original = reader.read(source.plan.receipt)
    level = logging.getLogger("artisan").level
    result = replay_execution(
        source_id,
        delta_root=manager.committer.delta_root,
        operation=DataTransformer,
        working_root=tmp_path / "diagnostic-work",
        staging_root=str(tmp_path / "diagnostic-staging"),
    )
    assert logging.getLogger("artisan").level == level
    assert result.step_result.status is StepStatus.SUCCEEDED, result.step_result.error
    assert result.diagnostic_status == "complete", result.diagnostic_errors
    assert result.execution_run_id
    assert result.execution_run_id != source_id
    assert result.step_run_id != transformed.step_run_id
    replayed = read_execution_source(manager.committer, result.execution_run_id)
    assert isinstance(replayed, ScientificExecutionSource)
    assert replayed.admission.attempt.replay_of_execution_run_id == source_id
    assert _input_ids(replayed.admission.boundary, "dataset") == inputs
    assert [
        item.artifact.artifact_id
        for item in reader.read(replayed.plan.receipt).output("dataset")
    ] == [item.artifact.artifact_id for item in original.output("dataset")]
    assert lookup_assignment(replayed.plan.admission, reader=reader) is None
    assert read_execution_source(manager.committer, source_id) == source
    assert reader.read(source.plan.receipt).projection == original.projection


def test_replay_composite_reexecutes_one_outer_boundary(tmp_path: Path) -> None:
    with _manager(tmp_path) as manager:
        step = manager.run(GenerateAndScore, params={"count": 2, "seed": 19})
        assert step.status is StepStatus.SUCCEEDED, step.error
        source = _scientific_source(manager, step.step_run_id)
    result = replay_execution(
        source.plan.receipt.delivery.delivery_id,
        delta_root=manager.committer.delta_root,
        operation=GenerateAndScore,
        definitions=(
            DataGenerator,
            TransformAndScore,
            DataTransformer,
            MetricCalculator,
        ),
        working_root=tmp_path / "diagnostic-work",
    )
    assert result.step_result.status is StepStatus.SUCCEEDED, result.step_result.error
    selected = read_retained_execution_membership(
        manager.committer, pipeline_run_id=result.pipeline_run_id
    )
    assert len(selected) == 1
    diagnostic = selected[0].source
    assert isinstance(diagnostic, ScientificExecutionSource)
    reader = RetainedReader(manager.committer)
    for role in ("dataset", "metrics"):
        assert [
            item.artifact.artifact_id
            for item in reader.read(diagnostic.plan.receipt).output(role)
        ] == [
            item.artifact.artifact_id
            for item in reader.read(source.plan.receipt).output(role)
        ]
    assert lookup_assignment(diagnostic.plan.admission, reader=reader) is None


@pytest.mark.parametrize("operation", [IngestCSV, Merge])
def test_replay_curator_uses_captured_inputs_after_original_file_removal(
    tmp_path: Path, operation: type[IngestCSV] | type[Merge]
) -> None:
    path = tmp_path / "input.csv"
    path.write_text("id,x\n0,1\n1,2\n")
    with _manager(tmp_path) as manager:
        ingested = manager.run(IngestCSV, inputs=[str(path)])
        assert ingested.status is StepStatus.SUCCEEDED, ingested.error
        step = (
            ingested
            if operation is IngestCSV
            else manager.run(Merge, inputs={"datasets": ingested.output("csv")})
        )
        assert step.status is StepStatus.SUCCEEDED, step.error
        selected = [
            item.source
            for item in read_retained_execution_membership(
                manager.committer, pipeline_run_id=manager.config.pipeline_run_id
            )
            if item.state.step_run_id == step.step_run_id
        ]
        assert len(selected) == 1
        source = selected[0]
        assert isinstance(source, CuratorExecutionSource)
        source_id = source.execution_record["execution_run_id"]
    path.unlink()
    result = replay_execution(
        source_id,
        delta_root=manager.committer.delta_root,
        operation=operation,
        working_root=tmp_path / "diagnostic-work",
    )
    assert result.step_result.status is StepStatus.SUCCEEDED, result.step_result.error
    assert result.execution_run_id
    assert result.execution_run_id != source_id
    assert result.step_run_id != step.step_run_id
    assert result.diagnostic_status == "complete", result.diagnostic_errors
    assert read_execution_source(manager.committer, source_id) == source
    diagnostic = read_execution_source(manager.committer, result.execution_run_id)
    assert isinstance(diagnostic, CuratorExecutionSource)
    assert diagnostic.execution_record["replay_of_execution_run_id"] == source_id
