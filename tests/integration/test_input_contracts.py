"""Explicit contracts survive admission, native execution, cache and replay."""

from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.input_contract_operations import OffsetGenerator, SharedOffset, metric

from artisan.orchestration import replay_execution
from artisan.orchestration.engine.retained_execution_query import (
    read_retained_execution_membership,
)
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.storage.core.execution_source import (
    ScientificExecutionSource,
    read_execution_source,
)
from artisan.storage.core.retained_reader import RetainedReader

from .conftest import _SerialNativePipelineRunner, create_native_pipeline

pytestmark = pytest.mark.integration


def test_shared_input_and_optional_parents_survive_retained_cache_and_replay(
    tmp_path: Path,
) -> None:
    with create_native_pipeline(
        name="contracts",
        delta_root=tmp_path / "delta",
        staging_root=tmp_path / "staging",
        working_root=tmp_path / "work",
    ) as pipeline:
        generated = pipeline.submit(OffsetGenerator)
        inputs = {
            "source": [metric(0), metric(1), metric(0)],
            "offset": generated.output("offset"),
            "context": [metric(20), metric(21), metric(22)],
        }
        result = pipeline.run(SharedOffset, inputs=inputs)
        assert result.status == StepStatus.SUCCEEDED, result.error
        assert result.total_count == 2
        sources = [
            item.source
            for item in read_retained_execution_membership(
                pipeline.committer, pipeline_run_id=pipeline.config.pipeline_run_id
            )
            if item.state.step_run_id == result.step_run_id
        ]
        assert len(sources) == 2
        assert all(isinstance(source, ScientificExecutionSource) for source in sources)
        original = sources[0]
        assert isinstance(original, ScientificExecutionSource)
        reader = RetainedReader(pipeline.committer)
        for source in sources:
            assert isinstance(source, ScientificExecutionSource)
            assert source.admission.recipe.schema_version == 2
            assert len(source.admission.boundary.input_roles()["offset"]) == 1
            accepted = reader.read(source.plan.receipt)
            for item in accepted.output("metrics"):
                parents = [
                    edge.source
                    for edge in accepted.projection.parents
                    if edge.target == item.reference.address
                ]
                assert [
                    parent.index for parent in parents if parent.role == "offset"
                ] == [0]
        cached = pipeline.run(SharedOffset, inputs=inputs)
        assert cached.status == StepStatus.SUCCEEDED, cached.error
        assert cached.disposition == StepDisposition.CACHE_HIT
        changed = pipeline.run(SharedOffset, inputs={**inputs, "offset": [metric(11)]})
        assert changed.status == StepStatus.SUCCEEDED, changed.error
        assert changed.disposition != StepDisposition.CACHE_HIT
    diagnostic = replay_execution(
        original.plan.receipt.delivery.delivery_id,
        delta_root=pipeline.committer.delta_root,
        operation=SharedOffset,
        step_runner=_SerialNativePipelineRunner(pipeline.committer),
        working_root=tmp_path / "replay-work",
        staging_root=str(tmp_path / "replay-staging"),
    )
    assert diagnostic.step_result.status == StepStatus.SUCCEEDED, (
        diagnostic.step_result.error
    )
    selected = read_execution_source(pipeline.committer, diagnostic.execution_run_id)
    assert isinstance(selected, ScientificExecutionSource)
    assert selected.admission.recipe == original.admission.recipe
    assert len(selected.admission.boundary.input_roles()["offset"]) == 1
    assert [
        item.artifact.artifact_id
        for item in reader.read(selected.plan.receipt).output("metrics")
    ] == [
        item.artifact.artifact_id
        for item in reader.read(original.plan.receipt).output("metrics")
    ]


def test_output_reference_shared_cardinality_fails_before_dispatch(
    tmp_path: Path,
) -> None:
    with create_native_pipeline(
        name="cardinality",
        delta_root=tmp_path / "delta",
        staging_root=tmp_path / "staging",
        working_root=tmp_path / "work",
    ) as pipeline:
        # A selected result reference may resolve to several ordinary occurrences.
        produced = pipeline.run(
            SharedOffset,
            inputs={"source": [metric(0), metric(1)], "offset": [metric(10)]},
        )
        assert produced.status == StepStatus.SUCCEEDED, produced.error
        rejected = pipeline.run(
            SharedOffset, inputs={"source": [], "offset": produced.output("metrics")}
        )
        assert rejected.status == StepStatus.FAILED
        assert "exactly one occurrence" in str(rejected.error)
        skipped = pipeline.run(
            SharedOffset, inputs={"source": [], "offset": [metric(10)]}
        )
        assert skipped.status == StepStatus.SKIPPED
        assert skipped.total_count == 0
