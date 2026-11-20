"""Fixed inputs survive native execution, accepted storage, caching and replay."""

from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.fixed_default_operations import (
    DefaultOffset,
    DefaultPair,
    NestedDefaultPair,
    OtherDefaultOffset,
)
from fixtures.input_contract_operations import metric

from artisan.orchestration import replay_execution
from artisan.orchestration.engine.retained_execution_query import (
    read_retained_execution_membership,
)
from artisan.schemas import InputSpec
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.storage.core.execution_source import (
    ScientificExecutionSource,
    read_execution_source,
)
from artisan.storage.core.retained_reader import RetainedReader

from .conftest import _SerialNativePipelineRunner, create_native_pipeline

pytestmark = pytest.mark.integration


def test_root_default_and_explicit_same_artifact_share_retained_cache(
    tmp_path: Path,
) -> None:
    with create_native_pipeline(
        name="default-root",
        delta_root=tmp_path / "delta",
        staging_root=tmp_path / "staging",
        working_root=tmp_path / "work",
    ) as pipeline:
        inputs = {"source": [metric(index) for index in range(3)]}
        first = pipeline.run(DefaultOffset, inputs=inputs)
        assert first.status == StepStatus.SUCCEEDED, first.error
        assert first.total_count == 2
        explicit = pipeline.run(
            DefaultOffset, inputs={**inputs, "offset": [metric(10)]}
        )
        assert explicit.status == StepStatus.SUCCEEDED, explicit.error
        assert explicit.disposition == StepDisposition.CACHE_HIT
        changed = pipeline.run(DefaultOffset, inputs={**inputs, "offset": [metric(30)]})
        assert changed.status == StepStatus.SUCCEEDED, changed.error
        assert changed.disposition != StepDisposition.CACHE_HIT


def test_nested_defaults_keep_old_selection_on_replay_after_declaration_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with create_native_pipeline(
        name="nested-defaults",
        delta_root=tmp_path / "delta",
        staging_root=tmp_path / "staging",
        working_root=tmp_path / "work",
    ) as pipeline:
        inputs = {"source": [metric(index) for index in range(4)]}
        first = pipeline.run(NestedDefaultPair, inputs=inputs)
        assert first.status == StepStatus.SUCCEEDED, first.error
        assert first.total_count == 2
        sources = [
            item.source
            for item in read_retained_execution_membership(
                pipeline.committer, pipeline_run_id=pipeline.config.pipeline_run_id
            )
            if item.state.step_run_id == first.step_run_id
        ]
        assert len(sources) == 2
        original = sources[0]
        assert isinstance(original, ScientificExecutionSource)
        assert original.admission.recipe.schema_version == 3
        assert list(original.admission.boundary.input_roles()) == ["source"]
        assert len(original.admission.boundary.frozen_context.constants) == 3
        reader = RetainedReader(pipeline.committer)
        accepted = reader.read(original.plan.receipt)
        positions = {
            address
            for item in accepted.projection.frozen_context.constants
            for address in item.occurrences
        }
        assert any(edge.source in positions for edge in accepted.projection.parents)
        cached = pipeline.run(NestedDefaultPair, inputs=inputs)
        assert cached.status == StepStatus.SUCCEEDED, cached.error
        assert cached.disposition == StepDisposition.CACHE_HIT
        monkeypatch.setattr(
            DefaultOffset,
            "inputs",
            {
                **DefaultOffset.inputs,
                "offset": InputSpec(
                    artifact_type="metric", materialize=False, default=(metric(30),)
                ),
            },
        )
        changed = pipeline.run(NestedDefaultPair, inputs=inputs)
        assert changed.status == StepStatus.SUCCEEDED, changed.error
        assert changed.disposition != StepDisposition.CACHE_HIT
    diagnostic = replay_execution(
        original.plan.receipt.delivery.delivery_id,
        delta_root=pipeline.committer.delta_root,
        operation=NestedDefaultPair,
        definitions=(DefaultPair, DefaultOffset, OtherDefaultOffset),
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
    assert [
        item.artifact.artifact_id
        for item in reader.read(selected.plan.receipt).output("metrics")
    ] == [item.artifact.artifact_id for item in accepted.output("metrics")]
