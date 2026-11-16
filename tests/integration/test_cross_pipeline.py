"""Public source imports select accepted outputs and preserve required support."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from artisan.operations.curator import IngestPipelineStep, Merge
from artisan.operations.examples import (
    DataGenerator,
    DataTransformerConfig,
    MetricCalculator,
)
from artisan.orchestration import PipelineManager
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.storage import ArtifactStore

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("artifact_type", [None, "csv"])
def test_public_import_union_deduplicates_aliases_and_filters_types(
    native_pipeline: Callable[[str], PipelineManager], artifact_type: str | None
) -> None:
    with native_pipeline("source") as source:
        generated = source.run(DataGenerator, params={"count": 2})
        measured = source.run(
            MetricCalculator, inputs={"dataset": generated.output("datasets")}
        )
        merged = source.run(
            Merge, inputs=[generated.output("datasets"), generated.output("datasets")]
        )
        assert all(
            item.status == StepStatus.SUCCEEDED
            for item in (generated, measured, merged)
        )
    with native_pipeline("destination") as destination:
        params = {
            "source_delta_root": source.config.delta_root,
            "source_files_root": source.config.files_root,
            "source_run_id": source.config.pipeline_run_id,
            "source_step": merged.step_number,
            "include_prior_steps": True,
            "artifact_type": artifact_type,
        }
        first = destination.run(IngestPipelineStep, params=params)
        second = destination.run(IngestPipelineStep, params=params)
        assert first.status == second.status == StepStatus.SUCCEEDED, (
            first.error,
            second.error,
        )
        assert first.disposition == second.disposition == StepDisposition.EXECUTED
        assert first.output_roles == frozenset(
            {"csv"} if artifact_type else {"csv", "metric"}
        )
        store = ArtifactStore(
            destination.config.delta_root, files_root=destination.config.files_root
        )
        for role in first.output_roles:
            selected = store.get_artifacts_by_type(
                store.provenance.load_artifact_ids_by_type(role),
                role,
            )
            assert len(selected) == 2
            assert all(
                item.metadata["imported_from"]["pipeline_run_id"]
                == source.config.pipeline_run_id
                for item in selected.values()
            )
            assert all(
                store.provenance.get_direct_ancestors(item.artifact_id) == []
                for item in selected.values()
            )


def test_config_import_rejects_foreign_reference_even_with_redrafted_csv(
    native_pipeline: Callable[[str], PipelineManager],
) -> None:
    with native_pipeline("source") as source:
        generated = source.run(DataGenerator, params={"count": 1})
        configs = source.run(
            DataTransformerConfig, inputs={"dataset": generated.output("datasets")}
        )
        assert configs.status == StepStatus.SUCCEEDED, configs.error
    with native_pipeline("destination") as destination:
        imported = destination.run(
            IngestPipelineStep,
            params={
                "source_delta_root": source.config.delta_root,
                "source_files_root": source.config.files_root,
                "source_run_id": source.config.pipeline_run_id,
                "source_step": configs.step_number,
                "include_prior_steps": True,
            },
        )
        assert imported.status == StepStatus.FAILED
        assert "referent" in imported.error.lower()
        assert (
            source.tracker.current_state(configs.step_run_id).status
            == StepStatus.SUCCEEDED
        )
        store = ArtifactStore(
            destination.config.delta_root, files_root=destination.config.files_root
        )
        assert not store.provenance.load_artifact_ids_by_type("execution_config")
        assert not store.provenance.load_artifact_ids_by_type("csv")
