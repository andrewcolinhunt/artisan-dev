"""Integration test for explicit output->output lineage via source_original_name.

A curator returns ``ArtifactResult`` with two output roles, declaring an
output->output edge by referencing a co-produced artifact's
``original_name`` rather than its (not-yet-assigned) ``artifact_id``.
The test verifies the resulting provenance edge is staged.
"""

from __future__ import annotations

from enum import StrEnum, auto
from typing import ClassVar

import polars as pl
import pytest

pytestmark = pytest.mark.integration

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.examples import DataGenerator
from artisan.orchestration import PipelineManager
from artisan.orchestration.runners import Runner
from artisan.schemas import ArtifactResult, LineageMapping
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.artifact_store import ArtifactStore

from .conftest import get_artifact_edges


class StructureAndMetricCurator(OperationDefinition):
    """Curator producing two output roles linked by explicit output lineage.

    For each input dataset, emit a ``structures`` metric and a derived
    ``metrics`` artifact. The metric declares its parent via
    ``source_original_name`` because the structure's ``artifact_id`` is
    not yet assigned at curator return time.
    """

    name = "structure_and_metric_curator"
    description = "Emit structures and metrics with explicit output->output lineage"

    class InputRole(StrEnum):
        datasets = auto()

    class OutputRole(StrEnum):
        structures = auto()
        metrics = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.datasets: InputSpec(artifact_type=ArtifactTypes.DATA),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.structures: OutputSpec(artifact_type=ArtifactTypes.METRIC),
        OutputRole.metrics: OutputSpec(artifact_type=ArtifactTypes.METRIC),
    }

    def execute_curator(
        self,
        inputs: dict[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> ArtifactResult:
        dataset_ids = inputs["datasets"]["artifact_id"].to_list()

        structures: list = []
        metrics: list = []
        lineage_metrics: list[LineageMapping] = []

        for i, dataset_id in enumerate(dataset_ids):
            structure = MetricArtifact.draft(
                content={"dataset_id": dataset_id, "structure_index": i},
                original_name=f"sample_{i:03d}_structure.json",
                step_number=step_number,
            )
            metric = MetricArtifact.draft(
                content={"dataset_id": dataset_id, "energy": -float(i)},
                original_name=f"sample_{i:03d}_structure_energy.json",
                step_number=step_number,
            )
            structures.append(structure)
            metrics.append(metric)
            lineage_metrics.append(
                LineageMapping(
                    draft_original_name=metric.original_name,
                    source_original_name=structure.original_name,
                    source_role="structures",
                )
            )

        return ArtifactResult(
            success=True,
            artifacts={"structures": structures, "metrics": metrics},
            lineage={"metrics": lineage_metrics},
        )


def test_explicit_output_to_output_lineage(pipeline_env: dict[str, str]) -> None:
    """source_original_name produces an output->output edge in the staged store."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_explicit_output_lineage",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 2, "seed": 7},
        step_runner=Runner.LOCAL,
    )

    pipeline.run(
        StructureAndMetricCurator,
        inputs={"datasets": step0.output("datasets")},
        step_runner=Runner.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    artifact_store = ArtifactStore(delta_root)
    step1_metric_ids = artifact_store.load_artifact_ids_by_type(
        ArtifactTypes.METRIC, step_numbers=[1]
    )
    step1_metrics = artifact_store.get_artifacts_by_type(
        list(step1_metric_ids), ArtifactTypes.METRIC
    )
    by_name = {a.original_name: a for a in step1_metrics.values()}

    structure_a = by_name["sample_000_structure"]
    metric_a = by_name["sample_000_structure_energy"]
    assert structure_a.artifact_id is not None
    assert metric_a.artifact_id is not None

    edge_targets = get_artifact_edges(delta_root, structure_a.artifact_id)
    assert metric_a.artifact_id in edge_targets, (
        f"Expected output->output edge from structure {structure_a.artifact_id} "
        f"to metric {metric_a.artifact_id}, got targets {edge_targets}."
    )
