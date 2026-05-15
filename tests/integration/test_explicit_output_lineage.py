"""Integration test for explicit output->output lineage via source_original_name.

A curator returns ``ArtifactResult`` with two output roles, declaring an
output->output edge by referencing a co-produced artifact's
``original_name`` rather than its (not-yet-assigned) ``artifact_id``.
The test verifies the resulting provenance edge is staged.
"""

from __future__ import annotations

import json
import os
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, ClassVar

import polars as pl
import pytest
from pydantic import BaseModel

pytestmark = pytest.mark.integration

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.base.per_artifact import PerArtifact
from artisan.operations.examples import DataGenerator
from artisan.orchestration import PipelineManager
from artisan.orchestration.runners import Runner
from artisan.schemas import ArtifactResult, LineageMapping
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.artifact_store import ArtifactStore

from .conftest import get_artifact_edges, load_artifact_edges


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


class TwoInputParity(OperationDefinition):
    """Two-input creator op for auto-detect / explicit-lineage parity.

    A ``Params`` flag toggles between returning ``ArtifactResult(lineage=None)``
    (auto-detect) and returning explicit primary + co-input ``LineageMapping``s
    matching the ZIP pairing. The same input fixtures and same execute body
    produce identical content-addressed outputs in both modes, so any edge-set
    difference observed downstream is attributable to the validator's
    explicit-lineage path.
    """

    name = "two_input_parity"
    description = "Two-input ZIP op toggling auto-detect vs explicit lineage"

    class Params(BaseModel):
        use_explicit_lineage: bool = False

    params: Params = Params()

    class InputRole(StrEnum):
        primary = auto()
        secondary = auto()

    class OutputRole(StrEnum):
        result = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.primary: InputSpec(artifact_type=ArtifactTypes.DATA),
        InputRole.secondary: InputSpec(artifact_type=ArtifactTypes.DATA),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.result: OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            infer_lineage_from={"inputs": ["primary", "secondary"]},
        ),
    }
    group_by: GroupByStrategy | None = GroupByStrategy.ZIP

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            role: PerArtifact([a.materialized_path for a in artifacts])
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute(self, inputs: ExecuteInput) -> dict[str, Any]:
        out_dir = inputs.execute_dir
        os.makedirs(out_dir, exist_ok=True)

        primary_paths = inputs.inputs.get("primary", [])
        secondary_paths = inputs.inputs.get("secondary", [])
        if isinstance(primary_paths, (str, Path)):
            primary_paths = [primary_paths]
        if isinstance(secondary_paths, (str, Path)):
            secondary_paths = [secondary_paths]

        primary = Path(primary_paths[0])
        secondary = Path(secondary_paths[0])
        out_path = Path(out_dir) / f"{primary.stem}__{secondary.stem}.json"
        out_path.write_text(
            json.dumps(
                {
                    "primary": primary.read_text(),
                    "secondary": secondary.read_text(),
                }
            )
        )
        return {}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        drafts: list[MetricArtifact] = []
        for f in inputs.file_outputs:
            if not f.endswith(".json"):
                continue
            content = json.loads(Path(f).read_text())
            drafts.append(
                MetricArtifact.draft(
                    content=content,
                    original_name=Path(f).stem,
                    step_number=inputs.step_number,
                )
            )

        if not self.params.use_explicit_lineage:
            return ArtifactResult(success=True, artifacts={"result": drafts})

        # Explicit ZIP lineage: each draft descends from primary[i] + secondary[i]
        # for the same unit. With artifacts_per_unit=1 (default), each execute
        # call processes one pair and produces one draft, so indices align.
        primary_arts = inputs.input_artifacts["primary"]
        secondary_arts = inputs.input_artifacts["secondary"]
        lineage_mappings: list[LineageMapping] = []
        for idx, draft in enumerate(drafts):
            primary_art = primary_arts[idx % len(primary_arts)]
            secondary_art = secondary_arts[idx % len(secondary_arts)]
            lineage_mappings.append(
                LineageMapping(
                    draft_original_name=draft.original_name,
                    source_artifact_id=primary_art.artifact_id,
                    source_role="primary",
                )
            )
            lineage_mappings.append(
                LineageMapping(
                    draft_original_name=draft.original_name,
                    source_artifact_id=secondary_art.artifact_id,
                    source_role="secondary",
                )
            )
        return ArtifactResult(
            success=True,
            artifacts={"result": drafts},
            lineage={"result": lineage_mappings},
        )


def _run_parity_pipeline(
    root: Path,
    *,
    use_explicit_lineage: bool,
) -> str:
    """Run the parity pipeline in an isolated delta_root and return its path."""
    delta_root = root / "delta"
    staging_root = root / "staging"
    working_root = root / "working"
    delta_root.mkdir(parents=True)
    staging_root.mkdir()
    working_root.mkdir()

    pipeline = PipelineManager.create(
        name=f"parity_{'explicit' if use_explicit_lineage else 'auto'}",
        delta_root=str(delta_root),
        staging_root=str(staging_root),
        working_root=str(working_root),
    )
    primary = pipeline.run(
        DataGenerator,
        params={"count": 2, "seed": 7},
        step_runner=Runner.LOCAL,
    )
    secondary = pipeline.run(
        DataGenerator,
        params={"count": 2, "seed": 19},
        step_runner=Runner.LOCAL,
    )
    pipeline.run(
        TwoInputParity,
        inputs={
            "primary": primary.output("datasets"),
            "secondary": secondary.output("datasets"),
        },
        params={"use_explicit_lineage": use_explicit_lineage},
        step_runner=Runner.LOCAL,
    )
    result = pipeline.finalize()
    assert result["overall_success"], (
        f"parity pipeline (use_explicit_lineage={use_explicit_lineage}) failed"
    )
    return str(delta_root)


def test_explicit_lineage_with_coinput_edges_matches_autodetect(
    tmp_path: Path,
) -> None:
    """Auto-detect and explicit lineage produce identical artifact_edges.

    Runs the same two-input ZIP op twice — once relying on auto-detect, once
    declaring explicit primary + co-input ``LineageMapping``s — and asserts
    the resulting ``artifact_edges`` rows match on
    ``(source_artifact_id, target_artifact_id, source_role, target_role)``.
    Without the validator relaxation, the explicit run would have raised
    ``LineageIntegrityError`` on the second mapping per draft.
    """
    auto_root = _run_parity_pipeline(tmp_path / "auto", use_explicit_lineage=False)
    explicit_root = _run_parity_pipeline(
        tmp_path / "explicit", use_explicit_lineage=True
    )

    auto_store = ArtifactStore(auto_root)
    explicit_store = ArtifactStore(explicit_root)
    auto_targets = auto_store.load_artifact_ids_by_type(
        ArtifactTypes.METRIC, step_numbers=[2]
    )
    explicit_targets = explicit_store.load_artifact_ids_by_type(
        ArtifactTypes.METRIC, step_numbers=[2]
    )
    assert auto_targets == explicit_targets, (
        "Step-2 metric artifact_ids must match across runs "
        "(inputs and outputs are content-addressed and deterministic)."
    )
    assert auto_targets, "expected step-2 metric outputs"

    auto_edges = load_artifact_edges(auto_root, auto_targets)
    explicit_edges = load_artifact_edges(explicit_root, explicit_targets)

    cols = [
        "source_artifact_id",
        "target_artifact_id",
        "source_role",
        "target_role",
    ]
    auto_tuples = set(map(tuple, auto_edges.select(cols).iter_rows()))
    explicit_tuples = set(map(tuple, explicit_edges.select(cols).iter_rows()))

    assert auto_tuples == explicit_tuples, (
        f"Edge set diff — only in auto: {auto_tuples - explicit_tuples}; "
        f"only in explicit: {explicit_tuples - auto_tuples}"
    )
    # Sanity: each output draft should have at least 2 parents (primary + secondary).
    assert len(auto_tuples) >= 2 * len(auto_targets), (
        "Expected primary + co-input edges; got only "
        f"{len(auto_tuples)} edges for {len(auto_targets)} outputs."
    )
