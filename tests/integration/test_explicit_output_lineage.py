"""Persistence regressions for operation-owned explicit parent declarations."""

from __future__ import annotations

import json
import os
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, ClassVar

import polars as pl
import pytest
from pydantic import BaseModel, Field

pytestmark = pytest.mark.integration

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.examples import DataGenerator
from artisan.orchestration import PipelineManager
from artisan.orchestration.runners import Runner
from artisan.schemas import ArtifactResult, ExecutionConfigArtifact, LineageMapping
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

from .conftest import (
    get_artifact_edges,
    load_artifact_edges,
    prepare_paired_files,
    read_table,
)


class StructureAndMetricCurator(OperationDefinition):
    """Curator producing two output roles linked by explicit output lineage.

    For each input dataset, emit a ``structures`` metric and a derived
    ``metrics`` artifact. The metric declares its parent via
    its exact output index before the structure has a finalized artifact ID.
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
        OutputRole.structures: OutputSpec(
            artifact_type=ArtifactTypes.METRIC, derives_from={"inputs": ["datasets"]}
        ),
        OutputRole.metrics: OutputSpec(
            artifact_type=ArtifactTypes.METRIC, derives_from={"outputs": ["structures"]}
        ),
    }

    def execute_curator(
        self,
        inputs: dict[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> ArtifactResult:
        dataset_ids = inputs["datasets"]["artifact_id"].to_list()

        result = ArtifactResult()

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
            parent_index = result.add_artifact(
                "structures", structure, sources={"datasets": [dataset_id]}
            )
            result.add_artifact(
                "metrics", metric, sources={"structures": [parent_index]}
            )

        return result


def test_explicit_output_to_output_lineage(pipeline_env: dict[str, str]) -> None:
    """An output index resolves to the exact persisted sibling artifact."""
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
    step1_metric_ids = artifact_store.provenance.load_artifact_ids_by_type(
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
    """Two-input creator proving manual and helper declarations are equivalent."""

    name = "two_input_parity"
    description = "Two-input ZIP operation with explicit parent sets"

    class Params(BaseModel):
        use_helper: bool = Field(
            default=False, description="Build mappings through add_artifact."
        )

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
            derives_from={"inputs": ["primary", "secondary"]},
        ),
    }
    group_by: GroupByStrategy | None = GroupByStrategy.ZIP

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return prepare_paired_files(inputs)

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
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
        return {
            "records": [
                {"path": str(out_path), "sources": inputs.inputs["source_ids"][0]}
            ]
        }

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult(artifacts={"result": []}, lineage={"result": []})
        for record in inputs.memory_outputs["records"]:
            draft = MetricArtifact.draft(
                content=json.loads(Path(record["path"]).read_text()),
                original_name="identical-name.json",
                step_number=inputs.step_number,
            )
            if self.params.use_helper:
                result.add_artifact("result", draft, sources=record["sources"])
            else:
                index = len(result.artifacts["result"])
                result.artifacts["result"].append(draft)
                result.lineage["result"].extend(
                    LineageMapping(
                        draft_index=index, source_role=role, source_artifact_id=source
                    )
                    for role, sources in record["sources"].items()
                    for source in sources
                )
        return result


def _run_parity_pipeline(
    root: Path,
    *,
    use_helper: bool,
) -> str:
    """Run the parity pipeline in an isolated delta_root and return its path."""
    delta_root = root / "delta"
    staging_root = root / "staging"
    working_root = root / "working"
    delta_root.mkdir(parents=True)
    staging_root.mkdir()
    working_root.mkdir()

    pipeline = PipelineManager.create(
        name=f"parity_{'helper' if use_helper else 'manual'}",
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
        params={"use_helper": use_helper},
        step_runner=Runner.LOCAL,
    )
    result = pipeline.finalize()
    assert result["overall_success"], (
        f"parity pipeline (use_helper={use_helper}) failed"
    )
    return str(delta_root)


def test_manual_and_helper_persist_identical_edges_and_groups(tmp_path: Path) -> None:
    """Both authoring forms persist exact parents despite duplicate output names."""
    manual_root = _run_parity_pipeline(tmp_path / "manual", use_helper=False)
    helper_root = _run_parity_pipeline(tmp_path / "helper", use_helper=True)
    targets = ArtifactStore(manual_root).provenance.load_artifact_ids_by_type(
        ArtifactTypes.METRIC, step_numbers=[2]
    )
    assert targets == ArtifactStore(helper_root).provenance.load_artifact_ids_by_type(
        ArtifactTypes.METRIC, step_numbers=[2]
    )
    assert len(targets) == 2
    columns = [
        "source_artifact_id",
        "target_artifact_id",
        "source_role",
        "target_role",
        "group_id",
    ]
    manual = load_artifact_edges(manual_root, targets)
    helper = load_artifact_edges(helper_root, targets)
    assert set(manual.select(columns).iter_rows()) == set(
        helper.select(columns).iter_rows()
    )
    assert manual.height == 4
    assert manual["group_id"].null_count() == 0
    for target in targets:
        rows = manual.filter(pl.col("target_artifact_id") == target)
        assert set(rows["source_role"]) == {"primary", "secondary"}
        assert rows["group_id"].n_unique() == 1


class ConfigDeclarationCurator(OperationDefinition):
    """Declare config references in the operation, including same-role fan-in."""

    name = "config_declaration_curator"

    class Params(BaseModel):
        declare_all: bool = Field(
            default=True, description="Declare every referenced artifact as a parent."
        )

    params: Params = Params()

    class InputRole(StrEnum):
        referenced = auto()

    class OutputRole(StrEnum):
        config = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {
        "referenced": InputSpec(artifact_type="data"),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "config": OutputSpec(
            artifact_type="config", derives_from={"inputs": ["referenced"]}
        ),
    }

    def execute_curator(self, inputs, step_number, artifact_store) -> ArtifactResult:
        ids = inputs["referenced"]["artifact_id"].to_list()
        config = ExecutionConfigArtifact.draft(
            content={"sources": [{"$artifact": source} for source in [*ids, ids[0]]]},
            original_name="no-filename-correspondence.json",
            step_number=step_number,
        )
        parents = sorted(set(config.get_artifact_references()))
        if not self.params.declare_all:
            parents = parents[:1]
        result = ArtifactResult()
        result.add_artifact("config", config, sources={"referenced": parents})
        return result


@pytest.mark.parametrize("declare_all", [True, False])
def test_config_parents_are_operation_declared_once(pipeline_env, declare_all) -> None:
    """Repeated references deduplicate; the executor never supplements declarations."""
    pipeline = PipelineManager.create(name="explicit_config", **pipeline_env)
    generated = pipeline.run(DataGenerator, params={"count": 2, "seed": 7})
    pipeline.run(
        ConfigDeclarationCurator,
        inputs={"referenced": generated.output("datasets")},
        params={"declare_all": declare_all},
    )
    assert pipeline.finalize()["overall_success"]
    store = ArtifactStore(pipeline_env["delta_root"])
    ids = store.provenance.load_artifact_ids_by_type("config", step_numbers=[1])
    assert len(ids) == 1
    configs = store.get_artifacts_by_type(list(ids), "config")
    refs = sorted(set(next(iter(configs.values())).get_artifact_references()))
    assert len(refs) == 2
    edges = load_artifact_edges(pipeline_env["delta_root"], ids)
    expected = refs if declare_all else refs[:1]
    assert sorted(edges["source_artifact_id"]) == expected
    assert set(edges["source_role"]) == {"referenced"}
    assert set(edges["target_role"]) == {"config"}
    assert edges["group_id"].null_count() == (0 if declare_all else 1)


class DeclarationCreator(OperationDefinition):
    """Emit a perfectly named output and optionally omit its declaration."""

    name = "declaration_creator"

    class Params(BaseModel):
        omit: bool = Field(
            default=False, description="Omit lineage to exercise failure behavior."
        )

    params: Params = Params()

    class InputRole(StrEnum):
        dataset = auto()

    class OutputRole(StrEnum):
        result = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {
        "dataset": InputSpec(artifact_type="data"),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "result": OutputSpec(
            artifact_type="metric", derives_from={"inputs": ["dataset"]}
        ),
    }

    def preprocess(self, inputs: PreprocessInput) -> dict:
        return {}

    def execute_function(self, inputs: ExecuteInput) -> None:
        pass

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        parent = inputs.input_artifacts["dataset"][0]
        result = ArtifactResult()
        result.add_artifact(
            "result",
            MetricArtifact.draft(
                content={"score": 1},
                original_name=parent.original_name,
                step_number=inputs.step_number,
            ),
            sources={"dataset": [parent.artifact_id]},
        )
        if self.params.omit:
            result.lineage = {}
        return result


class DeclarationCurator(DeclarationCreator):
    """Exercise the same required declaration on the curator boundary."""

    name = "declaration_curator"
    execute_function = OperationDefinition.execute_function

    def execute_curator(self, inputs, step_number, artifact_store) -> ArtifactResult:
        parent_id = inputs["dataset"]["artifact_id"][0]
        parent = artifact_store.get_artifacts_by_type([parent_id], "data")[parent_id]
        return self.postprocess(
            PostprocessInput(
                input_artifacts={"dataset": [parent]},
                step_number=step_number,
                memory_outputs=None,
                file_outputs=[],
                postprocess_dir="",
            )
        )


@pytest.mark.parametrize("operation", [DeclarationCreator, DeclarationCurator])
@pytest.mark.parametrize("omit", [True, False])
def test_missing_declaration_never_falls_back_to_names(
    pipeline_env, operation, omit
) -> None:
    """Matching filenames cannot rescue a missing declaration or stage success."""
    pipeline = PipelineManager.create(name="required_declaration", **pipeline_env)
    generated = pipeline.run(DataGenerator, params={"count": 1})
    step = pipeline.run(
        operation,
        inputs={"dataset": generated.output("datasets")},
        params={"omit": omit},
    )
    assert bool(step.failed_count) is omit
    assert pipeline.finalize()["overall_success"] is (not omit)
    executions = read_table(pipeline_env["delta_root"], "orchestration/executions")
    current = executions.filter(pl.col("origin_step_number") == 1)
    assert current.height == 1
    assert current["success"][0] is (not omit)
