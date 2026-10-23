"""Tests for DataTransformerScript operation."""

from __future__ import annotations

import csv
import glob
import json
import os
from pathlib import Path

from fixtures.csv import make_csv

from artisan.operations.examples import DataTransformerScript
from artisan.schemas import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.artifact import ExecutionConfigArtifact
from artisan.schemas.artifact.data import DataArtifact


def _setup_grouped_inputs(
    tmp_path: Path,
    scale_factor: float = 2.0,
    noise_amplitude: float = 0.0,
    seed: int = 42,
):
    """Create a dataset artifact + config artifact pair for testing."""
    input_dir = tmp_path / "materialized"
    input_dir.mkdir(parents=True)

    # Create and materialize a data artifact
    csv_content = make_csv(rows=3, seed=1)
    data_path = str(input_dir / "dataset_00000.csv")
    with open(data_path, "wb") as fh:
        fh.write(csv_content)

    data_artifact = DataArtifact.draft(
        content=csv_content,
        original_name="dataset_00000.csv",
        step_number=0,
    )
    data_artifact.finalize()
    data_artifact = data_artifact.model_copy(update={"materialized_path": data_path})

    # Create config referencing the data artifact
    config_content = {
        "input": data_path,
        "scale_factor": scale_factor,
        "noise_amplitude": noise_amplitude,
        "seed": seed,
    }
    config_artifact = ExecutionConfigArtifact.draft(
        content=config_content,
        original_name="dataset_00000_config_0.json",
        step_number=0,
    )
    config_artifact.finalize()

    # Materialize config
    config_path = str(input_dir / "dataset_00000_config_0.json")
    with open(config_path, "w") as fh:
        fh.write(json.dumps(config_content, indent=2))
    config_artifact = config_artifact.model_copy(
        update={"materialized_path": config_path}
    )

    return data_artifact, config_artifact


class TestDataTransformerScript:
    def test_duplicate_config_names_keep_separate_outputs_and_parents(
        self, tmp_path: Path
    ):
        pairs = [
            _setup_grouped_inputs(tmp_path / str(factor), scale_factor=factor)
            for factor in (2.0, 3.0)
        ]
        input_artifacts = {
            "dataset": [pair[0] for pair in pairs],
            "config": [pair[1] for pair in pairs],
        }
        execute_dir = tmp_path / "execute"
        execute_dir.mkdir()
        op = DataTransformerScript()
        prepared = op.preprocess(
            PreprocessInput(
                input_artifacts=input_artifacts, preprocess_dir=str(tmp_path / "pre")
            )
        )
        raw = op.execute_function(
            ExecuteInput(inputs=prepared, execute_dir=str(execute_dir))
        )
        result = op.postprocess(
            PostprocessInput(
                memory_outputs=json.loads(json.dumps(raw)),
                file_outputs=[str(path) for path in execute_dir.iterdir()],
                input_artifacts=input_artifacts,
                step_number=1,
                postprocess_dir=str(tmp_path / "post"),
            )
        )
        outputs = result.artifacts["dataset"]
        assert len(list(execute_dir.glob("*.csv"))) == 2
        assert outputs[0].original_name == outputs[1].original_name
        assert outputs[0].content != outputs[1].content
        assert [
            mapping.source_artifact_id for mapping in result.lineage["dataset"]
        ] == [pair[1].artifact_id for pair in pairs]

    def test_basic_transform(self, tmp_path: Path):
        data_art, config_art = _setup_grouped_inputs(
            tmp_path, scale_factor=2.0, noise_amplitude=0.0
        )
        op = DataTransformerScript()

        execute_dir = str(tmp_path / "execute")
        os.makedirs(execute_dir, exist_ok=True)

        input_artifacts = {"dataset": [data_art], "config": [config_art]}

        prepared = op.preprocess(
            PreprocessInput(
                input_artifacts=input_artifacts,
                preprocess_dir=str(tmp_path / "pre"),
            )
        )
        raw = op.execute_function(
            ExecuteInput(inputs=prepared, execute_dir=execute_dir)
        )

        output_files = [
            f
            for f in glob.glob(os.path.join(execute_dir, "**", "*.csv"), recursive=True)
            if os.path.isfile(f)
        ]
        assert len(output_files) == 1

        result = op.postprocess(
            PostprocessInput(
                file_outputs=output_files,
                memory_outputs=raw,
                input_artifacts=input_artifacts,
                step_number=1,
                postprocess_dir=str(tmp_path / "post"),
            )
        )
        assert result.success
        assert len(result.artifacts["dataset"]) == 1
        assert len(result.lineage["dataset"]) == 1
        assert result.lineage["dataset"][0].source_artifact_id == config_art.artifact_id
        assert result.lineage["dataset"][0].source_role == "config"

    def test_config_parameter_application(self, tmp_path: Path):
        data_art, config_art = _setup_grouped_inputs(
            tmp_path, scale_factor=3.0, noise_amplitude=0.0
        )
        op = DataTransformerScript()

        execute_dir = str(tmp_path / "execute")
        os.makedirs(execute_dir, exist_ok=True)

        input_artifacts = {"dataset": [data_art], "config": [config_art]}

        prepared = op.preprocess(
            PreprocessInput(
                input_artifacts=input_artifacts,
                preprocess_dir=str(tmp_path / "pre"),
            )
        )
        op.execute_function(ExecuteInput(inputs=prepared, execute_dir=execute_dir))

        output_files = [
            f
            for f in glob.glob(os.path.join(execute_dir, "**", "*.csv"), recursive=True)
            if os.path.isfile(f)
        ]
        assert len(output_files) == 1

        # Verify scale was applied
        with open(data_art.materialized_path) as f:
            original = list(csv.DictReader(f))
        with open(output_files[0]) as f:
            transformed = list(csv.DictReader(f))

        for orig, trans in zip(original, transformed, strict=True):
            assert float(trans["x"]) == round(float(orig["x"]) * 3.0, 4)

    def test_output_file_naming(self, tmp_path: Path):
        data_art, config_art = _setup_grouped_inputs(tmp_path)
        op = DataTransformerScript()

        execute_dir = str(tmp_path / "execute")
        os.makedirs(execute_dir, exist_ok=True)

        input_artifacts = {"dataset": [data_art], "config": [config_art]}

        prepared = op.preprocess(
            PreprocessInput(
                input_artifacts=input_artifacts,
                preprocess_dir=str(tmp_path / "pre"),
            )
        )
        op.execute_function(ExecuteInput(inputs=prepared, execute_dir=execute_dir))

        output_files = [
            f
            for f in glob.glob(os.path.join(execute_dir, "**", "*.csv"), recursive=True)
            if os.path.isfile(f)
        ]
        # Distinct configs may share a human name; their files must not collide.
        assert (
            os.path.basename(output_files[0])
            == f"{config_art.artifact_id}_variant_0.csv"
        )
