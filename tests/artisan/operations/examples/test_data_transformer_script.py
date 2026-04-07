"""Tests for DataTransformerScript operation."""

from __future__ import annotations

import csv
import glob
import json
import os
from pathlib import Path

from fixtures.csv import make_csv

from artisan.schemas.artifact.data import DataArtifact
from artisan.operations.examples import DataTransformerScript
from artisan.schemas.artifact import ExecutionConfigArtifact
from artisan.schemas import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)


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
    config_artifact = config_artifact.model_copy(update={"materialized_path": config_path})

    return data_artifact, config_artifact


class TestDataTransformerScript:
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
        op.execute(ExecuteInput(inputs=prepared, execute_dir=execute_dir))

        output_files = [
            f for f in glob.glob(os.path.join(execute_dir, "**", "*.csv"), recursive=True)
            if os.path.isfile(f)
        ]
        assert len(output_files) == 1

        result = op.postprocess(
            PostprocessInput(
                file_outputs=output_files,
                memory_outputs=None,
                input_artifacts=input_artifacts,
                step_number=1,
                postprocess_dir=str(tmp_path / "post"),
            )
        )
        assert result.success
        assert len(result.artifacts["dataset"]) == 1

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
        op.execute(ExecuteInput(inputs=prepared, execute_dir=execute_dir))

        output_files = [
            f for f in glob.glob(os.path.join(execute_dir, "**", "*.csv"), recursive=True)
            if os.path.isfile(f)
        ]
        assert len(output_files) == 1

        # Verify scale was applied
        with open(data_art.materialized_path) as f:
            original = list(csv.DictReader(f))
        with open(output_files[0]) as f:
            transformed = list(csv.DictReader(f))

        for orig, trans in zip(original, transformed):
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
        op.execute(ExecuteInput(inputs=prepared, execute_dir=execute_dir))

        output_files = [
            f for f in glob.glob(os.path.join(execute_dir, "**", "*.csv"), recursive=True)
            if os.path.isfile(f)
        ]
        # Output should use config original_name as basename
        assert os.path.basename(output_files[0]) == "dataset_00000_config_0_variant_0.csv"
