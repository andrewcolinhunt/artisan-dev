"""Tests for DataTransformerScript operation."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from conftest import make_csv

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
    data_path = input_dir / "dataset_00000.csv"
    data_path.write_bytes(csv_content)

    data_artifact = DataArtifact.draft(
        content=csv_content,
        original_name="dataset_00000.csv",
        step_number=0,
    )
    data_artifact.finalize()
    data_artifact = data_artifact.model_copy(update={"materialized_path": data_path})

    # Create config referencing the data artifact
    config_content = {
        "input": str(data_path),  # Already resolved
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
    config_path = input_dir / "dataset_00000_config_0.json"
    config_path.write_text(json.dumps(config_content, indent=2))
    config_artifact = config_artifact.model_copy(update={"materialized_path": config_path})

    return data_artifact, config_artifact


class TestDataTransformerScript:
    def test_basic_transform(self, tmp_path: Path):
        data_art, config_art = _setup_grouped_inputs(
            tmp_path, scale_factor=2.0, noise_amplitude=0.0
        )
        op = DataTransformerScript()

        execute_dir = tmp_path / "execute"
        execute_dir.mkdir()

        input_artifacts = {"dataset": [data_art], "config": [config_art]}

        prepared = op.preprocess(
            PreprocessInput(
                input_artifacts=input_artifacts,
                preprocess_dir=tmp_path / "pre",
            )
        )
        op.execute(ExecuteInput(inputs=prepared, execute_dir=execute_dir))

        output_files = [f for f in execute_dir.glob("**/*.csv") if f.is_file()]
        assert len(output_files) == 1

        result = op.postprocess(
            PostprocessInput(
                file_outputs=output_files,
                memory_outputs=None,
                input_artifacts=input_artifacts,
                step_number=1,
                postprocess_dir=tmp_path / "post",
            )
        )
        assert result.success
        assert len(result.artifacts["dataset"]) == 1

    def test_config_parameter_application(self, tmp_path: Path):
        data_art, config_art = _setup_grouped_inputs(
            tmp_path, scale_factor=3.0, noise_amplitude=0.0
        )
        op = DataTransformerScript()

        execute_dir = tmp_path / "execute"
        execute_dir.mkdir()

        input_artifacts = {"dataset": [data_art], "config": [config_art]}

        prepared = op.preprocess(
            PreprocessInput(
                input_artifacts=input_artifacts,
                preprocess_dir=tmp_path / "pre",
            )
        )
        op.execute(ExecuteInput(inputs=prepared, execute_dir=execute_dir))

        output_files = [f for f in execute_dir.glob("**/*.csv") if f.is_file()]
        assert len(output_files) == 1

        # Verify scale was applied
        with data_art.materialized_path.open() as f:
            original = list(csv.DictReader(f))
        with output_files[0].open() as f:
            transformed = list(csv.DictReader(f))

        for orig, trans in zip(original, transformed):
            assert float(trans["x"]) == round(float(orig["x"]) * 3.0, 4)

    def test_output_file_naming(self, tmp_path: Path):
        data_art, config_art = _setup_grouped_inputs(tmp_path)
        op = DataTransformerScript()

        execute_dir = tmp_path / "execute"
        execute_dir.mkdir()

        input_artifacts = {"dataset": [data_art], "config": [config_art]}

        prepared = op.preprocess(
            PreprocessInput(
                input_artifacts=input_artifacts,
                preprocess_dir=tmp_path / "pre",
            )
        )
        op.execute(ExecuteInput(inputs=prepared, execute_dir=execute_dir))

        output_files = [f for f in execute_dir.glob("**/*.csv") if f.is_file()]
        # Output should use config original_name as basename
        assert output_files[0].name == "dataset_00000_config_0_variant_0.csv"
