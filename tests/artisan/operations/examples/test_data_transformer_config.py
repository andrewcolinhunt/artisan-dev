"""Tests for DataTransformerConfig operation."""

from __future__ import annotations

from pathlib import Path

from artisan.schemas.artifact.data import DataArtifact
from artisan.operations.examples import DataTransformerConfig
from artisan.schemas import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)


def _mock_data_artifact(name: str = "dataset_00000.csv") -> DataArtifact:
    return DataArtifact.draft(
        content=b"id,x\n1,2.0\n",
        original_name=name,
        step_number=0,
    ).finalize()


def _run_config_op(operation, artifact, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    execute_dir = output_dir / "execute"
    execute_dir.mkdir()

    input_artifacts = {"dataset": [artifact]}

    prepared = operation.preprocess(
        PreprocessInput(input_artifacts=input_artifacts, preprocess_dir=output_dir / "pre")
    )
    raw = operation.execute(ExecuteInput(inputs=prepared, execute_dir=execute_dir))
    return operation.postprocess(
        PostprocessInput(
            file_outputs=[],
            memory_outputs=raw,
            input_artifacts=input_artifacts,
            step_number=1,
            postprocess_dir=output_dir / "post",
        )
    )


class TestDataTransformerConfig:
    def test_single_config(self, tmp_path: Path):
        op = DataTransformerConfig(
            params=DataTransformerConfig.Params(
                scale_factors=[1.0], noise_amplitudes=[0.0], seed=42
            )
        )
        result = _run_config_op(op, _mock_data_artifact(), tmp_path / "out")
        assert result.success
        assert len(result.artifacts["config"]) == 1

    def test_cartesian_product(self, tmp_path: Path):
        op = DataTransformerConfig(
            params=DataTransformerConfig.Params(
                scale_factors=[0.5, 1.0, 2.0],
                noise_amplitudes=[0.0, 0.1],
                seed=42,
            )
        )
        result = _run_config_op(op, _mock_data_artifact(), tmp_path / "out")
        assert result.success
        assert len(result.artifacts["config"]) == 6  # 3 × 2

    def test_artifact_reference(self, tmp_path: Path):
        artifact = _mock_data_artifact()
        op = DataTransformerConfig(
            params=DataTransformerConfig.Params(scale_factors=[1.0], noise_amplitudes=[0.0])
        )
        result = _run_config_op(op, artifact, tmp_path / "out")
        config = result.artifacts["config"][0]
        values = config.values
        assert values["input"]["$artifact"] == artifact.artifact_id

    def test_config_naming(self, tmp_path: Path):
        op = DataTransformerConfig(
            params=DataTransformerConfig.Params(
                scale_factors=[0.5, 1.0], noise_amplitudes=[0.0]
            )
        )
        result = _run_config_op(op, _mock_data_artifact(), tmp_path / "out")
        names = [c.original_name for c in result.artifacts["config"]]
        assert names == ["dataset_00000_config_0", "dataset_00000_config_1"]

    def test_seed_inclusion(self, tmp_path: Path):
        op = DataTransformerConfig(
            params=DataTransformerConfig.Params(
                scale_factors=[1.0], noise_amplitudes=[0.0], seed=42
            )
        )
        result = _run_config_op(op, _mock_data_artifact(), tmp_path / "out")
        for config in result.artifacts["config"]:
            assert config.values["seed"] == 42

    def test_config_contains_parameters(self, tmp_path: Path):
        op = DataTransformerConfig(
            params=DataTransformerConfig.Params(
                scale_factors=[2.5], noise_amplitudes=[0.3], seed=123
            )
        )
        result = _run_config_op(op, _mock_data_artifact(), tmp_path / "out")
        values = result.artifacts["config"][0].values
        assert values["scale_factor"] == 2.5
        assert values["noise_amplitude"] == 0.3
        assert values["seed"] == 123
