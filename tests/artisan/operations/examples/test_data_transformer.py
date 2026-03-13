"""Tests for DataTransformer operation."""

from __future__ import annotations

import csv
from pathlib import Path

from conftest import run_operation_lifecycle
from fixtures.csv import make_csv

from artisan.operations.examples import DataTransformer


def _write_input(tmp_path: Path, name: str = "input.csv", **kwargs) -> Path:
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    path = input_dir / name
    path.write_bytes(make_csv(**kwargs))
    return path


def _read_csv(path: Path) -> list[dict]:
    with path.open() as f:
        return list(csv.DictReader(f))


class TestDataTransformer:
    def test_scale_only(self, tmp_path: Path):
        path = _write_input(tmp_path, rows=3, seed=1)
        op = DataTransformer(
            params=DataTransformer.Params(scale_factor=2.0, noise_amplitude=0.0, seed=42)
        )
        result = run_operation_lifecycle(
            op, inputs={"dataset": path}, output_dir=tmp_path / "output"
        )
        assert result.success
        originals = _read_csv(path)
        outputs = result.artifacts["dataset"]
        assert len(outputs) == 1

        # Read the output file
        out_content = outputs[0].content
        out_path = tmp_path / "check.csv"
        out_path.write_bytes(out_content)
        transformed = _read_csv(out_path)

        for orig, trans in zip(originals, transformed):
            assert float(trans["x"]) == round(float(orig["x"]) * 2.0, 4)
            assert float(trans["score"]) == round(float(orig["score"]) * 2.0, 4)

    def test_noise_only(self, tmp_path: Path):
        path = _write_input(tmp_path, rows=3, seed=1)
        op = DataTransformer(
            params=DataTransformer.Params(scale_factor=1.0, noise_amplitude=0.5, seed=42)
        )
        result = run_operation_lifecycle(
            op, inputs={"dataset": path}, output_dir=tmp_path / "output"
        )
        assert result.success
        originals = _read_csv(path)
        out_path = tmp_path / "check.csv"
        out_path.write_bytes(result.artifacts["dataset"][0].content)
        transformed = _read_csv(out_path)

        for orig, trans in zip(originals, transformed):
            diff = abs(float(trans["x"]) - float(orig["x"]))
            assert diff <= 0.5001  # noise within amplitude

    def test_variants(self, tmp_path: Path):
        path = _write_input(tmp_path, rows=2, seed=1)
        op = DataTransformer(
            params=DataTransformer.Params(scale_factor=1.0, noise_amplitude=0.1, variants=3, seed=42)
        )
        result = run_operation_lifecycle(
            op, inputs={"dataset": path}, output_dir=tmp_path / "output"
        )
        assert result.success
        assert len(result.artifacts["dataset"]) == 3

    def test_reproducibility(self, tmp_path: Path):
        path = _write_input(tmp_path, rows=3, seed=1)
        params = DataTransformer.Params(scale_factor=1.5, noise_amplitude=0.2, seed=42)

        op1 = DataTransformer(params=params)
        r1 = run_operation_lifecycle(
            op1, inputs={"dataset": path}, output_dir=tmp_path / "out1"
        )
        op2 = DataTransformer(params=params)
        r2 = run_operation_lifecycle(
            op2, inputs={"dataset": path}, output_dir=tmp_path / "out2"
        )
        assert r1.artifacts["dataset"][0].content == r2.artifacts["dataset"][0].content

    def test_different_seeds_different_output(self, tmp_path: Path):
        path = _write_input(tmp_path, rows=3, seed=1)

        op1 = DataTransformer(
            params=DataTransformer.Params(noise_amplitude=0.5, seed=42)
        )
        r1 = run_operation_lifecycle(
            op1, inputs={"dataset": path}, output_dir=tmp_path / "out1"
        )
        op2 = DataTransformer(
            params=DataTransformer.Params(noise_amplitude=0.5, seed=99)
        )
        r2 = run_operation_lifecycle(
            op2, inputs={"dataset": path}, output_dir=tmp_path / "out2"
        )
        assert r1.artifacts["dataset"][0].content != r2.artifacts["dataset"][0].content

    def test_identity_transform(self, tmp_path: Path):
        path = _write_input(tmp_path, rows=3, seed=1)
        op = DataTransformer(
            params=DataTransformer.Params(scale_factor=1.0, noise_amplitude=0.0, seed=42)
        )
        result = run_operation_lifecycle(
            op, inputs={"dataset": path}, output_dir=tmp_path / "output"
        )
        # With scale=1 and noise=0, numeric values should be unchanged
        originals = _read_csv(path)
        out_path = tmp_path / "check.csv"
        out_path.write_bytes(result.artifacts["dataset"][0].content)
        transformed = _read_csv(out_path)

        for orig, trans in zip(originals, transformed):
            assert float(trans["x"]) == float(orig["x"])
            assert float(trans["score"]) == float(orig["score"])

    def test_metadata(self, tmp_path: Path):
        path = _write_input(tmp_path, rows=2, seed=1)
        op = DataTransformer(
            params=DataTransformer.Params(scale_factor=2.0, noise_amplitude=0.1, seed=7)
        )
        result = run_operation_lifecycle(
            op, inputs={"dataset": path}, output_dir=tmp_path / "output"
        )
        assert result.metadata["operation"] == "data_transformer"
        assert result.metadata["scale_factor"] == 2.0
        assert result.metadata["noise_amplitude"] == 0.1
        assert result.metadata["seed"] == 7
