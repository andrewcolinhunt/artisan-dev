"""Tests for DataGenerator operation."""

import csv
import glob
import os
from pathlib import Path

from artisan.operations.examples import DataGenerator
from artisan.schemas import ExecuteInput, PostprocessInput


class TestDataGenerator:
    def _run(self, output_dir: Path, count: int = 3, rows: int = 5, seed: int = 42):
        op = DataGenerator(params=DataGenerator.Params(count=count, rows_per_file=rows, seed=seed))
        execute_dir = str(output_dir / "execute")
        os.makedirs(execute_dir, exist_ok=True)

        result = op.execute(ExecuteInput(inputs={}, execute_dir=execute_dir))
        files = sorted(
            f for f in glob.glob(os.path.join(execute_dir, "**", "*.csv"), recursive=True)
            if os.path.isfile(f)
        )

        post_result = op.postprocess(
            PostprocessInput(
                file_outputs=files,
                memory_outputs=result,
                input_artifacts={},
                step_number=1,
                postprocess_dir=str(output_dir / "postprocess"),
            )
        )
        return result, files, post_result

    def test_generates_correct_count(self, tmp_path: Path):
        _, files, post_result = self._run(tmp_path, count=5)
        assert len(files) == 5
        assert len(post_result.artifacts["datasets"]) == 5

    def test_csv_format(self, tmp_path: Path):
        _, files, _ = self._run(tmp_path, count=1, rows=3)
        with open(files[0]) as f:
            reader = csv.DictReader(f)
            assert reader.fieldnames == ["id", "x", "y", "z", "score"]
            rows = list(reader)
            assert len(rows) == 3

    def test_reproducible_with_seed(self, tmp_path: Path):
        _, files_a, _ = self._run(tmp_path / "a", seed=42)
        _, files_b, _ = self._run(tmp_path / "b", seed=42)
        for a, b in zip(files_a, files_b):
            with open(a, "rb") as fa, open(b, "rb") as fb:
                assert fa.read() == fb.read()

    def test_different_seeds_different_output(self, tmp_path: Path):
        _, files_a, _ = self._run(tmp_path / "a", count=1, seed=42)
        _, files_b, _ = self._run(tmp_path / "b", count=1, seed=99)
        with open(files_a[0], "rb") as fa, open(files_b[0], "rb") as fb:
            assert fa.read() != fb.read()

    def test_filenames(self, tmp_path: Path):
        _, files, _ = self._run(tmp_path, count=3)
        names = [os.path.basename(f) for f in files]
        assert names == ["dataset_00000.csv", "dataset_00001.csv", "dataset_00002.csv"]

    def test_value_ranges(self, tmp_path: Path):
        _, files, _ = self._run(tmp_path, count=1, rows=100, seed=1)
        with open(files[0]) as f:
            reader = csv.DictReader(f)
            for row in reader:
                assert 0.0 <= float(row["x"]) <= 10.0
                assert 0.0 <= float(row["y"]) <= 10.0
                assert 0.0 <= float(row["z"]) <= 10.0
                assert 0.0 <= float(row["score"]) <= 1.0

    def test_metadata(self, tmp_path: Path):
        _, _, post_result = self._run(tmp_path, count=2, seed=7)
        assert post_result.metadata["operation"] == "data_generator"
        assert post_result.metadata["count"] == 2
        assert post_result.metadata["seed"] == 7

    def test_artifact_columns_and_row_count(self, tmp_path: Path):
        _, _, post_result = self._run(tmp_path, count=1, rows=8)
        artifact = post_result.artifacts["datasets"][0]
        assert artifact.columns == ["id", "x", "y", "z", "score"]
        assert artifact.row_count == 8
