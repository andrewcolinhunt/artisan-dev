"""Tests for DataGeneratorWithMetrics operation."""

from __future__ import annotations

import csv
import glob
import io
import json
import os
from pathlib import Path
from statistics import mean

from artisan.operations.examples import DataGeneratorWithMetrics
from artisan.schemas import ExecuteInput, PostprocessInput


class TestDataGeneratorWithMetrics:
    def _run(self, output_dir: Path, count: int = 3, rows: int = 10, seed: int = 42):
        op = DataGeneratorWithMetrics(
            params=DataGeneratorWithMetrics.Params(
                count=count, rows_per_file=rows, seed=seed
            )
        )
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

    def test_both_outputs_present(self, tmp_path: Path):
        _, _, post_result = self._run(tmp_path, count=2)
        assert "datasets" in post_result.artifacts
        assert "metrics" in post_result.artifacts
        assert len(post_result.artifacts["datasets"]) == 2
        assert len(post_result.artifacts["metrics"]) == 2

    def test_output_to_output_lineage_spec(self):
        op = DataGeneratorWithMetrics()
        assert op.outputs["metrics"].infer_lineage_from == {"outputs": ["datasets"]}
        assert op.outputs["datasets"].infer_lineage_from == {"inputs": []}

    def test_metric_values_match_data(self, tmp_path: Path):
        _, files, post_result = self._run(tmp_path, count=1, rows=5, seed=42)

        # Read the generated CSV and compute expected stats
        with open(files[0]) as f:
            reader = csv.DictReader(f)
            xs = []
            for row in reader:
                xs.append(float(row["x"]))

        metric = post_result.artifacts["metrics"][0]
        metric_values = json.loads(metric.content.decode("utf-8"))
        assert metric_values["mean_x"] == mean(xs)
        assert metric_values["row_count"] == 5

    def test_reproducibility(self, tmp_path: Path):
        _, _, r1 = self._run(tmp_path / "a", count=2, seed=42)
        _, _, r2 = self._run(tmp_path / "b", count=2, seed=42)

        for a, b in zip(
            r1.artifacts["datasets"], r2.artifacts["datasets"]
        ):
            assert a.content == b.content

    def test_count(self, tmp_path: Path):
        _, files, post_result = self._run(tmp_path, count=5)
        assert len(files) == 5
        assert len(post_result.artifacts["datasets"]) == 5
        assert len(post_result.artifacts["metrics"]) == 5
