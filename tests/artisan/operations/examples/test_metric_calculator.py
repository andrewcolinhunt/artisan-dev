"""Tests for MetricCalculator operation (CSV statistics)."""

from __future__ import annotations

import json
from pathlib import Path

from conftest import run_operation_lifecycle
from fixtures.csv import make_csv

from artisan.operations.examples import MetricCalculator


def _write_csv(tmp_path: Path, name: str, content: bytes) -> Path:
    """Write CSV content to a file and return the path."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    path = tmp_path / name
    path.write_bytes(content)
    return path


def _get_metrics(artifact) -> dict:
    return json.loads(artifact.content.decode("utf-8"))


class TestMetricCalculator:
    def test_correct_statistics(self, tmp_path: Path):
        # Known data: id,x,y,z,score — scores are 0.5 and 1.0
        csv_content = b"id,x,y,z,score\n0,2.0,4.0,6.0,0.5\n1,4.0,6.0,8.0,1.0\n"
        path = _write_csv(tmp_path / "input", "data.csv", csv_content)

        op = MetricCalculator()
        result = run_operation_lifecycle(
            op, inputs={"dataset": path}, output_dir=tmp_path / "output"
        )

        assert result.success
        metrics = _get_metrics(result.artifacts["metrics"][0])
        assert metrics["distribution"]["min"] == 0.5
        assert metrics["distribution"]["max"] == 1.0
        assert metrics["distribution"]["median"] == 0.75
        assert metrics["distribution"]["range"] == 0.5
        assert metrics["summary"]["row_count"] == 2

    def test_multiple_inputs(self, tmp_path: Path):
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        paths = [
            _write_csv(input_dir, f"d{i}.csv", make_csv(rows=5, seed=i))
            for i in range(3)
        ]

        op = MetricCalculator()
        result = run_operation_lifecycle(
            op, inputs={"dataset": paths}, output_dir=tmp_path / "output"
        )

        assert result.success
        assert len(result.artifacts["metrics"]) == 3

    def test_single_row_edge_case(self, tmp_path: Path):
        csv_content = b"id,x,y,z,score\n0,5.0,5.0,5.0,0.8\n"
        path = _write_csv(tmp_path / "input", "one.csv", csv_content)

        op = MetricCalculator()
        result = run_operation_lifecycle(
            op, inputs={"dataset": path}, output_dir=tmp_path / "output"
        )

        assert result.success
        metrics = _get_metrics(result.artifacts["metrics"][0])
        assert metrics["distribution"]["min"] == 0.8
        assert metrics["distribution"]["max"] == 0.8
        assert metrics["distribution"]["median"] == 0.8
        assert metrics["distribution"]["range"] == 0.0
        assert metrics["summary"]["cv"] == 0.0  # stdev undefined for 1 value
        assert metrics["summary"]["row_count"] == 1

    def test_metadata(self, tmp_path: Path):
        path = _write_csv(tmp_path / "input", "d.csv", make_csv())

        op = MetricCalculator()
        result = run_operation_lifecycle(
            op, inputs={"dataset": path}, output_dir=tmp_path / "output"
        )

        assert result.metadata["operation"] == "metric_calculator"
        assert result.metadata["n_datasets"] == 1

    def test_metric_names(self, tmp_path: Path):
        path = _write_csv(tmp_path / "input", "d.csv", make_csv())

        op = MetricCalculator()
        result = run_operation_lifecycle(
            op, inputs={"dataset": path}, output_dir=tmp_path / "output"
        )

        metrics = _get_metrics(result.artifacts["metrics"][0])
        expected_keys = {"distribution", "summary"}
        assert set(metrics.keys()) == expected_keys

    def test_metric_naming_convention(self, tmp_path: Path):
        path = _write_csv(tmp_path / "input", "my_data.csv", make_csv())

        op = MetricCalculator()
        result = run_operation_lifecycle(
            op, inputs={"dataset": path}, output_dir=tmp_path / "output"
        )

        artifact = result.artifacts["metrics"][0]
        assert artifact.original_name == "my_data_metrics"
