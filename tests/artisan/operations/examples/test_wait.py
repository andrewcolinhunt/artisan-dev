"""Tests for Wait operation."""

import csv
from pathlib import Path

from artisan.operations.examples import Wait
from artisan.schemas import ExecuteInput, PostprocessInput


class TestWait:
    def _run(self, output_dir: Path, duration: float = 0.0):
        op = Wait(params=Wait.Params(duration=duration))
        execute_dir = output_dir / "execute"
        execute_dir.mkdir(parents=True)

        result = op.execute(ExecuteInput(inputs={}, execute_dir=execute_dir))
        files = sorted(f for f in execute_dir.glob("**/*.csv") if f.is_file())

        post_result = op.postprocess(
            PostprocessInput(
                file_outputs=files,
                memory_outputs=result,
                input_artifacts={},
                step_number=1,
                postprocess_dir=output_dir / "postprocess",
            )
        )
        return result, files, post_result

    def test_produces_marker_file(self, tmp_path: Path):
        _, files, _ = self._run(tmp_path)
        assert len(files) == 1
        assert files[0].name == "wait_marker.csv"

    def test_csv_content(self, tmp_path: Path):
        _, files, _ = self._run(tmp_path, duration=0.05)
        with files[0].open() as f:
            reader = csv.DictReader(f)
            assert reader.fieldnames == ["requested", "actual"]
            rows = list(reader)
            assert len(rows) == 1
            assert float(rows[0]["requested"]) == 0.05
            assert float(rows[0]["actual"]) >= 0.0

    def test_zero_duration(self, tmp_path: Path):
        result, _, _ = self._run(tmp_path, duration=0.0)
        assert result["elapsed"] < 0.5

    def test_postprocess_artifacts(self, tmp_path: Path):
        _, _, post_result = self._run(tmp_path)
        assert post_result.success is True
        assert "output" in post_result.artifacts
        assert len(post_result.artifacts["output"]) == 1

    def test_metadata(self, tmp_path: Path):
        _, _, post_result = self._run(tmp_path, duration=0.0)
        assert post_result.metadata["operation"] == "wait"
        assert post_result.metadata["duration"] == 0.0
        assert "elapsed" in post_result.metadata
