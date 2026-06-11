"""Tests for WaitTool operation."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

from artisan.operations.examples import WaitTool
from artisan.schemas import ExecuteInput, PostprocessInput

pytestmark = pytest.mark.skipif(
    shutil.which("bash") is None, reason="bash not on PATH"
)


class TestWaitTool:
    def test_build_command_counts_and_writes_marker(self):
        op = WaitTool(params=WaitTool.Params(seconds=3))
        cmd = op.build_command({"dataset": "/inputs/sample.csv"})
        assert cmd[0] == "bash"
        assert cmd[1] == "-c"
        assert "seq 1 3" in cmd[2]
        assert "tick" in cmd[2]
        assert "_waited.csv" in cmd[2]  # named after the input stem for lineage
        assert "sample.csv" in cmd[2]

    def test_execute_ticks_and_emits_marker(self, tmp_path: Path):
        source = tmp_path / "sample.csv"
        source.write_text("a,b\n1,2\n")
        execute_dir = tmp_path / "execute"
        execute_dir.mkdir()
        log_path = tmp_path / "tool_output.log"

        op = WaitTool(params=WaitTool.Params(seconds=1))
        result = op.execute(
            ExecuteInput(
                inputs={"dataset": str(source)},
                execute_dir=str(execute_dir),
                log_path=str(log_path),
            )
        )

        assert result is None
        marker = (execute_dir / "sample_waited.csv").read_text()
        assert marker.startswith("seconds,host,source\n1,")
        assert marker.rstrip().endswith("sample.csv")
        assert "tick 1 / 1" in log_path.read_text()

    def test_preprocess_slices_per_artifact(self):
        from artisan.operations.base.per_artifact import PerArtifact

        class _Artifact:
            def __init__(self, path: str) -> None:
                self.materialized_path = path

        class _Pre:
            input_artifacts = {"dataset": [_Artifact("/a.csv"), _Artifact("/b.csv")]}

        prepared = WaitTool().preprocess(_Pre())
        assert isinstance(prepared["dataset"], PerArtifact)
        assert list(prepared["dataset"]) == ["/a.csv", "/b.csv"]

    def test_postprocess_builds_one_artifact_per_marker(self, tmp_path: Path):
        markers = []
        for host in ("host-a", "host-b"):
            path = tmp_path / f"{host}.csv"
            path.write_text(f"seconds,host,source\n1,{host},x.csv\n")
            markers.append(str(path))

        result = WaitTool().postprocess(
            PostprocessInput(
                file_outputs=markers,
                memory_outputs=None,
                input_artifacts={},
                step_number=1,
                postprocess_dir=str(tmp_path / "post"),
            )
        )

        assert result.success is True
        assert len(result.artifacts["output"]) == 2
