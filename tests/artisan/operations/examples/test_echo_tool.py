"""Tests for EchoTool operation."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

from artisan.operations.examples import EchoTool
from artisan.schemas import ExecuteInput, PostprocessInput

pytestmark = pytest.mark.skipif(
    shutil.which("bash") is None, reason="bash not on PATH"
)


def _run(tmp_path: Path, **params: str) -> tuple[EchoTool, str]:
    """Execute the op for real and return (op, execute_dir)."""
    op = EchoTool(params=EchoTool.Params(**params))
    execute_dir = str(tmp_path / "execute")
    os.makedirs(execute_dir, exist_ok=True)
    result = op.execute(
        ExecuteInput(
            inputs={},
            execute_dir=execute_dir,
            log_path=str(tmp_path / "tool_output.log"),
        )
    )
    assert result is None  # tool-op contract: products are files
    return op, execute_dir


class TestEchoTool:
    def test_build_command_uses_tool_parts(self):
        op = EchoTool(params=EchoTool.Params(text="hi", filename="out.txt"))
        cmd = op.build_command({})
        assert cmd[0] == "bash"
        assert cmd[1] == "-c"
        assert "hi" in cmd[2]
        assert "out.txt" in cmd[2]

    def test_execute_writes_file(self, tmp_path: Path):
        _, execute_dir = _run(tmp_path, text="echo test", filename="echo.txt")
        assert Path(execute_dir, "echo.txt").read_text() == "echo test\n"

    def test_postprocess_builds_artifact(self, tmp_path: Path):
        op, execute_dir = _run(tmp_path)
        files = [os.path.join(execute_dir, f) for f in os.listdir(execute_dir)]
        result = op.postprocess(
            PostprocessInput(
                file_outputs=files,
                memory_outputs=None,
                input_artifacts={},
                step_number=1,
                postprocess_dir=str(tmp_path / "postprocess"),
            )
        )
        assert result.success is True
        assert len(result.artifacts["output"]) == 1

    def test_postprocess_ignores_other_files(self, tmp_path: Path):
        op, execute_dir = _run(tmp_path)
        stray = os.path.join(execute_dir, "stray.log")
        Path(stray).write_text("noise")
        result = op.postprocess(
            PostprocessInput(
                file_outputs=[stray, os.path.join(execute_dir, "echo.txt")],
                memory_outputs=None,
                input_artifacts={},
                step_number=1,
                postprocess_dir=str(tmp_path / "postprocess"),
            )
        )
        assert len(result.artifacts["output"]) == 1
