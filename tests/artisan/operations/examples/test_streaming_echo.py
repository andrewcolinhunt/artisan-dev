"""Tests for StreamingEcho operation."""

from __future__ import annotations

import csv
import glob
import os
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

from artisan.operations.examples import StreamingEcho
from artisan.schemas import ExecuteInput, PostprocessInput


def _setup(tmp_path: Path, seconds: int) -> tuple[StreamingEcho, str, str]:
    """Build the op + execute_dir + log_path for one test run."""
    op = StreamingEcho(params=StreamingEcho.Params(seconds=seconds))
    execute_dir = str(tmp_path / "execute")
    os.makedirs(execute_dir, exist_ok=True)
    log_path = str(tmp_path / "tool_output.log")
    return op, execute_dir, log_path


def _postprocess(op: StreamingEcho, execute_dir: str, mem: dict, tmp_path: Path):
    """Run postprocess against any CSV files produced by execute."""
    files = sorted(
        f
        for f in glob.glob(os.path.join(execute_dir, "**", "*.csv"), recursive=True)
        if os.path.isfile(f)
    )
    return files, op.postprocess(
        PostprocessInput(
            file_outputs=files,
            memory_outputs=mem,
            input_artifacts={},
            step_number=1,
            postprocess_dir=str(tmp_path / "postprocess"),
        )
    )


class TestStreamingEcho:
    """Unit tests with ``run_command`` mocked — fast, deterministic."""

    def test_execute_drives_streaming_run_command(self, tmp_path: Path):
        """``run_command`` called with ``stream_output=True`` and ``log_path``."""
        op, execute_dir, log_path = _setup(tmp_path, seconds=2)

        with patch(
            "artisan.operations.examples.streaming_echo.run_command"
        ) as mock_run:
            op.execute(
                ExecuteInput(
                    inputs={}, execute_dir=execute_dir, log_path=log_path
                )
            )

        mock_run.assert_called_once()
        kwargs = mock_run.call_args.kwargs
        assert kwargs["stream_output"] is True
        assert kwargs["log_path"] == log_path
        assert kwargs["cwd"] == execute_dir
        cmd = mock_run.call_args.args[1]
        assert cmd[0] == "bash"
        assert cmd[1] == "-c"
        assert "seq 1 2" in cmd[2]

    def test_produces_marker_file(self, tmp_path: Path):
        op, execute_dir, log_path = _setup(tmp_path, seconds=3)

        with patch("artisan.operations.examples.streaming_echo.run_command"):
            mem = op.execute(
                ExecuteInput(
                    inputs={}, execute_dir=execute_dir, log_path=log_path
                )
            )

        files, _ = _postprocess(op, execute_dir, mem, tmp_path)
        assert len(files) == 1
        assert os.path.basename(files[0]) == "streaming_echo_marker.csv"

    def test_marker_csv_content(self, tmp_path: Path):
        op, execute_dir, log_path = _setup(tmp_path, seconds=4)

        with patch("artisan.operations.examples.streaming_echo.run_command"):
            op.execute(
                ExecuteInput(
                    inputs={}, execute_dir=execute_dir, log_path=log_path
                )
            )

        marker = os.path.join(execute_dir, "streaming_echo_marker.csv")
        with open(marker) as f:
            reader = csv.DictReader(f)
            assert reader.fieldnames == ["lines"]
            rows = list(reader)
            assert len(rows) == 1
            assert int(rows[0]["lines"]) == 4

    def test_postprocess_returns_artifact(self, tmp_path: Path):
        op, execute_dir, log_path = _setup(tmp_path, seconds=1)

        with patch("artisan.operations.examples.streaming_echo.run_command"):
            mem = op.execute(
                ExecuteInput(
                    inputs={}, execute_dir=execute_dir, log_path=log_path
                )
            )

        _, post_result = _postprocess(op, execute_dir, mem, tmp_path)
        assert post_result.success is True
        assert "output" in post_result.artifacts
        assert len(post_result.artifacts["output"]) == 1

    def test_metadata(self, tmp_path: Path):
        op, execute_dir, log_path = _setup(tmp_path, seconds=3)

        with patch("artisan.operations.examples.streaming_echo.run_command"):
            mem = op.execute(
                ExecuteInput(
                    inputs={}, execute_dir=execute_dir, log_path=log_path
                )
            )

        _, post_result = _postprocess(op, execute_dir, mem, tmp_path)
        assert post_result.metadata["operation"] == "streaming_echo"
        assert post_result.metadata["lines"] == 3


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash not on PATH")
@pytest.mark.filterwarnings("ignore::ResourceWarning")
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_real_bash_streams_to_log_path(tmp_path: Path):
    """End-to-end: bash actually emits the lines into ``log_path``.

    Drives the full ``run_command(stream_output=True)`` path with a real
    subprocess. ``seconds=1`` keeps wall-clock under two seconds. The
    ResourceWarning filter swallows a pre-existing pipe-cleanup quirk
    in ``_run_with_streaming`` (Popen.stdout closed by GC, not
    explicitly) — the warning is benign and unrelated to this test's
    assertion.
    """
    op, execute_dir, log_path = _setup(tmp_path, seconds=1)
    op.execute(ExecuteInput(inputs={}, execute_dir=execute_dir, log_path=log_path))
    contents = Path(log_path).read_text()
    assert "streaming_echo line 1 / 1" in contents
