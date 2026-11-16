"""StreamingEcho command construction, marker parsing and selected command execution."""

from __future__ import annotations

import csv
import shutil
from pathlib import Path

import pytest
from fixtures.commands import execute_command

from artisan.operations.examples import StreamingEcho
from artisan.schemas import ExecuteInput, PostprocessInput


def test_command_uses_literal_count_and_selected_bash() -> None:
    operation = StreamingEcho(params=StreamingEcho.Params(seconds=3))
    command = operation.execute_command({})
    assert command[:2] == ["bash", "-c"]
    assert command[-2:] == ["streaming-echo", "3"]
    assert 'seq 1 "$1"' in command[2]
    assert "streaming_echo_marker.csv" in command[2]


@pytest.mark.parametrize("seconds", [1, 3, 4])
def test_postprocess_preserves_marker_and_metadata(
    tmp_path: Path, seconds: int
) -> None:
    operation = StreamingEcho(params=StreamingEcho.Params(seconds=seconds))
    marker = tmp_path / "streaming_echo_marker.csv"
    marker.write_text(f"lines\n{seconds}\n")
    result = operation.postprocess(
        PostprocessInput(
            file_outputs=[str(marker)],
            step_number=1,
            postprocess_dir=str(tmp_path / "postprocess"),
        )
    )
    assert result.success
    assert result.metadata == {"operation": "streaming_echo", "lines": seconds}
    (artifact,) = result.artifacts["output"]
    assert artifact.original_name == marker.stem
    assert artifact.content == marker.read_bytes()


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash not on PATH")
def test_real_bash_streams_and_writes_marker(tmp_path: Path) -> None:
    operation = StreamingEcho(params=StreamingEcho.Params(seconds=1))
    execute_dir = tmp_path / "execute"
    execute_dir.mkdir()
    log_path = tmp_path / "tool_output.log"
    assert (
        execute_command(
            operation,
            ExecuteInput(execute_dir=str(execute_dir), log_path=str(log_path)),
        )
        is None
    )
    assert "streaming_echo line 1 / 1" in log_path.read_text()
    with (execute_dir / "streaming_echo_marker.csv").open() as stream:
        assert list(csv.DictReader(stream)) == [{"lines": "1"}]
