"""Tests for failure log file writing."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from fsspec.implementations.local import LocalFileSystem

from artisan.execution.recording.recorder import _write_failure_log
from artisan.utils.log_paths import failure_log_relative_path

START = datetime(2026, 9, 19, 1, 2, 3, tzinfo=UTC)


class TestWriteFailureLog:
    """Tests for _write_failure_log helper."""

    def test_creates_file(self, tmp_path: Path) -> None:
        """Failure log file is created at expected path."""
        _write_failure_log(
            fs=LocalFileSystem(),
            timestamp_start=START,
            failure_logs_root=str(tmp_path),
            execution_run_id="run123",
            operation_name="tool_a",
            step_number=2,
            compute_backend="slurm",
            error="ValueError: bad input",
        )
        log_path = tmp_path / failure_log_relative_path("run123", START)
        assert log_path.exists()

    def test_contains_error(self, tmp_path: Path) -> None:
        """Error string appears in log content."""
        _write_failure_log(
            fs=LocalFileSystem(),
            timestamp_start=START,
            failure_logs_root=str(tmp_path),
            execution_run_id="run456",
            operation_name="tool_b",
            step_number=3,
            compute_backend="local",
            error="RuntimeError: command failed\nTraceback...",
        )
        content = (tmp_path / failure_log_relative_path("run456", START)).read_text()
        assert "RuntimeError: command failed" in content
        assert "=== Error ===" in content

    def test_contains_tool_output_tail(self, tmp_path: Path) -> None:
        """Tool output section present when tool_output is provided."""
        lines = [f"line {i}" for i in range(200)]
        _write_failure_log(
            fs=LocalFileSystem(),
            timestamp_start=START,
            failure_logs_root=str(tmp_path),
            execution_run_id="run789",
            operation_name="tool_c",
            step_number=1,
            compute_backend="slurm",
            error="ExternalToolError: exit code 1",
            tool_output="\n".join(lines),
        )
        content = (tmp_path / failure_log_relative_path("run789", START)).read_text()
        assert "=== Tool Output (last 100 lines) ===" in content
        assert "line 199" in content
        assert "line 99" not in content

    def test_no_tool_output(self, tmp_path: Path) -> None:
        """Log file works without tool_output."""
        _write_failure_log(
            fs=LocalFileSystem(),
            timestamp_start=START,
            failure_logs_root=str(tmp_path),
            execution_run_id="run_no_tool",
            operation_name="tool_a",
            step_number=0,
            compute_backend="local",
            error="Some error",
        )
        content = (
            tmp_path / failure_log_relative_path("run_no_tool", START)
        ).read_text()
        assert "=== Error ===" in content
        assert "Tool Output" not in content

    def test_none_root_noop(self) -> None:
        """No file created when failure_logs_root is None."""
        _write_failure_log(
            fs=LocalFileSystem(),
            timestamp_start=START,
            failure_logs_root=None,
            execution_run_id="run_ignored",
            operation_name="tool_a",
            step_number=0,
            compute_backend="local",
            error="Some error",
        )

    def test_directory_structure(self, tmp_path: Path) -> None:
        """Log files are organized under UTC date subdirectories."""
        _write_failure_log(
            fs=LocalFileSystem(),
            timestamp_start=START,
            failure_logs_root=str(tmp_path),
            execution_run_id="run_a",
            operation_name="tool_a",
            step_number=5,
            compute_backend="slurm",
            error="Error A",
        )
        _write_failure_log(
            fs=LocalFileSystem(),
            timestamp_start=START,
            failure_logs_root=str(tmp_path),
            execution_run_id="run_b",
            operation_name="tool_a",
            step_number=5,
            compute_backend="slurm",
            error="Error B",
        )
        date_dir = tmp_path / "20260919"
        assert date_dir.is_dir()
        log_files = list(date_dir.glob("*.log"))
        assert len(log_files) == 2

    def test_header_fields(self, tmp_path: Path) -> None:
        """Log header contains run_id, operation, step, step_runner."""
        _write_failure_log(
            fs=LocalFileSystem(),
            timestamp_start=START,
            failure_logs_root=str(tmp_path),
            execution_run_id="header_test",
            operation_name="tool_b",
            step_number=7,
            compute_backend="slurm",
            error="test",
        )
        content = (
            tmp_path / failure_log_relative_path("header_test", START)
        ).read_text()
        assert "Run ID:    header_test" in content
        assert "Operation: tool_b" in content
        assert "Step:      7" in content
        assert "Backend:   slurm" in content


def test_duplicate_failure_preserves_original_content(tmp_path: Path) -> None:
    kwargs = {
        "fs": LocalFileSystem(),
        "failure_logs_root": str(tmp_path),
        "execution_run_id": "id",
        "timestamp_start": START,
        "operation_name": "op",
        "step_number": 1,
        "compute_backend": "local",
    }
    _write_failure_log(**kwargs, error="original", tool_output="original output")
    log_path = tmp_path / failure_log_relative_path("id", START)
    content = log_path.read_text()
    _write_failure_log(**kwargs, error="replacement", tool_output="replacement output")
    assert log_path.read_text() == content
    assert "original" in content
    assert "original output" in content
    assert "replacement" not in content


def test_failure_log_write_error_is_best_effort(tmp_path: Path) -> None:
    tmp_path.joinpath("20260919").touch()
    _write_failure_log(
        str(tmp_path),
        "id",
        START,
        "op",
        1,
        "local",
        "original error",
        fs=LocalFileSystem(),
    )


@pytest.mark.parametrize("backend", ["local", pytest.param("s3", marks=pytest.mark.s3)])
def test_shared_failure_log_is_bounded_utf8_and_first_write_immutable(
    tmp_path: Path,
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
    backend: str,
) -> None:
    from artisan.execution.recording import recorder
    from artisan.utils.path import uri_join

    fs, root = LocalFileSystem(), str(tmp_path / "failure-logs")
    if backend == "s3":
        fs, _, prefix = request.getfixturevalue("s3_fs")
        root = uri_join(prefix, "failure-logs")
    monkeypatch.setattr(recorder, "MAX_DIAGNOSTIC_BYTES", 1024)
    arguments = {
        "fs": fs,
        "failure_logs_root": root,
        "execution_run_id": "bounded",
        "timestamp_start": START,
        "operation_name": "op",
        "step_number": 1,
        "compute_backend": "local",
    }
    _write_failure_log(**arguments, error="ü" * 2000)
    path = uri_join(root, failure_log_relative_path("bounded", START))
    original = fs.cat_file(path)
    assert len(original) <= 1024
    assert original.decode("utf-8").endswith("[truncated]")
    _write_failure_log(**arguments, error="replacement")
    assert fs.cat_file(path) == original


def test_writer_and_logger_failures_cannot_replace_authored_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from artisan.execution.recording import recorder

    def unavailable(*args: object, **kwargs: object) -> None:
        raise OSError

    monkeypatch.setattr(recorder, "publish_immutable_bytes", unavailable)
    monkeypatch.setattr(recorder.logger, "debug", unavailable)
    _write_failure_log(
        str(tmp_path),
        "id",
        START,
        "op",
        1,
        "local",
        "original error",
        fs=LocalFileSystem(),
    )
    assert not (tmp_path / failure_log_relative_path("id", START)).exists()
