"""Tests for failure log file writing."""

from __future__ import annotations

from pathlib import Path

from artisan.execution.staging.recorder import _write_failure_log


class TestWriteFailureLog:
    """Tests for _write_failure_log helper."""

    def test_creates_file(self, tmp_path: Path) -> None:
        """Failure log file is created at expected path."""
        _write_failure_log(
            failure_logs_root=tmp_path,
            execution_run_id="run123",
            operation_name="tool_a",
            step_number=2,
            compute_backend="slurm",
            error="ValueError: bad input",
        )
        log_path = tmp_path / "step_2_tool_a" / "run123.log"
        assert log_path.exists()

    def test_contains_error(self, tmp_path: Path) -> None:
        """Error string appears in log content."""
        _write_failure_log(
            failure_logs_root=tmp_path,
            execution_run_id="run456",
            operation_name="tool_b",
            step_number=3,
            compute_backend="local",
            error="RuntimeError: command failed\nTraceback...",
        )
        content = (tmp_path / "step_3_tool_b" / "run456.log").read_text()
        assert "RuntimeError: command failed" in content
        assert "=== Error ===" in content

    def test_contains_tool_output_tail(self, tmp_path: Path) -> None:
        """Tool output section present when tool_output is provided."""
        lines = [f"line {i}" for i in range(200)]
        _write_failure_log(
            failure_logs_root=tmp_path,
            execution_run_id="run789",
            operation_name="tool_c",
            step_number=1,
            compute_backend="slurm",
            error="ExternalToolError: exit code 1",
            tool_output="\n".join(lines),
        )
        content = (tmp_path / "step_1_tool_c" / "run789.log").read_text()
        assert "=== Tool Output (last 100 lines) ===" in content
        assert "line 199" in content
        assert "line 99" not in content

    def test_no_tool_output(self, tmp_path: Path) -> None:
        """Log file works without tool_output."""
        _write_failure_log(
            failure_logs_root=tmp_path,
            execution_run_id="run_no_tool",
            operation_name="tool_a",
            step_number=0,
            compute_backend="local",
            error="Some error",
        )
        content = (tmp_path / "step_0_tool_a" / "run_no_tool.log").read_text()
        assert "=== Error ===" in content
        assert "Tool Output" not in content

    def test_none_root_noop(self) -> None:
        """No file created when failure_logs_root is None."""
        # Should not raise
        _write_failure_log(
            failure_logs_root=None,
            execution_run_id="run_ignored",
            operation_name="tool_a",
            step_number=0,
            compute_backend="local",
            error="Some error",
        )

    def test_directory_structure(self, tmp_path: Path) -> None:
        """Log files are organized under step_{N}_{op}/ subdirectories."""
        _write_failure_log(
            failure_logs_root=tmp_path,
            execution_run_id="run_a",
            operation_name="tool_a",
            step_number=5,
            compute_backend="slurm",
            error="Error A",
        )
        _write_failure_log(
            failure_logs_root=tmp_path,
            execution_run_id="run_b",
            operation_name="tool_a",
            step_number=5,
            compute_backend="slurm",
            error="Error B",
        )
        step_dir = tmp_path / "step_5_tool_a"
        assert step_dir.is_dir()
        log_files = list(step_dir.glob("*.log"))
        assert len(log_files) == 2

    def test_header_fields(self, tmp_path: Path) -> None:
        """Log header contains run_id, operation, step, backend."""
        _write_failure_log(
            failure_logs_root=tmp_path,
            execution_run_id="header_test",
            operation_name="tool_b",
            step_number=7,
            compute_backend="slurm",
            error="test",
        )
        content = (tmp_path / "step_7_tool_b" / "header_test.log").read_text()
        assert "Run ID:    header_test" in content
        assert "Operation: tool_b" in content
        assert "Step:      7" in content
        assert "Backend:   slurm" in content
