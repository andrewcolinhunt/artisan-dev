"""Tests for tool output capture in execution recording."""

from __future__ import annotations

from pathlib import Path

from artisan.execution.staging.recorder import _read_tool_output
from artisan.schemas.specs.input_models import ExecuteInput


class TestReadToolOutput:
    """Tests for _read_tool_output helper."""

    def test_missing_file_returns_none(self, tmp_path: Path) -> None:
        """Non-existent file returns None."""
        result = _read_tool_output(tmp_path / "missing.log")
        assert result is None

    def test_none_path_returns_none(self) -> None:
        """None path returns None."""
        result = _read_tool_output(None)
        assert result is None

    def test_normal_file(self, tmp_path: Path) -> None:
        """Readable file returns its content."""
        log = tmp_path / "tool.log"
        log.write_text("line 1\nline 2\n")
        result = _read_tool_output(log)
        assert result == "line 1\nline 2\n"

    def test_truncation(self, tmp_path: Path) -> None:
        """File >500K chars is truncated to tail with prefix."""
        log = tmp_path / "big.log"
        content = "x" * 600_000
        log.write_text(content)
        result = _read_tool_output(log)
        assert result.startswith("[truncated]\n")
        assert len(result) < 600_000
        # Should keep last 500K chars
        assert result.endswith("x" * 100)


class TestExecuteInputLogPath:
    """Tests for log_path field on ExecuteInput."""

    def test_defaults_to_none(self, tmp_path: Path) -> None:
        """log_path defaults to None when not provided."""
        ei = ExecuteInput(execute_dir=tmp_path)
        assert ei.log_path is None

    def test_accepts_path(self, tmp_path: Path) -> None:
        """log_path can be set to a Path."""
        log = tmp_path / "tool.log"
        ei = ExecuteInput(execute_dir=tmp_path, log_path=log)
        assert ei.log_path == log
