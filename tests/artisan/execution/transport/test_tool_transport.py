"""Tests for tool transport functions."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from artisan.execution.transport.tool_transport import (
    restore_tool_files,
    snapshot_tool_files,
)


class TestSnapshotToolFiles:
    def test_round_trip_preserves_file(self, tmp_path):
        """snapshot → restore preserves tool script at original path."""
        script = tmp_path / "scripts" / "run.py"
        script.parent.mkdir(parents=True)
        script.write_bytes(b"#!/usr/bin/env python\nprint('hello')")

        operation = MagicMock()
        operation.tool = MagicMock()
        operation.tool.executable = str(script)

        snapshot = snapshot_tool_files(operation)
        assert str(script) in snapshot
        assert snapshot[str(script)] == b"#!/usr/bin/env python\nprint('hello')"

        # Remove original, restore, verify
        script.unlink()
        restore_tool_files(snapshot)
        assert script.read_bytes() == b"#!/usr/bin/env python\nprint('hello')"

    def test_no_tool_returns_empty(self):
        """Operation with no tool returns empty dict."""
        operation = MagicMock(spec=[])
        operation.tool = None
        assert snapshot_tool_files(operation) == {}

    def test_no_executable_returns_empty(self):
        """Tool with no executable returns empty dict."""
        operation = MagicMock()
        operation.tool = MagicMock()
        operation.tool.executable = None
        assert snapshot_tool_files(operation) == {}

    def test_relative_path_returns_empty(self):
        """Relative tool path returns empty dict (assumed on PATH)."""
        operation = MagicMock()
        operation.tool = MagicMock()
        operation.tool.executable = "python"
        assert snapshot_tool_files(operation) == {}

    def test_missing_executable_raises(self):
        """Absolute path to non-existent file raises FileNotFoundError."""
        operation = MagicMock()
        operation.tool = MagicMock()
        operation.tool.executable = "/nonexistent/tool.sh"

        with pytest.raises(FileNotFoundError, match="Tool executable not found"):
            snapshot_tool_files(operation)


class TestRestoreToolFiles:
    def test_creates_parent_directories(self, tmp_path):
        """restore_tool_files creates intermediate directories."""
        target = tmp_path / "deep" / "nested" / "dir" / "script.sh"
        tool_files = {str(target): b"#!/bin/bash\necho done"}

        restore_tool_files(tool_files)

        assert target.read_bytes() == b"#!/bin/bash\necho done"

    def test_empty_dict_noop(self):
        """Empty tool_files dict is a no-op."""
        restore_tool_files({})  # should not raise
