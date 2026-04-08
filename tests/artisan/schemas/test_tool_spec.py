"""Tests for ToolSpec."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from artisan.schemas.operation_config.tool_spec import ToolSpec


class TestToolSpec:
    def test_minimal(self):
        ts = ToolSpec(executable="samtools")
        assert ts.executable == "samtools"
        assert ts.interpreter is None
        assert ts.subcommand is None

    def test_full(self):
        ts = ToolSpec(
            executable="/opt/tools/gatk",
            interpreter="python -u",
            subcommand="HaplotypeCaller",
        )
        assert ts.executable == "/opt/tools/gatk"
        assert ts.interpreter == "python -u"
        assert ts.subcommand == "HaplotypeCaller"

    def test_parts_no_interpreter(self):
        ts = ToolSpec(executable="samtools", subcommand="sort")
        assert ts.parts() == ["samtools", "sort"]

    def test_parts_with_interpreter(self):
        ts = ToolSpec(executable="transform.py", interpreter="python -u")
        assert ts.parts() == ["python", "-u", "transform.py"]

    def test_parts_full(self):
        ts = ToolSpec(
            executable="/app/run.py",
            interpreter="python",
            subcommand="train",
        )
        assert ts.parts() == ["python", "/app/run.py", "train"]

    def test_parts_path_executable(self):
        ts = ToolSpec(executable="/opt/bin/tool")
        assert ts.parts() == ["/opt/bin/tool"]

    def test_model_copy(self):
        ts = ToolSpec(executable="samtools")
        updated = ts.model_copy(update={"subcommand": "sort"})
        assert updated.subcommand == "sort"
        assert ts.subcommand is None

    def test_round_trip(self):
        ts = ToolSpec(executable="samtools", subcommand="sort")
        data = ts.model_dump()
        restored = ToolSpec.model_validate(data)
        assert restored == ts

    @patch("shutil.which", return_value="/usr/bin/samtools")
    def test_validate_tool_on_path(self, mock_which):
        ts = ToolSpec(executable="samtools")
        ts.validate_tool()  # should not raise

    @patch("shutil.which", return_value=None)
    def test_validate_tool_not_found(self, mock_which):
        ts = ToolSpec(executable="nonexistent_tool")
        with pytest.raises(FileNotFoundError, match="nonexistent_tool"):
            ts.validate_tool()

    def test_validate_tool_existing_path(self, tmp_path):
        script = tmp_path / "run.py"
        script.touch()
        ts = ToolSpec(executable=str(script))
        ts.validate_tool()  # should not raise
