"""Tests for tool-output log capture constants."""

from __future__ import annotations

from artisan.execution.transport.log_constants import (
    MAX_TOOL_OUTPUT_BYTES,
    TOOL_OUTPUT_FILENAME,
)


def test_tool_output_filename_value() -> None:
    """Filename matches the path historically synthesized in creator_phases."""
    assert TOOL_OUTPUT_FILENAME == "tool_output.log"


def test_max_tool_output_bytes_value() -> None:
    """Byte cap is 500_000, matching the recorder's char cap by integer value."""
    assert MAX_TOOL_OUTPUT_BYTES == 500_000
