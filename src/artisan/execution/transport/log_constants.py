"""Constants for tool-output log capture across compute backends.

The framework synthesizes a unit-level ``tool_output.log`` at
``<sandbox_root>/tool_output.log``. In-process execution leaves the file on
disk where the recorder reads it. Remote compute providers use the same path
inside their container; the bytes ferry back via a dedicated return-tuple
element, tail-truncated to ``MAX_TOOL_OUTPUT_BYTES`` to bound transport cost.
"""

from __future__ import annotations

TOOL_OUTPUT_FILENAME = "tool_output.log"
MAX_TOOL_OUTPUT_BYTES = 500_000
