"""Constants for tool-output log capture across compute backends.

Local execution writes ``<sandbox_root>/tool_output.log`` for the recorder.
Endpoint workers write ``outputs/tool_output.log`` and return a byte-bounded
tail in ``ToolManifest.log_tail``. Opt-in debug capture returns the full log
in its independent diagnostic archive.
"""

from __future__ import annotations

TOOL_OUTPUT_FILENAME = "tool_output.log"
MAX_TOOL_OUTPUT_BYTES = 500_000
