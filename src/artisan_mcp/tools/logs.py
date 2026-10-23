"""Thin named MCP adapters for shared bounded inspections."""

from __future__ import annotations

from fastmcp import Context, FastMCP
from fastmcp.tools import ToolResult

from artisan.inspection.models import (
    LogsResponse,
    output_schema,
)
from artisan.inspection.requests import (
    TailLines,
)
from artisan_mcp._boundary import inspection_tool
from artisan_mcp._common import READ_ONLY


def register(mcp: FastMCP) -> None:
    """Register individual read-only tools using the core request definitions."""

    @mcp.tool(annotations=READ_ONLY, output_schema=output_schema(LogsResponse))
    def artisan_get_step_logs(
        ctx: Context,
        pipeline_run_id: str,
        step_name: str,
        tail_lines: TailLines = 200,
    ) -> ToolResult:
        """Tail the failure logs of a failed step to read its error output.

        This reads the human failure log files a failed execution writes, not
        success-step logs. It resolves the step name to its number, finds that
        step's failed-execution log paths via the failures report, reads them,
        and returns the last tail_lines lines with a truncated flag. The
        request is capped at 1,000 lines and 256 KiB. A step that did not fail
        — or a store with no failures — yields an empty lines list. Returned bytes are separately bounded;
        truncation_reasons and partial_first_line describe the excerpt. Use it
        after artisan_get_run_status flags a failed step.
        """
        return inspection_tool(
            "get_step_logs",
            {
                "pipeline_run_id": pipeline_run_id,
                "step_name": step_name,
                "tail_lines": tail_lines,
            },
            ctx=ctx,
        )
