"""Thin named MCP adapters for shared bounded inspections."""

from __future__ import annotations

from typing import Literal

from fastmcp import Context, FastMCP
from fastmcp.tools import ToolResult

from artisan.inspection.models import (
    DiagnosisResult,
    PageResponse,
    RunRow,
    StatusResponse,
    output_schema,
)
from artisan.inspection.requests import (
    DiagnosisView,
    PageLimit,
)
from artisan_mcp._boundary import inspection_tool
from artisan_mcp._common import READ_ONLY


def register(mcp: FastMCP) -> None:
    """Register individual read-only tools using the core request definitions."""

    @mcp.tool(annotations=READ_ONLY, output_schema=output_schema(PageResponse[RunRow]))
    def artisan_list_runs(
        ctx: Context, limit: PageLimit = 20, cursor: str | None = None
    ) -> ToolResult:
        """List persisted pipeline runs in deterministic recency order to select a run.

        Each bounded page contains rollups and continuation metadata. The latest
        updated step determines last_status, which is not a whole-run quality
        verdict. Equal start times use run ID order. Changing selected evidence
        invalidates the cursor; restart rather than combining different views.
        """
        return inspection_tool("list_runs", {"limit": limit, "cursor": cursor}, ctx=ctx)

    @mcp.tool(annotations=READ_ONLY, output_schema=output_schema(StatusResponse))
    def artisan_get_run_status(
        ctx: Context,
        pipeline_run_id: str,
        limit: PageLimit = 50,
        cursor: str | None = None,
    ) -> ToolResult:
        """Get the run rollup and a page of current per-step lifecycle statuses.

        Pending and running attempts remain visible; an unknown run has an empty
        steps page. last_status is the latest updated step, not overall success.
        The current reader omits separate cancellation evidence: failed status
        does not prove external work stopped. Use diagnosis to inspect failures.
        """
        return inspection_tool(
            "get_run_status",
            {"pipeline_run_id": pipeline_run_id, "limit": limit, "cursor": cursor},
            ctx=ctx,
        )

    @mcp.tool(annotations=READ_ONLY, output_schema=output_schema(DiagnosisResult))
    def artisan_diagnose_run(
        ctx: Context,
        pipeline_run_id: str,
        view: DiagnosisView = "summary",
        detail: Literal["full", "identity"] = "full",
        limit: PageLimit = 50,
        cursor: str | None = None,
    ) -> ToolResult:
        """Diagnose selected execution failures with explicit counts and graph coverage.

        Summary returns advisory actions and coverage, not hidden samples. Page
        failures, similar_runs or upstream_edges for evidence; identity failure
        detail omits large diagnostics. Recent-run windows and seed/depth limits
        remain separate from page exhaustion. Empty failures do not prove cleanup.
        """
        return inspection_tool(
            "diagnose_run",
            {
                "pipeline_run_id": pipeline_run_id,
                "view": view,
                "detail": detail,
                "limit": limit,
                "cursor": cursor,
            },
            ctx=ctx,
        )
