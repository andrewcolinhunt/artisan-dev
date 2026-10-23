"""Thin named MCP adapters for shared bounded inspections."""

from __future__ import annotations

from typing import Literal

from fastmcp import Context, FastMCP
from fastmcp.tools import ToolResult

from artisan.inspection.models import (
    ProvenanceResponse,
    output_schema,
)
from artisan.inspection.requests import (
    PageLimit,
    ProvenanceDepth,
)
from artisan_mcp._boundary import inspection_tool
from artisan_mcp._common import READ_ONLY


def register(mcp: FastMCP) -> None:
    """Register individual read-only tools using the core request definitions."""

    @mcp.tool(annotations=READ_ONLY, output_schema=output_schema(ProvenanceResponse))
    def artisan_get_provenance_graph(
        ctx: Context,
        artifact_id: str,
        direction: Literal["backward", "forward"] = "backward",
        depth: ProvenanceDepth = 3,
        limit: PageLimit = 50,
        cursor: str | None = None,
    ) -> ToolResult:
        """Walk artifact provenance and return a page of selected directed edges.

        Depth is 1-10; traversal_truncated reports remaining neighbors past that
        boundary independently of edges.has_more. Exhausting pages covers only
        the selected walk, not complete scientific provenance. Unknown artifacts
        yield empty edges. Discover actual artifact IDs before requesting walks.
        """
        return inspection_tool(
            "get_provenance_graph",
            {
                "artifact_id": artifact_id,
                "direction": direction,
                "depth": depth,
                "limit": limit,
                "cursor": cursor,
            },
            ctx=ctx,
        )
