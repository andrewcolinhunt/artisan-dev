"""Thin named MCP adapters for shared bounded inspections."""

from __future__ import annotations

from typing import Literal

from fastmcp import Context, FastMCP
from fastmcp.tools import ToolResult

from artisan.inspection.models import (
    CapabilityResult,
    OperationDescription,
    OperationList,
    output_schema,
)
from artisan.inspection.requests import (
    CapabilityView,
    OperationSection,
    PageLimit,
)
from artisan_mcp._boundary import inspection_tool
from artisan_mcp._common import READ_ONLY


def register(mcp: FastMCP) -> None:
    """Register individual read-only tools using the core request definitions."""

    @mcp.tool(annotations=READ_ONLY, output_schema=output_schema(CapabilityResult))
    def artisan_capabilities(
        ctx: Context,
        view: CapabilityView = "summary",
        limit: PageLimit = 50,
        cursor: str | None = None,
    ) -> ToolResult:
        """Report versions, read-only behavior and startup discovery coverage.

        The default summary includes exact discovery counts and withholds the
        configured root. Select sources, errors or name_collisions to page that
        collection. No store read or store health claim is made. Echo a returned
        cursor with the same view; restart if the selected evidence changed.
        """
        return inspection_tool(
            "capabilities", {"view": view, "limit": limit, "cursor": cursor}, ctx=ctx
        )

    @mcp.tool(annotations=READ_ONLY, output_schema=output_schema(OperationList))
    def artisan_list_operations(
        query: str | None = None,
        kind: Literal["creator", "curator"] | None = None,
        tag: str | None = None,
        detail: Literal["summary", "identity"] = "summary",
        limit: PageLimit = 50,
        cursor: str | None = None,
    ) -> ToolResult:
        """List registered operations using AND-combined query, kind and tag filters.

        Summary rows contain descriptions and roles, never parameter schemas.
        Use detail='identity' if one summary is too large. Each page reports its
        selected total, omissions and continuation. Use artisan_describe_operation
        for a candidate's complete contract or selected metadata section.
        """
        return inspection_tool(
            "list_operations",
            {
                "query": query,
                "kind": kind,
                "tag": tag,
                "detail": detail,
                "limit": limit,
                "cursor": cursor,
            },
        )

    @mcp.tool(annotations=READ_ONLY, output_schema=output_schema(OperationDescription))
    def artisan_describe_operation(
        name: str,
        section: OperationSection = "all",
        limit: PageLimit = 50,
        cursor: str | None = None,
    ) -> ToolResult:
        """Describe an operation's contracts, parameters and declared usage examples.

        The all section returns complete metadata when it fits. Select summary,
        inputs, outputs, params, examples or tags to narrow evidence. Role/example
        lists page; parameter schemas remain whole with their references. An
        oversized indivisible section requires source or Python inspection.
        """
        return inspection_tool(
            "describe_operation",
            {"name": name, "section": section, "limit": limit, "cursor": cursor},
        )
