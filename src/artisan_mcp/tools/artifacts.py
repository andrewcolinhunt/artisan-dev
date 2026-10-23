"""Thin named MCP adapters for shared bounded inspections."""

from __future__ import annotations

from fastmcp import Context, FastMCP
from fastmcp.tools import ToolResult

from artisan.inspection.catalog import ArtifactPage
from artisan.inspection.models import (
    output_schema,
)
from artisan.inspection.requests import (
    PageLimit,
)
from artisan_mcp._boundary import inspection_tool
from artisan_mcp._common import READ_ONLY


def register(mcp: FastMCP) -> None:
    """Register individual read-only tools using the core request definitions."""

    @mcp.tool(annotations=READ_ONLY, output_schema=output_schema(ArtifactPage))
    def artisan_query_artifacts(
        ctx: Context,
        artifact_type: str | None = None,
        pipeline_run_id: str | None = None,
        metadata_keys: list[str] | None = None,
        limit: PageLimit = 50,
        cursor: str | None = None,
    ) -> ToolResult:
        """Find artifact references by type and/or accepted run membership.

        References preserve origin and current step numbers; cached and passthrough
        outputs can have different origins. Metadata is omitted unless exact
        top-level metadata_keys are selected. No artifact content is returned.
        Follow the cursor for all selected occurrences and use IDs for provenance.
        """
        return inspection_tool(
            "query_artifacts",
            {
                "artifact_type": artifact_type,
                "pipeline_run_id": pipeline_run_id,
                "metadata_keys": metadata_keys,
                "limit": limit,
                "cursor": cursor,
            },
            ctx=ctx,
        )

    @mcp.tool(annotations=READ_ONLY, output_schema=output_schema(ArtifactPage))
    def artisan_get_step_result(
        ctx: Context,
        pipeline_run_id: str,
        step_name: str,
        artifact_type: str | None = None,
        metadata_keys: list[str] | None = None,
        limit: PageLimit = 50,
        cursor: str | None = None,
    ) -> ToolResult:
        """Get a flat page of one step's accepted output references, optionally by type.

        Direct, cached and passthrough outputs retain current/origin identity.
        Metadata requires explicit metadata_keys; payloads and role grouping are
        not returned. Unknown steps give empty pages. Repeated step names select
        the existing first match, so resolve uniqueness through run status first.
        """
        return inspection_tool(
            "get_step_result",
            {
                "pipeline_run_id": pipeline_run_id,
                "step_name": step_name,
                "artifact_type": artifact_type,
                "metadata_keys": metadata_keys,
                "limit": limit,
                "cursor": cursor,
            },
            ctx=ctx,
        )
