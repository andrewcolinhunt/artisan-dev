"""Paged provenance edges with independent traversal-coverage metadata."""

from __future__ import annotations

from typing import Any, Literal

from fastmcp import Context, FastMCP
from fastmcp.tools import ToolResult
from pydantic import StrictInt

from artisan_mcp._boundary import render_tool, require_delta_root, validate_int_range
from artisan_mcp._common import READ_ONLY
from artisan_mcp._evidence import MAX_PAYLOAD_BYTES, json_ready
from artisan_mcp._pagination import paginate
from artisan_mcp.models import ProvenanceResponse, output_schema

MAX_PROVENANCE_DEPTH = 10


def provenance_payload(
    root: str,
    artifact_id: str,
    direction: Literal["backward", "forward"] = "backward",
    depth: StrictInt = 3,
    limit: StrictInt = 50,
    cursor: str | None = None,
    *,
    budget: int = MAX_PAYLOAD_BYTES,
) -> dict[str, Any]:
    """Page the exact selected edge set, keeping its depth boundary visible."""
    from artisan.provenance import provenance_edges

    validate_int_range(limit, field="limit", minimum=1, maximum=100)
    validate_int_range(depth, field="depth", minimum=1, maximum=MAX_PROVENANCE_DEPTH)
    result = json_ready(
        provenance_edges(root, artifact_id, direction=direction, depth=depth)
    )
    edges = sorted(
        result.pop("edges"),
        key=lambda row: (row["source_artifact_id"], row["target_artifact_id"]),
    )
    result["traversal_truncated"] = result.pop("truncated")
    return paginate(
        edges,
        limit,
        cursor,
        tool="artisan_get_provenance_graph",
        arguments={"artifact_id": artifact_id, "direction": direction, "depth": depth},
        scope=root,
        header=result,
        page_key="edges",
        budget=budget,
    )


def register(mcp: FastMCP) -> None:
    """Register the read-only provenance tool."""

    @mcp.tool(annotations=READ_ONLY, output_schema=output_schema(ProvenanceResponse))
    def artisan_get_provenance_graph(
        ctx: Context,
        artifact_id: str,
        direction: Literal["backward", "forward"] = "backward",
        depth: StrictInt = 3,
        limit: StrictInt = 50,
        cursor: str | None = None,
    ) -> ToolResult:
        """Walk artifact provenance and return a page of selected directed edges.

        Depth is 1-10; traversal_truncated reports remaining neighbors past that
        boundary independently of edges.has_more. Exhausting pages covers only
        the selected walk, not complete scientific provenance. Unknown artifacts
        yield empty edges. Discover actual artifact IDs before requesting walks.
        """
        config = ctx.lifespan_context["config"]
        return render_tool(
            lambda: provenance_payload(
                require_delta_root(config), artifact_id, direction, depth, limit, cursor
            ),
            ProvenanceResponse,
        )
