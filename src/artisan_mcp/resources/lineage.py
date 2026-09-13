"""Lineage resource: the macro (step-level) pipeline graph as Graphviz DOT.

Returns the DOT source of the existing macro renderer via its cheap
``.source`` attribute. Note: ``build_macro_graph`` renders the whole steps
table, not a single run — at single-run scale (the common case) this equals
the run's graph; a run-scoped renderer would need a core change (out of
scope for this read-only adapter).
"""

from __future__ import annotations

from fastmcp import Context, FastMCP

from artisan_mcp._boundary import boundary, require_delta_root
from artisan_mcp._common import MAX_RESOURCE_CHARS

_OVERSIZED_GRAPH = """digraph pipeline {
  label="Lineage exceeds the MCP resource limit; use artifact provenance tools.";
}
"""
_UNAVAILABLE_GRAPH = """digraph pipeline {
  label="Lineage is unavailable; inspect the server logs.";
}
"""


def register(mcp: FastMCP) -> None:
    """Attach the lineage resource to ``mcp``."""

    @mcp.resource(
        "artisan://lineage/run/{pipeline_run_id}", mime_type="text/vnd.graphviz"
    )
    async def run_lineage(pipeline_run_id: str, ctx: Context) -> str:
        """Macro (step-level) pipeline lineage as Graphviz DOT source."""
        from artisan.visualization.graph.macro import build_macro_graph

        config = ctx.lifespan_context["config"]

        def payload() -> str:
            root = require_delta_root(config)
            source = build_macro_graph(root).source
            return source if len(source) <= MAX_RESOURCE_CHARS else _OVERSIZED_GRAPH

        result = boundary(payload)
        return result if isinstance(result, str) else _UNAVAILABLE_GRAPH
