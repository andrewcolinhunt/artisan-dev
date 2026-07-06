"""Lineage resource: the macro (step-level) pipeline graph as Graphviz DOT.

Returns the DOT source of the existing macro renderer via its cheap
``.source`` attribute. Note: ``build_macro_graph`` renders the whole steps
table, not a single run — at single-run scale (the common case) this equals
the run's graph; a run-scoped renderer would need a core change (out of
Phase 1 scope).
"""

from __future__ import annotations

from fastmcp import Context, FastMCP

from artisan_mcp._boundary import require_delta_root


def register(mcp: FastMCP) -> None:
    """Attach the lineage resource to ``mcp``."""

    @mcp.resource(
        "artisan://lineage/run/{pipeline_run_id}", mime_type="text/vnd.graphviz"
    )
    async def run_lineage(pipeline_run_id: str, ctx: Context) -> str:
        """Macro (step-level) pipeline lineage as Graphviz DOT source."""
        from artisan.visualization.graph.macro import build_macro_graph

        config = ctx.lifespan_context["config"]
        root = require_delta_root(config)
        return build_macro_graph(root).source
