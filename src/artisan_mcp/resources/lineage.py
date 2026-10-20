"""Whole run-scoped Graphviz DOT or a valid explicit omission placeholder."""

from __future__ import annotations

import asyncio

from fastmcp import Context, FastMCP

from artisan_mcp._boundary import capture, require_delta_root
from artisan_mcp._evidence import MAX_PAYLOAD_BYTES

_OVERSIZED_GRAPH = """digraph pipeline {
  label="Macro lineage omitted: response exceeds the byte budget. Discover run artifacts, then inspect selected provenance walks; those are not macro graph pagination. Use Python for the complete macro graph.";
}
"""
_UNAVAILABLE_GRAPH = """digraph pipeline {
  label="Macro lineage unavailable; inspect the configured store and server diagnostics.";
}
"""


def register(mcp: FastMCP) -> None:
    """Register the DOT resource without slicing its syntax or changing MIME."""

    @mcp.resource(
        "artisan://lineage/run/{pipeline_run_id}", mime_type="text/vnd.graphviz"
    )
    async def run_lineage(pipeline_run_id: str, ctx: Context) -> str:
        """Read the complete run macro graph or an explicit unavailable graph."""
        config = ctx.lifespan_context["config"]

        def payload() -> str:
            from artisan.visualization.graph.macro import build_macro_graph

            root = require_delta_root(config)
            source = build_macro_graph(root, pipeline_run_id=pipeline_run_id).source
            return (
                source
                if len(source.encode("utf-8")) <= MAX_PAYLOAD_BYTES
                else _OVERSIZED_GRAPH
            )

        result = await asyncio.to_thread(capture, payload)
        if result.error is not None:
            return _UNAVAILABLE_GRAPH
        assert result.value is not None
        return result.value
