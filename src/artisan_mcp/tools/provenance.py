"""Provenance tool: get_provenance_graph — a thin wrap of provenance_edges."""

from __future__ import annotations

from fastmcp import Context, FastMCP

from artisan_mcp._boundary import boundary, require_delta_root

_READ_ONLY = {"readOnlyHint": True, "idempotentHint": True}


def register(mcp: FastMCP) -> None:
    """Attach the provenance tool to ``mcp``."""

    @mcp.tool(annotations=_READ_ONLY)
    async def artisan_get_provenance_graph(
        ctx: Context,
        artifact_id: str,
        direction: str = "backward",
        depth: int = 3,
    ) -> dict:
        """Walk provenance edges around one artifact to trace where it came from.

        Returns a bounded edge list (source_artifact_id, target_artifact_id
        pairs) up to depth hops from the artifact, plus a truncated flag when
        neighbors remained past the bound. Direction 'backward' walks toward
        ancestors (what produced this), 'forward' toward descendants (what
        it fed). Edges only — no rendered graph and no artifact content. Get
        an artifact_id from artisan_query_artifacts or
        artisan_get_step_result. An unknown artifact yields empty edges.
        """
        config = ctx.lifespan_context["config"]

        def payload() -> dict:
            from artisan.provenance import provenance_edges

            root = require_delta_root(config)
            return provenance_edges(
                root, artifact_id, direction=direction, depth=depth
            ).model_dump()

        return boundary(payload)
