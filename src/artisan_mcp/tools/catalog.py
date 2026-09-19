"""Catalog tools: capabilities, list_operations, describe_operation.

Serialize the registry populated by startup discovery. Unknown operations and
invalid pagination return error envelopes through the shared boundary;
argument-schema errors are handled by MCP.
"""

from __future__ import annotations

from typing import Any, Literal

from fastmcp import Context, FastMCP

from artisan_mcp._boundary import boundary
from artisan_mcp._common import READ_ONLY
from artisan_mcp._pagination import paginate


def register(mcp: FastMCP) -> None:
    """Attach the catalog tools to ``mcp``."""

    @mcp.tool(annotations=READ_ONLY)
    async def artisan_capabilities(ctx: Context) -> dict[str, Any]:
        """Report what this server can do before you plan any other call.

        Returns the artisan and server versions, whether the server is
        read-only, and the startup discovery report — which op modules loaded,
        which failed to import, and any name collisions. The configured Delta
        root is intentionally withheld. Not a store read: it never touches
        Delta tables, so it works even when delta_root is unset.
        """
        from artisan.registry import capabilities

        state = ctx.lifespan_context
        from artisan_mcp import __version__

        return capabilities(
            server_version=__version__,
            discovery=state["discovery"],
            delta_root=None,
            read_only=True,
        ).model_dump()

    @mcp.tool(annotations=READ_ONLY)
    async def artisan_list_operations(
        query: str | None = None,
        kind: Literal["creator", "curator"] | None = None,
        tag: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """List registered operations to discover what a pipeline can be built from.

        Filters AND together: kind ('creator' or 'curator'), a
        case-insensitive substring query over name/description, and an exact
        tag. Returns a page of lightweight summaries (name, kind,
        description, input/output roles, tags) with has_more and next_cursor
        for paging, capped at 100 items — pass next_cursor back as cursor to
        continue. Use this to find an operation by capability; use
        artisan_describe_operation for one op's full parameter schema and
        examples. Summaries only, never parameter schemas.
        """
        from artisan.registry import list_operations

        def payload() -> dict[str, Any]:
            summaries = list_operations(kind=kind, query=query, tag=tag)
            return paginate([s.model_dump() for s in summaries], limit, cursor)

        return boundary(payload)

    @mcp.tool(annotations=READ_ONLY)
    async def artisan_describe_operation(name: str) -> dict[str, Any]:
        """Describe one operation in full before configuring it in a pipeline.

        Returns the complete metadata for the named op: input and output
        roles with their artifact types, the JSON Schema for its parameters
        (with descriptions), declared usage examples, and the source module.
        Use this once artisan_list_operations has surfaced a candidate and
        you need its exact parameters. An unknown name returns an
        unknown_operation error envelope carrying did-you-mean suggestions
        rather than raising.
        """
        from artisan.registry import describe

        return boundary(lambda: describe(name).model_dump())
