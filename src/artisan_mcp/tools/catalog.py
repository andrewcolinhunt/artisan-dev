"""Catalog tools: capabilities, list_operations, describe_operation.

Pure serialization veneers over ``artisan.registry`` — the lifespan ran
``discover()`` once, so the registry is populated when these fire. Only
``describe`` is fallible (unknown op → envelope); it goes through the
shared boundary.
"""

from __future__ import annotations

from fastmcp import Context, FastMCP

from artisan_mcp._boundary import boundary
from artisan_mcp._pagination import paginate

_READ_ONLY = {"readOnlyHint": True, "idempotentHint": True}


def register(mcp: FastMCP) -> None:
    """Attach the catalog tools to ``mcp``."""

    @mcp.tool(annotations=_READ_ONLY)
    async def artisan_capabilities(ctx: Context) -> dict:
        """Report what this server can do before you plan any other call.

        Returns the artisan and server versions, whether the server is
        read-only (write tools are hidden unless ARTISAN_WRITE is set), the
        configured Delta root (or null when unset), and the startup
        discovery report — which op modules loaded, which failed to import,
        and any name collisions. Call this first to learn whether a store is
        configured and which operations are available. Not a store read: it
        never touches Delta tables, so it works even when delta_root is
        unset.
        """
        from artisan.registry import capabilities

        state = ctx.lifespan_context
        config = state["config"]
        from artisan_mcp import __version__

        return capabilities(
            server_version=__version__,
            discovery=state["discovery"],
            delta_root=config.delta_root,
            read_only=not config.write_enabled,
        ).model_dump()

    @mcp.tool(annotations=_READ_ONLY)
    async def artisan_list_operations(
        query: str | None = None,
        kind: str | None = None,
        tag: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> dict:
        """List registered operations to discover what a pipeline can be built from.

        Filters AND together: kind ('creator' or 'curator'), a
        case-insensitive substring query over name/description, and an exact
        tag. Returns a page of lightweight summaries (name, kind,
        description, input/output roles, tags) with has_more and next_cursor
        for paging — pass next_cursor back as cursor to continue. Use this to
        find an operation by capability; use artisan_describe_operation for
        one op's full parameter schema and examples. Summaries only, never
        parameter schemas.
        """
        from artisan.registry import list_operations

        summaries = list_operations(kind=kind, query=query, tag=tag)
        return paginate([s.model_dump() for s in summaries], limit, cursor)

    @mcp.tool(annotations=_READ_ONLY)
    async def artisan_describe_operation(name: str) -> dict:
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
