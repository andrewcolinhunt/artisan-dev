"""FastMCP application factory.

``build_mcp_app`` wires the lifespan (which runs operation discovery once)
and registers the read-only tools, resources, and prompts. Tool bodies read
the lifespan state via ``ctx.lifespan_context`` and call the artisan core
directly — no server-side logic.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from fastmcp import FastMCP

from artisan_mcp.config import ArtisanMCPConfig

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


def build_mcp_app(config: ArtisanMCPConfig | None = None) -> FastMCP:
    """Construct the FastMCP server.

    Args:
        config: Server configuration. Defaults to one resolved from the
            environment.

    Returns:
        A ``FastMCP`` instance with the read-only surface registered.
    """
    cfg = config or ArtisanMCPConfig()

    @asynccontextmanager
    async def lifespan(_app: FastMCP) -> AsyncIterator[dict]:
        # Discovery runs once at startup; tools read the report and config
        # back via ctx.lifespan_context. Kept a plain dict per the design.
        from artisan.registry import discover

        report = discover(extra_modules=cfg.load_modules or None)
        yield {"discovery": report, "config": cfg}

    from artisan_mcp import prompts, resources, tools

    mcp = FastMCP("artisan", lifespan=lifespan)
    tools.catalog.register(mcp)
    tools.runs.register(mcp)
    tools.artifacts.register(mcp)
    tools.logs.register(mcp)
    tools.provenance.register(mcp)
    resources.catalog.register(mcp)
    resources.runs.register(mcp)
    resources.lineage.register(mcp)
    prompts.register(mcp)
    return mcp
