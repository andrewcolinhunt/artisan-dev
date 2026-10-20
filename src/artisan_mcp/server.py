"""FastMCP application factory.

``build_mcp_app`` wires the lifespan (which runs operation discovery once)
and registers the read-only tools, resources, and prompts. Tool bodies read
the lifespan state via ``ctx.lifespan_context`` and call the artisan core
directly — no server-side logic.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

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
    from artisan.utils.application_modules import resolve_application_modules

    cfg = config or ArtisanMCPConfig()
    trusted_modules = resolve_application_modules(cfg.load_modules)

    @asynccontextmanager
    async def lifespan(_app: FastMCP) -> AsyncIterator[dict[str, Any]]:
        # Discovery runs once at startup; FastMCP exposes this mapping to tools
        # as ctx.lifespan_context.
        from artisan.registry import discover

        report = await asyncio.to_thread(
            discover, list(trusted_modules), include_environment=False
        )
        yield {"discovery": report, "config": cfg, "trusted_modules": trusted_modules}

    from artisan_mcp import __version__, prompts, resources, tools

    mcp = FastMCP(
        "artisan", version=__version__, lifespan=lifespan, mask_error_details=True
    )
    tools.catalog.register(mcp)
    tools.runs.register(mcp)
    tools.artifacts.register(mcp)
    tools.logs.register(mcp)
    tools.provenance.register(mcp)
    tools.outcomes.register(mcp)
    resources.catalog.register(mcp)
    resources.runs.register(mcp)
    resources.lineage.register(mcp)
    prompts.register(mcp)
    return mcp
