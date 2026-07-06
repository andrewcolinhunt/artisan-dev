"""``artisan-mcp`` console script.

A thin Typer wrapper: each flag sets the matching ``ARTISAN_*`` env var, then
``ArtisanMCPConfig`` resolves the server exactly as it would for a
client-launched process. ``--print-config`` dumps the resolved config and
exits; otherwise the server runs over the chosen transport.
"""

from __future__ import annotations

import json
import os

import typer

app = typer.Typer(
    add_completion=False,
    help="Run the Artisan MCP server (read-only by default).",
)


@app.callback(invoke_without_command=True)
def serve(
    write: bool | None = typer.Option(
        None,
        "--write/--no-write",
        help="Register write tools (Phase 2). Sets ARTISAN_WRITE.",
    ),
    delta_root: str | None = typer.Option(
        None,
        "--delta-root",
        help="Delta Lake root the store-reading tools read. Sets ARTISAN_DELTA_ROOT.",
    ),
    load: list[str] | None = typer.Option(
        None,
        "--load",
        help="Extra op module to import (repeatable). Sets ARTISAN_LOAD_MODULES.",
    ),
    transport: str = typer.Option(
        "stdio",
        "--transport",
        help="MCP transport: 'stdio' (default) or 'http'.",
    ),
    print_config: bool = typer.Option(
        False,
        "--print-config",
        help="Print the resolved configuration as JSON and exit.",
    ),
) -> None:
    """Resolve configuration from flags + environment and run the server."""
    if write is not None:
        os.environ["ARTISAN_WRITE"] = "true" if write else "false"
    if delta_root is not None:
        os.environ["ARTISAN_DELTA_ROOT"] = delta_root
    if load:
        os.environ["ARTISAN_LOAD_MODULES"] = ",".join(load)

    from artisan_mcp.config import ArtisanMCPConfig
    from artisan_mcp.server import build_mcp_app

    config = ArtisanMCPConfig()
    if print_config:
        typer.echo(json.dumps(config.model_dump(), indent=2))
        return

    build_mcp_app(config).run(transport=transport)


def main() -> None:
    """Entry point for the ``artisan-mcp`` console script."""
    app()


if __name__ == "__main__":
    main()
