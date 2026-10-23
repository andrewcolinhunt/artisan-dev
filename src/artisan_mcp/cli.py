"""``artisan-mcp`` console script with lazy optional dependencies."""

from __future__ import annotations

import json
import os
import sys
from enum import Enum
from typing import TYPE_CHECKING, Any

from artisan_mcp import _MCP_EXTRA_MESSAGE

if TYPE_CHECKING:
    from collections.abc import Sequence


class Transport(str, Enum):
    """Supported MCP transports."""

    STDIO = "stdio"
    HTTP = "http"


def _build_app() -> Any:
    """Create the Typer app only when the console entry point runs."""
    import typer

    app = typer.Typer(
        add_completion=False,
        help="Run the read-only Artisan MCP server.",
    )

    @app.callback(invoke_without_command=True)
    def serve(
        delta_root: str | None = typer.Option(
            None,
            "--delta-root",
            help=(
                "Delta Lake root the store-reading tools read. Sets ARTISAN_DELTA_ROOT."
            ),
        ),
        load: list[str] | None = typer.Option(
            None,
            "--load",
            help="Extra op module to import (repeatable). Sets ARTISAN_LOAD_MODULES.",
        ),
        transport: Transport = typer.Option(
            Transport.STDIO,
            "--transport",
            help="MCP transport: 'stdio' (default) or 'http'.",
        ),
        print_config: bool = typer.Option(
            False,
            "--print-config",
            help="Print the resolved configuration as JSON and exit.",
        ),
    ) -> None:
        """Resolve configuration from flags and environment, then run."""
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
        build_mcp_app(config).run(transport=transport.value)

    return app


def main(argv: Sequence[str] | None = None) -> None:
    """Entry point for the ``artisan-mcp`` console script."""
    try:
        app = _build_app()
        app(args=list(argv) if argv is not None else None)
    except ModuleNotFoundError as exc:
        if exc.name not in {"typer", "fastmcp", "pydantic_settings"}:
            raise
        sys.stderr.write(_MCP_EXTRA_MESSAGE + "\n")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
