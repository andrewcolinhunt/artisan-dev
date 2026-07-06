"""Shared fixtures for artisan_mcp tests.

Tools are exercised through FastMCP's in-memory ``Client(app)``. Async
bodies run via ``asyncio.run`` so the suite needs no async-pytest plugin.
The ``make_app`` factory sets the ``ARTISAN_*`` env the server reads, and
loads ``artisan.operations.examples`` by default (bare discovery yields
curator builtins only).
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import pytest
from fastmcp import Client

from artisan_mcp import build_mcp_app
from artisan_mcp.config import ArtisanMCPConfig

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from fastmcp import FastMCP

_DEFAULT_LOAD = "artisan.operations.examples"


@pytest.fixture
def make_app(monkeypatch) -> Callable[..., FastMCP]:
    """Return a factory building a server under a controlled environment."""

    def _make(
        *,
        delta_root: Path | str | None = None,
        write: bool = False,
        load_modules: str | None = _DEFAULT_LOAD,
    ) -> FastMCP:
        if delta_root is not None:
            monkeypatch.setenv("ARTISAN_DELTA_ROOT", str(delta_root))
        else:
            monkeypatch.delenv("ARTISAN_DELTA_ROOT", raising=False)
        monkeypatch.setenv("ARTISAN_WRITE", "true" if write else "false")
        if load_modules:
            monkeypatch.setenv("ARTISAN_LOAD_MODULES", load_modules)
        else:
            monkeypatch.delenv("ARTISAN_LOAD_MODULES", raising=False)
        return build_mcp_app(ArtisanMCPConfig())

    return _make


@pytest.fixture
def invoke() -> Callable[..., Any]:
    """Return a helper calling one tool via the in-memory client."""

    def _invoke(app: FastMCP, name: str, args: dict | None = None) -> Any:
        async def _run() -> Any:
            async with Client(app) as client:
                result = await client.call_tool(name, args or {})
                return result.data

        return asyncio.run(_run())

    return _invoke


@pytest.fixture
def read_resource() -> Callable[..., Any]:
    """Return a helper reading one resource URI via the in-memory client."""

    def _read(app: FastMCP, uri: str) -> Any:
        async def _run() -> Any:
            async with Client(app) as client:
                contents = await client.read_resource(uri)
                return contents[0]

        return asyncio.run(_run())

    return _read


@pytest.fixture
def tool_names() -> Callable[[Any], set[str]]:
    """Return a helper listing the registered tool names of an app."""

    def _names(app: FastMCP) -> set[str]:
        async def _run() -> set[str]:
            async with Client(app) as client:
                return {t.name for t in await client.list_tools()}

        return asyncio.run(_run())

    return _names
