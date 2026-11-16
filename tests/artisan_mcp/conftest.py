"""Shared fixtures for artisan_mcp tests.

Tools are exercised through FastMCP's in-memory ``Client(app)``. Async
bodies run via ``asyncio.run`` so the suite needs no async-pytest plugin.
The ``make_app`` factory sets the ``ARTISAN_*`` env the server reads, and
loads ``artisan.operations.examples`` by default (bare discovery yields
curator builtins only).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
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
        load_modules: str | None = _DEFAULT_LOAD,
    ) -> FastMCP:
        if delta_root is not None:
            monkeypatch.setenv("ARTISAN_DELTA_ROOT", str(delta_root))
        else:
            monkeypatch.delenv("ARTISAN_DELTA_ROOT", raising=False)
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
                # structured_content is the dict every tool returns; .data is
                # None for an empty dict, so prefer the structured form.
                return result.structured_content

        return asyncio.run(_run())

    return _invoke


@pytest.fixture
def invoke_result() -> Callable[..., Any]:
    """Return a helper preserving FastMCP tool-error metadata."""

    def _invoke(app: FastMCP, name: str, args: dict | None = None) -> Any:
        async def _run() -> Any:
            async with Client(app) as client:
                return await client.call_tool(name, args or {}, raise_on_error=False)

        return asyncio.run(_run())

    return _invoke


@pytest.fixture
def invoke_error(invoke_result) -> Callable[..., Any]:
    """Require native error signaling and return its structured recovery data."""

    def call(app: FastMCP, name: str, args: dict | None = None) -> Any:
        result = invoke_result(app, name, args)
        assert result.is_error is True
        assert result.data is None
        assert isinstance(result.structured_content, dict)
        return result.structured_content

    return call


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


@pytest.fixture
def seeded_run(tmp_path: Path) -> SimpleNamespace:
    """Create accepted outputs, an exact failed curator, and unrelated content."""
    from fixtures.inspection_curator import commit_curator_record
    from fixtures.retained_inspection import commit_inspection_attempt

    from artisan.schemas import CSVArtifact, MetricArtifact
    from artisan.utils.log_paths import failure_log_relative_path

    delta_root = tmp_path / "delta"
    run_id = "run-1"
    artifacts = [
        CSVArtifact.draft(
            f"value\n{index}\n".encode(),
            f"dataset-{index}.csv",
            1,
            metadata={"seed": True},
        ).finalize()
        for index in range(2)
    ]
    commit_inspection_attempt(
        delta_root,
        run_id=run_id,
        number=1,
        name="generate",
        outputs={"data": artifacts},
    )
    envelope = {
        "error_type": "runtime",
        "code": "op_execute_failed",
        "message": "transform blew up",
        "recovery_hint": "REPORT_TO_USER",
        "field": None,
        "suggestions": [],
    }
    timestamp = datetime.now(UTC)
    exec_id = commit_curator_record(
        delta_root,
        run_id=run_id,
        number=2,
        name="transform",
        error="transform blew up",
        envelope=envelope,
        timestamp=timestamp,
    )
    metric = MetricArtifact.draft({"score": 0.5}, "unrelated.json", 0).finalize()
    commit_inspection_attempt(
        delta_root,
        run_id="unrelated-run",
        outputs={"metrics": [metric]},
    )
    log_path = (
        tmp_path / "logs/failures" / failure_log_relative_path(exec_id, timestamp)
    )
    log_path.parent.mkdir(parents=True)
    log_path.write_text("\n".join(f"line {i}" for i in range(1, 11)))
    return SimpleNamespace(
        delta_root=delta_root,
        run_id=run_id,
        exec_id=exec_id,
        log_path=log_path,
        envelope=envelope,
        data_ids={item.artifact_id for item in artifacts},
        metric_id=metric.artifact_id,
    )
