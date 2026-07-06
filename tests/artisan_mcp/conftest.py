"""Shared fixtures for artisan_mcp tests.

Tools are exercised through FastMCP's in-memory ``Client(app)``. Async
bodies run via ``asyncio.run`` so the suite needs no async-pytest plugin.
The ``make_app`` factory sets the ``ARTISAN_*`` env the server reads, and
loads ``artisan.operations.examples`` by default (bare discovery yields
curator builtins only).
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import polars as pl
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
                # structured_content is the dict every tool returns; .data is
                # None for an empty dict, so prefer the structured form.
                return result.structured_content

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


@pytest.fixture
def seeded_run(tmp_path: Path) -> SimpleNamespace:
    """Seed a complete single-run store and return its handles.

    Layout: ``<tmp>/delta`` is the Delta root; failure logs live at
    ``<tmp>/logs/failures`` (runs_dir = parent of delta_root). One run
    ``run-1`` with a completed ``generate`` step (two data artifacts) and a
    failed ``transform`` step (one metric + a failed execution with an
    error envelope and a written failure log).
    """
    delta_root = tmp_path / "delta"
    run_id = "run-1"
    _seed_steps(
        delta_root,
        run_id,
        [(1, "generate", "completed"), (2, "transform", "failed")],
    )
    _seed_index(
        delta_root,
        [("a" * 32, "data", 1), ("b" * 32, "data", 1), ("c" * 32, "metric", 2)],
    )
    exec_id = "exec-2"
    envelope = {
        "error_type": "runtime",
        "code": "op_execute_failed",
        "message": "transform blew up",
        "recovery_hint": "REPORT_TO_USER",
        "field": None,
        "suggestions": [],
    }
    _seed_executions(delta_root, run_id, exec_id, envelope)

    log_dir = tmp_path / "logs" / "failures" / "step_2_transform"
    log_dir.mkdir(parents=True)
    (log_dir / f"{exec_id}.log").write_text(
        "\n".join(f"line {i}" for i in range(1, 11))
    )

    return SimpleNamespace(
        delta_root=delta_root,
        run_id=run_id,
        exec_id=exec_id,
        envelope=envelope,
        data_ids={"a" * 32, "b" * 32},
        metric_id="c" * 32,
    )


def _seed_steps(root: Path, run_id: str, steps: list[tuple[int, str, str]]) -> None:
    from artisan.schemas.enums import TablePath
    from artisan.storage.core.table_schemas import STEPS_SCHEMA

    rows = []
    t0 = datetime(2026, 7, 1, tzinfo=UTC)
    for i, (number, name, status) in enumerate(steps):
        for j, row_status in enumerate(["running", status]):
            rows.append(
                {
                    "step_run_id": f"{run_id}-step-{number}",
                    "step_spec_id": f"spec-{number}",
                    "pipeline_run_id": run_id,
                    "step_number": number,
                    "step_name": name,
                    "status": row_status,
                    "operation_class": "DataGenerator",
                    "params_json": "{}",
                    "input_refs_json": "{}",
                    "compute_backend": "local",
                    "compute_options_json": "{}",
                    "output_roles_json": "[]",
                    "output_types_json": "[]",
                    "total_count": 1,
                    "succeeded_count": 1,
                    "failed_count": 1 if status == "failed" else 0,
                    "timestamp": t0 + timedelta(minutes=10 * i + j),
                    "duration_seconds": 1.5,
                    "error": "transform blew up" if status == "failed" else None,
                    "dispatch_error": None,
                    "commit_error": None,
                    "metadata": "{}",
                }
            )
    pl.DataFrame(rows, schema=STEPS_SCHEMA).write_delta(str(root / TablePath.STEPS))


def _seed_index(root: Path, entries: list[tuple[str, str, int]]) -> None:
    from artisan.schemas.enums import TablePath
    from artisan.storage.core.table_schemas import ARTIFACT_INDEX_SCHEMA

    df = pl.DataFrame(
        {
            "artifact_id": [e[0] for e in entries],
            "artifact_type": [e[1] for e in entries],
            "origin_step_number": [e[2] for e in entries],
            "metadata": ['{"seed": true}'] * len(entries),
        },
        schema=ARTIFACT_INDEX_SCHEMA,
    )
    df.write_delta(str(root / TablePath.ARTIFACT_INDEX))


def _seed_executions(root: Path, run_id: str, exec_id: str, envelope: dict) -> None:
    from artisan.schemas.enums import TablePath
    from artisan.storage.core.table_schemas import EXECUTIONS_SCHEMA

    row = dict.fromkeys(EXECUTIONS_SCHEMA)
    row.update(
        execution_run_id=exec_id,
        execution_spec_id="espec",
        step_run_id=f"{run_id}-step-2",
        origin_step_number=2,
        operation_name="transform",
        params="{}",
        user_overrides="{}",
        timestamp_start=datetime(2026, 7, 1, tzinfo=UTC),
        source_worker=0,
        compute_backend="local",
        success=False,
        error="transform blew up",
        error_envelope=json.dumps(envelope),
        tool_output="",
        worker_log="",
        metadata="{}",
    )
    pl.DataFrame([row], schema=EXECUTIONS_SCHEMA).write_delta(
        str(root / TablePath.EXECUTIONS)
    )
