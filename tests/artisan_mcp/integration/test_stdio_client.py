"""Verify the installed read-only MCP console through the SDK stdio transport.

This file also runs unchanged outside the checkout against an installed wheel.
Keep its fixtures local and import Artisan only through public package facades.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sysconfig
import tempfile
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from artisan.operations.curator import IngestData
from artisan.operations.examples import DataTransformer
from artisan.orchestration import PipelineManager, StepStatus
from artisan.storage import ArtifactStore
from artisan.visualization import inspect_failures, inspect_step

pytestmark = pytest.mark.integration

_MARKER = "mcp-smoke-invalid-number"
_EXAMPLES = "artisan.operations.examples"
_READ_TOOLS = {
    "artisan_capabilities",
    "artisan_list_operations",
    "artisan_describe_operation",
    "artisan_list_runs",
    "artisan_get_run_status",
    "artisan_get_step_result",
    "artisan_query_artifacts",
    "artisan_get_step_logs",
    "artisan_get_provenance_graph",
    "artisan_diagnose_run",
}


def _create_run(work_root: Path) -> tuple[str, str, str, str]:
    """Execute a committed ingest and numeric-parse failure using shipped ops."""
    csv_path = work_root / "invalid.csv"
    csv_path.write_text(f"id,x\n0,{_MARKER}\n", encoding="utf-8")
    delta_root = str(work_root / "runs" / "delta")
    with PipelineManager.create(
        name="mcp_smoke",
        delta_root=delta_root,
        staging_root=str(work_root / "runs" / "staging"),
        working_root=str(work_root / "working"),
    ) as pipeline:
        ingest = pipeline.run(IngestData, inputs=[str(csv_path)], name="ingest")
        transform = pipeline.run(
            DataTransformer,
            inputs={"dataset": ingest.output("data")},
            name="transform",
        )
        run_id = pipeline.config.pipeline_run_id
    assert ingest.status is StepStatus.SUCCEEDED
    assert transform.status is StepStatus.FAILED
    assert inspect_step(delta_root, 0, pipeline_run_id=run_id).height == 1
    artifact_ids = ArtifactStore(delta_root).provenance.load_artifact_ids_by_type(
        "data", step_numbers=[0]
    )
    assert len(artifact_ids) == 1
    failures = inspect_failures(delta_root, pipeline_run_id=run_id)
    assert failures.height == 1
    failure = failures.row(0, named=True)
    assert _MARKER in failure["error"]
    assert failure["log"]
    log_path = work_root / "runs" / "logs" / "failures" / failure["log"]
    assert log_path.is_file()
    assert log_path.stat().st_size > 0
    return run_id, next(iter(artifact_ids)), failure["execution_run_id"], failure["log"]


def _snapshot(root: Path) -> dict[str, str]:
    """Digest every persisted store and log file without retaining file bytes."""
    digests = {}
    for path in sorted(root.rglob("*")):
        if path.is_file():
            with path.open("rb") as stream:
                digests[path.relative_to(root).as_posix()] = hashlib.file_digest(
                    stream, "sha256"
                ).hexdigest()
    return digests


def _stderr_tail(path: Path) -> str:
    """Read only the final 8 KiB of subprocess diagnostics."""
    with path.open("rb") as stream:
        size = stream.seek(0, os.SEEK_END)
        stream.seek(max(0, size - 8192))
        return stream.read(8192).decode("utf-8", errors="replace")


@asynccontextmanager
async def _session(
    work_root: Path, args: list[str], env: dict[str, str]
) -> AsyncIterator[ClientSession]:
    """Initialize and close the installed console within one bounded session."""
    console = Path(sysconfig.get_path("scripts")) / "artisan-mcp"
    assert console.is_file(), f"Installed MCP console missing: {console}"
    cwd = work_root / "client-cwd"
    cwd.mkdir()
    stderr_path = work_root / "mcp-stderr.log"
    protocol_errors: list[Exception] = []

    async def handle_message(message: object) -> None:
        if isinstance(message, Exception):
            protocol_errors.append(message)

    parameters = StdioServerParameters(
        command=str(console), args=["--transport", "stdio", *args], env=env, cwd=cwd
    )
    with stderr_path.open("w", encoding="utf-8") as stderr:
        try:
            async with asyncio.timeout(60):
                async with stdio_client(parameters, errlog=stderr) as (read, write):
                    async with ClientSession(
                        read,
                        write,
                        read_timeout_seconds=timedelta(seconds=30),
                        message_handler=handle_message,
                    ) as session:
                        await session.initialize()
                        yield session
            # Closing the transport can also deliver malformed stdout messages.
            assert not protocol_errors, repr(protocol_errors)
        except Exception as exc:
            stderr.flush()
            if protocol_errors:
                exc.add_note(f"MCP protocol errors: {protocol_errors!r}")
            exc.add_note(f"MCP stderr (last 8 KiB):\n{_stderr_tail(stderr_path)}")
            raise


async def _call(
    session: ClientSession, name: str, arguments: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Require a structured response without MCP-level tool failure."""
    response = await session.call_tool(name, arguments or {})
    assert response.isError is False, response
    assert isinstance(response.structuredContent, dict), response
    return response.structuredContent


async def _catalog(session: ClientSession) -> dict[str, Any]:
    """Verify the exact read-only surface and successful example discovery."""
    tools = (await session.list_tools()).tools
    assert {tool.name for tool in tools} == _READ_TOOLS
    for tool in tools:
        assert tool.annotations is not None
        assert tool.annotations.readOnlyHint is True
    capabilities = await _call(session, "artisan_capabilities")
    assert capabilities["read_only"] is True
    assert capabilities["delta_root"] is None
    assert capabilities["discovery"]["errors"] == []
    assert capabilities["discovery"]["name_collisions"] == []
    assert _EXAMPLES in {
        source["module"] for source in capabilities["discovery"]["sources"]
    }
    operations = await _call(
        session, "artisan_list_operations", {"query": "data_transformer"}
    )
    assert "data_transformer" in {item["name"] for item in operations["items"]}
    operation = await _call(
        session, "artisan_describe_operation", {"name": "data_transformer"}
    )
    assert operation["name"] == "data_transformer"
    assert "scale_factor" in operation["params_schema"]["properties"]
    return capabilities


async def _inspect_run(
    session: ClientSession, run_id: str, artifact_id: str, execution_id: str, log: str
) -> None:
    """Read genuine persisted identities, failure evidence, and bounded logs."""
    runs = await _call(session, "artisan_list_runs")
    assert run_id in {item["pipeline_run_id"] for item in runs["items"]}
    run_args = {"pipeline_run_id": run_id}
    status = await _call(session, "artisan_get_run_status", run_args)
    assert status["pipeline_run_id"] == run_id
    assert status["step_count"] == 2
    assert len(status["steps"]) == 2
    assert {step["name"]: step["status"] for step in status["steps"]} == {
        "ingest": "succeeded",
        "transform": "failed",
    }
    result = await _call(
        session, "artisan_get_step_result", {**run_args, "step_name": "ingest"}
    )
    assert {artifact["artifact_id"] for artifact in result["data"]} == {artifact_id}
    diagnosis = await _call(session, "artisan_diagnose_run", run_args)
    assert len(diagnosis["failed_steps"]) == 1
    failure = diagnosis["failed_steps"][0]
    assert failure["execution_run_id"] == execution_id
    assert _MARKER in failure["error"]
    assert failure["log"] == log
    log_args = {**run_args, "step_name": "transform"}
    tail = await _call(session, "artisan_get_step_logs", {**log_args, "tail_lines": 3})
    assert 0 < len(tail["lines"]) <= 3
    assert tail["truncated"] is True
    assert _MARKER in tail["lines"][-1]
    oversized = await _call(
        session, "artisan_get_step_logs", {**log_args, "tail_lines": 1001}
    )
    assert oversized["code"] == "param_type_mismatch"
    assert oversized["field"] == "tail_lines"
    assert "lines" not in oversized
    assert "truncated" not in oversized
    success_logs = await _call(
        session, "artisan_get_step_logs", {**run_args, "step_name": "ingest"}
    )
    assert success_logs == {"lines": [], "truncated": False}


@pytest.mark.parametrize("mode", ["flags", "environment"])
def test_stdio_inspects_real_run_without_modifying_store(mode: str) -> None:
    """The installed console works from an unrelated cwd in both config modes."""
    with tempfile.TemporaryDirectory(prefix="artisan-mcp-stdio-") as temporary:
        work_root = Path(temporary)
        expected = _create_run(work_root)
        delta_root = str(work_root / "runs" / "delta")
        if mode == "flags":
            args = ["--delta-root", delta_root, "--load", _EXAMPLES]
            env = {
                "ARTISAN_DELTA_ROOT": str(work_root / "wrong-root"),
                "ARTISAN_LOAD_MODULES": "mcp_smoke_module_that_does_not_exist",
            }
        else:
            args = []
            env = {"ARTISAN_DELTA_ROOT": delta_root, "ARTISAN_LOAD_MODULES": _EXAMPLES}
        before = _snapshot(work_root / "runs")
        assert before

        async def exercise() -> None:
            async with _session(work_root, args, env) as session:
                capabilities = await _catalog(session)
                assert delta_root not in json.dumps(capabilities)
                await _inspect_run(session, *expected)

        asyncio.run(exercise())
        assert _snapshot(work_root / "runs") == before


def test_stdio_without_root_keeps_catalog_and_reports_configuration_error() -> None:
    """Catalog-only launch works; store access returns its configuration envelope."""
    with tempfile.TemporaryDirectory(prefix="artisan-mcp-catalog-") as temporary:

        async def exercise() -> None:
            async with _session(
                Path(temporary), [], {"ARTISAN_LOAD_MODULES": _EXAMPLES}
            ) as session:
                await _catalog(session)
                result = await _call(session, "artisan_list_runs")
                assert result["code"] == "delta_root_unset"
                assert result["recovery_hint"] == "CHECK_INPUT"
                assert "items" not in result

        asyncio.run(exercise())
