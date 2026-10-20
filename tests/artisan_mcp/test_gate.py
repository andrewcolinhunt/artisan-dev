"""The MCP surface is honestly and unconditionally read-only."""

from __future__ import annotations

import pytest

_READ_TOOLS = {
    "artisan_capabilities",
    "artisan_assess_outcomes",
    "artisan_compare_runs",
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


class TestReadOnlySurface:
    def test_read_only_registers_twelve_tools(self, make_app, tool_names) -> None:
        assert tool_names(make_app()) == _READ_TOOLS

    def test_capabilities_report_read_only(self, make_app, invoke) -> None:
        assert invoke(make_app(), "artisan_capabilities")["read_only"] is True


def test_failed_startup_discovery_blocks_store_tools_resources_and_prompts(
    make_app, monkeypatch, tmp_path
):
    import asyncio
    import importlib
    import json

    from fastmcp import Client

    readers = importlib.import_module("artisan.inspection.runs")
    monkeypatch.setattr(
        readers, "runs_payload", lambda **kwargs: pytest.fail("store opened")
    )
    (tmp_path / "inspection_broken_app.py").write_text(
        "raise RuntimeError('startup failure')\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    app = make_app(delta_root=tmp_path, load_modules="inspection_broken_app")

    async def check():
        async with Client(app) as client:
            result = await client.call_tool("artisan_capabilities", {"view": "errors"})
            assert result.structured_content["total_items"] == 1
            assert (
                result.structured_content["items"][0]["module"]
                == "inspection_broken_app"
            )
            operations = await client.call_tool("artisan_list_operations")
            assert not operations.is_error
            result = await client.call_tool("artisan_list_runs", raise_on_error=False)
            assert result.is_error
            assert (
                result.structured_content["code"] == "application_registration_failed"
            )
            resource = await client.read_resource("artisan://runs")
            assert (
                json.loads(resource[0].text)["code"]
                == "application_registration_failed"
            )
            prompt = await client.get_prompt(
                "artisan/explain-run", {"pipeline_run_id": "run"}
            )
            assert "application_registration_failed" in prompt.messages[0].content.text
            lineage = await client.read_resource("artisan://lineage/run/run")
            assert "Macro lineage unavailable" in lineage[0].text
            result = await client.call_tool("artisan_capabilities")
            assert result.structured_content["error_count"] == 1

    asyncio.run(check())


def test_mcp_snapshots_trusted_modules_and_explicit_initialization_version(
    make_app, monkeypatch, seeded_run
):
    import asyncio

    from fastmcp import Client

    from artisan_mcp import __version__

    app = make_app(delta_root=seeded_run.delta_root)
    monkeypatch.setenv("ARTISAN_LOAD_MODULES", "not_authorized_after_factory")

    async def check():
        async with Client(app) as client:
            assert client.initialize_result.serverInfo.version == __version__
            discovery = await client.call_tool("artisan_capabilities")
            assert discovery.structured_content["error_count"] == 0
            runs = await client.call_tool("artisan_list_runs")
            assert runs.structured_content["total_items"] == 1

    asyncio.run(check())
