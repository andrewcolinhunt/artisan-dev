"""End-to-end: every tool, resource, and prompt against a seeded store.

Marked integration; exercises the full read-only surface through FastMCP's
in-memory client so a regression in any wiring surfaces in one place.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


class TestEndToEnd:
    def test_capabilities(self, make_app, invoke, seeded_run) -> None:
        cap = invoke(make_app(delta_root=seeded_run.delta_root), "artisan_capabilities")
        assert cap["read_only"] is True
        assert cap["delta_root"] == str(seeded_run.delta_root)

    def test_catalog_chain(self, make_app, invoke, seeded_run) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        page = invoke(app, "artisan_list_operations", {"limit": 5})
        assert page["items"]
        meta = invoke(
            app, "artisan_describe_operation", {"name": page["items"][0]["name"]}
        )
        assert meta["name"] == page["items"][0]["name"]

    def test_run_inspection_chain(self, make_app, invoke, seeded_run) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        runs = invoke(app, "artisan_list_runs")
        assert seeded_run.run_id in {r["pipeline_run_id"] for r in runs["items"]}

        status = invoke(
            app, "artisan_get_run_status", {"pipeline_run_id": seeded_run.run_id}
        )
        assert status["step_count"] == 2

        result = invoke(
            app,
            "artisan_get_step_result",
            {"pipeline_run_id": seeded_run.run_id, "step_name": "generate"},
        )
        assert set(result["data"][0]) >= {"artifact_id", "artifact_type"}

        logs = invoke(
            app,
            "artisan_get_step_logs",
            {"pipeline_run_id": seeded_run.run_id, "step_name": "transform"},
        )
        assert logs["lines"]

    def test_artifacts_and_provenance(self, make_app, invoke, seeded_run) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        page = invoke(app, "artisan_query_artifacts", {"artifact_type": "data"})
        artifact_id = page["items"][0]["artifact_id"]
        graph = invoke(
            app, "artisan_get_provenance_graph", {"artifact_id": artifact_id}
        )
        assert graph["artifact_id"] == artifact_id
        assert "edges" in graph

    def test_diagnose(self, make_app, invoke, seeded_run) -> None:
        diag = invoke(
            make_app(delta_root=seeded_run.delta_root),
            "artisan_diagnose_run",
            {"pipeline_run_id": seeded_run.run_id},
        )
        assert diag["failed_steps"]
        assert diag["suggested_actions"]

    def test_resources(self, make_app, read_resource, seeded_run) -> None:
        import json

        app = make_app(delta_root=seeded_run.delta_root)
        runs = json.loads(read_resource(app, "artisan://runs").text)
        assert runs["items"]
        detail = json.loads(
            read_resource(app, f"artisan://runs/{seeded_run.run_id}").text
        )
        assert detail["pipeline_run_id"] == seeded_run.run_id
        dot = read_resource(app, f"artisan://lineage/run/{seeded_run.run_id}").text
        assert "digraph" in dot

    def test_prompts(self, make_app, seeded_run) -> None:
        import asyncio

        from fastmcp import Client

        app = make_app(delta_root=seeded_run.delta_root)

        async def _run() -> None:
            async with Client(app) as client:
                names = {p.name for p in await client.list_prompts()}
                assert {
                    "artisan/diagnose-failure",
                    "artisan/explain-run",
                    "artisan/walk-lineage",
                } <= names
                got = await client.get_prompt(
                    "artisan/diagnose-failure",
                    {"pipeline_run_id": seeded_run.run_id},
                )
                text = got.messages[0].content.text
                assert seeded_run.run_id in text

        asyncio.run(_run())
