"""An inspection client recovers from changing evidence before claiming completeness."""

from __future__ import annotations

import asyncio

from fastmcp import Client
from fixtures.outcome_store import append_metric_attempt

from artisan.schemas import MetricArtifact


def test_client_restarts_changed_view_and_finds_late_page_artifact(
    tmp_path, make_app
) -> None:
    root = tmp_path / "delta"
    artifacts = [
        MetricArtifact.draft({"score": index}, f"metric-{index}.json", 0).finalize()
        for index in range(130)
    ]
    selection = append_metric_attempt(root, artifacts)
    target = MetricArtifact.draft({"score": 999}, "later-step.json", 1).finalize()
    app = make_app(delta_root=root)
    query = {"pipeline_run_id": selection.pipeline_run_id, "limit": 25}

    async def inspect() -> None:
        async with Client(app) as client:
            first = await client.call_tool("artisan_query_artifacts", query)
            old_page = first.structured_content
            assert old_page["has_more"]
            append_metric_attempt(root, [target], step_number=1, step_name="later")
            continuation = old_page["follow_up"]
            changed = await client.call_tool(
                "artisan_" + continuation["capability"],
                continuation["arguments"],
                raise_on_error=False,
            )
            assert changed.is_error
            assert changed.structured_content["code"] == "evidence_view_changed"

            page = (
                await client.call_tool("artisan_query_artifacts", query)
            ).structured_content
            view_id = page["view_id"]
            assert view_id != old_page["view_id"]
            collected = []
            while True:
                assert page["view_id"] == view_id
                assert page["total_items"] == 131
                collected.extend(item["artifact_id"] for item in page["items"])
                if not page["has_more"]:
                    break
                continuation = page["follow_up"]
                page = (
                    await client.call_tool(
                        "artisan_" + continuation["capability"],
                        continuation["arguments"],
                    )
                ).structured_content
            assert len(collected) == len(set(collected)) == 131
            assert collected[-1] == target.artifact_id

    asyncio.run(inspect())
