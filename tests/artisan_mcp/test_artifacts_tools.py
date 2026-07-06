"""Tests for artisan_query_artifacts and artisan_get_step_result."""

from __future__ import annotations


class TestQueryArtifacts:
    def test_all_refs(self, make_app, invoke, seeded_run) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        page = invoke(app, "artisan_query_artifacts")
        ids = {item["artifact_id"] for item in page["items"]}
        assert ids == seeded_run.data_ids | {seeded_run.metric_id}

    def test_type_filter(self, make_app, invoke, seeded_run) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        page = invoke(app, "artisan_query_artifacts", {"artifact_type": "metric"})
        assert [item["artifact_id"] for item in page["items"]] == [seeded_run.metric_id]

    def test_run_filter(self, make_app, invoke, seeded_run) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        page = invoke(
            app, "artisan_query_artifacts", {"pipeline_run_id": seeded_run.run_id}
        )
        assert all(
            item["pipeline_run_id"] == seeded_run.run_id for item in page["items"]
        )

    def test_pagination(self, make_app, invoke, seeded_run) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        page = invoke(app, "artisan_query_artifacts", {"limit": 2})
        assert len(page["items"]) == 2
        assert page["has_more"] is True
        assert page["next_cursor"] == "2"

    def test_refs_carry_no_payload(self, make_app, invoke, seeded_run) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        page = invoke(app, "artisan_query_artifacts")
        for item in page["items"]:
            assert set(item) == {
                "artifact_id",
                "artifact_type",
                "origin_step_number",
                "pipeline_run_id",
                "metadata",
            }
            assert "content" not in item


class TestGetStepResult:
    def test_groups_by_type(self, make_app, invoke, seeded_run) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        result = invoke(
            app,
            "artisan_get_step_result",
            {"pipeline_run_id": seeded_run.run_id, "step_name": "generate"},
        )
        assert set(result) == {"data"}
        assert {r["artifact_id"] for r in result["data"]} == seeded_run.data_ids

    def test_failed_step_partial_output(self, make_app, invoke, seeded_run) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        result = invoke(
            app,
            "artisan_get_step_result",
            {"pipeline_run_id": seeded_run.run_id, "step_name": "transform"},
        )
        assert set(result) == {"metric"}
        assert result["metric"][0]["artifact_id"] == seeded_run.metric_id

    def test_unknown_step_is_empty(self, make_app, invoke, seeded_run) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        result = invoke(
            app,
            "artisan_get_step_result",
            {"pipeline_run_id": seeded_run.run_id, "step_name": "nope"},
        )
        assert result == {}
