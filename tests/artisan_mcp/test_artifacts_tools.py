"""Tests for artisan_query_artifacts and artisan_get_step_result."""

from __future__ import annotations

from fixtures.cache_isolation_store import build_cache_isolation_store


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
        assert {item["artifact_id"] for item in page["items"]} == seeded_run.data_ids

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
                "current_step_number",
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

    def test_failed_step_has_no_accepted_output(
        self, make_app, invoke, seeded_run
    ) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        result = invoke(
            app,
            "artisan_get_step_result",
            {"pipeline_run_id": seeded_run.run_id, "step_name": "transform"},
        )
        assert result == {}

    def test_unknown_step_is_empty(self, make_app, invoke, seeded_run) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        result = invoke(
            app,
            "artisan_get_step_result",
            {"pipeline_run_id": seeded_run.run_id, "step_name": "nope"},
        )
        assert result == {}

    def test_cached_step_uses_current_step_and_excludes_other_run(
        self, make_app, invoke, tmp_path
    ) -> None:
        store = build_cache_isolation_store(tmp_path)
        app = make_app(delta_root=store.root)

        page = invoke(
            app,
            "artisan_query_artifacts",
            {"pipeline_run_id": store.current_run},
        )
        result = invoke(
            app,
            "artisan_get_step_result",
            {
                "pipeline_run_id": store.current_run,
                "step_name": "current_metric_cached",
            },
        )

        assert {item["artifact_id"] for item in page["items"]} == {
            store.data_id,
            store.metric_id,
        }
        assert store.other_data_id not in {
            item["artifact_id"] for item in page["items"]
        }
        assert result["metric"][0]["artifact_id"] == store.metric_id
        assert result["metric"][0]["origin_step_number"] == 1
        assert result["metric"][0]["current_step_number"] == 5
