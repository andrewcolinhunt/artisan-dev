"""Tests for artisan_list_runs, artisan_get_run_status, artisan_get_step_logs."""

from __future__ import annotations


class TestListRuns:
    def test_lists_seeded_run(self, make_app, invoke, seeded_run) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        page = invoke(app, "artisan_list_runs")
        ids = [row["pipeline_run_id"] for row in page["items"]]
        assert seeded_run.run_id in ids
        assert page["has_more"] is False

    def test_empty_root_yields_empty_page(self, make_app, invoke, tmp_path) -> None:
        page = invoke(make_app(delta_root=tmp_path), "artisan_list_runs")
        assert page["items"] == []

    def test_unset_root_returns_envelope(self, make_app, invoke) -> None:
        env = invoke(make_app(delta_root=None), "artisan_list_runs")
        assert env["code"] == "delta_root_unset"
        assert env["recovery_hint"] == "CHECK_INPUT"


class TestGetRunStatus:
    def test_terminal_step_statuses(self, make_app, invoke, seeded_run) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        status = invoke(
            app, "artisan_get_run_status", {"pipeline_run_id": seeded_run.run_id}
        )
        assert status["pipeline_run_id"] == seeded_run.run_id
        assert status["step_count"] == 2
        by_name = {s["name"]: s["status"] for s in status["steps"]}
        assert by_name == {"generate": "ok", "transform": "failed"}

    def test_unknown_run_is_empty(self, make_app, invoke, seeded_run) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        status = invoke(app, "artisan_get_run_status", {"pipeline_run_id": "nope"})
        assert status["steps"] == []
        assert status["last_status"] is None


class TestDiagnoseRun:
    def test_diagnoses_seeded_failure(self, make_app, invoke, seeded_run) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        diag = invoke(
            app, "artisan_diagnose_run", {"pipeline_run_id": seeded_run.run_id}
        )
        assert diag["last_status"] == "failed"
        assert [s["operation"] for s in diag["failed_steps"]] == ["transform"]
        assert diag["failed_steps"][0]["code"] == "op_execute_failed"
        assert diag["suggested_actions"]


class TestRunResources:
    def test_runs_list_resource(self, make_app, read_resource, seeded_run) -> None:
        import json

        app = make_app(delta_root=seeded_run.delta_root)
        content = read_resource(app, "artisan://runs")
        data = json.loads(content.text)
        assert seeded_run.run_id in {r["pipeline_run_id"] for r in data["items"]}

    def test_run_detail_resource(self, make_app, read_resource, seeded_run) -> None:
        import json

        app = make_app(delta_root=seeded_run.delta_root)
        content = read_resource(app, f"artisan://runs/{seeded_run.run_id}")
        status = json.loads(content.text)
        assert status["pipeline_run_id"] == seeded_run.run_id
        assert len(status["steps"]) == 2


class TestGetStepLogs:
    def test_reads_failure_log(self, make_app, invoke, seeded_run) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        result = invoke(
            app,
            "artisan_get_step_logs",
            {"pipeline_run_id": seeded_run.run_id, "step_name": "transform"},
        )
        assert result["lines"]
        assert "line 10" in result["lines"]
        assert result["truncated"] is False

    def test_tail_truncates(self, make_app, invoke, seeded_run) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        result = invoke(
            app,
            "artisan_get_step_logs",
            {
                "pipeline_run_id": seeded_run.run_id,
                "step_name": "transform",
                "tail_lines": 3,
            },
        )
        assert result["lines"] == ["line 8", "line 9", "line 10"]
        assert result["truncated"] is True

    def test_successful_step_has_no_failure_log(
        self, make_app, invoke, seeded_run
    ) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        result = invoke(
            app,
            "artisan_get_step_logs",
            {"pipeline_run_id": seeded_run.run_id, "step_name": "generate"},
        )
        assert result["lines"] == []
        assert result["truncated"] is False
