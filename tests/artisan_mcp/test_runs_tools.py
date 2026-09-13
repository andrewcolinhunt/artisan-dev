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
        assert data["has_more"] is False
        assert data["next_cursor"] is None

    def test_runs_list_resource_is_bounded(
        self, make_app, read_resource, monkeypatch, tmp_path
    ) -> None:
        import json
        from datetime import UTC, datetime

        import polars as pl

        from artisan.orchestration import run_history

        rows = {
            "pipeline_run_id": [f"run-{index}" for index in range(101)],
            "step_count": [1] * 101,
            "last_status": ["completed"] * 101,
            "started_at": [datetime(2026, 1, 1, tzinfo=UTC)] * 101,
            "ended_at": [datetime(2026, 1, 1, tzinfo=UTC)] * 101,
        }
        monkeypatch.setattr(run_history, "list_runs", lambda _root: pl.DataFrame(rows))

        content = read_resource(make_app(delta_root=tmp_path), "artisan://runs")
        data = json.loads(content.text)

        assert len(data["items"]) == 100
        assert data["has_more"] is True
        assert data["next_cursor"] == "100"

    def test_run_detail_resource(self, make_app, read_resource, seeded_run) -> None:
        import json

        app = make_app(delta_root=seeded_run.delta_root)
        content = read_resource(app, f"artisan://runs/{seeded_run.run_id}")
        status = json.loads(content.text)
        assert status["pipeline_run_id"] == seeded_run.run_id
        assert len(status["steps"]) == 2
        assert status["steps_truncated"] is False

    def test_run_detail_resource_bounds_steps(
        self, make_app, read_resource, monkeypatch, tmp_path
    ) -> None:
        import importlib
        import json
        from types import SimpleNamespace

        module = importlib.import_module("artisan.orchestration.run_status")
        payload = {
            "pipeline_run_id": "large-run",
            "last_status": "completed",
            "step_count": 101,
            "started_at": None,
            "ended_at": None,
            "steps": [{"step_number": index} for index in range(101)],
        }
        monkeypatch.setattr(
            module,
            "run_status",
            lambda _root, _run_id: SimpleNamespace(model_dump=lambda: payload),
        )

        content = read_resource(
            make_app(delta_root=tmp_path), "artisan://runs/large-run"
        )
        status = json.loads(content.text)

        assert len(status["steps"]) == 100
        assert status["steps_truncated"] is True


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

    def test_rejects_out_of_range_tail(self, make_app, invoke, seeded_run) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        for tail_lines in (0, -1, 1001):
            result = invoke(
                app,
                "artisan_get_step_logs",
                {
                    "pipeline_run_id": seeded_run.run_id,
                    "step_name": "transform",
                    "tail_lines": tail_lines,
                },
            )
            assert result["code"] == "param_type_mismatch"
            assert result["field"] == "tail_lines"

    def test_large_single_line_is_byte_bounded(
        self, make_app, invoke, seeded_run
    ) -> None:
        path = (
            seeded_run.delta_root.parent
            / "logs"
            / "failures"
            / "step_2_transform"
            / f"{seeded_run.exec_id}.log"
        )
        path.write_bytes(b"x" * (300 * 1024))

        result = invoke(
            make_app(delta_root=seeded_run.delta_root),
            "artisan_get_step_logs",
            {
                "pipeline_run_id": seeded_run.run_id,
                "step_name": "transform",
                "tail_lines": 1,
            },
        )

        assert len(result["lines"]) == 1
        assert len(result["lines"][0].encode()) <= 256 * 1024
        assert result["truncated"] is True
