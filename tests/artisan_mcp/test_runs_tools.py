"""Tests for artisan_list_runs, artisan_get_run_status, artisan_get_step_logs."""

from __future__ import annotations

import pytest
from fixtures.store_format import publish_test_store
from fsspec.implementations.local import LocalFileSystem


class TestListRuns:
    def test_lists_seeded_run(self, make_app, invoke, seeded_run) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        page = invoke(app, "artisan_list_runs")
        ids = [row["pipeline_run_id"] for row in page["items"]]
        assert seeded_run.run_id in ids
        assert page["has_more"] is False

    def test_empty_root_yields_empty_page(self, make_app, invoke, tmp_path) -> None:
        publish_test_store(str(tmp_path), LocalFileSystem())
        page = invoke(make_app(delta_root=tmp_path), "artisan_list_runs")
        assert page["items"] == []

    def test_unset_root_returns_envelope(self, make_app, invoke_error) -> None:
        env = invoke_error(make_app(delta_root=None), "artisan_list_runs")
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
        by_name = {s["name"]: s["status"] for s in status["steps"]["items"]}
        assert by_name == {"generate": "succeeded", "transform": "failed"}

    def test_unknown_run_is_empty(self, make_app, invoke, seeded_run) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        status = invoke(app, "artisan_get_run_status", {"pipeline_run_id": "nope"})
        assert status["steps"]["items"] == []
        assert status["last_status"] is None


class TestDiagnoseRun:
    def test_diagnoses_seeded_failure(self, make_app, invoke, seeded_run) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        diag = invoke(
            app, "artisan_diagnose_run", {"pipeline_run_id": seeded_run.run_id}
        )
        assert diag["last_status"] == "failed"
        assert diag["failed_execution_count"] == 1
        failures = invoke(
            app,
            "artisan_diagnose_run",
            {"pipeline_run_id": seeded_run.run_id, "view": "failures"},
        )
        assert [s["operation"] for s in failures["items"]] == ["transform"]
        assert failures["items"][0]["code"] == "op_execute_failed"
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
            "last_status": ["succeeded"] * 101,
            "started_at": [datetime(2026, 1, 1, tzinfo=UTC)] * 101,
            "ended_at": [datetime(2026, 1, 1, tzinfo=UTC)] * 101,
        }
        monkeypatch.setattr(run_history, "list_runs", lambda _root: pl.DataFrame(rows))

        content = read_resource(make_app(delta_root=tmp_path), "artisan://runs")
        data = json.loads(content.text)

        assert len(data["items"]) == 20
        assert data["has_more"] is True
        assert data["next_cursor"] is not None

    def test_run_detail_resource(self, make_app, read_resource, seeded_run) -> None:
        import json

        app = make_app(delta_root=seeded_run.delta_root)
        content = read_resource(app, f"artisan://runs/{seeded_run.run_id}")
        status = json.loads(content.text)
        assert status["pipeline_run_id"] == seeded_run.run_id
        assert len(status["steps"]["items"]) == 2
        assert status["steps"]["has_more"] is False

    def test_run_detail_resource_bounds_steps(
        self, make_app, read_resource, monkeypatch, tmp_path
    ) -> None:
        import importlib
        import json

        from artisan.orchestration.run_status import RunStatus

        module = importlib.import_module("artisan.orchestration.run_status")
        payload = {
            "pipeline_run_id": "large-run",
            "last_status": "succeeded",
            "step_count": 101,
            "started_at": None,
            "ended_at": None,
            "steps": [
                {
                    "step_number": index,
                    "name": f"step-{index}",
                    "status": "succeeded",
                    "produced": "1 csv",
                    "duration": "1s",
                }
                for index in range(101)
            ],
        }
        monkeypatch.setattr(
            module,
            "run_status",
            lambda _root, _run_id: RunStatus.model_validate(payload),
        )

        content = read_resource(
            make_app(delta_root=tmp_path), "artisan://runs/large-run"
        )
        status = json.loads(content.text)

        assert len(status["steps"]["items"]) == 50
        assert status["steps"]["has_more"] is True


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

    def test_rejects_out_of_range_tail(
        self, make_app, invoke_result, seeded_run
    ) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        for tail_lines in (0, -1, 1001):
            result = invoke_result(
                app,
                "artisan_get_step_logs",
                {
                    "pipeline_run_id": seeded_run.run_id,
                    "step_name": "transform",
                    "tail_lines": tail_lines,
                },
            )
            assert result.is_error is True
            assert result.structured_content is None

    def test_large_single_line_is_byte_bounded(
        self, make_app, invoke, seeded_run
    ) -> None:
        path = seeded_run.log_path
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


@pytest.mark.parametrize("escape", ["traversal", "symlink"])
def test_get_step_logs_withholds_outside_root_contents(
    make_app, invoke, seeded_run, tmp_path, monkeypatch, escape: str
) -> None:
    import importlib

    import polars as pl

    module = importlib.import_module("artisan.visualization.inspect")
    outside = tmp_path / "outside-secret.log"
    outside.write_text("outside secret contents")
    if escape == "traversal":
        relative = "../../outside-secret.log"
    else:
        link = tmp_path / "logs" / "failures" / "escape.log"
        link.symlink_to(outside)
        relative = "escape.log"
    monkeypatch.setattr(
        module,
        "inspect_failures",
        lambda *args, **kwargs: pl.DataFrame({"step": [2], "log": [relative]}),
    )
    result = invoke(
        make_app(delta_root=seeded_run.delta_root),
        "artisan_get_step_logs",
        {
            "pipeline_run_id": seeded_run.run_id,
            "step_name": "transform",
        },
    )
    assert result["lines"] == []
    assert "outside secret" not in str(result)


def test_get_step_logs_caps_files_in_source_recency_order(
    make_app, invoke, seeded_run, tmp_path, monkeypatch
) -> None:
    import importlib
    from datetime import UTC, datetime, timedelta

    import polars as pl

    from artisan.inspection.logs import MAX_LOG_FILES
    from artisan.utils.log_paths import failure_log_relative_path

    module = importlib.import_module("artisan.visualization.inspect")
    start = datetime(2026, 9, 19, tzinfo=UTC)
    rows = []
    for index in range(MAX_LOG_FILES + 3):
        # The final two starts tie; execution ID gives a stable order.
        timestamp = start + timedelta(seconds=min(index, MAX_LOG_FILES + 1))
        run_id = f"id-{index:03d}"
        relative = failure_log_relative_path(run_id, timestamp)
        path = tmp_path / "logs" / "failures" / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(run_id)
        rows.append(
            {
                "step": 2,
                "log": relative,
                "timestamp_start": timestamp,
                "execution_run_id": run_id,
            }
        )
    ordered = pl.DataFrame(list(reversed(rows))).sort(
        "timestamp_start", "execution_run_id", "step"
    )
    monkeypatch.setattr(module, "inspect_failures", lambda *args, **kwargs: ordered)
    result = invoke(
        make_app(delta_root=seeded_run.delta_root),
        "artisan_get_step_logs",
        {
            "pipeline_run_id": seeded_run.run_id,
            "step_name": "transform",
            "tail_lines": 1000,
        },
    )
    assert result["lines"] == [row["execution_run_id"] for row in rows[-MAX_LOG_FILES:]]
    assert result["truncated"] is True


def test_get_step_logs_caps_aggregate_bytes(
    make_app, invoke, seeded_run, tmp_path, monkeypatch
) -> None:
    import importlib

    import polars as pl

    from artisan.inspection.logs import MAX_LOG_BYTES

    module = importlib.import_module("artisan.visualization.inspect")
    paths = []
    for index in range(4):
        relative = f"part-{index}.log"
        (tmp_path / "logs" / "failures" / relative).write_text(
            str(index) * (MAX_LOG_BYTES // 2)
        )
        paths.append(relative)
    monkeypatch.setattr(
        module,
        "inspect_failures",
        lambda *args, **kwargs: pl.DataFrame({"step": [2] * 4, "log": paths}),
    )
    result = invoke(
        make_app(delta_root=seeded_run.delta_root),
        "artisan_get_step_logs",
        {
            "pipeline_run_id": seeded_run.run_id,
            "step_name": "transform",
            "tail_lines": 1000,
        },
    )
    content = "\n".join(result["lines"])
    assert len(content.encode()) <= MAX_LOG_BYTES
    assert "0" not in content
    assert "1" not in content
    assert "3" in content
    assert "2" not in content
    assert result["partial_first_line"] is True
    assert "bytes" in result["truncation_reasons"]
    assert result["truncated"] is True


@pytest.mark.parametrize(("byte_limit", "partial"), [(8, True), (5, False)])
def test_log_source_cut_distinguishes_line_boundary(
    tmp_path, monkeypatch, byte_limit, partial
):
    from artisan.inspection import logs

    path = tmp_path / "failure.log"
    path.write_bytes(b"oldprefix\nnew\n")
    monkeypatch.setattr(logs, "MAX_LOG_BYTES", byte_limit)
    result = logs._tail_logs([str(path)], 20)
    assert result["lines"][-1] == "new"
    assert result["partial_first_line"] is partial
    assert result["truncation_reasons"] == ["bytes"]


def test_log_dropped_partial_oldest_chunk_does_not_mark_newest_line(
    tmp_path, monkeypatch
):
    from artisan.inspection import logs

    older, newer = tmp_path / "old.log", tmp_path / "new.log"
    older.write_bytes(b"long old line\n")
    newer.write_bytes(b"new")
    monkeypatch.setattr(logs, "MAX_LOG_BYTES", 16)
    result = logs._tail_logs([str(older), str(newer)], 1)
    assert result["lines"] == ["new"]
    assert result["partial_first_line"] is False
    assert result["truncation_reasons"] == ["bytes", "lines"]


def test_log_invalid_utf8_source_cut_retains_replacement_policy(tmp_path, monkeypatch):
    from artisan.inspection import logs

    path = tmp_path / "failure.log"
    path.write_text("雪" * 5)
    monkeypatch.setattr(logs, "MAX_LOG_BYTES", 8)
    result = logs._tail_logs([str(path)], 20)
    assert result["lines"] == ["\ufffd雪雪"]
    assert result["partial_first_line"] is True
    assert result["truncation_reasons"] == ["bytes"]


def test_log_single_line_returns_largest_fitting_unicode_suffix(tmp_path, monkeypatch):
    from artisan.inspection import logs
    from artisan.inspection._evidence import encode_json

    original = '雪😀"\\' * 500
    path = tmp_path / "failure.log"
    path.write_text(original)
    monkeypatch.setattr(logs, "MAX_PAYLOAD_BYTES", 1_000)
    result = logs._tail_logs([str(path)], 20)
    suffix = result["lines"][0]
    assert original.endswith(suffix)
    assert len(encode_json(result)) <= 1_000
    previous = original[len(original) - len(suffix) - 1]
    assert len(encode_json({**result, "lines": [previous + suffix]})) > 1_000
    assert result["partial_first_line"] is True
    assert result["truncation_reasons"] == ["bytes"]
