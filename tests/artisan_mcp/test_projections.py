"""Resources preserve portable shared continuations."""

from __future__ import annotations

import importlib
import json

import polars as pl


def test_resource_continuation_is_usable_by_tool(
    make_app, read_resource, invoke, monkeypatch, tmp_path
):
    module = importlib.import_module("artisan.orchestration.run_history")
    rows = [
        {
            "pipeline_run_id": f"run-{index:03d}",
            "started_at": None,
            "ended_at": None,
            "last_status": None,
            "step_count": 0,
        }
        for index in range(27)
    ]
    monkeypatch.setattr(module, "list_runs", lambda _root: pl.DataFrame(rows))
    app = make_app(delta_root=tmp_path)
    first = json.loads(read_resource(app, "artisan://runs").text)
    follow = first["follow_up"]
    second = invoke(app, "artisan_" + follow["capability"], follow["arguments"])
    assert first["view_id"] == second["view_id"]
    assert first["items"] + second["items"] == rows
