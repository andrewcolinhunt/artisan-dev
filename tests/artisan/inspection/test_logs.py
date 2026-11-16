"""Unavailable failure log references never become synthetic filesystem paths."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from artisan.inspection.logs import logs_payload


def test_null_failure_log_is_skipped_without_hiding_available_logs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from artisan.orchestration import run_status
    from artisan.visualization import inspect

    root = tmp_path / "store"
    log = tmp_path / "logs" / "failures" / "known.log"
    log.parent.mkdir(parents=True)
    log.write_text("observed failure\n")
    monkeypatch.setattr(run_status, "resolve_step_number", lambda *args, **kwargs: 1)
    monkeypatch.setattr(
        inspect,
        "inspect_failures",
        lambda *args, **kwargs: pl.DataFrame(
            {"step": [1, 1], "log": [None, "known.log"]}
        ),
    )
    result = logs_payload(str(root), "run", "step")
    assert result["lines"] == ["observed failure"]
    assert result["truncated"] is False
