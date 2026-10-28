"""Tests for the diagnose_run composite reader."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

import polars as pl
import pytest
from fixtures.logical_commit_store import commit_test_step

from artisan.errors import IncompatibleStoreError
from artisan.provenance.reader import ProvenanceEdges
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.storage.core.table_schemas import (
    ARTIFACT_EDGES_SCHEMA,
    ARTIFACT_INDEX_SCHEMA,
    EXECUTION_EDGES_SCHEMA,
    EXECUTIONS_SCHEMA,
)
from artisan.visualization.inspect import RunDiagnosis, diagnose_run

A, B = "a" * 32, "b" * 32


def _step_rows(
    run_id: str, number: int, name: str, status: str, minute: int
) -> list[dict]:
    timestamp = datetime(2026, 7, 1, tzinfo=UTC) + timedelta(minutes=minute)
    pending = {
        "step_run_id": f"{run_id}-step-{number}",
        "step_spec_id": None,
        "pipeline_run_id": run_id,
        "step_number": number,
        "step_name": name,
        "status": "pending",
        "state_sequence": 0,
        "disposition": None,
        "cancellation_status": None,
        "operation_class": "DataGenerator",
        "params_json": "{}",
        "input_refs_json": "{}",
        "compute_backend": "local",
        "compute_options_json": "{}",
        "output_roles_json": "[]",
        "output_types_json": "{}",
        "total_count": None,
        "succeeded_count": None,
        "failed_count": None,
        "timestamp": timestamp - timedelta(microseconds=2),
        "duration_seconds": None,
        "error": None,
        "metadata": None,
    }
    running = {
        **pending,
        "status": "running",
        "state_sequence": 1,
        "timestamp": timestamp - timedelta(microseconds=1),
    }
    if status == "running":
        return [pending, running]
    terminal = {
        **running,
        "step_spec_id": f"spec-{number}",
        "status": status,
        "state_sequence": 2,
        "disposition": "executed" if status == "succeeded" else None,
        "total_count": 1,
        "succeeded_count": 1 if status == "succeeded" else 0,
        "failed_count": 0 if status == "succeeded" else 1,
        "timestamp": timestamp,
        "duration_seconds": 1.0,
        "error": "step failed" if status == "failed" else None,
        "metadata": "{}",
    }
    return [pending, running, terminal]


@pytest.fixture
def failed_store(tmp_path: Path) -> Path:
    """Seed run-1 (a failed transform) plus an older failed run-0."""
    envelope = {
        "error_type": "validation",
        "code": "artifact_validation_failed",
        "message": "bad row",
        "recovery_hint": "CHECK_INPUT",
        "field": "params.x",
        "suggestions": [],
    }
    exec_row = dict.fromkeys(EXECUTIONS_SCHEMA)
    exec_row.update(
        execution_run_id="exec-2",
        execution_spec_id="espec",
        step_run_id="run-1-step-2",
        origin_step_number=2,
        operation_name="transform",
        params="{}",
        user_overrides="{}",
        timestamp_start=datetime(2026, 7, 1, tzinfo=UTC),
        source_worker=0,
        compute_backend="local",
        success=False,
        error="ValueError: bad row",
        error_envelope=json.dumps(envelope),
        tool_output="",
        worker_log="",
        metadata="{}",
    )
    execution_edges = pl.DataFrame(
        {
            "execution_run_id": ["exec-2"],
            "direction": ["output"],
            "role": ["data"],
            "artifact_id": [B],
        },
        schema=EXECUTION_EDGES_SCHEMA,
    )

    artifact_index = pl.DataFrame(
        {
            "artifact_id": [B],
            "artifact_type": ["csv"],
            "origin_step_number": [2],
            "metadata": ["{}"],
        },
        schema=ARTIFACT_INDEX_SCHEMA,
    )

    artifact_edges = pl.DataFrame(
        {
            "execution_run_id": ["exec-2"],
            "source_artifact_id": [A],
            "target_artifact_id": [B],
            "source_artifact_type": ["csv"],
            "target_artifact_type": ["csv"],
            "source_role": ["input"],
            "target_role": ["output"],
            "group_id": [None],
            "step_boundary": [True],
        },
        schema=ARTIFACT_EDGES_SCHEMA,
    )
    staging_root = str(tmp_path / "staging")
    for run_id, number, name, status, minute in (
        ("run-0", 1, "generate", "failed", 0),
        ("run-1", 1, "generate", "succeeded", 10),
    ):
        commit_test_step(
            tmp_path,
            staging_root,
            _step_rows(run_id, number, name, status, minute),
            {},
        )
    commit_test_step(
        tmp_path,
        staging_root,
        _step_rows("run-1", 2, "transform", "failed", 20),
        {
            TablePath.EXECUTIONS.value: pl.DataFrame(
                [exec_row], schema=EXECUTIONS_SCHEMA
            ),
            TablePath.EXECUTION_EDGES.value: execution_edges,
            TablePath.ARTIFACT_INDEX.value: artifact_index,
            TablePath.ARTIFACT_EDGES.value: artifact_edges,
        },
    )
    return tmp_path


class TestDiagnoseRun:
    def test_failed_steps_and_status(self, failed_store) -> None:
        diag = diagnose_run(str(failed_store), "run-1")
        assert isinstance(diag, RunDiagnosis)
        assert diag.last_status == "failed"
        assert len(diag.failed_steps) == 1
        assert diag.failed_steps[0]["code"] == "artifact_validation_failed"

    def test_similar_runs_excludes_self(self, failed_store) -> None:
        diag = diagnose_run(str(failed_store), "run-1")
        ids = {r["pipeline_run_id"] for r in diag.similar_runs}
        assert ids == {"run-0"}

    def test_suggested_actions_from_recovery_hint(self, failed_store) -> None:
        diag = diagnose_run(str(failed_store), "run-1")
        assert any("inputs and parameters" in a for a in diag.suggested_actions)

    def test_upstream_edges_walked(self, failed_store) -> None:
        diag = diagnose_run(str(failed_store), "run-1")
        assert {"source_artifact_id": A, "target_artifact_id": B} in diag.upstream_edges

    def test_missing_executions_raises(self, tmp_path) -> None:
        """A root without the current-format contract fails before inspection."""
        with pytest.raises(IncompatibleStoreError):
            diagnose_run(str(tmp_path), "run-1")

    def test_steps_but_no_executions_degrades(self, tmp_path) -> None:
        """A real store with no executions recorded yields an empty diagnosis."""
        steps = _step_rows("run-1", 1, "generate", "running", 0)
        commit_test_step(
            tmp_path,
            str(tmp_path / "staging"),
            steps,
            {},
        )

        diag = diagnose_run(str(tmp_path), "run-1")
        assert isinstance(diag, RunDiagnosis)
        assert diag.failed_steps == []
        assert diag.last_status == "running"
        assert diag.suggested_actions == []
        assert diag.upstream_scope.model_dump() == {
            "seed_limit": 10,
            "seed_count": 0,
            "total_seed_count": 0,
            "seed_selection_truncated": False,
            "depth": 2,
            "traversal_truncated": False,
        }


def test_similar_runs_sort_before_window_and_count_all_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The five-row window uses deterministic recency, not reader row order."""
    from artisan.orchestration import run_history
    from artisan.visualization import inspect

    time = datetime(2026, 7, 1, tzinfo=UTC)
    rows = [
        ("old", time - timedelta(days=1), "failed"),
        ("tie-z", time, "failed"),
        ("unknown", None, "failed"),
        ("current", time + timedelta(days=4), "failed"),
        ("new", time + timedelta(days=1), "failed"),
        ("tie-a", time, "failed"),
        ("success", time + timedelta(days=3), "succeeded"),
        ("newest", time + timedelta(days=2), "failed"),
        ("tie-m", time, "failed"),
    ]
    runs = pl.DataFrame(
        rows,
        schema=["pipeline_run_id", "started_at", "last_status"],
        orient="row",
    )
    monkeypatch.setattr(run_history, "list_runs", lambda *args, **kwargs: runs)
    monkeypatch.setattr(
        inspect, "inspect_failures", lambda *args, **kwargs: pl.DataFrame()
    )

    diagnosis = diagnose_run("unused", "current")

    assert diagnosis.last_status == "failed"
    assert diagnosis.matching_failed_run_count == 7
    assert diagnosis.similar_run_limit == 5
    assert [row["pipeline_run_id"] for row in diagnosis.similar_runs] == [
        "newest",
        "new",
        "tie-a",
        "tie-m",
        "tie-z",
    ]


@pytest.fixture
def failed_reader_rows(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, str]]:
    """Isolate diagnosis aggregation while leaving seed filtering intact."""
    from artisan.orchestration import run_history
    from artisan.visualization import inspect

    failures = [
        {"execution_run_id": "failed-execution", "recovery_hint": "unknown-a"},
        {"execution_run_id": "failed-execution", "recovery_hint": "unknown-b"},
        {"execution_run_id": "failed-execution", "recovery_hint": "CHECK_INPUT"},
    ]
    monkeypatch.setattr(
        inspect, "inspect_failures", lambda *args, **kwargs: pl.DataFrame(failures)
    )
    monkeypatch.setattr(
        run_history, "list_runs", lambda *args, **kwargs: pl.DataFrame()
    )
    return failures


def test_missing_seed_table_has_unknown_coverage(
    tmp_path: Path, failed_reader_rows: list[dict[str, str]]
) -> None:
    diagnosis = diagnose_run(str(tmp_path), "run")

    assert diagnosis.failed_steps == failed_reader_rows
    assert diagnosis.upstream_edges == []
    assert diagnosis.upstream_scope.seed_count == 0
    assert diagnosis.upstream_scope.total_seed_count is None
    assert diagnosis.upstream_scope.seed_selection_truncated is None
    assert diagnosis.upstream_scope.traversal_truncated is None


@pytest.mark.parametrize("output_count", [0, 1, 12])
def test_seed_selection_precedes_walk_and_reports_coverage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failed_reader_rows: list[dict[str, str]],
    output_count: int,
) -> None:
    """Only distinct failed outputs seed walks; truncation survives later walks."""
    import artisan.provenance
    from artisan.visualization import inspect

    (tmp_path / TablePath.EXECUTION_EDGES.value).mkdir(parents=True)
    seed_ids = [f"{index:032x}" for index in range(output_count)]
    rows = [
        {
            "execution_run_id": "failed-execution",
            "direction": "output",
            "role": "data",
            "artifact_id": artifact_id,
        }
        for artifact_id in reversed(seed_ids)
    ]
    rows += rows[:1]
    rows += [
        {
            "execution_run_id": execution,
            "direction": direction,
            "role": "data",
            "artifact_id": f"excluded-{direction}",
        }
        for execution, direction in [
            ("failed-execution", "input"),
            ("successful-execution", "output"),
        ]
    ]
    frame = pl.DataFrame(rows, schema=EXECUTION_EDGES_SCHEMA)
    monkeypatch.setattr(inspect, "scan_committed", lambda *args, **kwargs: frame.lazy())
    walked: list[str] = []

    def walk(
        root: str,
        artifact_id: str,
        *,
        direction: Literal["backward", "forward"],
        depth: int,
        storage: StorageConfig,
    ) -> ProvenanceEdges:
        assert root == str(tmp_path)
        assert direction == "backward"
        assert depth == 2
        walked.append(artifact_id)
        return ProvenanceEdges(
            artifact_id=artifact_id,
            direction=direction,
            depth=depth,
            edges=[
                {"source_artifact_id": "z", "target_artifact_id": artifact_id},
                {"source_artifact_id": "a", "target_artifact_id": "shared"},
            ],
            truncated=artifact_id == seed_ids[0],
        )

    monkeypatch.setattr(artisan.provenance, "provenance_edges", walk)

    diagnosis = diagnose_run(str(tmp_path), "run")

    assert walked == seed_ids[:10]
    assert diagnosis.upstream_scope.model_dump() == {
        "seed_limit": 10,
        "seed_count": min(output_count, 10),
        "total_seed_count": output_count,
        "seed_selection_truncated": output_count > 10,
        "depth": 2,
        "traversal_truncated": bool(output_count),
    }
    expected = [
        {"source_artifact_id": "a", "target_artifact_id": "shared"},
        *[
            {"source_artifact_id": "z", "target_artifact_id": artifact_id}
            for artifact_id in seed_ids[:10]
        ],
    ]
    assert diagnosis.upstream_edges == (expected if output_count else [])


def test_suggested_actions_deduplicate_unknown_hints(
    tmp_path: Path, failed_reader_rows: list[dict[str, str]]
) -> None:
    diagnosis = diagnose_run(str(tmp_path), "run")

    assert diagnosis.suggested_actions == [
        "Check the operation's inputs and parameters against its schema.",
        "Read the failure log for the full error and traceback.",
    ]


def test_failed_step_without_failed_execution_remains_visible(tmp_path: Path) -> None:
    commit_test_step(
        tmp_path,
        str(tmp_path / "staging"),
        _step_rows("failed-run", 1, "generate", "failed", 0),
        {},
    )

    diagnosis = diagnose_run(str(tmp_path), "failed-run")

    assert diagnosis.last_status == "failed"
    assert diagnosis.failed_steps == []
    assert diagnosis.upstream_edges == []
    assert diagnosis.upstream_scope.total_seed_count == 0
    assert diagnosis.upstream_scope.traversal_truncated is False
