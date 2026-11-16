"""Diagnosis preserves selection, coverage and ranking under one retained read."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fixtures.inspection_curator import commit_curator_record
from fixtures.retained_inspection import commit_inspection_attempt, inspection_store

from artisan.errors import ArtisanError, IncompatibleStoreError
from artisan.storage.io.retained_read import RetainedControlProof
from artisan.visualization import inspect
from artisan.visualization.inspect import (
    DiagnosisUpstreamScope,
    RunDiagnosis,
    _diagnosis,
    _upstream_from_seeds,
    diagnose_run,
)


@pytest.fixture
def failed_store(tmp_path: Path) -> Path:
    root = tmp_path / "delta"
    commit_inspection_attempt(root, run_id="run-0", number=1, status="failed")
    commit_inspection_attempt(root, run_id="run-1", number=1, name="generate")
    envelope = ArtisanError(
        "artifact_validation_failed",
        "bad row",
        error_type="validation",
        recovery_hint="CHECK_INPUT",
        field="params.x",
    ).to_dict()
    commit_curator_record(
        root, run_id="run-1", number=2, error="ValueError: bad row", envelope=envelope
    )
    return root


def test_failed_steps_status_similar_runs_and_actions(failed_store: Path) -> None:
    diag = diagnose_run(str(failed_store), "run-1")
    assert isinstance(diag, RunDiagnosis)
    assert diag.last_status == "failed"
    assert len(diag.failed_steps) == 1
    assert diag.failed_steps[0]["code"] == "artifact_validation_failed"
    assert {row["pipeline_run_id"] for row in diag.similar_runs} == {"run-0"}
    assert any("inputs and parameters" in action for action in diag.suggested_actions)


def test_failed_execution_does_not_publish_output_edges(failed_store: Path) -> None:
    diag = diagnose_run(str(failed_store), "run-1")
    assert diag.upstream_edges == []
    assert diag.upstream_scope.total_seed_count == 0
    assert diag.upstream_scope.traversal_truncated is False


def test_unlabeled_store_rejects_before_inspection(tmp_path: Path) -> None:
    with pytest.raises(IncompatibleStoreError):
        diagnose_run(str(tmp_path), "run-1")


@pytest.mark.parametrize("status", ["running", "failed"])
def test_steps_without_executions_preserve_status(tmp_path: Path, status: str) -> None:
    commit_inspection_attempt(
        tmp_path, run_id="run-1", status=status, failed_delivery=False
    )
    diag = diagnose_run(str(tmp_path), "run-1")
    assert diag.last_status == status
    assert diag.failed_steps == []
    assert diag.suggested_actions == []
    assert diag.upstream_scope.model_dump() == {
        "seed_limit": 10,
        "seed_count": 0,
        "total_seed_count": 0,
        "seed_selection_truncated": False,
        "depth": 2,
        "traversal_truncated": False,
    }


def test_similar_runs_sort_before_window_and_count_all_matches() -> None:
    time = datetime(2026, 7, 1, tzinfo=UTC)
    values = [
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
    rows = [
        dict(zip(("pipeline_run_id", "started_at", "last_status"), item, strict=True))
        for item in values
    ]
    diag = _diagnosis("current", [], rows, [], DiagnosisUpstreamScope())
    assert diag.last_status == "failed"
    assert diag.matching_failed_run_count == 7
    assert diag.similar_run_limit == 5
    assert [row["pipeline_run_id"] for row in diag.similar_runs] == [
        "newest",
        "new",
        "tie-a",
        "tie-m",
        "tie-z",
    ]


@pytest.mark.parametrize("output_count", [0, 1, 12])
def test_seed_selection_precedes_walk_and_reports_coverage(output_count: int) -> None:
    ids = [f"{index:032x}" for index in range(output_count)]
    adjacency = {item: ["z", "shared"] for item in ids}
    adjacency.update({"shared": ["a"], "a": ["b"]})
    edges, scope = _upstream_from_seeds([*reversed(ids), *ids[:1]], adjacency)
    assert scope.model_dump() == {
        "seed_limit": 10,
        "seed_count": min(output_count, 10),
        "total_seed_count": output_count,
        "seed_selection_truncated": output_count > 10,
        "depth": 2,
        "traversal_truncated": bool(output_count),
    }
    assert {
        row["target_artifact_id"] for row in edges if row["source_artifact_id"] == "z"
    } == set(ids[:10])
    assert edges.count(
        {"source_artifact_id": "a", "target_artifact_id": "shared"}
    ) == int(bool(output_count))


def test_suggested_actions_deduplicate_unknown_hints() -> None:
    failures = [
        {"execution_run_id": "failed", "recovery_hint": hint}
        for hint in ("unknown-a", "unknown-b", "CHECK_INPUT")
    ]
    result = _diagnosis("run", failures, [], [], DiagnosisUpstreamScope())
    assert result.suggested_actions == [
        "Check the operation's inputs and parameters against its schema.",
        "Read the failure log for the full error and traceback.",
    ]


def test_diagnosis_uses_one_proof_and_captured_lifecycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    commit_inspection_attempt(tmp_path, run_id="run", status="failed")
    committer = inspection_store(tmp_path)
    monkeypatch.setattr(
        inspect, "open_configured_store", lambda *args, **kwargs: committer
    )
    prove = committer._control_proof
    count = 0

    def counted() -> RetainedControlProof:
        nonlocal count
        count += 1
        return prove()

    monkeypatch.setattr(committer, "_control_proof", counted)
    acquire = inspect._failure_upstream_edges
    changed = False

    def insert_retry(*args, **kwargs):
        nonlocal changed
        if not changed:
            changed = True
            commit_inspection_attempt(tmp_path, run_id="run", status="pending")
        return acquire(*args, **kwargs)

    monkeypatch.setattr(inspect, "_failure_upstream_edges", insert_retry)
    before = diagnose_run(str(tmp_path), "run")
    assert count == 1
    assert before.last_status == "failed"
    assert len(before.failed_steps) == 1
    after = diagnose_run(str(tmp_path), "run")
    assert count == 2
    assert after.last_status == "pending"
    assert after.failed_steps == []
