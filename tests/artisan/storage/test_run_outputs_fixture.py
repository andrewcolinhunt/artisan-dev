"""Shared output fixtures preserve real acceptance and exact ordered membership."""

from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.retained_inspection import commit_inspection_attempt, inspection_store
from fixtures.run_outputs import commit_outputs
from fsspec.implementations.memory import MemoryFileSystem

from artisan.orchestration.engine.current_outputs import read_current_step_outputs
from artisan.schemas import MetricArtifact
from artisan.storage.core.artifact_query import query_artifacts


def test_fixture_preserves_duplicate_outputs_and_original_reused_producer(
    tmp_path: Path,
) -> None:
    metric = MetricArtifact.draft({"value": 1}, "metric.json", 0).finalize()
    step, delivery = commit_outputs(
        str(tmp_path),
        artifacts=[metric],
        output_ids={"metrics": [metric.artifact_id] * 2},
        status="partial",
    )
    committer = inspection_store(tmp_path)
    roles = read_current_step_outputs(committer, step)
    assert [item.artifact.artifact_id for item in roles[0].occurrences] == [
        metric.artifact_id
    ] * 2
    assert {item.reference.delivery_id for item in roles[0].occurrences} == {delivery}
    reused_step, reused_delivery = commit_outputs(
        str(tmp_path),
        run_id="reused",
        number=1,
        output_ids={"metrics": [metric.artifact_id] * 2},
        reused_execution_id=delivery,
    )
    assert reused_delivery == delivery
    assert read_current_step_outputs(committer, reused_step) == roles


def test_fixture_failed_and_pending_attempts_never_publish_outputs(
    tmp_path: Path,
) -> None:
    metric = MetricArtifact.draft({"value": 1}, "metric.json", 0).finalize()
    for run_id, status in (("failed", "failed"), ("pending", "pending")):
        commit_outputs(str(tmp_path), run_id=run_id, artifacts=[metric], status=status)
        assert query_artifacts(str(tmp_path), pipeline_run_id=run_id) == []
    assert query_artifacts(str(tmp_path)) == []


def test_fixture_rejects_unknown_ids_and_wrong_store_binding(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="No supplied or accepted"):
        commit_outputs(str(tmp_path), output_ids={"data": ["f" * 32]})
    committer = inspection_store(tmp_path)
    with pytest.raises(ValueError, match="another Delta root"):
        commit_inspection_attempt(str(tmp_path / "other"), committer=committer)
    with pytest.raises(ValueError, match="explicit files root"):
        commit_outputs("memory://store", fs=MemoryFileSystem())
