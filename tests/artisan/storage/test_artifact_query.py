"""Public metadata references use accepted content and exact current membership."""

from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.retained_inspection import commit_inspection_attempt, inspection_store

from artisan.schemas import CSVArtifact, MetricArtifact
from artisan.storage.core.artifact_query import ArtifactRef, query_artifacts


def test_global_reference_filters_keep_metadata_without_payload(tmp_path: Path) -> None:
    csv = CSVArtifact.draft(b"x\n1\n", "data.csv", 1, metadata={"note": "x"}).finalize()
    metric = MetricArtifact.draft({"score": 2}, "score.json", 2).finalize()
    commit_inspection_attempt(tmp_path, number=1, outputs={"data": [csv]})
    commit_inspection_attempt(tmp_path, number=2, outputs={"metric": [metric]})
    refs = query_artifacts(str(tmp_path))
    assert [ref.artifact_id for ref in refs] == [csv.artifact_id, metric.artifact_id]
    assert all(isinstance(ref, ArtifactRef) for ref in refs)
    assert refs[0].metadata == {"note": "x"}
    assert all("content" not in ref.model_dump() for ref in refs)
    assert all(
        ref.pipeline_run_id is None and ref.current_step_number is None for ref in refs
    )
    assert [
        ref.artifact_id
        for ref in query_artifacts(str(tmp_path), artifact_type="metric")
    ] == [metric.artifact_id]


def test_exact_run_membership_keeps_original_origin_for_reuse(tmp_path: Path) -> None:
    source = CSVArtifact.draft(b"x\n1\n", "source.csv", 1).finalize()
    other = CSVArtifact.draft(b"x\n2\n", "other.csv", 7).finalize()
    _, execution = commit_inspection_attempt(
        tmp_path, number=1, outputs={"data": [source]}
    )
    commit_inspection_attempt(
        tmp_path,
        run_id="current",
        number=7,
        outputs={"data": [source]},
        reused_execution_id=execution,
    )
    commit_inspection_attempt(
        tmp_path, run_id="other", number=7, outputs={"data": [other]}
    )
    refs = query_artifacts(str(tmp_path), pipeline_run_id="current")
    assert len(refs) == 1
    assert (
        refs[0].artifact_id,
        refs[0].origin_step_number,
        refs[0].current_step_number,
        refs[0].pipeline_run_id,
    ) == (source.artifact_id, 1, 7, "current")
    assert query_artifacts(str(tmp_path), pipeline_run_id="unknown") == []


@pytest.mark.parametrize(
    "status", ["pending", "running", "failed", "cancelled", "skipped"]
)
def test_latest_unusable_attempt_never_exposes_old_success(
    tmp_path: Path, status: str
) -> None:
    artifact = MetricArtifact.draft({"score": 1}, "score.json", 0).finalize()
    commit_inspection_attempt(tmp_path, outputs={"metric": [artifact]})
    commit_inspection_attempt(tmp_path, outputs={"metric": []}, status=status)
    assert query_artifacts(str(tmp_path), pipeline_run_id="source-run") == []
    assert [ref.artifact_id for ref in query_artifacts(str(tmp_path))] == [
        artifact.artifact_id
    ]


def test_empty_supported_store_has_no_references(tmp_path: Path) -> None:
    inspection_store(tmp_path)
    assert query_artifacts(str(tmp_path)) == []
