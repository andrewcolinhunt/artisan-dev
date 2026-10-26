"""Bound persisted-payload reads during commits and retained-staging cleanup."""

from __future__ import annotations

from typing import Any

import polars as pl
import pytest
from fsspec import AbstractFileSystem

from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.storage.core.committed_scan import read_committed
from artisan.storage.io import staged_frames
from artisan.storage.io.commit import DeltaCommitter
from artisan.storage.io.commit_plan import CommitPlan, build_commit_plan
from artisan.storage.io.staged_frames import StagedTable
from artisan.storage.io.staging import StagingManager

METRICS = "artifacts/metrics"


@pytest.fixture
def bounded_commit_env(
    backend_fs: tuple[AbstractFileSystem, StorageConfig, str],
) -> tuple[DeltaCommitter, AbstractFileSystem]:
    fs, storage, root = backend_fs
    return (
        DeltaCommitter(
            root,
            StagingManager(f"{root}/staging", fs),
            fs=fs,
            storage_options=storage.delta_storage_options(),
        ),
        fs,
    )


def _stage_registration(committer: DeltaCommitter, count: int) -> CommitPlan:
    metrics = pl.DataFrame(
        [
            {
                "artifact_id": f"{number:032x}",
                "origin_step_number": 0,
                "content": b"x" * 4096,
                "original_name": "metric.json",
                "extension": ".json",
                "metadata": "{}",
            }
            for number in range(count)
        ],
        schema=MetricArtifact.POLARS_SCHEMA,
    )
    index = metrics.select(
        "artifact_id",
        pl.lit("metric").alias("artifact_type"),
        "origin_step_number",
        "metadata",
    )
    owner = {
        "commit_kind": "input_registration",
        "step_run_id": "b" * 32,
        "step_number": 0,
        "operation_name": "ingest",
    }
    for table_path, frame in ((METRICS, metrics), (TablePath.ARTIFACT_INDEX, index)):
        committer.staging_manager.stage_orchestrator_dataframe(
            frame, table_path, **owner
        )
    return build_commit_plan(
        delta_root=committer.delta_base_path,
        staging_root=committer.staging_manager.staging_dir,
        fs=committer._fs,
        **owner,
    )


@pytest.mark.parametrize("staging_state", ["intact", "missing", "changed"])
@pytest.mark.parametrize("byte_limit", [1024, 12 * 1024])
def test_completed_cleanup_bounds_payloads_without_requiring_staged_evidence(
    bounded_commit_env: tuple[DeltaCommitter, AbstractFileSystem],
    monkeypatch: pytest.MonkeyPatch,
    staging_state: str,
    byte_limit: int,
) -> None:
    committer, fs = bounded_commit_env
    monkeypatch.setattr(staged_frames, "COMMIT_BATCH_BYTES", byte_limit)
    plan = _stage_registration(committer, 8)
    committer.commit_logical(plan, preserve_staging=True)
    staged_file = plan.table(METRICS).files[0]
    staged_path = f"{committer.staging_manager.staging_dir}/{staged_file.relative_path}"
    if staging_state == "missing":
        fs.rm(staged_path)
    elif staging_state == "changed":
        fs.pipe(staged_path, b"changed staging evidence")
    observed: list[int] = []
    original_add = StagedTable.add
    original_collect = StagedTable.collect

    def add(snapshot: StagedTable, frame: pl.DataFrame) -> None:
        if snapshot.table_path == METRICS:
            observed.append(frame.height)
            assert frame.height == 1 or frame.estimated_size() <= byte_limit
        original_add(snapshot, frame)

    def collect(snapshot: StagedTable) -> pl.DataFrame:
        assert snapshot.table_path != METRICS, "Cleanup collected all artifact payloads"
        return original_collect(snapshot)

    monkeypatch.setattr(StagedTable, "add", add)
    monkeypatch.setattr(StagedTable, "collect", collect)
    assert committer.commit_logical(plan) == {}
    assert sum(observed) >= 8
    assert fs.exists(staged_path) == (staging_state == "changed")
    if staging_state == "changed":
        assert fs.cat(staged_path) == b"changed staging evidence"
    assert (
        read_committed(
            committer.delta_base_path,
            METRICS,
            fs=fs,
            storage_options=committer._storage_options,
        )["content"].to_list()
        == [b"x" * 4096] * 8
    )


@pytest.mark.parametrize("artifact_count", [3, 25])
def test_commit_scans_do_not_grow_with_payload_batch_count(
    bounded_commit_env: tuple[DeltaCommitter, AbstractFileSystem],
    monkeypatch: pytest.MonkeyPatch,
    artifact_count: int,
) -> None:
    committer, _fs = bounded_commit_env
    monkeypatch.setattr(staged_frames, "COMMIT_BATCH_ROWS", 2)
    plan = _stage_registration(committer, artifact_count)
    original_scan = pl.scan_delta
    scans: list[str] = []
    metric_path = f"{committer.delta_base_path}/{METRICS}"

    def scan(source: Any, *args: Any, **kwargs: Any) -> pl.LazyFrame:
        if source == metric_path:
            scans.append(metric_path)
        return original_scan(source, *args, **kwargs)

    monkeypatch.setattr(pl, "scan_delta", scan)
    committer.commit_logical(plan, preserve_staging=True)
    assert 0 < len(scans) <= 6
    scans.clear()
    committer.commit_logical(plan)
    assert 0 < len(scans) <= 3
