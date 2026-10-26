"""Bound persisted-payload reads during commits and retained-staging cleanup."""

from __future__ import annotations

import tempfile
from collections import Counter
from collections.abc import Iterator
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
from artisan.storage.io.commit_plan import (
    CommitPlan,
    PlannedTable,
    build_commit_plan,
    commit_plan_path,
    read_commit_plan,
)
from artisan.storage.io.staged_frames import StagedTable
from artisan.storage.io.staging import StagingManager
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest

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


def _legacy_registration_plan(
    committer: DeltaCommitter, plan: CommitPlan
) -> CommitPlan:
    payload = plan.model_dump(mode="json", exclude={"plan_digest"})
    for table in payload["tables"]:
        table.pop("digest_version")
        path = f"{committer.staging_manager.staging_dir}/{table['files'][0]['relative_path']}"
        with committer._fs.open(path, "rb") as stream:
            frame = (
                pl.read_parquet(stream)
                .sort(table["natural_key"])
                .drop("origin_step_number")
            )
        rows = frame.to_dicts()
        for row in rows:
            if "content" in row:
                row["content"] = {"$binary": row["content"].hex()}
        table["table_plan_key"] = compute_content_digest(
            canonical_json_bytes(
                {
                    "logical_commit_id": plan.logical_commit_id,
                    "table_path": table["table_path"],
                    "rows": rows,
                }
            )
        )
    payload["plan_digest"] = compute_content_digest(canonical_json_bytes(payload))
    committer._fs.pipe(
        commit_plan_path(committer.delta_base_path, plan.step_run_id, plan.commit_kind),
        canonical_json_bytes(payload),
    )
    return read_commit_plan(
        committer.delta_base_path, committer._fs, plan.step_run_id, plan.commit_kind
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
    original_matching = committer._iter_matching_rows
    original_collect = StagedTable.collect

    def matching(table: PlannedTable, keys: pl.DataFrame) -> Iterator[pl.DataFrame]:
        for frame in original_matching(table, keys):
            if table.table_path == METRICS:
                observed.append(frame.height)
                assert frame.height == 1 or frame.estimated_size() <= byte_limit
            yield frame

    def collect(table: StagedTable) -> pl.DataFrame:
        assert table.table_path != METRICS, "Cleanup collected all artifact payloads"
        return original_collect(table)

    monkeypatch.setattr(committer, "_iter_matching_rows", matching)
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


def test_commit_and_completed_cleanup_do_not_create_temporary_snapshots(
    bounded_commit_env: tuple[DeltaCommitter, AbstractFileSystem],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    committer, _fs = bounded_commit_env

    def temporary_file(*args: Any, **kwargs: Any) -> None:
        pytest.fail("Commit created a temporary payload snapshot")

    monkeypatch.setattr(tempfile, "TemporaryFile", temporary_file)
    plan = _stage_registration(committer, 8)
    prepared = committer.prepare_logical(plan)
    committer.commit_logical(plan, prepared=prepared, preserve_staging=True)
    assert committer.commit_logical(plan) == {}


@pytest.mark.parametrize("digest_version", [1, 2])
def test_commit_and_cleanup_preserve_legacy_and_new_visible_content(
    bounded_commit_env: tuple[DeltaCommitter, AbstractFileSystem],
    monkeypatch: pytest.MonkeyPatch,
    digest_version: int,
) -> None:
    committer, fs = bounded_commit_env

    def temporary_file(*args: Any, **kwargs: Any) -> None:
        pytest.fail("Commit created a temporary payload snapshot")

    monkeypatch.setattr(tempfile, "TemporaryFile", temporary_file)
    plan = _stage_registration(committer, 8)
    if digest_version == 1:
        plan = _legacy_registration_plan(committer, plan)
    assert all(table.digest_version == digest_version for table in plan.tables)
    committer.commit_logical(plan, preserve_staging=True)
    before = read_committed(
        committer.delta_base_path,
        METRICS,
        fs=fs,
        storage_options=committer._storage_options,
    ).sort("artifact_id")
    assert before["content"].to_list() == [b"x" * 4096] * 8
    metric_path = f"{committer.staging_manager.staging_dir}/{plan.table(METRICS).files[0].relative_path}"
    index_path = f"{committer.staging_manager.staging_dir}/{plan.table(TablePath.ARTIFACT_INDEX.value).files[0].relative_path}"
    fs.rm(metric_path)
    assert fs.exists(index_path)

    assert committer.commit_logical(plan) == {}

    assert not fs.exists(index_path)
    assert (
        read_committed(
            committer.delta_base_path,
            METRICS,
            fs=fs,
            storage_options=committer._storage_options,
        )
        .sort("artifact_id")
        .equals(before)
    )


@pytest.mark.parametrize("artifact_count", [3, 25])
def test_staged_file_reads_do_not_grow_with_payload_batch_count(
    bounded_commit_env: tuple[DeltaCommitter, AbstractFileSystem],
    monkeypatch: pytest.MonkeyPatch,
    artifact_count: int,
) -> None:
    committer, fs = bounded_commit_env
    monkeypatch.setattr(staged_frames, "COMMIT_BATCH_ROWS", 1)
    plan = _stage_registration(committer, artifact_count)
    staged_paths = {
        f"{committer.staging_manager.staging_dir}/{file.relative_path}"
        for table in plan.tables
        for file in table.files
    }
    original_open = fs.open
    opens: Counter[str] = Counter()

    def open_file(path: str, mode: str = "rb", *args: Any, **kwargs: Any) -> Any:
        if str(path) in staged_paths and "r" in mode:
            opens[str(path)] += 1
        return original_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(fs, "open", open_file)
    committer.commit_logical(plan, preserve_staging=True)
    assert set(opens) == staged_paths
    assert all(count <= 4 for count in opens.values())
