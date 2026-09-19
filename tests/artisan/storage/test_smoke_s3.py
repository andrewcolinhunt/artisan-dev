"""Smoke test: DeltaCommitter round-trip against MinIO.

Exercise MinIO fixtures and `StorageConfig.delta_storage_options` together.
Check latency to catch slow EC2 instance-metadata probes when
`AWS_EC2_METADATA_DISABLED` is not taking effect.
"""

from __future__ import annotations

import time

import polars as pl

from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import ARTIFACT_INDEX_SCHEMA
from artisan.storage.io.commit import DeltaCommitter
from artisan.storage.io.commit_plan import build_commit_plan
from artisan.storage.io.staging import StagingManager


def test_delta_commit_roundtrip_on_minio(s3_fs):
    """End-to-end DeltaCommitter round-trip against per-test MinIO bucket."""
    fs, storage, uri_prefix = s3_fs

    delta_root = f"{uri_prefix}/delta"
    staging_root = f"{uri_prefix}/staging"

    # Per-op latency probe — bucket already created in fixture; here we
    # measure the DeltaCommitter init + first PUT. Both must finish well
    # under the IMDS-timeout wall (3 s per probe, one per op normally).
    init_start = time.perf_counter()
    staging_manager = StagingManager(staging_root, fs)
    committer = DeltaCommitter(
        delta_root,
        staging_manager,
        fs=fs,
        storage_options=storage.delta_storage_options(),
    )
    committer.initialize_tables()
    init_elapsed = time.perf_counter() - init_start

    df = pl.DataFrame(
        {
            "artifact_id": ["a" * 32, "b" * 32],
            "artifact_type": ["data", "metric"],
            "origin_step_number": [0, 0],
            "metadata": ["{}", "{}"],
        },
        schema=ARTIFACT_INDEX_SCHEMA,
    )

    write_start = time.perf_counter()
    step_run_id = "c" * 32
    staging_manager.stage_orchestrator_dataframe(
        df,
        TablePath.ARTIFACT_INDEX.value,
        commit_kind="input_registration",
        step_run_id=step_run_id,
        step_number=0,
        operation_name="smoke_input_registration",
    )
    plan = build_commit_plan(
        delta_root=delta_root,
        staging_root=staging_root,
        fs=fs,
        commit_kind="input_registration",
        step_run_id=step_run_id,
        step_number=0,
        operation_name="smoke_input_registration",
    )
    rows = committer.commit_logical(plan)
    write_elapsed = time.perf_counter() - write_start

    assert rows == {"index": 2}
    assert init_elapsed < 0.5, (
        f"DeltaCommitter init took {init_elapsed:.2f}s — likely IMDS probe; "
        f"check AWS_EC2_METADATA_DISABLED is set in the test process."
    )
    assert write_elapsed < 5.0, (
        f"first logical commit PUT took {write_elapsed:.2f}s — too slow"
    )

    table_uri = f"{delta_root}/{TablePath.ARTIFACT_INDEX.value}"
    read_back = pl.read_delta(
        table_uri, storage_options=storage.delta_storage_options()
    )
    assert read_back.height == 2
    assert set(read_back["artifact_id"].to_list()) == {"a" * 32, "b" * 32}
