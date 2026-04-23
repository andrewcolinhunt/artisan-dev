"""Smoke test: DeltaCommitter round-trip against MinIO.

Validates the PR 3 fixture chain (`minio_endpoint` → `s3_fs`) and
PR 1's `StorageConfig.delta_options` plumbing together. Also asserts
per-op latency is reasonable — a regression here usually means the
EC2 IMDS probe is firing (3 s timeout per op) because
`AWS_EC2_METADATA_DISABLED` isn't taking effect.
"""

from __future__ import annotations

import time

import polars as pl
import pytest

from artisan.storage.io.commit import DeltaCommitter
from artisan.storage.io.staging import StagingManager


@pytest.mark.integration
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
    init_elapsed = time.perf_counter() - init_start

    df = pl.DataFrame(
        {
            "artifact_id": ["smoke-1", "smoke-2"],
            "origin_step_number": [0, 0],
            "payload": ["alpha", "beta"],
        }
    )

    write_start = time.perf_counter()
    rows = committer.commit_dataframe(df, "smoke")
    write_elapsed = time.perf_counter() - write_start

    assert rows == 2
    assert init_elapsed < 0.5, (
        f"DeltaCommitter init took {init_elapsed:.2f}s — likely IMDS probe; "
        f"check AWS_EC2_METADATA_DISABLED is set in the test process."
    )
    assert write_elapsed < 5.0, (
        f"first commit_dataframe PUT took {write_elapsed:.2f}s — too slow"
    )

    # Read back and verify
    table_uri = f"{delta_root}/smoke"
    read_back = pl.read_delta(
        table_uri, storage_options=storage.delta_storage_options()
    )
    assert read_back.height == 2
    assert set(read_back["artifact_id"].to_list()) == {"smoke-1", "smoke-2"}
