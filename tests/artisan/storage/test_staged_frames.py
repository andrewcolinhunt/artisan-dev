"""Payload bounds and digest compatibility for temporary staged snapshots."""

from __future__ import annotations

import gc
import weakref

import polars as pl
import pytest

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.storage.io import staged_frames
from artisan.storage.io.staged_frames import StagedTable
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest


def _metrics(count: int = 7) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "artifact_id": f"{number:032x}",
                "origin_step_number": 3,
                "content": bytes([number]) * 4096,
                "original_name": "résultat.json",
                "extension": ".json",
                "metadata": None,
            }
            for number in reversed(range(count))
        ],
        schema=MetricArtifact.POLARS_SCHEMA,
    )


def test_snapshot_preserves_existing_digest_and_deduplicates_across_shards() -> None:
    frame = _metrics()
    snapshot = StagedTable("artifacts/metrics", frame.schema)
    for shard in frame.iter_slices(2):
        snapshot.add(shard)
    snapshot.add(frame.head(1))
    rows = []
    for row in (
        frame.sort("artifact_id").drop("origin_step_number").iter_rows(named=True)
    ):
        row["content"] = {"$binary": row["content"].hex()}
        rows.append(row)
    expected = compute_content_digest(
        canonical_json_bytes(
            {
                "logical_commit_id": "input_registration:" + "a" * 32,
                "table_path": "artifacts/metrics",
                "rows": rows,
            }
        )
    )
    assert snapshot.plan_key("input_registration:" + "a" * 32) == expected
    assert snapshot.collect().equals(frame.sort("artifact_id"))
    with pytest.raises(StoreIntegrityError, match="Conflicting natural keys"):
        snapshot.add(frame.head(1).with_columns(pl.lit(b"changed").alias("content")))


@pytest.mark.parametrize("byte_limit", [1, 12000, 1000000])
def test_snapshot_bounds_batches_and_releases_source_frames(
    monkeypatch, byte_limit: int
) -> None:
    monkeypatch.setattr(staged_frames, "COMMIT_BATCH_ROWS", 3)
    monkeypatch.setattr(staged_frames, "COMMIT_BATCH_BYTES", byte_limit)
    frame = _metrics()
    expected_keys = frame.sort("artifact_id")["artifact_id"].to_list()
    serialized_row_size = len(frame.head(1).serialize())
    source = weakref.ref(frame)
    snapshot = StagedTable("artifacts/metrics", frame.schema)
    snapshot.add(frame)
    del frame
    gc.collect()
    assert source() is None
    keys = []
    for batch in snapshot.batches():
        assert batch.height <= 3
        assert batch.height == 1 or batch.height * serialized_row_size <= byte_limit
        keys.extend(batch["artifact_id"].to_list())
    assert keys == expected_keys
    stream = snapshot._stream
    del snapshot
    gc.collect()
    assert stream.closed


def test_snapshot_empty_table_has_existing_empty_digest() -> None:
    snapshot = StagedTable("artifacts/metrics", pl.Schema(MetricArtifact.POLARS_SCHEMA))
    assert list(snapshot.batches()) == []
    assert snapshot.collect().schema == MetricArtifact.POLARS_SCHEMA
    assert snapshot.plan_key("test") == compute_content_digest(
        canonical_json_bytes(
            {"logical_commit_id": "test", "table_path": "artifacts/metrics", "rows": []}
        )
    )
