"""Payload bounds and integrity when reading original staged Parquet files."""

from __future__ import annotations

import gc
import weakref
from collections import Counter
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.storage.io import staged_frames
from artisan.storage.io.staged_frames import StagedFile, StagedTable
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest

METRICS = "artifacts/metrics"


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


def _source(path: Path, frame: pl.DataFrame) -> StagedFile:
    frame.write_parquet(path)
    data = path.read_bytes()
    return StagedFile(
        fs=LocalFileSystem(),
        path=str(path),
        size_bytes=len(data),
        digest=compute_content_digest(data),
    )


def test_staged_table_digest_is_order_independent_and_deduplicates_shards(
    tmp_path: Path,
) -> None:
    frame = _metrics()
    first = StagedTable(METRICS, frame.schema)
    for index, shard in enumerate(frame.iter_slices(2)):
        first.add(shard, _source(tmp_path / f"shard-{index}.parquet", shard))
    first.add(frame.head(1), _source(tmp_path / "duplicate.parquet", frame.head(1)))
    reordered = frame.reverse()
    second = StagedTable(METRICS, frame.schema)
    second.add(reordered, _source(tmp_path / "reordered.parquet", reordered))
    logical_commit_id = "input_registration:" + "a" * 32
    assert first.plan_key(logical_commit_id) == second.plan_key(logical_commit_id)
    assert first.collect().sort("artifact_id").equals(frame.sort("artifact_id"))
    changed = frame.head(1).with_columns(pl.lit(b"changed").alias("content"))
    with pytest.raises(StoreIntegrityError, match="Conflicting natural keys"):
        first.add(changed, _source(tmp_path / "conflicting.parquet", changed))


def test_staged_table_digest_covers_payload_but_excludes_global_origin(
    tmp_path: Path,
) -> None:
    frames = (
        _metrics(),
        _metrics().with_columns(pl.lit(4, dtype=pl.Int32).alias("origin_step_number")),
        _metrics().with_columns(pl.lit(b"changed").alias("content")),
    )
    digests = []
    for index, frame in enumerate(frames):
        table = StagedTable(METRICS, frame.schema)
        table.add(frame, _source(tmp_path / f"rows-{index}.parquet", frame))
        digests.append(table.plan_key("test"))
    assert digests[0] == digests[1]
    assert digests[0] != digests[2]


def test_staged_table_legacy_digest_preserves_original_canonical_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    frame = _metrics()
    table = StagedTable(METRICS, frame.schema)
    source = _source(tmp_path / "metrics.parquet", frame)
    table.add(frame, source)
    rows = frame.sort("artifact_id").drop("origin_step_number").to_dicts()
    for row in rows:
        row["content"] = {"$binary": row["content"].hex()}
    expected = compute_content_digest(
        canonical_json_bytes(
            {"logical_commit_id": "test", "table_path": METRICS, "rows": rows}
        )
    )
    original_open = source.fs.open
    opens: list[str] = []

    def open_file(path: str, *args: Any, **kwargs: Any) -> Any:
        opens.append(str(path))
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(source.fs, "open", open_file)
    assert table.plan_key("test", digest_version=1) == expected
    assert opens == [source.path]


@pytest.mark.parametrize("byte_limit", [1, 12000, 1000000])
def test_staged_table_bounds_batches_and_releases_source_frames(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, byte_limit: int
) -> None:
    monkeypatch.setattr(staged_frames, "COMMIT_BATCH_ROWS", 3)
    monkeypatch.setattr(staged_frames, "COMMIT_BATCH_BYTES", byte_limit)
    frame = _metrics()
    expected_keys = frame["artifact_id"].to_list()
    reference = weakref.ref(frame)
    table = StagedTable(METRICS, frame.schema)
    table.add(frame, _source(tmp_path / "metrics.parquet", frame))
    del frame
    gc.collect()
    assert reference() is None
    keys = []
    for batch in table.batches():
        assert batch.height <= 3
        assert batch.height == 1 or batch.estimated_size() <= byte_limit
        keys.extend(batch["artifact_id"].to_list())
    assert keys == expected_keys


@pytest.mark.parametrize("selected", [False, True])
def test_staged_table_opens_each_source_once_for_all_output_batches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, selected: bool
) -> None:
    monkeypatch.setattr(staged_frames, "COMMIT_BATCH_ROWS", 1)
    frame = _metrics(20)
    table = StagedTable(METRICS, frame.schema)
    sources = []
    for index, shard in enumerate(frame.iter_slices(5)):
        source = _source(tmp_path / f"shard-{index}.parquet", shard)
        table.add(shard, source)
        sources.append(source)
    fs = sources[0].fs
    original_open = fs.open
    opens: Counter[str] = Counter()

    def open_file(path: str, *args: Any, **kwargs: Any) -> Any:
        opens[str(path)] += 1
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(fs, "open", open_file)
    chosen = {(value,) for value in frame["artifact_id"][::2]} if selected else None
    batches = list(table.batches(chosen))
    assert sum(batch.height for batch in batches) == (10 if selected else 20)
    assert opens == Counter({source.path: 1 for source in sources})


@pytest.mark.parametrize("damage", ["changed", "missing"])
def test_staged_table_rejects_source_changes_after_preparation(
    tmp_path: Path, damage: str
) -> None:
    frame = _metrics()
    path = tmp_path / "metrics.parquet"
    table = StagedTable(METRICS, frame.schema)
    table.add(frame, _source(path, frame))
    if damage == "changed":
        frame.with_columns(pl.lit(b"changed").alias("content")).write_parquet(path)
    else:
        path.unlink()
    with pytest.raises(StoreIntegrityError):
        list(table.batches())


@pytest.mark.parametrize("selected", [False, True])
def test_staged_table_coalesces_small_sources_without_reading_unselected_files(
    tmp_path: Path, selected: bool
) -> None:
    frame = _metrics(15)
    table = StagedTable(METRICS, frame.schema)
    chosen = {(value,) for value in frame["artifact_id"][::3]} if selected else None
    for index, shard in enumerate(frame.iter_slices(1)):
        path = tmp_path / f"shard-{index}.parquet"
        table.add(shard, _source(path, shard))
        if chosen is not None and (shard["artifact_id"][0],) not in chosen:
            path.unlink()

    batches = list(table.batches(chosen))

    assert len(batches) == 1
    expected = frame[::3] if selected else frame
    assert batches[0].equals(expected)


def test_staged_table_empty_table_has_stable_digest() -> None:
    first = StagedTable(METRICS, pl.Schema(MetricArtifact.POLARS_SCHEMA))
    second = StagedTable(METRICS, pl.Schema(MetricArtifact.POLARS_SCHEMA))
    assert list(first.batches()) == []
    assert first.collect().schema == MetricArtifact.POLARS_SCHEMA
    assert first.plan_key("test") == second.plan_key("test")
    assert first.plan_key("test") != first.plan_key("another")
