"""Bounded Delta effects preserve one atomic transaction and exact schemas."""

from __future__ import annotations

import errno
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from pathlib import Path
from threading import Event

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from deltalake import DeltaTable
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.storage.io import delta_files
from artisan.storage.io.delta_files import DeltaSnapshot, append_table_files
from artisan.utils.hashing import STREAM_CHUNK_BYTES

SCHEMA = {"id": pl.String, "payload": pl.Binary, "origin_step_number": pl.Int64}


def _table(root, fs, options=None, *, partitioned=True):
    path = f"{root}/bounded"
    DeltaTable.create(
        path,
        delta_files._arrow_schema(SCHEMA),
        partition_by=["origin_step_number"] if partitioned else [],
        storage_options=options or {},
    )
    return DeltaSnapshot(path, SCHEMA, fs=fs, storage_options=options)


def _frames(count=5):
    for index in range(count):
        yield pl.DataFrame(
            {
                "id": [f"item-{index}"],
                "payload": [bytes([index]) * 128],
                "origin_step_number": [7],
            },
            schema=SCHEMA,
        )


@pytest.mark.parametrize("partitioned", [False, True])
def test_append_multiple_files_has_exactly_one_version(backend_fs, partitioned):
    fs, storage, root = backend_fs
    options = storage.delta_storage_options()
    snapshot = _table(root, fs, options, partitioned=partitioned)
    assert (
        append_table_files(
            snapshot,
            _frames(),
            step_number=7,
            statistics_columns=["id"],
            target_bytes=1,
        )
        == 5
    )
    current = DeltaSnapshot(snapshot.path, SCHEMA, fs=fs, storage_options=options)
    assert current.delta.version() == 1
    assert len(current.files) == 5
    assert pl.concat(current.iter_frames()).sort("id").equals(pl.concat(_frames()))
    assert current.delta.to_pyarrow_table().num_rows == 5
    assert (
        list(snapshot.iter_frames()) == []
    )  # A snapshot stays at its original version.
    for item in current.files:
        with (
            fs.open(f"{current.path}/{item.path}", "rb") as stream,
            pq.ParquetFile(stream) as parquet,
        ):
            assert (
                "origin_step_number" not in parquet.schema_arrow.names
            ) == partitioned
    if partitioned:
        partition = pl.concat(current.iter_frames(["origin_step_number"]))
        assert partition.to_dict(as_series=False) == {"origin_step_number": [7] * 5}


def test_failed_input_never_registers_completed_files(backend_fs):
    fs, storage, root = backend_fs
    snapshot = _table(root, fs, storage.delta_storage_options())

    def failed_frames():
        yield from _frames(2)
        msg = "source interrupted"
        raise OSError(msg)

    with pytest.raises(OSError, match="source interrupted"):
        append_table_files(
            snapshot,
            failed_frames(),
            step_number=7,
            statistics_columns=["id"],
            target_bytes=1,
        )
    assert (
        DeltaTable(
            snapshot.path, storage_options=storage.delta_storage_options()
        ).version()
        == 0
    )
    assert len(fs.glob(f"{snapshot.path}/origin_step_number=7/*.parquet")) == 2
    assert list(snapshot.iter_frames()) == []


def test_delta_scratch_disk_full_preserves_source_and_registers_no_files(
    backend_fs, tmp_path, monkeypatch
):
    fs, storage, root = backend_fs
    options = storage.delta_storage_options()
    snapshot = _table(root, fs, options)
    source = tmp_path / "staged-source.parquet"
    pl.concat(_frames(3)).write_parquet(source)
    original = source.read_bytes()
    write = pq.ParquetWriter.write_table
    calls = []

    def full_on_second_file(self, *args, **kwargs):
        calls.append(self)
        if len(calls) == 2:
            raise OSError(errno.ENOSPC, "No space left on device")
        return write(self, *args, **kwargs)

    monkeypatch.setattr(pq.ParquetWriter, "write_table", full_on_second_file)
    monkeypatch.setattr(
        snapshot.delta,
        "create_write_transaction",
        lambda *_args, **_kwargs: pytest.fail(
            "Disk exhaustion must not publish a transaction"
        ),
    )
    with pytest.raises(OSError) as caught:
        append_table_files(
            snapshot,
            [pl.read_parquet(source)],
            step_number=7,
            statistics_columns=["id"],
            target_bytes=1,
        )
    assert caught.value.errno == errno.ENOSPC
    assert len(calls) == 2
    assert source.read_bytes() == original
    assert len(fs.glob(f"{snapshot.path}/origin_step_number=7/*.parquet")) == 1
    current = DeltaSnapshot(snapshot.path, SCHEMA, fs=fs, storage_options=options)
    assert current.delta.version() == 0
    assert list(current.iter_frames()) == []


def test_throttled_s3_upload_blocks_next_file_and_bounds_active_buffer(
    s3_fs, monkeypatch
):
    from s3fs.core import S3File

    fs, storage, root = s3_fs
    options = storage.delta_storage_options()
    snapshot = _table(root, fs, options)
    entered, release, advanced = Event(), Event(), Event()
    upload = S3File._upload_chunk
    active = set()
    buffered = []
    produced = []

    def delayed_upload(self, final=False):
        active.add(self.path)
        try:
            assert len(active) == 1
            buffered.append(self.buffer.tell())
            if not entered.is_set():
                entered.set()
                assert release.wait(timeout=10), "Timed out releasing throttled upload"
            return upload(self, final=final)
        finally:
            active.remove(self.path)

    def frames():
        for index in range(3):
            produced.append(index)
            if index:
                advanced.set()
            yield pl.DataFrame(
                {
                    "id": [str(index)],
                    "payload": [os.urandom(6 * STREAM_CHUNK_BYTES)],
                    "origin_step_number": [7],
                },
                schema=SCHEMA,
            )

    monkeypatch.setattr(S3File, "_upload_chunk", delayed_upload)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            append_table_files,
            snapshot,
            frames(),
            step_number=7,
            statistics_columns=["id"],
            target_bytes=1,
        )
        try:
            assert entered.wait(timeout=10), "First upload never started"
            assert not advanced.wait(timeout=0.1)
            assert produced == [0]
            assert len(active) == 1
        finally:
            release.set()
        assert future.result(timeout=30) == 3
    assert not active
    assert max(buffered) <= 5 * STREAM_CHUNK_BYTES
    assert not fs.transaction.files
    current = DeltaSnapshot(snapshot.path, SCHEMA, fs=fs, storage_options=options)
    assert current.delta.version() == 1
    assert len(current.files) == 3


def test_completed_file_scratch_is_released_before_next_file(backend_fs, monkeypatch):
    fs, storage, root = backend_fs
    snapshot = _table(root, fs, storage.delta_storage_options())
    publish = delta_files.publish_immutable_file
    published = []

    def observe(destination_fs, path, source, **kwargs):
        local = Path(source)
        assert list(local.parent.glob("*.parquet")) == [local]
        publish(destination_fs, path, source, **kwargs)
        assert local.exists()  # Publication itself preserves its caller's source.
        published.append(local)

    commit = snapshot.delta.create_write_transaction

    def commit_after_release(*args, **kwargs):
        assert len(published) == 5
        assert all(not local.exists() for local in published)
        return commit(*args, **kwargs)

    monkeypatch.setattr(delta_files, "publish_immutable_file", observe)
    monkeypatch.setattr(
        snapshot.delta, "create_write_transaction", commit_after_release
    )
    assert (
        append_table_files(
            snapshot,
            _frames(),
            step_number=7,
            statistics_columns=["id"],
            target_bytes=1,
        )
        == 5
    )
    current = DeltaSnapshot(
        snapshot.path, SCHEMA, fs=fs, storage_options=storage.delta_storage_options()
    )
    assert current.delta.version() == 1
    assert pl.concat(current.iter_frames()).height == 5


def test_ambiguous_transaction_keeps_committed_files(backend_fs, monkeypatch):
    fs, storage, root = backend_fs
    options = storage.delta_storage_options()
    snapshot = _table(root, fs, options)
    commit = snapshot.delta.create_write_transaction

    def committed_then_lost(*args, **kwargs):
        commit(*args, **kwargs)
        msg = "lost commit acknowledgement"
        raise OSError(msg)

    monkeypatch.setattr(snapshot.delta, "create_write_transaction", committed_then_lost)
    with pytest.raises(OSError, match="lost commit acknowledgement"):
        append_table_files(
            snapshot,
            _frames(2),
            step_number=7,
            statistics_columns=["id"],
            target_bytes=1,
        )
    current = DeltaSnapshot(snapshot.path, SCHEMA, fs=fs, storage_options=options)
    assert current.delta.version() == 1
    assert pl.concat(current.iter_frames()).height == 2
    assert all(fs.exists(f"{current.path}/{item.path}") for item in current.files)


def test_projection_selects_only_needed_row_groups(backend_fs, monkeypatch):
    fs, storage, root = backend_fs
    options = storage.delta_storage_options()
    snapshot = _table(root, fs, options)
    append_table_files(snapshot, _frames(4), step_number=7, statistics_columns=["id"])
    current = DeltaSnapshot(snapshot.path, SCHEMA, fs=fs, storage_options=options)
    assert len(current.files) == 1
    reads = []
    iter_batches = pq.ParquetFile.iter_batches

    def observed(self, *args, **kwargs):
        reads.append(kwargs)
        return iter_batches(self, *args, **kwargs)

    monkeypatch.setattr(pq.ParquetFile, "iter_batches", observed)
    path = current.files[0].path
    located = list(
        current.iter_located(["id", "origin_step_number"], rows_by_file={path: {2}})
    )
    assert [
        (file, indices, frame["id"].to_list()) for file, indices, frame in located
    ] == [(path, [2], ["item-2"])]
    assert len(reads) == 1
    assert reads[0]["row_groups"] == [2]
    assert reads[0]["columns"] == ["id"]
    assert reads[0]["use_threads"] is False
    reads.clear()
    assert (
        pl.concat(
            current.iter_frames(["origin_step_number"], rows_by_file={path: {1, 3}})
        ).height
        == 2
    )
    assert reads == []


@pytest.mark.parametrize(
    "change",
    ["type", "nullability", "metadata", "nested_nullability", "nested_metadata"],
)
def test_snapshot_rejects_inexact_arrow_contract(tmp_path, change):
    schema = {"id": pl.String, "nested": pl.Struct({"value": pl.String})}
    fields = list(delta_files._arrow_schema(schema))
    if change == "type":
        fields[0] = pa.field("id", pa.int32())
    elif change == "nullability":
        fields[0] = pa.field("id", pa.string(), nullable=False)
    elif change == "metadata":
        fields[0] = pa.field("id", pa.string(), metadata={"comment": "restricted"})
    else:
        child = pa.field(
            "value",
            pa.string(),
            nullable=change != "nested_nullability",
            metadata={"comment": "restricted"} if change == "nested_metadata" else None,
        )
        fields[1] = pa.field("nested", pa.struct([child]))
    DeltaTable.create(str(tmp_path), pa.schema(fields))
    with pytest.raises(StoreIntegrityError, match="unexpected schema"):
        DeltaSnapshot(str(tmp_path), schema, fs=LocalFileSystem())


@pytest.mark.parametrize(
    "configuration",
    [{"delta.appendOnly": "true"}, {"delta.enableChangeDataFeed": "true"}],
)
def test_snapshot_rejects_unimplemented_delta_contract(tmp_path, configuration):
    DeltaTable.create(
        str(tmp_path), delta_files._arrow_schema(SCHEMA), configuration=configuration
    )
    with pytest.raises(StoreIntegrityError, match="Unsupported Delta protocol"):
        DeltaSnapshot(str(tmp_path), SCHEMA, fs=LocalFileSystem())


@pytest.mark.parametrize("physical_change", ["nullability", "metadata", "corrupt"])
def test_snapshot_rejects_bad_physical_file(backend_fs, physical_change):
    fs, storage, root = backend_fs
    options = storage.delta_storage_options()
    snapshot = _table(root, fs, options)
    append_table_files(snapshot, _frames(1), step_number=7, statistics_columns=["id"])
    current = DeltaSnapshot(snapshot.path, SCHEMA, fs=fs, storage_options=options)
    path = f"{current.path}/{current.files[0].path}"
    with fs.open(path, "wb") as stream:
        if physical_change == "corrupt":
            stream.write(b"bad footer")
        else:
            field = pa.field(
                "id",
                pa.string(),
                nullable=physical_change != "nullability",
                metadata={"comment": "wrong"}
                if physical_change == "metadata"
                else None,
            )
            table = pa.Table.from_pydict(
                {"id": ["item-0"], "payload": [b"data"]},
                schema=pa.schema([field, pa.field("payload", pa.binary())]),
            )
            pq.write_table(table, stream)
    with pytest.raises(
        StoreIntegrityError,
        match="Unreadable Delta data file|unexpected physical schema",
    ):
        list(current.iter_frames())


@pytest.mark.parametrize(
    "path",
    [
        "",
        "/outside.parquet",
        "../outside.parquet",
        "a/../outside.parquet",
        "a//b",
        "a\\b",
        "%2e%2e/outside",
        "a/%2Foutside",
        "s3://elsewhere/key",
    ],
)
def test_snapshot_rejects_unsafe_active_path(path):
    with pytest.raises(StoreIntegrityError, match="escapes"):
        delta_files._relative_path(path)


def test_append_rejects_wrong_frame_or_partition_before_transaction(tmp_path):
    fs = LocalFileSystem()
    snapshot = _table(str(tmp_path), fs)
    wrong_schema = next(_frames()).with_columns(
        pl.col("origin_step_number").cast(pl.Int32)
    )
    wrong_origin = next(_frames()).with_columns(
        pl.lit(8, dtype=pl.Int64).alias("origin_step_number")
    )
    for frame, message in [
        (wrong_schema, "unexpected schema"),
        (wrong_origin, "unexpected origin"),
    ]:
        with pytest.raises(StoreIntegrityError, match=message):
            append_table_files(
                snapshot, [frame], step_number=7, statistics_columns=["id"]
            )
    assert snapshot.delta.version() == 0
    assert not fs.glob(f"{snapshot.path}/origin_step_number=*/*.parquet")


def test_row_selection_rejects_invalid_ordinals_and_foreign_files(tmp_path):
    snapshot = _table(str(tmp_path), LocalFileSystem())
    append_table_files(snapshot, _frames(2), step_number=7, statistics_columns=["id"])
    current = DeltaSnapshot(snapshot.path, SCHEMA, fs=LocalFileSystem())
    for ordinals in [{-1}, {2}, {True}]:
        with pytest.raises(StoreIntegrityError, match="invalid file ordinal"):
            list(current.iter_frames(rows_by_file={current.files[0].path: ordinals}))
    with pytest.raises(StoreIntegrityError, match="outside this snapshot"):
        list(current.iter_frames(rows_by_file={"unknown.parquet": {0}}))


def test_snapshot_rejects_local_active_symlink_escape(tmp_path):
    snapshot = _table(str(tmp_path), LocalFileSystem())
    append_table_files(snapshot, _frames(1), step_number=7, statistics_columns=["id"])
    active = next((tmp_path / "bounded/origin_step_number=7").glob("*.parquet"))
    outside = tmp_path / "outside.parquet"
    active.rename(outside)
    active.symlink_to(outside)
    with pytest.raises(StoreIntegrityError, match="escapes"):
        DeltaSnapshot(snapshot.path, SCHEMA, fs=LocalFileSystem())


@pytest.mark.parametrize(
    "timestamp_type", [pl.Datetime("us"), pl.Datetime("us", "UTC")]
)
def test_nested_and_temporal_values_round_trip_with_delta_statistics(
    tmp_path, timestamp_type
):
    schema = {
        "id": pl.String,
        "when": timestamp_type,
        "day": pl.Date,
        "nested": pl.List(pl.Struct({"value": pl.String})),
        "payload": pl.Binary,
    }
    frame = pl.DataFrame(
        {
            "id": ["one", None],
            "when": [datetime(2024, 1, 2, 3, 4, 5, 678901, tzinfo=UTC), None],
            "day": [date(2024, 1, 2), None],
            "nested": [[{"value": "entry"}], []],
            "payload": [b"secret", None],
        },
        schema=schema,
    )
    DeltaTable.create(str(tmp_path), delta_files._arrow_schema(schema))
    snapshot = DeltaSnapshot(str(tmp_path), schema, fs=LocalFileSystem())
    append_table_files(
        snapshot,
        [frame],
        step_number=7,
        statistics_columns=["id", "when", "day", "nested", "payload"],
    )
    current = DeltaSnapshot(str(tmp_path), schema, fs=LocalFileSystem())
    assert pl.concat(current.iter_frames()).equals(frame)
    actions = [
        json.loads(line)
        for line in (tmp_path / "_delta_log/00000000000000000001.json")
        .read_text()
        .splitlines()
    ]
    stats = json.loads(
        next(action["add"]["stats"] for action in actions if "add" in action)
    )
    assert stats["numRecords"] == 2
    assert stats["minValues"] == {
        "id": "one",
        "when": "2024-01-02T03:04:05.678901Z",
        "day": "2024-01-02",
    }
    assert stats["nullCount"]["payload"] == 1
