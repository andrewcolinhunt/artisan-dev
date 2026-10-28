"""Payload bounds and exact snapshots for sequential persistence I/O."""

from __future__ import annotations

import errno
import io
from contextlib import contextmanager
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import ARTIFACT_INDEX_SCHEMA
from artisan.storage.io import bounded_io, commit_plan
from artisan.storage.io.bounded_io import (
    capture_parquet,
    iter_parquet_frames,
    row_batches,
    value_size,
    write_parquet_rows,
)
from artisan.storage.io.staging import StagingManager
from artisan.utils.hashing import (
    STREAM_CHUNK_BYTES,
    compute_content_digest,
    compute_stream_digest,
)


def test_stream_snapshot_copy_never_requests_whole_source():
    payload = b"x" * (2 * STREAM_CHUNK_BYTES + 7)

    class BoundedReader(io.BytesIO):
        def read(self, size: int = -1) -> bytes:
            assert 0 < size <= STREAM_CHUNK_BYTES
            return super().read(size)

    output = io.BytesIO()
    digest, size = compute_stream_digest(BoundedReader(payload), destination=output)
    assert output.getvalue() == payload
    assert digest == compute_content_digest(payload)
    assert size == len(payload)


def test_row_batches_enforces_bytes_and_handles_one_oversized_row():
    rows = [{"payload": b"x" * size} for size in [8, 40, 40, 512, 8]]
    batches = list(row_batches(iter(rows), max_bytes=128, max_rows=4))
    assert [row for batch in batches for row in batch] == rows
    assert all(
        sum(value_size(row) for row in batch) <= 128 or len(batch) == 1
        for batch in batches
    )
    assert [rows[3]] in batches


@pytest.mark.parametrize("limits", [{"max_bytes": 0}, {"max_rows": 0}])
def test_row_batches_rejects_nonpositive_limits(limits):
    with pytest.raises(ValueError, match="positive"):
        list(row_batches([], **limits))


def test_write_parquet_rows_uses_multiple_bounded_groups(tmp_path):
    path = str(tmp_path / "rows.parquet")
    rows = [{"id": i, "payload": b"x" * (100 if i != 3 else 4000)} for i in range(9)]
    schema = {"id": pl.Int32, "payload": pl.Binary}
    assert write_parquet_rows(path, iter(rows), schema, max_bytes=512) == 9
    with pq.ParquetFile(path) as parquet:
        assert parquet.num_row_groups > 1
        assert all(
            "RLE_DICTIONARY" not in parquet.metadata.row_group(i).column(1).encodings
            for i in range(parquet.num_row_groups)
        )
    found = pl.concat(iter_parquet_frames(path, max_bytes=512))
    assert found.equals(pl.DataFrame(rows, schema=schema))
    assert found.schema == schema


def test_iter_parquet_frames_limits_dictionary_encoded_legacy_groups(tmp_path):
    path = tmp_path / "legacy.parquet"
    payload = "x" * (256 * 1024)
    pq.write_table(pa.table({"payload": [payload] * 20}), path, use_dictionary=True)
    frames = list(iter_parquet_frames(str(path), max_bytes=1024 * 1024))
    assert len(frames) == 20
    assert all(frame.height == 1 for frame in frames)
    assert frames[-1]["payload"][0] == payload


def test_iter_parquet_frames_projects_and_selects_row_groups(tmp_path):
    path = str(tmp_path / "groups.parquet")
    write_parquet_rows(
        path,
        ({"id": i, "payload": b"x" * 100} for i in range(4)),
        {"id": pl.Int64, "payload": pl.Binary},
        max_bytes=1,
    )
    frames = list(iter_parquet_frames(path, columns=["id"], row_groups=[2]))
    assert pl.concat(frames).to_dicts() == [{"id": 2}]


@pytest.mark.parametrize("nested", ["list", "struct"])
@pytest.mark.parametrize("dictionary", [False, True])
def test_iter_parquet_frames_limits_unprojected_nested_groups(
    tmp_path, nested, dictionary
):
    path = tmp_path / "nested.parquet"
    payload = "x" * (256 * 1024)
    value = [payload] if nested == "list" else {"value": payload}
    pq.write_table(pa.table({"nested": [value] * 20}), path, use_dictionary=dictionary)
    frames = list(iter_parquet_frames(str(path), max_bytes=1024 * 1024))
    assert len(frames) == 20
    assert all(frame.height == 1 for frame in frames)
    assert pl.concat(frames).height == 20


def test_capture_parquet_keeps_private_bytes_after_original_changes(tmp_path):
    path = str(tmp_path / "original.parquet")
    copy = str(tmp_path / "snapshot.parquet")
    schema = {"id": pl.Int64, "payload": pl.Binary}
    write_parquet_rows(path, [{"id": 1, "payload": b"original"}], schema)
    original = (tmp_path / "original.parquet").read_bytes()
    captured = capture_parquet(path, LocalFileSystem(), copy)
    write_parquet_rows(path, [{"id": 2, "payload": b"changed"}], schema)
    assert captured.digest == compute_content_digest(original)
    assert captured.size_bytes == len(original)
    assert captured.row_count == 1
    assert captured.schema == schema
    assert pl.concat(iter_parquet_frames(copy)).to_dicts() == [
        {"id": 1, "payload": b"original"}
    ]


def _exhaust_scratch(monkeypatch):
    attempted = []

    @contextmanager
    def full_destination(path, mode):
        attempted.append(Path(path))
        with open(path, mode) as target:

            class ExhaustedWriter:
                def write(self, data):
                    target.write(data[:16])
                    raise OSError(errno.ENOSPC, "No space left on device")

            yield ExhaustedWriter()

    monkeypatch.setattr(bounded_io, "open", full_destination, raising=False)
    return attempted


def test_capture_disk_full_preserves_original_and_reports_scratch_failure(
    tmp_path, monkeypatch
):
    source = tmp_path / "source.parquet"
    write_parquet_rows(str(source), [{"id": 1}], {"id": pl.Int64})
    original = source.read_bytes()
    attempted = _exhaust_scratch(monkeypatch)
    with pytest.raises(StoreIntegrityError, match="failed scratch capture") as caught:
        capture_parquet(str(source), LocalFileSystem(), str(tmp_path / "copy.parquet"))
    assert caught.value.__cause__.errno == errno.ENOSPC
    assert source.read_bytes() == original
    assert attempted[0].read_bytes() == original[:16]


def test_preparation_disk_full_removes_private_scratch_and_preserves_staging(
    backend_fs, monkeypatch
):
    fs, _storage, root = backend_fs
    staging_root = f"{root}/staging"
    staging = StagingManager(staging_root, fs)
    source = staging.stage_orchestrator_dataframe(
        pl.DataFrame(
            {
                "artifact_id": ["a" * 32],
                "artifact_type": ["csv"],
                "origin_step_number": [0],
                "metadata": ["{}"],
            },
            schema=ARTIFACT_INDEX_SCHEMA,
        ),
        TablePath.ARTIFACT_INDEX.value,
        step_run_id="b" * 32,
        step_number=0,
        operation_name="disk_full",
        commit_kind="input_registration",
    )
    original = fs.cat_file(source)
    attempted = _exhaust_scratch(monkeypatch)
    monkeypatch.setattr(
        commit_plan,
        "publish_commit_plan",
        lambda *_args: pytest.fail("A failed preparation cannot publish its plan"),
    )
    with pytest.raises(StoreIntegrityError, match="failed scratch capture") as caught:
        commit_plan.build_commit_plan(
            delta_root=root,
            staging_root=staging_root,
            fs=fs,
            commit_kind="input_registration",
            step_run_id="b" * 32,
            step_number=0,
            operation_name="disk_full",
        )
    assert caught.value.__cause__.errno == errno.ENOSPC
    assert attempted
    assert all(not path.parent.exists() for path in attempted)
    assert fs.cat_file(source) == original
    assert not fs.exists(
        commit_plan.commit_plan_path(root, "b" * 32, "input_registration")
    )


def test_write_parquet_rows_empty_file_retains_schema(tmp_path):
    path = str(tmp_path / "empty.parquet")
    schema = {"id": pl.Int64, "payload": pl.Binary}
    assert write_parquet_rows(path, [], schema) == 0
    assert list(iter_parquet_frames(path)) == []
    assert (
        capture_parquet(path, LocalFileSystem(), str(tmp_path / "copy.parquet")).schema
        == schema
    )
