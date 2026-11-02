"""Worker serialization limits row groups and reads bounded log chunks."""

from __future__ import annotations

import builtins

import polars as pl
import pyarrow.parquet as pq
from fsspec.implementations.local import LocalFileSystem

from artisan.execution.recording import parquet_writer, recorder
from artisan.schemas import CSVArtifact


def test_tool_output_reads_chunks_and_preserves_unicode_tail(tmp_path, monkeypatch):
    path = tmp_path / "tool.log"
    text = "é🙂line\n" * 100_000
    path.write_text(text)
    calls = []
    real_open = builtins.open

    class Reader:
        def __init__(self, *args, **kwargs):
            self.stream = real_open(*args, **kwargs)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.stream.close()

        def read(self, size=-1):
            assert 0 < size <= 64 * 1024
            calls.append(size)
            return self.stream.read(size)

    monkeypatch.setattr(recorder, "open", Reader, raising=False)
    result = recorder._read_tool_output(str(path))
    assert result == "[truncated]\n" + text[-500_000:]
    assert len(calls) > 1


def test_worker_content_uses_one_file_with_bounded_row_groups(tmp_path, monkeypatch):
    artifacts = [
        CSVArtifact.draft(
            b"value\n" + bytes(str(i), "ascii") * (64 * 1024),
            f"row{i}.csv",
            step_number=1,
        ).finalize()
        for i in range(6)
    ]
    write = parquet_writer.write_parquet_rows

    def small_batches(path, rows, schema, **kwargs):
        return write(path, rows, schema, max_bytes=128 * 1024, **kwargs)

    monkeypatch.setattr(parquet_writer, "write_parquet_rows", small_batches)
    parquet_writer._stage_artifacts_by_type(
        {"csv": artifacts}, str(tmp_path), LocalFileSystem()
    )
    path = tmp_path / "csvs.parquet"
    assert list(tmp_path.glob("*.parquet")) == [path]
    with pq.ParquetFile(path) as parquet:
        assert parquet.num_row_groups == 6
    assert pl.read_parquet(path)["content"].to_list() == [a.content for a in artifacts]
