"""Captured streams preserve source names and report reads or bypasses."""

from __future__ import annotations

import io
from contextlib import closing
from pathlib import Path
from typing import Literal

import pytest
from fsspec.implementations.local import LocalFileSystem

from artisan.execution.recording.curator_read_filesystem import CuratorReadFileSystem


@pytest.mark.parametrize("mode", ["rb", "r", "raw", "detach"])
def test_captured_stream_reads_names_and_owned_closure(
    tmp_path: Path, mode: Literal["rb", "r", "raw", "detach"]
) -> None:
    capture = tmp_path / "capture"
    capture.write_bytes(b"value\n1\n")
    source_uri = "file:///authored/source.csv"
    exclusions: list[str] = []
    adapter = CuratorReadFileSystem(
        LocalFileSystem(),
        source_uri,
        capture,
        exclude=lambda: exclusions.append("untracked_filesystem_access"),
    )
    raw: io.RawIOBase | None = None
    with closing(adapter):
        stream = adapter.open(source_uri, "r" if mode == "r" else "rb")
        assert stream.name == source_uri
        if mode == "raw":
            raw = stream.raw
        elif mode == "detach":
            raw = stream.detach()
        content = raw.read() if raw is not None else stream.read()
        assert content == ("value\n1\n" if mode == "r" else b"value\n1\n")
    assert adapter.read_observed is (mode in {"rb", "r"})
    assert bool(exclusions) is (mode in {"raw", "detach"})
    assert raw.closed if raw is not None else stream.closed


def test_protocol_stripped_open_keeps_original_source_name(tmp_path: Path) -> None:
    capture = tmp_path / "capture"
    capture.write_bytes(b"captured")
    source_uri = "file:///authored/source.csv"
    fs = LocalFileSystem()
    with closing(
        CuratorReadFileSystem(
            fs,
            source_uri,
            capture,
            exclude=lambda: pytest.fail("Supported captured read was excluded"),
        )
    ) as adapter:
        with adapter.open(fs._strip_protocol(source_uri)) as stream:
            assert stream.name == source_uri
            assert stream.read() == b"captured"
        assert adapter.read_observed
