"""File ingestion preserves occurrence order without holding converted payloads."""

from __future__ import annotations

import weakref
from unittest.mock import Mock

import polars as pl
from fsspec.implementations.local import LocalFileSystem

from artisan.operations.curator import IngestCSV
from artisan.schemas import ArtifactResult, ArtifactStreamResult, FileRefArtifact
from artisan.utils.hashing import compute_content_digest


def _file_ref(path, content):
    path.write_bytes(content)
    return FileRefArtifact.draft(
        path=str(path),
        content_hash=compute_content_digest(content),
        size_bytes=len(content),
        original_name=path.stem,
        extension=".csv",
        step_number=0,
    ).finalize()


def test_ingest_is_lazy_repeated_and_does_not_cache_descriptors(tmp_path, monkeypatch):
    a = _file_ref(tmp_path / "a.csv", b"value\n1\n")
    b = _file_ref(tmp_path / "b.csv", b"value\n2\n")
    store = Mock(filesystem=LocalFileSystem())
    store.get_artifacts_by_type.return_value = {a.artifact_id: a, b.artifact_id: b}
    convert = IngestCSV.convert_file
    prior = []
    names = []

    def tracked(self, file_ref, step_number, *, fs=None):
        assert fs is store.filesystem
        assert all(reference() is None for reference in prior)
        names.append(file_ref.original_name)
        artifact = convert(self, file_ref, step_number, fs=fs)
        prior.append(weakref.ref(artifact))
        return artifact

    monkeypatch.setattr(IngestCSV, "convert_file", tracked)
    result = IngestCSV().execute_curator(
        {
            "file": pl.DataFrame(
                {"artifact_id": [a.artifact_id, b.artifact_id, a.artifact_id]}
            )
        },
        1,
        store,
    )
    assert isinstance(result, ArtifactStreamResult)
    assert names == []
    for expected in [a.artifact_id, b.artifact_id, a.artifact_id]:
        emission = next(result.items)
        assert emission.sources == {"file": [expected]}
        del emission
    assert list(result.items) == []
    assert names == ["a", "b", "a"]
    assert a._cached_content is None
    assert b._cached_content is None
    store.get_artifacts_by_type.assert_called_once()


def test_ingest_missing_descriptor_fails_before_conversion(tmp_path, monkeypatch):
    a = _file_ref(tmp_path / "a.csv", b"value\n1\n")
    store = Mock(filesystem=LocalFileSystem())
    store.get_artifacts_by_type.return_value = {a.artifact_id: a}
    convert = Mock()
    monkeypatch.setattr(IngestCSV, "convert_file", convert)
    result = IngestCSV().execute_curator(
        {"file": pl.DataFrame({"artifact_id": [a.artifact_id, "b" * 32]})},
        1,
        store,
    )
    assert isinstance(result, ArtifactResult)
    assert not result.success
    convert.assert_not_called()
