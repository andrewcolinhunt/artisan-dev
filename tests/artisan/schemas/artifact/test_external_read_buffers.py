"""External file verification reads bounded chunks without backend readahead."""

from __future__ import annotations

from io import BytesIO

from artisan.schemas import FileRefArtifact
from artisan.utils.hashing import STREAM_CHUNK_BYTES, compute_content_digest


def test_file_ref_uses_configured_filesystem_without_payload_cache():
    payload = b"a" * (3 * STREAM_CHUNK_BYTES + 7)
    sizes = []
    opened = []

    class Reader(BytesIO):
        def read(self, size=-1):
            assert 0 < size <= STREAM_CHUNK_BYTES
            sizes.append(size)
            return super().read(size)

    class Filesystem:
        protocol = "memory"

        def open(self, path, mode, **kwargs):
            opened.append((path, mode, kwargs))
            assert kwargs == {"cache_type": "none"}
            return Reader(payload)

    artifact = FileRefArtifact.draft(
        path="memory://fixture/source.csv",
        content_hash=compute_content_digest(payload),
        size_bytes=len(payload),
        step_number=0,
    ).finalize()
    assert artifact.read_content(fs=Filesystem()) == payload
    assert len(opened) == 1
    assert len(sizes) == 5
