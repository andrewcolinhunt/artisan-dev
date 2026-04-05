"""Tests for hashing utilities."""

from __future__ import annotations

from artisan.utils.hashing import compute_artifact_id, compute_content_hash


class TestComputeContentHash:
    """Tests for the compute_content_hash wrapper."""

    def test_matches_compute_artifact_id(self) -> None:
        data = b"test content"
        assert compute_content_hash(data) == compute_artifact_id(data)

    def test_deterministic(self) -> None:
        data = b"same input"
        assert compute_content_hash(data) == compute_content_hash(data)

    def test_different_content_different_hash(self) -> None:
        assert compute_content_hash(b"a") != compute_content_hash(b"b")

    def test_returns_32_char_hex(self) -> None:
        result = compute_content_hash(b"anything")
        assert len(result) == 32
        int(result, 16)  # validates hex
