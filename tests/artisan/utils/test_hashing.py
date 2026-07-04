"""Tests for hashing utilities."""

from __future__ import annotations

from artisan.utils.hashing import compute_artifact_id


class TestComputeArtifactId:
    """Tests for compute_artifact_id."""

    def test_deterministic(self) -> None:
        data = b"same input"
        assert compute_artifact_id(data) == compute_artifact_id(data)

    def test_different_content_different_hash(self) -> None:
        assert compute_artifact_id(b"a") != compute_artifact_id(b"b")

    def test_returns_32_char_hex(self) -> None:
        result = compute_artifact_id(b"anything")
        assert len(result) == 32
        int(result, 16)  # validates hex
