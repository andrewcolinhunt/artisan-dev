"""Tests for Phase 4c artifact builder module.

Reference: Phase 4c - Executor (Step 8)
Reference: v4 design - artifact-centric execution model

In v4, operations construct draft Artifacts directly in postprocess() using
Artifact.draft() methods. The artifact_builder module provides:
- compute_artifact_id: Content-addressed hashing (re-exported from util.hashing)
- build_matched_artifacts: Build artifacts from MatchedOutput instances
- validate_passthrough_result: Validate passthrough results before staging
"""

from __future__ import annotations

from artisan.utils.hashing import compute_artifact_id


class TestComputeArtifactId:
    """Tests for compute_artifact_id function."""

    def test_compute_artifact_id_returns_32_char_hex(self):
        """Artifact ID should be 32 hex characters (xxh3_128)."""
        content = b"test content"
        artifact_id = compute_artifact_id(content)

        assert len(artifact_id) == 32
        assert all(c in "0123456789abcdef" for c in artifact_id)

    def test_compute_artifact_id_deterministic(self):
        """Same content should always produce same ID."""
        content = b"deterministic test"
        id1 = compute_artifact_id(content)
        id2 = compute_artifact_id(content)

        assert id1 == id2

    def test_compute_artifact_id_different_content_different_id(self):
        """Different content should produce different IDs."""
        id1 = compute_artifact_id(b"content A")
        id2 = compute_artifact_id(b"content B")

        assert id1 != id2
