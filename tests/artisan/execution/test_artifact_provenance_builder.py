"""Tests for lineage enrichment (enrich.py).

Tests cover:
- build_artifact_edges_from_dict (utility path)
- build_config_reference_edges (utility path)
"""

from __future__ import annotations

from unittest.mock import Mock

from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.types import ArtifactTypes


class TestBuildArtifactEdgesFromDict:
    """Tests for build_artifact_edges_from_dict function (utility path)."""

    def test_enriches_pairs_with_types_from_built_artifacts(self):
        """SourceTargetPairs are enriched with artifact types from built_artifacts dict."""
        from artisan.execution.lineage.enrich import (
            build_artifact_edges_from_dict,
        )
        from artisan.schemas.artifact.file_ref import FileRefArtifact
        from artisan.schemas.provenance.source_target_pair import SourceTargetPair

        # Create built artifacts
        file_ref = FileRefArtifact(
            artifact_id="e" * 32,
            origin_step_number=0,
            content_hash="c" * 32,
            path="/path/to/file.dat",
            size_bytes=100,
        )
        metric = MetricArtifact(
            artifact_id="s" * 32,
            origin_step_number=1,
            content=b'{"score": 0.5}',
        )

        built_artifacts = {
            "e" * 32: file_ref,
            "s" * 32: metric,
        }

        pairs = [
            SourceTargetPair(
                source="e" * 32,
                target="s" * 32,
                source_role="file",
                target_role="metric",
            )
        ]

        provenance = build_artifact_edges_from_dict(
            source_target_pairs=pairs,
            execution_run_id="r" * 32,
            built_artifacts=built_artifacts,
        )

        assert len(provenance) == 1
        prov = provenance[0]
        assert prov.execution_run_id == "r" * 32
        assert prov.source_artifact_id == "e" * 32
        assert prov.target_artifact_id == "s" * 32
        assert prov.source_artifact_type == "file_ref"
        assert prov.target_artifact_type == "metric"
        assert prov.source_role == "file"
        assert prov.target_role == "metric"

    def test_uses_unknown_for_missing_artifacts(self):
        """Returns UNKNOWN type when artifact not found in built_artifacts."""
        from artisan.execution.lineage.enrich import (
            build_artifact_edges_from_dict,
        )
        from artisan.schemas.provenance.source_target_pair import SourceTargetPair

        pairs = [
            SourceTargetPair(
                source="missing_source" + "0" * 18,
                target="missing_target" + "0" * 18,
                source_role="src",
                target_role="tgt",
            )
        ]

        provenance = build_artifact_edges_from_dict(
            source_target_pairs=pairs,
            execution_run_id="x" * 32,
            built_artifacts={},  # Empty - no artifacts found
        )

        assert len(provenance) == 1
        prov = provenance[0]
        assert prov.source_artifact_type == "UNKNOWN"
        assert prov.target_artifact_type == "UNKNOWN"

    def test_handles_mixed_known_unknown_artifacts(self):
        """Handles mix of known and unknown artifacts."""
        from artisan.execution.lineage.enrich import (
            build_artifact_edges_from_dict,
        )
        from artisan.schemas.provenance.source_target_pair import SourceTargetPair

        metric = MetricArtifact(
            artifact_id="s" * 32,
            origin_step_number=1,
            content=b'{"score": 0.5}',
        )

        built_artifacts = {"s" * 32: metric}

        pairs = [
            SourceTargetPair(
                source="unknown_source" + "0" * 18,
                target="s" * 32,  # Known
                source_role="src",
                target_role="metric",
            )
        ]

        provenance = build_artifact_edges_from_dict(
            source_target_pairs=pairs,
            execution_run_id="y" * 32,
            built_artifacts=built_artifacts,
        )

        assert len(provenance) == 1
        prov = provenance[0]
        assert prov.source_artifact_type == "UNKNOWN"
        assert prov.target_artifact_type == "metric"

    def test_returns_empty_for_empty_pairs(self):
        """Returns empty list for empty pairs."""
        from artisan.execution.lineage.enrich import (
            build_artifact_edges_from_dict,
        )

        provenance = build_artifact_edges_from_dict(
            source_target_pairs=[],
            execution_run_id="z" * 32,
            built_artifacts={},
        )

        assert provenance == []


class TestBuildConfigReferenceEdges:
    """Tests for build_config_reference_edges function."""

    def test_single_config_single_reference(self):
        """Single config with single reference creates one edge with correct type."""
        from artisan.execution.lineage.enrich import (
            build_config_reference_edges,
        )
        from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact

        config = ExecutionConfigArtifact.draft(
            content={"input_data": {"$artifact": "s" * 32}},
            original_name="config.json",
            step_number=1,
        ).finalize()

        mock_artifact_store = Mock()
        mock_artifact_store.load_artifact_type_map.return_value = {
            "s" * 32: ArtifactTypes.DATA,
        }

        edges = build_config_reference_edges(
            config_artifacts=[config],
            artifact_store=mock_artifact_store,
            execution_run_id="e" * 32,
        )

        assert len(edges) == 1
        edge = edges[0]
        assert edge.source_artifact_id == "s" * 32
        assert edge.target_artifact_id == config.artifact_id
        assert edge.source_artifact_type == "data"
        assert edge.target_artifact_type == "config"
        assert edge.source_role == "referenced"
        assert edge.target_role == "config"
        assert edge.execution_run_id == "e" * 32

    def test_config_with_multiple_references(self):
        """Config with multiple references creates multiple edges."""
        from artisan.execution.lineage.enrich import (
            build_config_reference_edges,
        )
        from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact

        config = ExecutionConfigArtifact.draft(
            content={
                "input_data": {"$artifact": "s" * 32},
                "reference_data": {"$artifact": "r" * 32},
            },
            original_name="config.json",
            step_number=1,
        ).finalize()

        mock_artifact_store = Mock()
        mock_artifact_store.load_artifact_type_map.return_value = {
            "s" * 32: ArtifactTypes.DATA,
            "r" * 32: ArtifactTypes.DATA,
        }

        edges = build_config_reference_edges(
            config_artifacts=[config],
            artifact_store=mock_artifact_store,
            execution_run_id="e" * 32,
        )

        assert len(edges) == 2
        source_ids = {edge.source_artifact_id for edge in edges}
        assert source_ids == {"s" * 32, "r" * 32}

    def test_empty_config_list(self):
        """Empty config list returns empty edges."""
        from artisan.execution.lineage.enrich import (
            build_config_reference_edges,
        )

        mock_artifact_store = Mock()

        edges = build_config_reference_edges(
            config_artifacts=[],
            artifact_store=mock_artifact_store,
            execution_run_id="e" * 32,
        )

        assert edges == []

    def test_config_without_references(self):
        """Config without artifact references returns no edges."""
        from artisan.execution.lineage.enrich import (
            build_config_reference_edges,
        )
        from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact

        config = ExecutionConfigArtifact.draft(
            content={"contig": "40-150", "length": 175},
            original_name="config.json",
            step_number=1,
        ).finalize()

        mock_artifact_store = Mock()

        edges = build_config_reference_edges(
            config_artifacts=[config],
            artifact_store=mock_artifact_store,
            execution_run_id="e" * 32,
        )

        assert edges == []
        mock_artifact_store.load_artifact_type_map.assert_not_called()

    def test_unknown_artifact_type_uses_unknown(self):
        """Unknown artifact type in store uses 'UNKNOWN' string."""
        from artisan.execution.lineage.enrich import (
            build_config_reference_edges,
        )
        from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact

        config = ExecutionConfigArtifact.draft(
            content={"input": {"$artifact": "u" * 32}},
            original_name="config.json",
            step_number=1,
        ).finalize()

        mock_artifact_store = Mock()
        mock_artifact_store.load_artifact_type_map.return_value = {}

        edges = build_config_reference_edges(
            config_artifacts=[config],
            artifact_store=mock_artifact_store,
            execution_run_id="e" * 32,
        )

        assert len(edges) == 1
        assert edges[0].source_artifact_type == "UNKNOWN"

    def test_non_config_artifacts_skipped(self):
        """Non-ExecutionConfigArtifact in list is skipped."""
        from artisan.execution.lineage.enrich import (
            build_config_reference_edges,
        )

        metric = MetricArtifact(
            artifact_id="s" * 32,
            origin_step_number=1,
            content=b'{"score": 0.5}',
        )

        mock_artifact_store = Mock()

        edges = build_config_reference_edges(
            config_artifacts=[metric],  # type: ignore  # Intentionally wrong type
            artifact_store=mock_artifact_store,
            execution_run_id="e" * 32,
        )

        assert edges == []
        mock_artifact_store.load_artifact_type_map.assert_not_called()
