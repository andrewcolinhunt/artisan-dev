"""Tests for artifact_store.py"""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import pytest

from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.types import ArtifactTypes
from fsspec.implementations.local import LocalFileSystem

from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.table_schemas import (
    ARTIFACT_EDGES_SCHEMA,
    ARTIFACT_INDEX_SCHEMA,
    EXECUTIONS_SCHEMA,
    STEPS_SCHEMA,
)

METRICS_SCHEMA = MetricArtifact.POLARS_SCHEMA
CONFIGS_SCHEMA = ExecutionConfigArtifact.POLARS_SCHEMA


class TestArtifactStorePrepare:
    """Tests for artifact preparation (no Delta Lake needed)."""

    @pytest.fixture
    def store(self, tmp_path):
        """Create an ArtifactStore with temporary base path."""
        return ArtifactStore(str(tmp_path), fs=LocalFileSystem())

    def test_prepare_artifact_index_entry(self, store):
        """Prepare artifact_index entry returns correct DataFrame."""
        df = store.prepare_artifact_index_entry(
            artifact_id="g" * 32,
            artifact_type=ArtifactTypes.METRIC,
            step_number=1,
        )

        assert df.shape == (1, 4)
        assert df["artifact_id"][0] == "g" * 32
        assert df["artifact_type"][0] == "metric"


class TestArtifactStoreFilesRoot:
    """Tests for the files_root parameter on ArtifactStore."""

    def test_files_root_defaults_to_none(self, tmp_path):
        """ArtifactStore without files_root has None."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        assert store.files_root is None

    def test_files_root_accepts_path(self, tmp_path):
        """ArtifactStore stores the provided files_root."""
        files_root = tmp_path / "files"
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem(), files_root=str(files_root))
        assert store.files_root == str(files_root)

    def test_backward_compatible_positional(self, tmp_path):
        """Existing positional-only callers still work."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        assert store.base_path == str(tmp_path)
        assert store.files_root is None


class TestArtifactStoreReadWithDelta:
    """Tests for artifact reading (requires Delta Lake)."""

    @pytest.fixture
    def store_with_data(self, tmp_path):
        """Create an ArtifactStore and populate with test data."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())

        # Create metrics table with test data
        metrics_path = tmp_path / "artifacts/metrics"
        metrics_data = {
            "artifact_id": ["a" * 32, "b" * 32],
            "origin_step_number": [1, 1],
            "content": [b'{"score": 0.5}', b'{"score": 0.8}'],
            "original_name": ["a", "b"],
            "extension": [".json", ".json"],
            "metadata": ["{}", "{}"],
            "external_path": [None, None],
        }
        df = pl.DataFrame(metrics_data, schema=METRICS_SCHEMA)
        df.write_delta(str(metrics_path))

        # Create artifact_index with test data
        index_path = tmp_path / "artifacts/index"
        index_data = {
            "artifact_id": ["a" * 32, "b" * 32],
            "artifact_type": ["metric", "metric"],
            "origin_step_number": [1, 1],
            "metadata": ["{}", "{}"],
        }
        pl.DataFrame(index_data, schema=ARTIFACT_INDEX_SCHEMA).write_delta(
            str(index_path)
        )

        return store

    def test_get_artifact_by_id_with_type(self, store_with_data):
        """Get artifact when type is known."""
        result = store_with_data.get_artifact(
            "a" * 32, artifact_type=ArtifactTypes.METRIC
        )

        assert result is not None
        assert result.artifact_id == "a" * 32
        assert result.content == b'{"score": 0.5}'

    def test_get_artifact_by_id_without_type(self, store_with_data):
        """Get artifact using index lookup."""
        result = store_with_data.get_artifact("a" * 32)

        assert result is not None
        assert result.artifact_id == "a" * 32

    def test_get_artifact_not_found(self, store_with_data):
        """Get nonexistent artifact returns None."""
        result = store_with_data.get_artifact("x" * 32)
        assert result is None

    def test_artifact_exists(self, store_with_data):
        """artifact_exists returns correct boolean."""
        assert store_with_data.artifact_exists("a" * 32) is True
        assert store_with_data.artifact_exists("x" * 32) is False


class TestBulkLoadMethods:
    """Tests for bulk-load methods (load_provenance_map, load_step_number_map)."""

    @pytest.fixture
    def store_with_provenance(self, tmp_path):
        """Create an ArtifactStore with provenance and index data.

        Chain: A -> B -> C (A is root, B has parent A, C has parent B).
        Also: A -> D (A has two children: B and D).
        Step numbers: A=0, B=1, C=2, D=1
        """
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())

        # Create artifact_index
        index_path = tmp_path / "artifacts/index"
        index_data = {
            "artifact_id": ["a" * 32, "b" * 32, "c" * 32, "d" * 32],
            "artifact_type": ["data", "data", "metric", "metric"],
            "origin_step_number": [0, 1, 2, 1],
            "metadata": ["{}", "{}", "{}", "{}"],
        }
        pl.DataFrame(index_data).cast(ARTIFACT_INDEX_SCHEMA).write_delta(
            str(index_path)
        )

        # Create artifact_edges: A -> B -> C, A -> D
        prov_path = tmp_path / "provenance/artifact_edges"
        prov_data = {
            "execution_run_id": ["x" * 32, "y" * 32, "z" * 32],
            "source_artifact_id": ["a" * 32, "b" * 32, "a" * 32],
            "target_artifact_id": ["b" * 32, "c" * 32, "d" * 32],
            "source_artifact_type": ["data", "data", "data"],
            "target_artifact_type": ["data", "metric", "metric"],
            "source_role": ["data", "data", "data"],
            "target_role": ["data", "metric", "metric"],
            "group_id": [None, None, None],
            "step_boundary": [True, True, True],
        }
        pl.DataFrame(prov_data).cast(ARTIFACT_EDGES_SCHEMA).write_delta(str(prov_path))

        return store

    def test_load_provenance_map(self, store_with_provenance):
        """Returns {target_id: [source_ids]} for all edges."""
        pmap = store_with_provenance.load_provenance_map()

        assert "b" * 32 in pmap
        assert pmap["b" * 32] == ["a" * 32]

        assert "c" * 32 in pmap
        assert pmap["c" * 32] == ["b" * 32]

        assert "d" * 32 in pmap
        assert pmap["d" * 32] == ["a" * 32]

        # Root has no entry (no edges where A is target)
        assert "a" * 32 not in pmap

    def test_load_provenance_map_empty_table(self, tmp_path):
        """Returns empty dict when no provenance table exists."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        assert store.load_provenance_map() == {}

    def test_load_step_number_map_all(self, store_with_provenance):
        """Returns all step numbers when no filter provided."""
        smap = store_with_provenance.load_step_number_map()

        assert smap["a" * 32] == 0
        assert smap["b" * 32] == 1
        assert smap["c" * 32] == 2
        assert smap["d" * 32] == 1
        assert len(smap) == 4

    def test_load_step_number_map_filtered(self, store_with_provenance):
        """Returns only requested artifact IDs when filtered."""
        smap = store_with_provenance.load_step_number_map({"a" * 32, "c" * 32})

        assert smap["a" * 32] == 0
        assert smap["c" * 32] == 2
        assert len(smap) == 2

    def test_load_step_number_map_empty_table(self, tmp_path):
        """Returns empty dict when no index table exists."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        assert store.load_step_number_map() == {}

    def test_load_step_number_map_nonexistent_ids(self, store_with_provenance):
        """Returns empty dict when all requested IDs don't exist."""
        smap = store_with_provenance.load_step_number_map({"z" * 32})
        assert smap == {}


class TestGetArtifactsByType:
    """Tests for get_artifacts_by_type() bulk loading."""

    @pytest.fixture
    def store_with_metrics(self, tmp_path):
        """Create an ArtifactStore with metrics table."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())

        metrics_path = tmp_path / "artifacts/metrics"
        metrics_data = {
            "artifact_id": ["m1" + "a" * 30, "m2" + "b" * 30, "m3" + "c" * 30],
            "origin_step_number": [1, 1, 2],
            "content": [
                b'{"score": 0.95}',
                b'{"score": 0.85}',
                b'{"score": 0.70}',
            ],
            "original_name": ["metric_1", "metric_2", "metric_3"],
            "extension": [".json", ".json", ".json"],
            "metadata": ["{}", "{}", "{}"],
            "external_path": [None, None, None],
        }
        pl.DataFrame(metrics_data, schema=METRICS_SCHEMA).write_delta(str(metrics_path))

        return store

    def test_bulk_load_all_found(self, store_with_metrics):
        """All requested IDs are found and returned."""
        ids = ["m1" + "a" * 30, "m2" + "b" * 30]
        result = store_with_metrics.get_artifacts_by_type(ids, ArtifactTypes.METRIC)

        assert len(result) == 2
        assert result["m1" + "a" * 30].values == {"score": 0.95}
        assert result["m2" + "b" * 30].values == {"score": 0.85}

    def test_bulk_load_missing_ids_omitted(self, store_with_metrics):
        """Missing IDs are silently omitted from the result."""
        ids = ["m1" + "a" * 30, "z" * 32]
        result = store_with_metrics.get_artifacts_by_type(ids, ArtifactTypes.METRIC)

        assert len(result) == 1
        assert "m1" + "a" * 30 in result
        assert "z" * 32 not in result

    def test_bulk_load_empty_list(self, store_with_metrics):
        """Empty ID list returns empty dict without scanning."""
        result = store_with_metrics.get_artifacts_by_type([], ArtifactTypes.METRIC)
        assert result == {}

    def test_bulk_load_all_missing(self, store_with_metrics):
        """All IDs missing returns empty dict."""
        result = store_with_metrics.get_artifacts_by_type(
            ["x" * 32, "y" * 32], ArtifactTypes.METRIC
        )
        assert result == {}

    def test_bulk_load_no_table(self, tmp_path):
        """Missing table returns empty dict."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        result = store.get_artifacts_by_type(["a" * 32], ArtifactTypes.METRIC)
        assert result == {}

    def test_bulk_load_configs(self, tmp_path):
        """Works for config type too (not just metrics)."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())

        configs_path = tmp_path / "artifacts/configs"
        configs_data = {
            "artifact_id": ["c1" + "a" * 30],
            "origin_step_number": [1],
            "content": [b'{"key": "val"}'],
            "original_name": ["config_1"],
            "extension": [".json"],
            "metadata": ["{}"],
            "external_path": [None],
        }
        pl.DataFrame(configs_data, schema=CONFIGS_SCHEMA).write_delta(str(configs_path))

        result = store.get_artifacts_by_type(["c1" + "a" * 30], ArtifactTypes.CONFIG)
        assert len(result) == 1
        assert result["c1" + "a" * 30].content == b'{"key": "val"}'


class TestArtifactStoreProvenanceQueries:
    """Tests for provenance and step number query methods."""

    @pytest.fixture
    def store_with_provenance(self, tmp_path):
        """Create an ArtifactStore with provenance data.

        Creates a simple chain: A -> B -> C
        Step numbers: A=0, B=1, C=2
        """
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())

        # Create artifact_index with test data
        index_path = tmp_path / "artifacts/index"
        index_data = {
            "artifact_id": ["a" * 32, "b" * 32, "c" * 32],
            "artifact_type": ["data", "data", "metric"],
            "origin_step_number": [0, 1, 2],
            "metadata": ["{}", "{}", "{}"],
        }
        pl.DataFrame(index_data).cast(ARTIFACT_INDEX_SCHEMA).write_delta(
            str(index_path)
        )

        # Create artifact_edges: A -> B -> C
        prov_path = tmp_path / "provenance/artifact_edges"
        prov_data = {
            "execution_run_id": ["x" * 32, "y" * 32],
            "source_artifact_id": ["a" * 32, "b" * 32],
            "target_artifact_id": ["b" * 32, "c" * 32],
            "source_artifact_type": ["data", "data"],
            "target_artifact_type": ["data", "metric"],
            "source_role": ["data", "data"],
            "target_role": ["data", "metric"],
            "group_id": [None, None],
            "step_boundary": [True, True],
        }
        pl.DataFrame(prov_data).cast(ARTIFACT_EDGES_SCHEMA).write_delta(str(prov_path))

        return store

    def test_get_ancestor_artifact_ids_with_parent(self, store_with_provenance):
        """Artifact with parent returns parent ID."""
        result = store_with_provenance.get_ancestor_artifact_ids("b" * 32)
        assert result == ["a" * 32]

    def test_get_ancestor_artifact_ids_chain(self, store_with_provenance):
        """Artifact at end of chain returns immediate parent only."""
        result = store_with_provenance.get_ancestor_artifact_ids("c" * 32)
        assert result == ["b" * 32]

    def test_get_ancestor_artifact_ids_no_parent(self, store_with_provenance):
        """Root artifact returns empty list."""
        result = store_with_provenance.get_ancestor_artifact_ids("a" * 32)
        assert result == []

    def test_get_ancestor_artifact_ids_nonexistent(self, store_with_provenance):
        """Nonexistent artifact returns empty list."""
        result = store_with_provenance.get_ancestor_artifact_ids("z" * 32)
        assert result == []

    def test_get_ancestor_artifact_ids_no_table(self, tmp_path):
        """Missing provenance table returns empty list."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        result = store.get_ancestor_artifact_ids("a" * 32)
        assert result == []

    def test_get_artifact_step_number(self, store_with_provenance):
        """Returns correct step number for each artifact."""
        assert store_with_provenance.get_artifact_step_number("a" * 32) == 0
        assert store_with_provenance.get_artifact_step_number("b" * 32) == 1
        assert store_with_provenance.get_artifact_step_number("c" * 32) == 2

    def test_get_artifact_step_number_nonexistent(self, store_with_provenance):
        """Returns None for nonexistent artifact."""
        assert store_with_provenance.get_artifact_step_number("z" * 32) is None

    def test_get_artifact_step_number_no_table(self, tmp_path):
        """Missing artifact_index table returns None."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        assert store.get_artifact_step_number("a" * 32) is None


class TestMetricOriginalNamePersistence:
    """Tests for MetricArtifact.original_name persistence.

    Verifies that original_name is correctly stored and retrieved from Delta Lake.
    This was a bug fix - original_name existed in memory but wasn't persisted.
    """

    @pytest.fixture
    def store_with_metrics(self, tmp_path):
        """Create an ArtifactStore with metric data including original_name."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())

        # Create metrics table with test data
        metrics_path = tmp_path / "artifacts/metrics"
        metrics_data = {
            "artifact_id": ["m1" + "a" * 30, "m2" + "b" * 30],
            "origin_step_number": [1, 1],
            "content": [b'{"score": 0.95}', b'{"score": 0.85}'],
            "original_name": ["sample_001_metrics", "sample_002_metrics"],
            "extension": [".json", ".json"],
            "metadata": ["{}", "{}"],
            "external_path": [None, None],
        }
        df = pl.DataFrame(metrics_data, schema=METRICS_SCHEMA)
        df.write_delta(str(metrics_path))

        # Create artifact_index for lookups
        index_path = tmp_path / "artifacts/index"
        index_data = {
            "artifact_id": ["m1" + "a" * 30, "m2" + "b" * 30],
            "artifact_type": ["metric", "metric"],
            "origin_step_number": [1, 1],
            "metadata": ["{}", "{}"],
        }
        pl.DataFrame(index_data, schema=ARTIFACT_INDEX_SCHEMA).write_delta(
            str(index_path)
        )

        return store

    def test_metric_original_name_round_trip(self, store_with_metrics):
        """Metric with original_name preserves it after storage round-trip."""
        result = store_with_metrics.get_artifact(
            "m1" + "a" * 30, artifact_type=ArtifactTypes.METRIC
        )

        assert result is not None
        assert result.original_name == "sample_001_metrics"  # Stem only

    def test_metric_second_original_name_round_trip(self, store_with_metrics):
        """Second metric preserves original_name after storage round-trip."""
        result = store_with_metrics.get_artifact(
            "m2" + "b" * 30, artifact_type=ArtifactTypes.METRIC
        )

        assert result is not None
        assert result.original_name == "sample_002_metrics"


class TestExecutionConfigArtifactRoundTrip:
    """Tests for ExecutionConfigArtifact storage round-trip."""

    @pytest.fixture
    def store_with_configs(self, tmp_path):
        """Create store with configs table."""
        import json

        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())

        # Create artifact_index
        index_path = tmp_path / "artifacts/index"
        index_data = {
            "artifact_id": ["c" * 32],
            "artifact_type": ["config"],
            "origin_step_number": [1],
            "metadata": ["{}"],
        }
        pl.DataFrame(index_data).write_delta(str(index_path))

        # Create configs table
        configs_path = tmp_path / "artifacts/configs"
        config_content = json.dumps(
            {"contig": "40-150,A8-10", "length": "175-275"}, sort_keys=True
        ).encode("utf-8")
        config_data = {
            "artifact_id": ["c" * 32],
            "origin_step_number": [1],
            "content": [config_content],
            "original_name": ["5w3x_motif_0_config"],  # Stem only
            "extension": [".json"],
            "metadata": ["{}"],
            "external_path": [None],
        }
        pl.DataFrame(config_data, schema=CONFIGS_SCHEMA).write_delta(str(configs_path))

        return store

    def test_get_artifact_by_id_with_type(self, store_with_configs):
        """Can retrieve ExecutionConfigArtifact by ID with type hint."""
        artifact = store_with_configs.get_artifact(
            "c" * 32, artifact_type=ArtifactTypes.CONFIG
        )

        assert artifact is not None
        assert isinstance(artifact, ExecutionConfigArtifact)
        assert artifact.artifact_id == "c" * 32
        assert artifact.original_name == "5w3x_motif_0_config"  # Stem only
        assert artifact.values["contig"] == "40-150,A8-10"
        assert artifact.values["length"] == "175-275"

    def test_get_artifact_by_id_without_type(self, store_with_configs):
        """Can retrieve ExecutionConfigArtifact by ID using artifact_index."""
        artifact = store_with_configs.get_artifact("c" * 32)

        assert artifact is not None
        assert isinstance(artifact, ExecutionConfigArtifact)
        assert artifact.values["contig"] == "40-150,A8-10"

    def test_id_only_mode(self, store_with_configs):
        """Can retrieve ID-only ExecutionConfigArtifact."""
        artifact = store_with_configs.get_artifact(
            "c" * 32, artifact_type=ArtifactTypes.CONFIG, hydrate=False
        )

        assert artifact is not None
        assert isinstance(artifact, ExecutionConfigArtifact)
        assert artifact.artifact_id == "c" * 32
        assert artifact.content is None
        assert not artifact.is_hydrated

    def test_original_name_persisted(self, store_with_configs):
        """original_name survives round-trip storage."""
        artifact = store_with_configs.get_artifact(
            "c" * 32, artifact_type=ArtifactTypes.CONFIG
        )

        assert artifact.original_name == "5w3x_motif_0_config"  # Stem only


class TestGetDescendantArtifactIds:
    """Tests for get_descendant_artifact_ids() forward provenance query."""

    @pytest.fixture
    def store_with_provenance(self, tmp_path):
        """Create ArtifactStore with provenance data for descendant queries.

        Graph: A -> B, A -> D, B -> C
        Types: A=data, B=data, C=metric, D=metric
        """
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())

        prov_path = tmp_path / "provenance/artifact_edges"
        prov_data = {
            "execution_run_id": ["x" * 32, "y" * 32, "z" * 32],
            "source_artifact_id": ["a" * 32, "b" * 32, "a" * 32],
            "target_artifact_id": ["b" * 32, "c" * 32, "d" * 32],
            "source_artifact_type": ["data", "data", "data"],
            "target_artifact_type": ["data", "metric", "metric"],
            "source_role": ["data", "data", "data"],
            "target_role": ["data", "metric", "metric"],
            "group_id": [None, None, None],
            "step_boundary": [True, True, True],
        }
        pl.DataFrame(prov_data).cast(ARTIFACT_EDGES_SCHEMA).write_delta(str(prov_path))

        return store

    def test_single_source_multiple_descendants(self, store_with_provenance):
        """Source A has two descendants: B and D."""
        result = store_with_provenance.get_descendant_artifact_ids({"a" * 32})

        assert "a" * 32 in result
        assert sorted(result["a" * 32]) == sorted(["b" * 32, "d" * 32])

    def test_type_filter_metric_only(self, store_with_provenance):
        """With METRIC filter, source A returns only D (metric), not B (data)."""
        result = store_with_provenance.get_descendant_artifact_ids(
            {"a" * 32}, target_artifact_type=ArtifactTypes.METRIC
        )

        assert result["a" * 32] == ["d" * 32]

    def test_leaf_node_no_descendants(self, store_with_provenance):
        """Leaf node C has no descendants."""
        result = store_with_provenance.get_descendant_artifact_ids({"c" * 32})
        assert result == {}

    def test_multiple_sources(self, store_with_provenance):
        """Query multiple sources at once."""
        result = store_with_provenance.get_descendant_artifact_ids({"a" * 32, "b" * 32})

        assert "a" * 32 in result
        assert "b" * 32 in result
        assert "c" * 32 in result["b" * 32]

    def test_empty_input(self, store_with_provenance):
        """Empty input returns empty dict without scanning."""
        result = store_with_provenance.get_descendant_artifact_ids(set())
        assert result == {}

    def test_missing_table(self, tmp_path):
        """Missing provenance table returns empty dict."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        result = store.get_descendant_artifact_ids({"a" * 32})
        assert result == {}


class TestLoadArtifactTypeMap:
    """Tests for load_artifact_type_map() bulk type resolution."""

    @pytest.fixture
    def store_with_index(self, tmp_path):
        """Create store with artifact_index containing mixed types."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        index_path = tmp_path / "artifacts/index"
        pl.DataFrame(
            {
                "artifact_id": ["a" * 32, "b" * 32, "c" * 32, "d" * 32],
                "artifact_type": [
                    "data",
                    "data",
                    "metric",
                    "config",
                ],
                "origin_step_number": [0, 1, 2, 1],
                "metadata": ["{}", "{}", "{}", "{}"],
            },
            schema=ARTIFACT_INDEX_SCHEMA,
        ).write_delta(str(index_path))
        return store

    def test_load_all(self, store_with_index):
        """Load all types when no filter provided."""
        result = store_with_index.load_artifact_type_map()
        assert len(result) == 4
        assert result["a" * 32] == "data"
        assert result["c" * 32] == "metric"
        assert result["d" * 32] == "config"

    def test_load_filtered(self, store_with_index):
        """Load only requested IDs."""
        result = store_with_index.load_artifact_type_map(["a" * 32, "c" * 32])
        assert len(result) == 2
        assert result["a" * 32] == "data"
        assert result["c" * 32] == "metric"

    def test_empty_index(self, tmp_path):
        """Missing index returns empty dict."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        assert store.load_artifact_type_map() == {}

    def test_nonexistent_ids(self, store_with_index):
        """Non-existent IDs return empty dict."""
        result = store_with_index.load_artifact_type_map(["z" * 32])
        assert result == {}


class TestLoadArtifactIdsByType:
    """Tests for load_artifact_ids_by_type() filtered index query."""

    @pytest.fixture
    def store_with_index(self, tmp_path):
        """Create store with artifact_index containing mixed types and steps."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        index_path = tmp_path / "artifacts/index"
        pl.DataFrame(
            {
                "artifact_id": ["a" * 32, "b" * 32, "c" * 32, "d" * 32],
                "artifact_type": ["data", "data", "metric", "metric"],
                "origin_step_number": [0, 1, 2, 2],
                "metadata": ["{}", "{}", "{}", "{}"],
            },
            schema=ARTIFACT_INDEX_SCHEMA,
        ).write_delta(str(index_path))
        return store

    def test_filter_by_type(self, store_with_index):
        """Returns only IDs of the requested type."""
        result = store_with_index.load_artifact_ids_by_type(ArtifactTypes.DATA)
        assert result == {"a" * 32, "b" * 32}

    def test_filter_by_type_and_step(self, store_with_index):
        """Filters by both type and step number."""
        result = store_with_index.load_artifact_ids_by_type(
            ArtifactTypes.DATA, step_numbers=[0]
        )
        assert result == {"a" * 32}

    def test_filter_by_type_and_ids(self, store_with_index):
        """Filters by type and specific artifact IDs."""
        result = store_with_index.load_artifact_ids_by_type(
            ArtifactTypes.METRIC, artifact_ids=["c" * 32]
        )
        assert result == {"c" * 32}

    def test_no_match(self, store_with_index):
        """Returns empty set when no match."""
        result = store_with_index.load_artifact_ids_by_type(ArtifactTypes.FILE_REF)
        assert result == set()

    def test_missing_index(self, tmp_path):
        """Missing index returns empty set."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        result = store.load_artifact_ids_by_type(ArtifactTypes.DATA)
        assert result == set()


class TestLoadForwardProvenanceMap:
    """Tests for load_forward_provenance_map()."""

    @pytest.fixture
    def store_with_provenance(self, tmp_path):
        """Create store with provenance: A -> B, A -> D, B -> C."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        prov_path = tmp_path / "provenance/artifact_edges"
        pl.DataFrame(
            {
                "execution_run_id": ["x" * 32, "y" * 32, "z" * 32],
                "source_artifact_id": ["a" * 32, "b" * 32, "a" * 32],
                "target_artifact_id": ["b" * 32, "c" * 32, "d" * 32],
                "source_artifact_type": ["data", "data", "data"],
                "target_artifact_type": ["data", "metric", "metric"],
                "source_role": ["data", "data", "data"],
                "target_role": ["data", "metric", "metric"],
                "group_id": [None, None, None],
                "step_boundary": [True, True, True],
            },
        ).cast(ARTIFACT_EDGES_SCHEMA).write_delta(str(prov_path))
        return store

    def test_forward_map(self, store_with_provenance):
        """Returns {source: [targets]} mapping."""
        result = store_with_provenance.load_forward_provenance_map()
        assert sorted(result["a" * 32]) == sorted(["b" * 32, "d" * 32])
        assert result["b" * 32] == ["c" * 32]
        assert "c" * 32 not in result  # leaf has no outgoing edges

    def test_missing_table(self, tmp_path):
        """Missing provenance table returns empty dict."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        assert store.load_forward_provenance_map() == {}


class TestLoadStepNameMap:
    """Tests for load_step_name_map()."""

    @pytest.fixture
    def store_with_steps(self, tmp_path):
        """Create store with steps table."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        ts = datetime(2025, 1, 1, tzinfo=UTC)
        steps_path = tmp_path / "orchestration/steps"
        pl.DataFrame(
            {
                "step_run_id": ["r0" + "0" * 30, "r1" + "0" * 30],
                "step_spec_id": ["s0" + "0" * 30, "s1" + "0" * 30],
                "pipeline_run_id": ["p" * 32, "p" * 32],
                "step_number": [0, 1],
                "step_name": ["ingest", "tool_c"],
                "status": ["completed", "completed"],
                "operation_class": ["Ingest", "ToolC"],
                "params_json": ["{}", "{}"],
                "input_refs_json": ["{}", "{}"],
                "compute_backend": ["local", "local"],
                "compute_options_json": ["{}", "{}"],
                "output_roles_json": ["{}", "{}"],
                "output_types_json": ["{}", "{}"],
                "total_count": [1, 1],
                "succeeded_count": [1, 1],
                "failed_count": [0, 0],
                "timestamp": [ts, ts],
                "duration_seconds": [1.0, 1.0],
                "error": [None, None],
                "dispatch_error": [None, None],
                "commit_error": [None, None],
                "metadata": ["{}", "{}"],
            },
            schema=STEPS_SCHEMA,
        ).write_delta(str(steps_path))
        return store

    def test_loads_step_names(self, store_with_steps):
        """Returns step_number -> step_name mapping."""
        result = store_with_steps.load_step_name_map()
        assert result[0] == "ingest"
        assert result[1] == "tool_c"

    def test_missing_tables(self, tmp_path):
        """Missing tables returns empty dict."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        assert store.load_step_name_map() == {}

    def test_fallback_to_executions(self, tmp_path):
        """Falls back to executions when steps table is missing."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        ts = datetime(2025, 1, 1, tzinfo=UTC)
        records_path = tmp_path / "orchestration/executions"
        pl.DataFrame(
            {
                "execution_run_id": ["e" * 32],
                "execution_spec_id": ["s" * 32],
                "step_run_id": [None],
                "origin_step_number": [0],
                "operation_name": ["ingest_fallback"],
                "params": ["{}"],
                "user_overrides": ["{}"],
                "timestamp_start": [ts],
                "timestamp_end": [ts],
                "source_worker": [0],
                "compute_backend": ["local"],
                "success": [True],
                "error": [None],
                "tool_output": [None],
                "worker_log": [None],
                "metadata": ["{}"],
            },
            schema=EXECUTIONS_SCHEMA,
        ).write_delta(str(records_path))
        result = store.load_step_name_map()
        assert result[0] == "ingest_fallback"


class TestGetAssociated:
    """Tests for get_associated() provenance-based association lookup."""

    @pytest.fixture
    def store_with_associated_metrics(self, tmp_path):
        """Create store with source metrics, associated metrics, and provenance edges.

        Graph: S1 -> M1 (metric), S1 -> M2 (metric), S2 -> M3 (metric)
        S1 and S2 are source configs, M1/M2/M3 are derived metrics.
        """
        import json

        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())

        # Create provenance edges: S1 -> M1, S1 -> M2, S2 -> M3
        prov_path = tmp_path / "provenance/artifact_edges"
        pl.DataFrame(
            {
                "execution_run_id": ["x" * 32, "y" * 32, "z" * 32],
                "source_artifact_id": [
                    "s1" + "a" * 30,
                    "s1" + "a" * 30,
                    "s2" + "b" * 30,
                ],
                "target_artifact_id": [
                    "m1" + "c" * 30,
                    "m2" + "d" * 30,
                    "m3" + "e" * 30,
                ],
                "source_artifact_type": ["config", "config", "config"],
                "target_artifact_type": [
                    "metric",
                    "metric",
                    "metric",
                ],
                "source_role": ["config", "config", "config"],
                "target_role": ["metric", "metric", "metric"],
                "group_id": [None, None, None],
                "step_boundary": [True, True, True],
            },
        ).cast(ARTIFACT_EDGES_SCHEMA).write_delta(str(prov_path))

        # Create metrics table
        metrics_path = tmp_path / "artifacts/metrics"
        pl.DataFrame(
            {
                "artifact_id": ["m1" + "c" * 30, "m2" + "d" * 30, "m3" + "e" * 30],
                "origin_step_number": [1, 1, 2],
                "content": [
                    json.dumps({"component": "A/CMP/1"}).encode(),
                    json.dumps({"site": "A/SER/30"}).encode(),
                    json.dumps({"pocket": "B/1-10"}).encode(),
                ],
                "original_name": ["s1_metric_1", "s1_metric_2", "s2_metric_1"],
                "extension": [".json", ".json", ".json"],
                "metadata": ["{}", "{}", "{}"],
                "external_path": [None, None, None],
            },
            schema=METRICS_SCHEMA,
        ).write_delta(str(metrics_path))

        return store

    def test_returns_matching_descendants(self, store_with_associated_metrics):
        """Returns metrics associated with a config via provenance."""
        result = store_with_associated_metrics.get_associated(
            {"s1" + "a" * 30}, "metric"
        )

        assert "s1" + "a" * 30 in result
        metrics = result["s1" + "a" * 30]
        assert len(metrics) == 2
        val_sets = {frozenset(m.values.items()) for m in metrics}
        assert frozenset({("component", "A/CMP/1")}) in val_sets
        assert frozenset({("site", "A/SER/30")}) in val_sets

    def test_multiple_sources(self, store_with_associated_metrics):
        """Returns metrics for multiple configs at once."""
        result = store_with_associated_metrics.get_associated(
            {"s1" + "a" * 30, "s2" + "b" * 30}, "metric"
        )

        assert len(result) == 2
        assert len(result["s1" + "a" * 30]) == 2
        assert len(result["s2" + "b" * 30]) == 1

    def test_empty_when_no_edges(self, store_with_associated_metrics):
        """Returns empty dict when no provenance edges exist for the source."""
        result = store_with_associated_metrics.get_associated({"z" * 32}, "metric")
        assert result == {}

    def test_filters_by_type(self, store_with_associated_metrics):
        """Only returns descendants of the requested type."""
        result = store_with_associated_metrics.get_associated(
            {"s1" + "a" * 30}, "config"
        )
        assert result == {}

    def test_empty_input(self, store_with_associated_metrics):
        """Empty input returns empty dict."""
        result = store_with_associated_metrics.get_associated(set(), "metric")
        assert result == {}

    def test_missing_provenance_table(self, tmp_path):
        """Missing provenance table returns empty dict."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        result = store.get_associated({"a" * 32}, "metric")
        assert result == {}
