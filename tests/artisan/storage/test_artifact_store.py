"""Tests for artifact_store.py"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import polars as pl
import pytest
from fixtures.execution_records import executions_df
from fixtures.store_format import publish_test_store
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import ArtifactIntegrityError
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.table_schemas import (
    ARTIFACT_EDGES_SCHEMA,
    ARTIFACT_INDEX_SCHEMA,
    ARTIFACT_LOCATIONS_SCHEMA,
    STEPS_SCHEMA,
)
from artisan.utils.hashing import compute_content_digest

METRICS_SCHEMA = MetricArtifact.POLARS_SCHEMA
CONFIGS_SCHEMA = ExecutionConfigArtifact.POLARS_SCHEMA


@pytest.fixture(autouse=True)
def _format_local_tmp_root(tmp_path) -> None:
    """Give direct local test stores the exact format-2 manifest."""
    publish_test_store(str(tmp_path), LocalFileSystem())


def _metric(values: dict, name: str, step: int = 1) -> MetricArtifact:
    """Build a finalized metric row with a valid format-2 identity."""
    return MetricArtifact.draft(values, f"{name}.json", step).finalize()  # type: ignore[return-value]


def _config(values: dict, name: str, step: int = 1) -> ExecutionConfigArtifact:
    """Build a finalized config row with a valid format-2 identity."""
    return ExecutionConfigArtifact.draft(values, f"{name}.json", step).finalize()  # type: ignore[return-value]


def _index_rows(
    artifacts: list[MetricArtifact | ExecutionConfigArtifact],
) -> list[dict]:
    """Build index rows for finalized test artifacts."""
    return [
        {
            "artifact_id": artifact.artifact_id,
            "artifact_type": artifact.artifact_type,
            "origin_step_number": artifact.origin_step_number,
            "metadata": json.dumps(artifact.metadata),
        }
        for artifact in artifacts
    ]


def _write_file_ref(
    root,
    content: bytes,
    locations: list[str],
) -> FileRefArtifact:
    """Write one external artifact and its optional location rows."""
    artifact = FileRefArtifact.draft(
        path=locations[0] if locations else "",
        content_hash=compute_content_digest(content),
        size_bytes=len(content),
        step_number=1,
        original_name="payload",
        extension=".bin",
    ).finalize()
    pl.DataFrame([artifact.to_row()], schema=FileRefArtifact.POLARS_SCHEMA).write_delta(
        str(root / "artifacts/file_refs")
    )
    pl.DataFrame(
        [
            {
                "artifact_id": artifact.artifact_id,
                "artifact_type": artifact.artifact_type,
                "origin_step_number": artifact.origin_step_number,
                "metadata": "{}",
            }
        ],
        schema=ARTIFACT_INDEX_SCHEMA,
    ).write_delta(str(root / "artifacts/index"))
    if locations:
        pl.DataFrame(
            [
                {"artifact_id": artifact.artifact_id, "uri": location}
                for location in locations
            ],
            schema=ARTIFACT_LOCATIONS_SCHEMA,
        ).write_delta(str(root / "artifacts/locations"))
    return artifact


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
        store = ArtifactStore(
            str(tmp_path), fs=LocalFileSystem(), files_root=str(files_root)
        )
        assert store.files_root == str(files_root)

    def test_backward_compatible_positional(self, tmp_path):
        """Existing positional-only callers still work."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        assert store.base_path == str(tmp_path)
        assert store.files_root is None


class TestExternalLocationSelection:
    """External hydration selects and verifies the shared location relation."""

    def test_missing_candidate_falls_back_to_next_location(self, tmp_path):
        content = b"verified"
        missing = str(tmp_path / "a-missing.bin")
        valid = tmp_path / "z-valid.bin"
        valid.write_bytes(content)
        artifact = _write_file_ref(tmp_path, content, [missing, str(valid)])

        loaded = ArtifactStore(str(tmp_path)).get_artifact(artifact.artifact_id)

        assert loaded is not None
        assert loaded.path == str(valid)

    def test_readable_wrong_candidate_fails_immediately(self, tmp_path):
        expected = b"verified"
        wrong = tmp_path / "a-wrong.bin"
        wrong.write_bytes(b"changed")
        valid = tmp_path / "z-valid.bin"
        valid.write_bytes(expected)
        artifact = _write_file_ref(tmp_path, expected, [str(wrong), str(valid)])

        with pytest.raises(ArtifactIntegrityError, match="failed integrity"):
            ArtifactStore(str(tmp_path)).get_artifact(artifact.artifact_id)

    def test_managed_location_precedes_unmanaged_location(self, tmp_path):
        content = b"verified"
        unmanaged = tmp_path / "a-unmanaged.bin"
        unmanaged.write_bytes(content)
        files_root = tmp_path / "managed"
        files_root.mkdir()
        managed = files_root / "z-managed.bin"
        managed.write_bytes(content)
        artifact = _write_file_ref(tmp_path, content, [str(unmanaged), str(managed)])

        loaded = ArtifactStore(str(tmp_path), files_root=str(files_root)).get_artifact(
            artifact.artifact_id
        )

        assert loaded is not None
        assert loaded.path == str(managed)

    def test_missing_location_relation_fails_with_integrity_error(self, tmp_path):
        artifact = _write_file_ref(tmp_path, b"verified", [])

        with pytest.raises(ArtifactIntegrityError, match="no readable verified"):
            ArtifactStore(str(tmp_path)).get_artifact(artifact.artifact_id)


class TestArtifactStoreFsDefault:
    """Tests for the fs parameter defaulting to LocalFileSystem."""

    def test_fs_defaults_to_local(self, tmp_path):
        """ArtifactStore without fs= uses LocalFileSystem."""
        store = ArtifactStore(str(tmp_path))
        assert isinstance(store._fs, LocalFileSystem)
        assert store.base_path == str(tmp_path)

    def test_fs_explicit_overrides_default(self, tmp_path):
        """Explicit fs= is used instead of the default."""
        explicit_fs = LocalFileSystem()
        store = ArtifactStore(str(tmp_path), fs=explicit_fs)
        assert store._fs is explicit_fs


class TestArtifactStoreReadWithDelta:
    """Tests for artifact reading (requires Delta Lake)."""

    @pytest.fixture
    def store_with_data(self, backend_fs):
        """Create an ArtifactStore and populate with test data."""
        fs, storage, root = backend_fs
        opts = storage.delta_storage_options()
        store = ArtifactStore(root, fs=fs, storage_options=opts)

        # Create metrics table with test data
        artifacts = [_metric({"score": 0.5}, "a"), _metric({"score": 0.8}, "b")]
        df = pl.DataFrame(
            [artifact.to_row() for artifact in artifacts], schema=METRICS_SCHEMA
        )
        df.write_delta(f"{root}/artifacts/metrics", storage_options=opts)

        # Create artifact_index with test data
        pl.DataFrame(_index_rows(artifacts), schema=ARTIFACT_INDEX_SCHEMA).write_delta(
            f"{root}/artifacts/index", storage_options=opts
        )

        return store, [artifact.artifact_id for artifact in artifacts]

    def test_get_artifact_by_id_with_type(self, store_with_data):
        """Get artifact when type is known."""
        store, ids = store_with_data
        result = store.get_artifact(ids[0], artifact_type=ArtifactTypes.METRIC)

        assert result is not None
        assert result.artifact_id == ids[0]
        assert result.content == b'{"score": 0.5}'

    def test_get_artifact_by_id_without_type(self, store_with_data):
        """Get artifact using index lookup."""
        store, ids = store_with_data
        result = store.get_artifact(ids[0])

        assert result is not None
        assert result.artifact_id == ids[0]

    def test_get_artifact_not_found(self, store_with_data):
        """Get nonexistent artifact returns None."""
        store, _ids = store_with_data
        result = store.get_artifact("x" * 32)
        assert result is None

    def test_artifact_exists(self, store_with_data):
        """artifact_exists returns correct boolean."""
        store, ids = store_with_data
        assert store.artifact_exists(ids[0]) is True
        assert store.artifact_exists("x" * 32) is False


class TestBulkLoadMethods:
    """Tests for bulk-load methods (provenance.load_backward_map, load_step_map)."""

    @pytest.fixture
    def store_with_provenance(self, backend_fs):
        """Create an ArtifactStore with provenance and index data.

        Chain: A -> B -> C (A is root, B has parent A, C has parent B).
        Also: A -> D (A has two children: B and D).
        Step numbers: A=0, B=1, C=2, D=1
        """
        fs, storage, root = backend_fs
        opts = storage.delta_storage_options()
        store = ArtifactStore(root, fs=fs, storage_options=opts)

        # Create artifact_index
        index_data = {
            "artifact_id": ["a" * 32, "b" * 32, "c" * 32, "d" * 32],
            "artifact_type": ["data", "data", "metric", "metric"],
            "origin_step_number": [0, 1, 2, 1],
            "metadata": ["{}", "{}", "{}", "{}"],
        }
        pl.DataFrame(index_data).cast(ARTIFACT_INDEX_SCHEMA).write_delta(
            f"{root}/artifacts/index", storage_options=opts
        )

        # Create artifact_edges: A -> B -> C, A -> D
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
        pl.DataFrame(prov_data).cast(ARTIFACT_EDGES_SCHEMA).write_delta(
            f"{root}/provenance/artifact_edges", storage_options=opts
        )

        return store

    def test_load_provenance_map(self, store_with_provenance):
        """Returns {target_id: [source_ids]} for all edges."""
        pmap = store_with_provenance.provenance.load_backward_map()

        assert "b" * 32 in pmap
        assert pmap["b" * 32] == ["a" * 32]

        assert "c" * 32 in pmap
        assert pmap["c" * 32] == ["b" * 32]

        assert "d" * 32 in pmap
        assert pmap["d" * 32] == ["a" * 32]

        # Root has no entry (no edges where A is target)
        assert "a" * 32 not in pmap

    def test_load_provenance_map_empty_table(self, backend_fs):
        """Returns empty dict when no provenance table exists."""
        fs, storage, root = backend_fs
        store = ArtifactStore(
            root, fs=fs, storage_options=storage.delta_storage_options()
        )
        assert store.provenance.load_backward_map() == {}

    def test_load_step_number_map_all(self, store_with_provenance):
        """Returns all step numbers when no filter provided."""
        smap = store_with_provenance.provenance.load_step_map()

        assert smap["a" * 32] == 0
        assert smap["b" * 32] == 1
        assert smap["c" * 32] == 2
        assert smap["d" * 32] == 1
        assert len(smap) == 4

    def test_load_step_number_map_filtered(self, store_with_provenance):
        """Returns only requested artifact IDs when filtered."""
        smap = store_with_provenance.provenance.load_step_map({"a" * 32, "c" * 32})

        assert smap["a" * 32] == 0
        assert smap["c" * 32] == 2
        assert len(smap) == 2

    def test_load_step_number_map_empty_table(self, backend_fs):
        """Returns empty dict when no index table exists."""
        fs, storage, root = backend_fs
        store = ArtifactStore(
            root, fs=fs, storage_options=storage.delta_storage_options()
        )
        assert store.provenance.load_step_map() == {}

    def test_load_step_number_map_nonexistent_ids(self, store_with_provenance):
        """Returns empty dict when all requested IDs don't exist."""
        smap = store_with_provenance.provenance.load_step_map({"z" * 32})
        assert smap == {}


class TestGetArtifactsByType:
    """Tests for get_artifacts_by_type() bulk loading."""

    @pytest.fixture
    def store_with_metrics(self, backend_fs):
        """Create an ArtifactStore with metrics table."""
        fs, storage, root = backend_fs
        opts = storage.delta_storage_options()
        store = ArtifactStore(root, fs=fs, storage_options=opts)

        artifacts = [
            _metric({"score": 0.95}, "metric_1", 1),
            _metric({"score": 0.85}, "metric_2", 1),
            _metric({"score": 0.70}, "metric_3", 2),
        ]
        pl.DataFrame(
            [artifact.to_row() for artifact in artifacts], schema=METRICS_SCHEMA
        ).write_delta(f"{root}/artifacts/metrics", storage_options=opts)

        return store, [artifact.artifact_id for artifact in artifacts]

    def test_bulk_load_all_found(self, store_with_metrics):
        """All requested IDs are found and returned."""
        store, ids = store_with_metrics
        result = store.get_artifacts_by_type(ids[:2], ArtifactTypes.METRIC)

        assert len(result) == 2
        assert result[ids[0]].values == {"score": 0.95}
        assert result[ids[1]].values == {"score": 0.85}

    def test_bulk_load_missing_ids_omitted(self, store_with_metrics):
        """Missing IDs are silently omitted from the result."""
        store, ids = store_with_metrics
        result = store.get_artifacts_by_type([ids[0], "z" * 32], ArtifactTypes.METRIC)

        assert len(result) == 1
        assert ids[0] in result
        assert "z" * 32 not in result

    def test_bulk_load_empty_list(self, store_with_metrics):
        """Empty ID list returns empty dict without scanning."""
        store, _ids = store_with_metrics
        result = store.get_artifacts_by_type([], ArtifactTypes.METRIC)
        assert result == {}

    def test_bulk_load_all_missing(self, store_with_metrics):
        """All IDs missing returns empty dict."""
        store, _ids = store_with_metrics
        result = store.get_artifacts_by_type(["x" * 32, "y" * 32], ArtifactTypes.METRIC)
        assert result == {}

    def test_bulk_load_no_table(self, backend_fs):
        """Missing table returns empty dict."""
        fs, storage, root = backend_fs
        store = ArtifactStore(
            root, fs=fs, storage_options=storage.delta_storage_options()
        )
        result = store.get_artifacts_by_type(["a" * 32], ArtifactTypes.METRIC)
        assert result == {}

    def test_bulk_load_configs(self, backend_fs):
        """Works for config type too (not just metrics)."""
        fs, storage, root = backend_fs
        opts = storage.delta_storage_options()
        store = ArtifactStore(root, fs=fs, storage_options=opts)

        artifact = _config({"key": "val"}, "config_1")
        pl.DataFrame([artifact.to_row()], schema=CONFIGS_SCHEMA).write_delta(
            f"{root}/artifacts/configs", storage_options=opts
        )

        assert artifact.artifact_id is not None
        result = store.get_artifacts_by_type(
            [artifact.artifact_id], ArtifactTypes.CONFIG
        )
        assert len(result) == 1
        assert result[artifact.artifact_id].content == b'{"key": "val"}'


class TestLoadOriginalNames:
    """Tests for load_original_names() used by NAME-based input pairing."""

    @pytest.fixture
    def store_with_names(self, backend_fs):
        """Store with two metrics + two configs, all with original_name."""
        fs, storage, root = backend_fs
        opts = storage.delta_storage_options()
        store = ArtifactStore(root, fs=fs, storage_options=opts)

        metric_ids = ["m1" + "a" * 30, "m2" + "b" * 30]
        config_ids = ["c1" + "a" * 30, "c2" + "b" * 30]

        index_data = {
            "artifact_id": metric_ids + config_ids,
            "artifact_type": ["metric", "metric", "config", "config"],
            "origin_step_number": [1, 1, 2, 2],
            "metadata": ["{}"] * 4,
        }
        pl.DataFrame(index_data, schema=ARTIFACT_INDEX_SCHEMA).write_delta(
            f"{root}/artifacts/index", storage_options=opts
        )

        metrics_data = {
            "artifact_id": metric_ids,
            "origin_step_number": [1, 1],
            "content": [b"{}", b"{}"],
            "original_name": ["sample_001.json", "sample_002.json"],
            "extension": [".json", ".json"],
            "metadata": ["{}", "{}"],
        }
        pl.DataFrame(metrics_data, schema=METRICS_SCHEMA).write_delta(
            f"{root}/artifacts/metrics", storage_options=opts
        )

        configs_data = {
            "artifact_id": config_ids,
            "origin_step_number": [2, 2],
            "content": [b"{}", b"{}"],
            "original_name": ["sample_001.cfg", "sample_002.cfg"],
            "extension": [".cfg", ".cfg"],
            "metadata": ["{}", "{}"],
        }
        pl.DataFrame(configs_data, schema=CONFIGS_SCHEMA).write_delta(
            f"{root}/artifacts/configs", storage_options=opts
        )

        return store, metric_ids, config_ids

    def test_empty_input(self, store_with_names):
        """Empty list returns empty dict without scanning."""
        store, _metric_ids, _config_ids = store_with_names
        assert store.load_original_names([]) == {}

    def test_single_type(self, store_with_names):
        """Returns names for IDs in a single content table."""
        store, metric_ids, _config_ids = store_with_names
        result = store.load_original_names(metric_ids)
        assert result == {
            metric_ids[0]: "sample_001.json",
            metric_ids[1]: "sample_002.json",
        }

    def test_multi_type(self, store_with_names):
        """Returns names spanning multiple content tables in one call."""
        store, metric_ids, config_ids = store_with_names
        result = store.load_original_names([metric_ids[0], config_ids[0]])
        assert result == {
            metric_ids[0]: "sample_001.json",
            config_ids[0]: "sample_001.cfg",
        }

    def test_missing_ids_omitted(self, store_with_names):
        """IDs not in the index are omitted, no raise."""
        store, metric_ids, _config_ids = store_with_names
        result = store.load_original_names([metric_ids[0], "z" * 32])
        assert result == {metric_ids[0]: "sample_001.json"}

    def test_all_missing_returns_empty(self, store_with_names):
        """When no IDs are found in the index, returns empty dict."""
        store, _metric_ids, _config_ids = store_with_names
        assert store.load_original_names(["x" * 32, "y" * 32]) == {}

    def test_no_index_table(self, backend_fs):
        """Returns empty dict when the artifact_index table is absent."""
        fs, storage, root = backend_fs
        store = ArtifactStore(
            root, fs=fs, storage_options=storage.delta_storage_options()
        )
        assert store.load_original_names(["a" * 32]) == {}

    def test_null_original_name_omitted(self, backend_fs):
        """Rows with null original_name are silently omitted."""
        fs, storage, root = backend_fs
        opts = storage.delta_storage_options()
        store = ArtifactStore(root, fs=fs, storage_options=opts)

        ids = ["m1" + "a" * 30, "m2" + "b" * 30]
        index_data = {
            "artifact_id": ids,
            "artifact_type": ["metric", "metric"],
            "origin_step_number": [1, 1],
            "metadata": ["{}", "{}"],
        }
        pl.DataFrame(index_data, schema=ARTIFACT_INDEX_SCHEMA).write_delta(
            f"{root}/artifacts/index", storage_options=opts
        )

        metrics_data = {
            "artifact_id": ids,
            "origin_step_number": [1, 1],
            "content": [b"{}", b"{}"],
            "original_name": ["named.json", None],
            "extension": [".json", ".json"],
            "metadata": ["{}", "{}"],
        }
        pl.DataFrame(metrics_data, schema=METRICS_SCHEMA).write_delta(
            f"{root}/artifacts/metrics", storage_options=opts
        )

        result = store.load_original_names(ids)
        assert result == {ids[0]: "named.json"}


class TestArtifactStoreProvenanceQueries:
    """Tests for provenance and step number query methods."""

    @pytest.fixture
    def store_with_provenance(self, backend_fs):
        """Create an ArtifactStore with provenance data.

        Creates a simple chain: A -> B -> C
        Step numbers: A=0, B=1, C=2
        """
        fs, storage, root = backend_fs
        opts = storage.delta_storage_options()
        store = ArtifactStore(root, fs=fs, storage_options=opts)

        # Create artifact_index with test data
        index_data = {
            "artifact_id": ["a" * 32, "b" * 32, "c" * 32],
            "artifact_type": ["data", "data", "metric"],
            "origin_step_number": [0, 1, 2],
            "metadata": ["{}", "{}", "{}"],
        }
        pl.DataFrame(index_data).cast(ARTIFACT_INDEX_SCHEMA).write_delta(
            f"{root}/artifacts/index", storage_options=opts
        )

        # Create artifact_edges: A -> B -> C
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
        pl.DataFrame(prov_data).cast(ARTIFACT_EDGES_SCHEMA).write_delta(
            f"{root}/provenance/artifact_edges", storage_options=opts
        )

        return store

    def test_get_ancestor_artifact_ids_with_parent(self, store_with_provenance):
        """Artifact with parent returns parent ID."""
        result = store_with_provenance.provenance.get_direct_ancestors("b" * 32)
        assert result == ["a" * 32]

    def test_get_ancestor_artifact_ids_chain(self, store_with_provenance):
        """Artifact at end of chain returns immediate parent only."""
        result = store_with_provenance.provenance.get_direct_ancestors("c" * 32)
        assert result == ["b" * 32]

    def test_get_ancestor_artifact_ids_no_parent(self, store_with_provenance):
        """Root artifact returns empty list."""
        result = store_with_provenance.provenance.get_direct_ancestors("a" * 32)
        assert result == []

    def test_get_ancestor_artifact_ids_nonexistent(self, store_with_provenance):
        """Nonexistent artifact returns empty list."""
        result = store_with_provenance.provenance.get_direct_ancestors("z" * 32)
        assert result == []

    def test_get_ancestor_artifact_ids_no_table(self, backend_fs):
        """Missing provenance table returns empty list."""
        fs, storage, root = backend_fs
        store = ArtifactStore(
            root, fs=fs, storage_options=storage.delta_storage_options()
        )
        result = store.provenance.get_direct_ancestors("a" * 32)
        assert result == []

    def test_get_artifact_step_number(self, store_with_provenance):
        """Returns correct step number for each artifact."""
        assert store_with_provenance.provenance.get_artifact_step_number("a" * 32) == 0
        assert store_with_provenance.provenance.get_artifact_step_number("b" * 32) == 1
        assert store_with_provenance.provenance.get_artifact_step_number("c" * 32) == 2

    def test_get_artifact_step_number_nonexistent(self, store_with_provenance):
        """Returns None for nonexistent artifact."""
        assert (
            store_with_provenance.provenance.get_artifact_step_number("z" * 32) is None
        )

    def test_get_artifact_step_number_no_table(self, backend_fs):
        """Missing artifact_index table returns None."""
        fs, storage, root = backend_fs
        store = ArtifactStore(
            root, fs=fs, storage_options=storage.delta_storage_options()
        )
        assert store.provenance.get_artifact_step_number("a" * 32) is None


class TestMetricOriginalNamePersistence:
    """Tests for MetricArtifact.original_name persistence.

    Verifies that original_name is correctly stored and retrieved from Delta Lake.
    This was a bug fix - original_name existed in memory but wasn't persisted.
    """

    @pytest.fixture
    def store_with_metrics(self, backend_fs):
        """Create an ArtifactStore with metric data including original_name."""
        fs, storage, root = backend_fs
        opts = storage.delta_storage_options()
        store = ArtifactStore(root, fs=fs, storage_options=opts)

        artifacts = [
            _metric({"score": 0.95}, "sample_001_metrics"),
            _metric({"score": 0.85}, "sample_002_metrics"),
        ]
        df = pl.DataFrame(
            [artifact.to_row() for artifact in artifacts], schema=METRICS_SCHEMA
        )
        df.write_delta(f"{root}/artifacts/metrics", storage_options=opts)

        # Create artifact_index for lookups
        pl.DataFrame(_index_rows(artifacts), schema=ARTIFACT_INDEX_SCHEMA).write_delta(
            f"{root}/artifacts/index", storage_options=opts
        )

        return store, [artifact.artifact_id for artifact in artifacts]

    def test_metric_original_name_round_trip(self, store_with_metrics):
        """Metric with original_name preserves it after storage round-trip."""
        store, ids = store_with_metrics
        result = store.get_artifact(ids[0], artifact_type=ArtifactTypes.METRIC)

        assert result is not None
        assert result.original_name == "sample_001_metrics"  # Stem only

    def test_metric_second_original_name_round_trip(self, store_with_metrics):
        """Second metric preserves original_name after storage round-trip."""
        store, ids = store_with_metrics
        result = store.get_artifact(ids[1], artifact_type=ArtifactTypes.METRIC)

        assert result is not None
        assert result.original_name == "sample_002_metrics"


class TestExecutionConfigArtifactRoundTrip:
    """Tests for ExecutionConfigArtifact storage round-trip."""

    @pytest.fixture
    def store_with_configs(self, backend_fs):
        """Create store with configs table."""
        fs, storage, root = backend_fs
        opts = storage.delta_storage_options()
        store = ArtifactStore(root, fs=fs, storage_options=opts)

        artifact = _config(
            {"contig": "40-150,A8-10", "length": "175-275"},
            "5w3x_motif_0_config",
        )
        pl.DataFrame(_index_rows([artifact]), schema=ARTIFACT_INDEX_SCHEMA).write_delta(
            f"{root}/artifacts/index", storage_options=opts
        )
        pl.DataFrame([artifact.to_row()], schema=CONFIGS_SCHEMA).write_delta(
            f"{root}/artifacts/configs", storage_options=opts
        )

        return store, artifact.artifact_id

    def test_get_artifact_by_id_with_type(self, store_with_configs):
        """Can retrieve ExecutionConfigArtifact by ID with type hint."""
        store, artifact_id = store_with_configs
        artifact = store.get_artifact(artifact_id, artifact_type=ArtifactTypes.CONFIG)

        assert artifact is not None
        assert isinstance(artifact, ExecutionConfigArtifact)
        assert artifact.artifact_id == artifact_id
        assert artifact.original_name == "5w3x_motif_0_config"  # Stem only
        assert artifact.values["contig"] == "40-150,A8-10"
        assert artifact.values["length"] == "175-275"

    def test_get_artifact_by_id_without_type(self, store_with_configs):
        """Can retrieve ExecutionConfigArtifact by ID using artifact_index."""
        store, artifact_id = store_with_configs
        artifact = store.get_artifact(artifact_id)

        assert artifact is not None
        assert isinstance(artifact, ExecutionConfigArtifact)
        assert artifact.values["contig"] == "40-150,A8-10"

    def test_id_only_mode(self, store_with_configs):
        """Can retrieve ID-only ExecutionConfigArtifact."""
        store, artifact_id = store_with_configs
        artifact = store.get_artifact(
            artifact_id, artifact_type=ArtifactTypes.CONFIG, hydrate=False
        )

        assert artifact is not None
        assert isinstance(artifact, ExecutionConfigArtifact)
        assert artifact.artifact_id == artifact_id
        assert artifact.content is None
        assert not artifact.is_hydrated

    def test_original_name_persisted(self, store_with_configs):
        """original_name survives round-trip storage."""
        store, artifact_id = store_with_configs
        artifact = store.get_artifact(artifact_id, artifact_type=ArtifactTypes.CONFIG)

        assert artifact.original_name == "5w3x_motif_0_config"  # Stem only


class TestGetDescendantArtifactIds:
    """Tests for provenance.get_direct_descendants() forward provenance query."""

    @pytest.fixture
    def store_with_provenance(self, backend_fs):
        """Create ArtifactStore with provenance data for descendant queries.

        Graph: A -> B, A -> D, B -> C
        Types: A=data, B=data, C=metric, D=metric
        """
        fs, storage, root = backend_fs
        opts = storage.delta_storage_options()
        store = ArtifactStore(root, fs=fs, storage_options=opts)

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
        pl.DataFrame(prov_data).cast(ARTIFACT_EDGES_SCHEMA).write_delta(
            f"{root}/provenance/artifact_edges", storage_options=opts
        )

        return store

    def test_single_source_multiple_descendants(self, store_with_provenance):
        """Source A has two descendants: B and D."""
        result = store_with_provenance.provenance.get_direct_descendants({"a" * 32})

        assert "a" * 32 in result
        assert sorted(result["a" * 32]) == sorted(["b" * 32, "d" * 32])

    def test_type_filter_metric_only(self, store_with_provenance):
        """With METRIC filter, source A returns only D (metric), not B (data)."""
        result = store_with_provenance.provenance.get_direct_descendants(
            {"a" * 32}, target_artifact_type=ArtifactTypes.METRIC
        )

        assert result["a" * 32] == ["d" * 32]

    def test_leaf_node_no_descendants(self, store_with_provenance):
        """Leaf node C has no descendants."""
        result = store_with_provenance.provenance.get_direct_descendants({"c" * 32})
        assert result == {}

    def test_multiple_sources(self, store_with_provenance):
        """Query multiple sources at once."""
        result = store_with_provenance.provenance.get_direct_descendants(
            {"a" * 32, "b" * 32}
        )

        assert "a" * 32 in result
        assert "b" * 32 in result
        assert "c" * 32 in result["b" * 32]

    def test_empty_input(self, store_with_provenance):
        """Empty input returns empty dict without scanning."""
        result = store_with_provenance.provenance.get_direct_descendants(set())
        assert result == {}

    def test_missing_table(self, backend_fs):
        """Missing provenance table returns empty dict."""
        fs, storage, root = backend_fs
        store = ArtifactStore(
            root, fs=fs, storage_options=storage.delta_storage_options()
        )
        result = store.provenance.get_direct_descendants({"a" * 32})
        assert result == {}


class TestLoadArtifactTypeMap:
    """Tests for provenance.load_type_map() bulk type resolution."""

    @pytest.fixture
    def store_with_index(self, backend_fs):
        """Create store with artifact_index containing mixed types."""
        fs, storage, root = backend_fs
        opts = storage.delta_storage_options()
        store = ArtifactStore(root, fs=fs, storage_options=opts)
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
        ).write_delta(f"{root}/artifacts/index", storage_options=opts)
        return store

    def test_load_all(self, store_with_index):
        """Load all types when no filter provided."""
        result = store_with_index.provenance.load_type_map()
        assert len(result) == 4
        assert result["a" * 32] == "data"
        assert result["c" * 32] == "metric"
        assert result["d" * 32] == "config"

    def test_load_filtered(self, store_with_index):
        """Load only requested IDs."""
        result = store_with_index.provenance.load_type_map(["a" * 32, "c" * 32])
        assert len(result) == 2
        assert result["a" * 32] == "data"
        assert result["c" * 32] == "metric"

    def test_empty_index(self, backend_fs):
        """Missing index returns empty dict."""
        fs, storage, root = backend_fs
        store = ArtifactStore(
            root, fs=fs, storage_options=storage.delta_storage_options()
        )
        assert store.provenance.load_type_map() == {}

    def test_nonexistent_ids(self, store_with_index):
        """Non-existent IDs return empty dict."""
        result = store_with_index.provenance.load_type_map(["z" * 32])
        assert result == {}


class TestLoadArtifactIdsByType:
    """Tests for load_artifact_ids_by_type() filtered index query."""

    @pytest.fixture
    def store_with_index(self, backend_fs):
        """Create store with artifact_index containing mixed types and steps."""
        fs, storage, root = backend_fs
        opts = storage.delta_storage_options()
        store = ArtifactStore(root, fs=fs, storage_options=opts)
        pl.DataFrame(
            {
                "artifact_id": ["a" * 32, "b" * 32, "c" * 32, "d" * 32],
                "artifact_type": ["data", "data", "metric", "metric"],
                "origin_step_number": [0, 1, 2, 2],
                "metadata": ["{}", "{}", "{}", "{}"],
            },
            schema=ARTIFACT_INDEX_SCHEMA,
        ).write_delta(f"{root}/artifacts/index", storage_options=opts)
        return store

    def test_filter_by_type(self, store_with_index):
        """Returns only IDs of the requested type."""
        result = store_with_index.provenance.load_artifact_ids_by_type(
            ArtifactTypes.DATA
        )
        assert result == {"a" * 32, "b" * 32}

    def test_filter_by_type_and_step(self, store_with_index):
        """Filters by both type and step number."""
        result = store_with_index.provenance.load_artifact_ids_by_type(
            ArtifactTypes.DATA, step_numbers=[0]
        )
        assert result == {"a" * 32}

    def test_filter_by_type_and_ids(self, store_with_index):
        """Filters by type and specific artifact IDs."""
        result = store_with_index.provenance.load_artifact_ids_by_type(
            ArtifactTypes.METRIC, artifact_ids=["c" * 32]
        )
        assert result == {"c" * 32}

    def test_no_match(self, store_with_index):
        """Returns empty set when no match."""
        result = store_with_index.provenance.load_artifact_ids_by_type(
            ArtifactTypes.FILE_REF
        )
        assert result == set()

    def test_missing_index(self, backend_fs):
        """Missing index returns empty set."""
        fs, storage, root = backend_fs
        store = ArtifactStore(
            root, fs=fs, storage_options=storage.delta_storage_options()
        )
        result = store.provenance.load_artifact_ids_by_type(ArtifactTypes.DATA)
        assert result == set()


class TestLoadForwardProvenanceMap:
    """Tests for provenance.load_forward_map()."""

    @pytest.fixture
    def store_with_provenance(self, backend_fs):
        """Create store with provenance: A -> B, A -> D, B -> C."""
        fs, storage, root = backend_fs
        opts = storage.delta_storage_options()
        store = ArtifactStore(root, fs=fs, storage_options=opts)
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
        ).cast(ARTIFACT_EDGES_SCHEMA).write_delta(
            f"{root}/provenance/artifact_edges", storage_options=opts
        )
        return store

    def test_forward_map(self, store_with_provenance):
        """Returns {source: [targets]} mapping."""
        result = store_with_provenance.provenance.load_forward_map()
        assert sorted(result["a" * 32]) == sorted(["b" * 32, "d" * 32])
        assert result["b" * 32] == ["c" * 32]
        assert "c" * 32 not in result  # leaf has no outgoing edges

    def test_missing_table(self, backend_fs):
        """Missing provenance table returns empty dict."""
        fs, storage, root = backend_fs
        store = ArtifactStore(
            root, fs=fs, storage_options=storage.delta_storage_options()
        )
        assert store.provenance.load_forward_map() == {}


class TestLoadStepNameMap:
    """Tests for load_step_name_map()."""

    @pytest.fixture
    def store_with_steps(self, backend_fs):
        """Create store with steps table."""
        fs, storage, root = backend_fs
        opts = storage.delta_storage_options()
        store = ArtifactStore(root, fs=fs, storage_options=opts)
        ts = datetime(2025, 1, 1, tzinfo=UTC)
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
        ).write_delta(f"{root}/orchestration/steps", storage_options=opts)
        return store

    def test_loads_step_names(self, store_with_steps):
        """Returns step_number -> step_name mapping."""
        result = store_with_steps.provenance.load_step_name_map()
        assert result[0] == "ingest"
        assert result[1] == "tool_c"

    def test_missing_tables(self, backend_fs):
        """Missing tables returns empty dict."""
        fs, storage, root = backend_fs
        store = ArtifactStore(
            root, fs=fs, storage_options=storage.delta_storage_options()
        )
        assert store.provenance.load_step_name_map() == {}

    def test_fallback_to_executions(self, backend_fs):
        """Falls back to executions when steps table is missing."""
        fs, storage, root = backend_fs
        opts = storage.delta_storage_options()
        store = ArtifactStore(root, fs=fs, storage_options=opts)
        ts = datetime(2025, 1, 1, tzinfo=UTC)
        executions_df(
            execution_run_id=["e" * 32],
            execution_spec_id=["s" * 32],
            step_run_id=[None],
            origin_step_number=[0],
            operation_name=["ingest_fallback"],
            params=["{}"],
            user_overrides=["{}"],
            timestamp_start=[ts],
            timestamp_end=[ts],
            source_worker=[0],
            compute_backend=["local"],
            success=[True],
            error=[None],
            tool_output=[None],
            worker_log=[None],
            metadata=["{}"],
        ).write_delta(f"{root}/orchestration/executions", storage_options=opts)
        result = store.provenance.load_step_name_map()
        assert result[0] == "ingest_fallback"


class TestGetAssociated:
    """Tests for get_associated() provenance-based association lookup."""

    @pytest.fixture
    def store_with_associated_metrics(self, backend_fs):
        """Create store with source metrics, associated metrics, and provenance edges.

        Graph: S1 -> M1 (metric), S1 -> M2 (metric), S2 -> M3 (metric)
        S1 and S2 are source configs, M1/M2/M3 are derived metrics.
        """
        fs, storage, root = backend_fs
        opts = storage.delta_storage_options()
        store = ArtifactStore(root, fs=fs, storage_options=opts)
        sources = [
            _config({"source": 1}, "source_1").artifact_id,
            _config({"source": 2}, "source_2").artifact_id,
        ]
        artifacts = [
            _metric({"component": "A/CMP/1"}, "s1_metric_1", 1),
            _metric({"site": "A/SER/30"}, "s1_metric_2", 1),
            _metric({"pocket": "B/1-10"}, "s2_metric_1", 2),
        ]
        target_ids = [artifact.artifact_id for artifact in artifacts]

        # Create provenance edges: S1 -> M1, S1 -> M2, S2 -> M3
        pl.DataFrame(
            {
                "execution_run_id": ["x" * 32, "y" * 32, "z" * 32],
                "source_artifact_id": [
                    sources[0],
                    sources[0],
                    sources[1],
                ],
                "target_artifact_id": target_ids,
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
        ).cast(ARTIFACT_EDGES_SCHEMA).write_delta(
            f"{root}/provenance/artifact_edges", storage_options=opts
        )

        # Create metrics table
        pl.DataFrame(
            [artifact.to_row() for artifact in artifacts], schema=METRICS_SCHEMA
        ).write_delta(f"{root}/artifacts/metrics", storage_options=opts)

        return store, sources

    def test_returns_matching_descendants(self, store_with_associated_metrics):
        """Returns metrics associated with a config via provenance."""
        store, sources = store_with_associated_metrics
        result = store.get_associated({sources[0]}, "metric")

        assert sources[0] in result
        metrics = result[sources[0]]
        assert len(metrics) == 2
        val_sets = {frozenset(m.values.items()) for m in metrics}
        assert frozenset({("component", "A/CMP/1")}) in val_sets
        assert frozenset({("site", "A/SER/30")}) in val_sets

    def test_multiple_sources(self, store_with_associated_metrics):
        """Returns metrics for multiple configs at once."""
        store, sources = store_with_associated_metrics
        result = store.get_associated(set(sources), "metric")

        assert len(result) == 2
        assert len(result[sources[0]]) == 2
        assert len(result[sources[1]]) == 1

    def test_empty_when_no_edges(self, store_with_associated_metrics):
        """Returns empty dict when no provenance edges exist for the source."""
        store, _sources = store_with_associated_metrics
        result = store.get_associated({"z" * 32}, "metric")
        assert result == {}

    def test_filters_by_type(self, store_with_associated_metrics):
        """Only returns descendants of the requested type."""
        store, sources = store_with_associated_metrics
        result = store.get_associated({sources[0]}, "config")
        assert result == {}

    def test_empty_input(self, store_with_associated_metrics):
        """Empty input returns empty dict."""
        store, _sources = store_with_associated_metrics
        result = store.get_associated(set(), "metric")
        assert result == {}

    def test_missing_provenance_table(self, backend_fs):
        """Missing provenance table returns empty dict."""
        fs, storage, root = backend_fs
        store = ArtifactStore(
            root, fs=fs, storage_options=storage.delta_storage_options()
        )
        result = store.get_associated({"a" * 32}, "metric")
        assert result == {}


class TestArtifactStoreBackendParametrized:
    """Smoke tests for ArtifactStore parametrized over [local, s3] backends.

    Uses the ``backend_fs`` fixture from ``tests/artisan/storage/conftest.py``
    to exercise an end-to-end seed-and-query round-trip on both filesystems.
    S3 params skip cleanly when MinIO is unavailable.
    """

    def test_get_artifact_type_after_seed(self, backend_fs):
        """Seed artifact_index via DeltaCommitter, then query via ArtifactStore."""
        from artisan.schemas.enums import TablePath
        from artisan.storage.io.commit import DeltaCommitter
        from artisan.storage.io.staging import StagingManager

        fs, storage, root = backend_fs
        delta_root = f"{root}/delta"
        staging_root = f"{root}/staging"
        publish_test_store(
            delta_root,
            fs,
            storage.delta_storage_options(),
        )

        sm = StagingManager(staging_root, fs)
        committer = DeltaCommitter(
            delta_root,
            sm,
            fs=fs,
            storage_options=storage.delta_storage_options(),
        )

        index_df = pl.DataFrame(
            {
                "artifact_id": ["a" * 32],
                "artifact_type": ["metric"],
                "origin_step_number": [0],
                "metadata": ["{}"],
            },
            schema=ARTIFACT_INDEX_SCHEMA,
        )
        rows = committer.commit_dataframe(index_df, TablePath.ARTIFACT_INDEX.value)
        assert rows == 1

        # Verify the seeded table is readable via pl.read_delta directly.
        table_uri = f"{delta_root}/{TablePath.ARTIFACT_INDEX.value}"
        verify = pl.read_delta(
            table_uri, storage_options=storage.delta_storage_options()
        )
        assert verify.shape[0] == 1

        # Now exercise ArtifactStore against the same step_runner.
        store = ArtifactStore(
            delta_root,
            fs=fs,
            storage_options=storage.delta_storage_options(),
        )
        assert store.get_artifact_type("a" * 32) == "metric"
        assert store.artifact_exists("a" * 32) is True
        assert store.get_artifact_type("z" * 32) is None
