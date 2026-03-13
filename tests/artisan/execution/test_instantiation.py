"""Tests for execution/inputs/instantiation.py."""

from __future__ import annotations

from unittest.mock import MagicMock

from artisan.execution.inputs.instantiation import instantiate_inputs
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.specs.input_spec import InputSpec


def _make_metric_alt(artifact_id: str) -> MetricArtifact:
    """Create a minimal MetricArtifact for testing (alternative content)."""
    return MetricArtifact(
        artifact_id=artifact_id,
        origin_step_number=1,
        content=b'{"value": 1.0}',
    )


def _make_metric(artifact_id: str) -> MetricArtifact:
    """Create a minimal MetricArtifact for testing."""
    return MetricArtifact(
        artifact_id=artifact_id,
        origin_step_number=2,
        content=b'{"score": 0.5}',
        original_name="test_metric",
        extension=".json",
    )


class TestInstantiateInputs:
    """Tests for instantiate_inputs() with bulk loading."""

    def test_empty_inputs(self):
        """Empty inputs dict returns empty result."""
        store = MagicMock()
        store.provenance.load_type_map.return_value = {}
        result, associated = instantiate_inputs({}, store, {})
        assert result == {}
        assert associated == {}

    def test_hydrated_role(self):
        """Hydrated role uses bulk get_artifacts_by_type."""
        s_id = "s" * 32

        store = MagicMock()
        store.provenance.load_type_map.return_value = {s_id: ArtifactTypes.METRIC}
        store.get_artifacts_by_type.return_value = {s_id: _make_metric_alt(s_id)}

        spec = InputSpec(hydrate=True)
        result, _ = instantiate_inputs({"data": [s_id]}, store, {"data": spec})

        assert len(result["data"]) == 1
        assert result["data"][0].artifact_id == s_id
        store.get_artifacts_by_type.assert_called_once_with(
            [s_id], ArtifactTypes.METRIC
        )

    def test_non_hydrated_role(self):
        """Non-hydrated role creates ID-only artifacts via bulk cache (no get_artifact)."""
        s_id = "s" * 32

        store = MagicMock()
        store.provenance.load_type_map.return_value = {s_id: ArtifactTypes.METRIC}
        store.get_artifacts_by_type.return_value = {}

        spec = InputSpec(hydrate=False)
        result, _ = instantiate_inputs(
            {"passthrough": [s_id]}, store, {"passthrough": spec}
        )

        assert len(result["passthrough"]) == 1
        assert result["passthrough"][0].artifact_id == s_id
        store.get_artifact.assert_not_called()

    def test_mixed_roles(self):
        """Mix of hydrated and non-hydrated roles."""
        s_id = "s" * 32
        m_id = "m" * 32

        store = MagicMock()
        store.provenance.load_type_map.return_value = {
            s_id: ArtifactTypes.METRIC,
            m_id: ArtifactTypes.METRIC,
        }
        store.get_artifacts_by_type.return_value = {m_id: _make_metric(m_id)}

        specs = {
            "passthrough": InputSpec(hydrate=False),
            "metrics": InputSpec(hydrate=True),
        }
        result, _ = instantiate_inputs(
            {"passthrough": [s_id], "metrics": [m_id]}, store, specs
        )

        assert len(result["passthrough"]) == 1
        assert len(result["metrics"]) == 1
        store.get_artifact.assert_not_called()

    def test_missing_ids_skipped(self):
        """IDs not in type_map are skipped."""
        store = MagicMock()
        store.provenance.load_type_map.return_value = {}
        store.get_artifacts_by_type.return_value = {}

        result, _ = instantiate_inputs({"data": ["missing" + "0" * 25]}, store, {})

        assert result["data"] == []

    def test_missing_artifact_in_type_map_warns(self, caplog):
        """Artifact ID not in type_map logs a warning."""
        import logging

        store = MagicMock()
        store.provenance.load_type_map.return_value = {}
        store.get_artifacts_by_type.return_value = {}

        missing_id = "x" * 32
        with caplog.at_level(logging.WARNING):
            result, _ = instantiate_inputs(
                {"data": [missing_id]},
                store,
                {"data": InputSpec(hydrate=False)},
            )

        assert result["data"] == []
        assert "not found in type map" in caplog.text

    def test_artifact_load_failure_warns(self, caplog):
        """Hydrated artifact that misses bulk cache falls through to get_artifact."""
        import logging

        s_id = "s" * 32
        store = MagicMock()
        store.provenance.load_type_map.return_value = {s_id: ArtifactTypes.METRIC}
        # Hydrated bulk load returns empty — s_id missed the batch
        store.get_artifacts_by_type.return_value = {}
        store.get_artifact.return_value = None  # fallback also fails

        with caplog.at_level(logging.WARNING):
            result, _ = instantiate_inputs(
                {"data": [s_id]},
                store,
                {"data": InputSpec(hydrate=True)},
            )

        assert result["data"] == []
        assert "could not be loaded" in caplog.text

    def test_default_hydrate(self):
        """Roles without spec use default_hydrate."""
        s_id = "s" * 32

        store = MagicMock()
        store.provenance.load_type_map.return_value = {s_id: ArtifactTypes.METRIC}
        store.get_artifacts_by_type.return_value = {s_id: _make_metric_alt(s_id)}

        # No spec for "data" role, default_hydrate=True
        result, _ = instantiate_inputs(
            {"data": [s_id]}, store, {}, default_hydrate=True
        )

        assert len(result["data"]) == 1
        store.get_artifacts_by_type.assert_called_once()

    def test_default_hydrate_false(self):
        """default_hydrate=False creates ID-only artifacts via bulk cache."""
        s_id = "s" * 32

        store = MagicMock()
        store.provenance.load_type_map.return_value = {s_id: ArtifactTypes.METRIC}
        store.get_artifacts_by_type.return_value = {}

        result, _ = instantiate_inputs(
            {"data": [s_id]}, store, {}, default_hydrate=False
        )

        assert len(result["data"]) == 1
        assert result["data"][0].artifact_id == s_id
        store.get_artifact.assert_not_called()

    def test_bulk_type_resolution_single_call(self):
        """All IDs across roles are resolved in a single load_type_map call."""
        s_id = "s" * 32
        m_id = "m" * 32

        store = MagicMock()
        store.provenance.load_type_map.return_value = {
            s_id: ArtifactTypes.METRIC,
            m_id: ArtifactTypes.METRIC,
        }
        store.get_artifacts_by_type.return_value = {}

        instantiate_inputs(
            {"s": [s_id], "m": [m_id]},
            store,
            {"s": InputSpec(hydrate=False), "m": InputSpec(hydrate=False)},
        )

        # Single call with all IDs
        store.provenance.load_type_map.assert_called_once()
        call_args = store.provenance.load_type_map.call_args[0][0]
        assert set(call_args) == {s_id, m_id}
        store.get_artifact.assert_not_called()

    def test_non_hydrated_bulk_multiple_types(self):
        """Non-hydrated bulk cache handles multiple artifact types correctly."""
        from artisan.schemas.artifact.file_ref import FileRefArtifact

        m_id = "m" * 32
        f_id = "f" * 32

        store = MagicMock()
        store.provenance.load_type_map.return_value = {
            m_id: ArtifactTypes.METRIC,
            f_id: ArtifactTypes.FILE_REF,
        }
        store.get_artifacts_by_type.return_value = {}

        specs = {
            "metrics": InputSpec(hydrate=False),
            "files": InputSpec(hydrate=False),
        }
        result, _ = instantiate_inputs(
            {"metrics": [m_id], "files": [f_id]}, store, specs
        )

        assert len(result["metrics"]) == 1
        assert isinstance(result["metrics"][0], MetricArtifact)
        assert result["metrics"][0].artifact_id == m_id

        assert len(result["files"]) == 1
        assert isinstance(result["files"][0], FileRefArtifact)
        assert result["files"][0].artifact_id == f_id
        store.get_artifact.assert_not_called()

    def test_with_associated_calls_get_associated(self):
        """with_associated triggers get_associated() for each type."""
        from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact

        s_id = "s" * 32
        assoc = ExecutionConfigArtifact.draft(
            content={"key": "value"},
            original_name="test_config",
            step_number=1,
        ).finalize()

        store = MagicMock()
        store.provenance.load_type_map.return_value = {s_id: ArtifactTypes.METRIC}
        store.get_artifacts_by_type.return_value = {s_id: _make_metric_alt(s_id)}
        store.get_associated.return_value = {s_id: [assoc]}

        spec = InputSpec(
            artifact_type="metric",
            with_associated=("config",),
        )
        result, associated = instantiate_inputs(
            {"metric": [s_id]}, store, {"metric": spec}
        )

        assert len(result["metric"]) == 1
        store.get_associated.assert_called_once_with({s_id}, "config")
        assert (s_id, "config") in associated
        assert associated[(s_id, "config")] == [assoc]

    def test_no_with_associated_skips_get_associated(self):
        """Specs without with_associated don't call get_associated()."""
        s_id = "s" * 32

        store = MagicMock()
        store.provenance.load_type_map.return_value = {s_id: ArtifactTypes.METRIC}
        store.get_artifacts_by_type.return_value = {s_id: _make_metric_alt(s_id)}

        spec = InputSpec(artifact_type="data")
        result, associated = instantiate_inputs({"data": [s_id]}, store, {"data": spec})

        assert len(result["data"]) == 1
        store.get_associated.assert_not_called()
        assert associated == {}
