"""Tests for artifacts.py"""

from __future__ import annotations

import json
import os

import pytest
from pydantic import ValidationError

from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.types import ArtifactTypes


class TestMetricArtifact:
    """Tests for MetricArtifact."""

    def test_create_valid_metric(self):
        """Create a valid metric artifact."""
        content = json.dumps({"score": 0.95, "confidence": 85.2}).encode()
        artifact = MetricArtifact(
            artifact_id="d" * 32,
            origin_step_number=1,
            content=content,
        )
        assert artifact.artifact_type == ArtifactTypes.METRIC

    def test_content_is_bytes(self):
        """Content must be bytes, not dict."""
        with pytest.raises(ValidationError):
            MetricArtifact(
                artifact_id="e" * 32,
                origin_step_number=1,
                content={"score": 0.95},  # Wrong type
            )


class TestFileRefArtifact:
    """Tests for FileRefArtifact."""

    def test_create_valid_file_ref(self):
        """Create a valid file ref artifact."""
        artifact = FileRefArtifact(
            artifact_id="j" * 32,
            origin_step_number=0,
            content_hash="k" * 32,
            path="/data/input.dat",
            size_bytes=1024,
        )
        assert artifact.artifact_type == ArtifactTypes.FILE_REF
        assert artifact.path == "/data/input.dat"
        assert artifact.content_hash == "k" * 32

    def test_no_content_field(self):
        """File refs have no content field."""
        assert "content" not in FileRefArtifact.model_fields

    def test_step_is_typically_zero(self):
        """File refs are typically step 0 (ingestion)."""
        artifact = FileRefArtifact(
            artifact_id="l" * 32,
            origin_step_number=0,
            content_hash="m" * 32,
            path="/data/input.dat",
            size_bytes=1024,
        )
        assert artifact.origin_step_number == 0


# =============================================================================
# Draft/Finalize Pattern Tests
# =============================================================================


class TestMetricArtifactDraftFinalize:
    """Tests for MetricArtifact draft/finalize pattern."""

    def test_draft_creates_artifact_from_dict(self):
        """draft() creates artifact from dict, encoding to JSON bytes."""
        artifact = MetricArtifact.draft(
            content={"score": 0.95, "confidence": 85.2},
            original_name="metrics.json",
            step_number=1,
        )
        assert artifact.artifact_id is None
        assert artifact.is_draft is True
        # Content should be JSON-encoded bytes
        decoded = json.loads(artifact.content.decode("utf-8"))
        assert decoded == {"confidence": 85.2, "score": 0.95}  # Sorted keys

    def test_draft_with_metadata(self):
        """draft() accepts optional metadata."""
        artifact = MetricArtifact.draft(
            content={"value": 1},
            original_name="test.json",
            step_number=0,
            metadata={"unit": "kcal/mol"},
        )
        assert artifact.metadata == {"unit": "kcal/mol"}

    def test_finalize_computes_artifact_id(self):
        """finalize() computes artifact_id from JSON content."""
        artifact = MetricArtifact.draft(
            content={"energy": -100.5},
            original_name="energy.json",
            step_number=1,
        )
        finalized = artifact.finalize()

        assert finalized.artifact_id is not None
        assert len(finalized.artifact_id) == 32
        assert finalized.is_finalized is True

    def test_sorted_keys_ensures_deterministic_hashing(self):
        """Dict key order doesn't affect artifact_id (keys are sorted)."""
        # Create with different key orders
        artifact1 = MetricArtifact.draft(
            content={"b": 2, "a": 1},
            original_name="test.json",
            step_number=1,
        ).finalize()
        artifact2 = MetricArtifact.draft(
            content={"a": 1, "b": 2},
            original_name="test.json",
            step_number=1,
        ).finalize()

        assert artifact1.artifact_id == artifact2.artifact_id


class TestFileRefArtifactFinalize:
    """Tests for FileRefArtifact finalize() behavior."""

    def test_finalize_computes_artifact_id_when_none(self):
        """FileRefArtifact.finalize() computes artifact_id using two-step hash."""
        artifact = FileRefArtifact(
            artifact_id=None,
            origin_step_number=0,
            content_hash="k" * 32,
            path="/data/input.dat",
            size_bytes=1024,
        )

        finalized = artifact.finalize()
        assert finalized is artifact
        assert finalized.artifact_id is not None
        assert len(finalized.artifact_id) == 32  # 32-char hex

    def test_finalize_succeeds_when_artifact_id_present(self):
        """FileRefArtifact.finalize() succeeds when artifact_id is set."""
        artifact = FileRefArtifact(
            artifact_id="e" * 32,
            origin_step_number=0,
            content_hash="k" * 32,
            path="/data/input.dat",
            size_bytes=1024,
        )

        finalized = artifact.finalize()
        assert finalized is artifact
        assert finalized.artifact_id == "e" * 32

    def test_draft_creates_with_none_artifact_id(self):
        """FileRefArtifact.draft() creates with artifact_id=None."""
        draft = FileRefArtifact.draft(
            path="/data/input.dat",
            content_hash="k" * 32,
            size_bytes=1024,
            step_number=0,
        )

        assert draft.artifact_id is None
        assert draft.is_draft is True

    def test_draft_then_finalize_computes_artifact_id(self):
        """FileRefArtifact.draft() then finalize() computes artifact_id."""
        draft = FileRefArtifact.draft(
            path="/data/input.dat",
            content_hash="k" * 32,
            size_bytes=1024,
            step_number=0,
        )
        finalized = draft.finalize()

        assert finalized.artifact_id is not None
        assert finalized.is_finalized is True
        assert len(finalized.artifact_id) == 32

    def test_same_metadata_produces_same_artifact_id(self):
        """Same content_hash, path, size_bytes produces same artifact_id."""
        draft1 = FileRefArtifact.draft(
            path="/data/input.dat",
            content_hash="k" * 32,
            size_bytes=1024,
            step_number=0,
        ).finalize()
        draft2 = FileRefArtifact.draft(
            path="/data/input.dat",
            content_hash="k" * 32,
            size_bytes=1024,
            step_number=5,  # Different step_number
        ).finalize()

        assert draft1.artifact_id == draft2.artifact_id

    def test_different_path_produces_different_artifact_id(self):
        """Different path produces different artifact_id (path is part of identity)."""
        draft1 = FileRefArtifact.draft(
            path="/data/input.dat",
            content_hash="k" * 32,
            size_bytes=1024,
            step_number=0,
        ).finalize()
        draft2 = FileRefArtifact.draft(
            path="/data/other.dat",  # Different path
            content_hash="k" * 32,
            size_bytes=1024,
            step_number=0,
        ).finalize()

        assert draft1.artifact_id != draft2.artifact_id


class TestMetricArtifactMaterializeFormat:
    """Tests for MetricArtifact format rejection."""

    def test_materialize_rejects_format(self, tmp_path):
        """materialize_to() raises ValueError when format is set."""
        artifact = MetricArtifact.draft(
            content={"score": 0.5},
            original_name="test.json",
            step_number=0,
        )
        with pytest.raises(ValueError, match="does not support format conversion"):
            artifact.materialize_to(str(tmp_path), format=".csv")


class TestExecutionConfigArtifactMaterializeFormat:
    """Tests for ExecutionConfigArtifact format rejection."""

    def test_materialize_rejects_format(self, tmp_path):
        """materialize_to() raises ValueError when format is set."""
        artifact = ExecutionConfigArtifact.draft(
            content={"contig": "40-150"},
            original_name="config.json",
            step_number=1,
        )
        with pytest.raises(ValueError, match="does not support format conversion"):
            artifact.materialize_to(str(tmp_path), format=".yaml")


class TestMaterializedPath:
    """Tests for materialized_path field."""

    def test_materialized_path_default_none(self):
        """materialized_path defaults to None."""
        artifact = MetricArtifact.draft(
            content={"score": 0.5},
            original_name="test.json",
            step_number=0,
        )
        assert artifact.materialized_path is None

    def test_materialized_path_can_be_set(self):
        """materialized_path can be set."""
        from pathlib import Path

        artifact = MetricArtifact.draft(
            content={"score": 0.5},
            original_name="test.json",
            step_number=0,
        )
        artifact.materialized_path = Path("/tmp/workdir/test.json")
        assert artifact.materialized_path == Path("/tmp/workdir/test.json")

    def test_materialized_path_excluded_from_serialization(self):
        """materialized_path is excluded from model serialization."""
        from pathlib import Path

        artifact = MetricArtifact.draft(
            content={"score": 0.5},
            original_name="test.json",
            step_number=0,
        )
        artifact.materialized_path = Path("/tmp/workdir/test.json")

        # Serialize to dict
        data = artifact.model_dump()

        # materialized_path should not be in the serialized data
        assert "materialized_path" not in data


# =============================================================================
# ExecutionConfigArtifact Tests
# =============================================================================


class TestExecutionConfigArtifact:
    """Tests for ExecutionConfigArtifact."""

    def test_create_valid_execution_config(self):
        """ExecutionConfigArtifact can be created with valid content."""
        content = json.dumps({"contig": "40-150,A8-10"}).encode("utf-8")
        artifact = ExecutionConfigArtifact(
            artifact_id="a" * 32,
            origin_step_number=1,
            content=content,
            original_name="config.json",
        )
        assert artifact.artifact_id == "a" * 32
        assert artifact.artifact_type == ArtifactTypes.CONFIG
        assert artifact.content == content

    def test_content_is_bytes(self):
        """Content must be bytes, not dict."""
        with pytest.raises(ValidationError):
            ExecutionConfigArtifact(
                artifact_id="a" * 32,
                origin_step_number=1,
                content={"contig": "40-150"},  # Should be bytes
            )


class TestExecutionConfigArtifactDraftFinalize:
    """Tests for ExecutionConfigArtifact draft/finalize pattern."""

    def test_draft_creates_artifact_from_dict(self):
        """draft() accepts dict, internally encodes to JSON bytes."""
        artifact = ExecutionConfigArtifact.draft(
            content={"contig": "40-150,A8-10", "length": "175-275"},
            original_name="5w3x_motif_0_config.json",
            step_number=1,
        )
        assert artifact.is_draft is True
        assert artifact.artifact_id is None
        assert artifact.content is not None

        # Verify content is valid JSON
        decoded = json.loads(artifact.content.decode("utf-8"))
        assert decoded["contig"] == "40-150,A8-10"
        assert decoded["length"] == "175-275"

    def test_sorted_keys_ensures_deterministic_hashing(self):
        """Keys are sorted for deterministic artifact IDs."""
        content1 = {"z_key": "z", "a_key": "a"}
        content2 = {"a_key": "a", "z_key": "z"}

        artifact1 = ExecutionConfigArtifact.draft(
            content=content1, original_name="test.json", step_number=1
        ).finalize()
        artifact2 = ExecutionConfigArtifact.draft(
            content=content2, original_name="test.json", step_number=1
        ).finalize()

        assert artifact1.artifact_id == artifact2.artifact_id

    def test_finalize_computes_artifact_id(self):
        """finalize() generates content-addressed ID."""
        artifact = ExecutionConfigArtifact.draft(
            content={"contig": "40-150"},
            original_name="config.json",
            step_number=1,
        )
        assert artifact.artifact_id is None

        artifact.finalize()

        assert artifact.artifact_id is not None
        assert len(artifact.artifact_id) == 32

    def test_finalize_is_idempotent(self):
        """Re-finalizing doesn't change artifact_id."""
        artifact = ExecutionConfigArtifact.draft(
            content={"contig": "40-150"},
            original_name="config.json",
            step_number=1,
        )
        artifact.finalize()
        first_id = artifact.artifact_id

        artifact.finalize()
        assert artifact.artifact_id == first_id

    def test_values_property_lazy_decode(self):
        """values property lazily decodes JSON."""
        artifact = ExecutionConfigArtifact.draft(
            content={"contig": "40-150", "ori_token": [1.0, 2.0, 3.0]},
            original_name="config.json",
            step_number=1,
        )

        values = artifact.values
        assert values["contig"] == "40-150"
        assert values["ori_token"] == [1.0, 2.0, 3.0]

    def test_values_raises_when_not_hydrated(self):
        """values property raises when content is None."""
        artifact = ExecutionConfigArtifact(
            artifact_id="a" * 32,
            artifact_type=ArtifactTypes.CONFIG,
        )
        with pytest.raises(ValueError, match="not hydrated"):
            _ = artifact.values

    def test_different_content_produces_different_artifact_id(self):
        """Different content produces different artifact IDs."""
        artifact1 = ExecutionConfigArtifact.draft(
            content={"contig": "40-150"},
            original_name="config.json",
            step_number=1,
        ).finalize()
        artifact2 = ExecutionConfigArtifact.draft(
            content={"contig": "50-200"},
            original_name="config.json",
            step_number=1,
        ).finalize()

        assert artifact1.artifact_id != artifact2.artifact_id

    def test_draft_with_metadata(self):
        """draft() accepts optional metadata."""
        artifact = ExecutionConfigArtifact.draft(
            content={"contig": "40-150"},
            original_name="config.json",
            step_number=1,
            metadata={"tool": "tool_c"},
        )
        assert artifact.metadata == {"tool": "tool_c"}


class TestExecutionConfigArtifactMaterialize:
    """Tests for ExecutionConfigArtifact materialization."""

    def test_materialize_to_writes_json(self, tmp_path):
        """materialize_to() writes content to disk using artifact_id."""
        artifact = ExecutionConfigArtifact.draft(
            content={"contig": "40-150", "length": "175-275"},
            original_name="5w3x_motif_0_config.json",
            step_number=1,
        ).finalize()

        directory = str(tmp_path)
        path = artifact.materialize_to(directory)

        expected = os.path.join(directory, f"{artifact.artifact_id}.json")
        assert path == expected
        assert os.path.exists(path)
        assert artifact.materialized_path == path

        # Verify content
        with open(path) as f:
            content = json.loads(f.read())
        assert content["contig"] == "40-150"

    def test_materialize_raises_when_not_hydrated(self, tmp_path):
        """materialize_to() raises when content is None."""
        artifact = ExecutionConfigArtifact(
            artifact_id="a" * 32,
            artifact_type=ArtifactTypes.CONFIG,
        )
        with pytest.raises(ValueError, match="not hydrated"):
            artifact.materialize_to(str(tmp_path))

    def test_materialize_uses_artifact_id(self, tmp_path):
        """materialize_to() always uses artifact_id for the filename."""
        content = json.dumps({"contig": "40-150"}).encode("utf-8")
        artifact = ExecutionConfigArtifact(
            artifact_id="a" * 32,
            origin_step_number=1,
            content=content,
            original_name=None,
        )

        directory = str(tmp_path)
        path = artifact.materialize_to(directory)

        expected = os.path.join(directory, f"{'a' * 32}.json")
        assert path == expected
        assert os.path.exists(path)


# =============================================================================
# Artifact Reference Tests
# =============================================================================


class TestFindArtifactReferences:
    """Tests for find_artifact_references() helper function."""

    def test_simple_reference(self):
        """Single reference in flat dict is found."""
        from artisan.schemas.artifact.execution_config import find_artifact_references

        obj = {"input_pdb": {"$artifact": "abc123"}, "contig": "40-150"}
        refs = find_artifact_references(obj)
        assert refs == ["abc123"]

    def test_nested_references(self):
        """References in nested structure are found."""
        from artisan.schemas.artifact.execution_config import find_artifact_references

        obj = {
            "inputs": [
                {"path": {"$artifact": "id1"}, "chain": "A"},
                {"path": {"$artifact": "id2"}, "chain": "B"},
            ],
            "params": {"template": {"$artifact": "id3"}},
        }
        refs = find_artifact_references(obj)
        assert set(refs) == {"id1", "id2", "id3"}

    def test_multiple_references(self):
        """Multiple references in same level are found."""
        from artisan.schemas.artifact.execution_config import find_artifact_references

        obj = {
            "input_pdb": {"$artifact": "struct_id"},
            "reference_pdb": {"$artifact": "ref_id"},
            "force_field": {"$artifact": "ff_id"},
        }
        refs = find_artifact_references(obj)
        assert set(refs) == {"struct_id", "ref_id", "ff_id"}

    def test_no_references(self):
        """Config with no references returns empty list."""
        from artisan.schemas.artifact.execution_config import find_artifact_references

        obj = {"num_designs": 10, "length": "100-150", "nested": {"key": "value"}}
        refs = find_artifact_references(obj)
        assert refs == []

    def test_not_artifact_marker(self):
        """Dict with $artifact key but other keys is NOT a reference."""
        from artisan.schemas.artifact.execution_config import find_artifact_references

        # This dict has $artifact but also another key, so it's not a reference
        obj = {"$artifact": "abc123", "extra_key": "value"}
        refs = find_artifact_references(obj)
        assert refs == []

    def test_reference_in_list(self):
        """Reference inside a list is found."""
        from artisan.schemas.artifact.execution_config import find_artifact_references

        obj = {"items": [{"$artifact": "id1"}, {"$artifact": "id2"}]}
        refs = find_artifact_references(obj)
        assert refs == ["id1", "id2"]

    def test_primitive_values_ignored(self):
        """Primitive values don't cause errors."""
        from artisan.schemas.artifact.execution_config import find_artifact_references

        obj = {"string": "value", "number": 42, "bool": True, "null": None}
        refs = find_artifact_references(obj)
        assert refs == []


class TestSubstituteReferences:
    """Tests for substitute_references() helper function."""

    def test_simple_substitution(self):
        """Single reference is substituted."""
        from artisan.schemas.artifact.execution_config import substitute_references

        obj = {"input_pdb": {"$artifact": "abc123"}}
        id_to_path = {"abc123": "/work/step_1/data.dat"}
        result = substitute_references(obj, id_to_path)
        assert result == {"input_pdb": "/work/step_1/data.dat"}

    def test_nested_substitution(self):
        """Nested references are substituted."""
        from artisan.schemas.artifact.execution_config import substitute_references

        obj = {
            "inputs": [{"path": {"$artifact": "id1"}}],
            "template": {"$artifact": "id2"},
        }
        id_to_path = {"id1": "/path/1.dat", "id2": "/path/2.dat"}
        result = substitute_references(obj, id_to_path)
        assert result == {
            "inputs": [{"path": "/path/1.dat"}],
            "template": "/path/2.dat",
        }

    def test_missing_path_raises(self):
        """Missing path raises ValueError."""
        from artisan.schemas.artifact.execution_config import substitute_references

        obj = {"input": {"$artifact": "missing_id"}}
        id_to_path = {}
        with pytest.raises(ValueError, match="No path for referenced artifact"):
            substitute_references(obj, id_to_path)

    def test_preserves_non_references(self):
        """Non-reference content is preserved unchanged."""
        from artisan.schemas.artifact.execution_config import substitute_references

        obj = {
            "contig": "40-150",
            "length": 175,
            "nested": {"key": "value"},
            "list": [1, 2, 3],
        }
        result = substitute_references(obj, {})
        assert result == obj

    def test_mixed_content(self):
        """References and non-references are handled together."""
        from artisan.schemas.artifact.execution_config import substitute_references

        obj = {
            "input_pdb": {"$artifact": "abc123"},
            "contig": "40-150",
            "length": 175,
        }
        id_to_path = {"abc123": "/path/to/file.dat"}
        result = substitute_references(obj, id_to_path)
        assert result == {
            "input_pdb": "/path/to/file.dat",
            "contig": "40-150",
            "length": 175,
        }


class TestGetArtifactReferences:
    """Tests for ExecutionConfigArtifact.get_artifact_references() method."""

    def test_returns_references(self):
        """get_artifact_references() returns list of referenced IDs."""
        artifact = ExecutionConfigArtifact.draft(
            content={"input_pdb": {"$artifact": "abc123"}, "contig": "40-150"},
            original_name="config.json",
            step_number=1,
        ).finalize()

        refs = artifact.get_artifact_references()
        assert refs == ["abc123"]

    def test_returns_empty_for_no_refs(self):
        """get_artifact_references() returns empty list when no references."""
        artifact = ExecutionConfigArtifact.draft(
            content={"contig": "40-150", "length": 175},
            original_name="config.json",
            step_number=1,
        ).finalize()

        refs = artifact.get_artifact_references()
        assert refs == []

    def test_caches_result(self):
        """get_artifact_references() caches the result."""
        artifact = ExecutionConfigArtifact.draft(
            content={"input_pdb": {"$artifact": "abc123"}},
            original_name="config.json",
            step_number=1,
        ).finalize()

        refs1 = artifact.get_artifact_references()
        refs2 = artifact.get_artifact_references()
        assert refs1 is refs2  # Same object (cached)

    def test_returns_empty_when_content_none(self):
        """get_artifact_references() returns empty when content is None."""
        artifact = ExecutionConfigArtifact(
            artifact_id="a" * 32,
            artifact_type=ArtifactTypes.CONFIG,
            origin_step_number=1,
            content=None,
        )

        refs = artifact.get_artifact_references()
        assert refs == []


class TestExecutionConfigArtifactMaterializeWithRefs:
    """Tests for ExecutionConfigArtifact.materialize_to() with artifact references."""

    def test_materialize_without_refs(self, tmp_path):
        """Config without references materializes normally."""
        artifact = ExecutionConfigArtifact.draft(
            content={"num_designs": 10, "length": "100-150"},
            original_name="config.json",
            step_number=1,
        ).finalize()

        path = artifact.materialize_to(str(tmp_path))

        assert os.path.exists(path)
        with open(path) as f:
            content = json.loads(f.read())
        assert content == {"length": "100-150", "num_designs": 10}

    def test_materialize_with_refs_substitutes_paths(self, tmp_path):
        """Config with references substitutes paths when resolved_paths provided."""

        artifact = ExecutionConfigArtifact.draft(
            content={"input_pdb": {"$artifact": "abc123"}, "contig": "40-150"},
            original_name="config.json",
            step_number=1,
        ).finalize()

        data_path = tmp_path / "data.dat"
        data_path.write_text("ATOM...")

        resolved_paths = {"abc123": str(data_path)}
        path = artifact.materialize_to(str(tmp_path), resolved_paths=resolved_paths)

        assert os.path.exists(path)
        with open(path) as f:
            content = json.loads(f.read())
        assert content["input_pdb"] == str(data_path)
        assert content["contig"] == "40-150"

    def test_materialize_with_refs_no_mapping_writes_as_is(self, tmp_path):
        """Config with refs but no mapping writes content as-is."""
        artifact = ExecutionConfigArtifact.draft(
            content={"input_pdb": {"$artifact": "abc123"}},
            original_name="config.json",
            step_number=1,
        ).finalize()

        path = artifact.materialize_to(str(tmp_path))  # No resolved_paths

        assert os.path.exists(path)
        with open(path) as f:
            content = json.loads(f.read())
        assert content["input_pdb"] == {"$artifact": "abc123"}  # Unchanged

    def test_materialize_missing_ref_raises(self, tmp_path):
        """Missing ref in mapping raises ValueError."""
        artifact = ExecutionConfigArtifact.draft(
            content={"input": {"$artifact": "missing_id"}},
            original_name="config.json",
            step_number=1,
        ).finalize()

        with pytest.raises(ValueError, match="Missing paths for referenced artifacts"):
            artifact.materialize_to(str(tmp_path), resolved_paths={})

    def test_materialize_multiple_refs(self, tmp_path):
        """Multiple references are all substituted."""
        artifact = ExecutionConfigArtifact.draft(
            content={
                "input_pdb": {"$artifact": "id1"},
                "reference_pdb": {"$artifact": "id2"},
                "force_field": {"$artifact": "id3"},
            },
            original_name="config.json",
            step_number=1,
        ).finalize()

        resolved_paths = {
            "id1": "/path/to/input.dat",
            "id2": "/path/to/reference.dat",
            "id3": "/path/to/ff.dat",
        }
        path = artifact.materialize_to(str(tmp_path), resolved_paths=resolved_paths)

        with open(path) as f:
            content = json.loads(f.read())
        assert content["input_pdb"] == "/path/to/input.dat"
        assert content["reference_pdb"] == "/path/to/reference.dat"
        assert content["force_field"] == "/path/to/ff.dat"
