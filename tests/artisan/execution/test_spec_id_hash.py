"""Tests for execution_spec_id computation."""

from __future__ import annotations

from artisan.utils.hashing import (
    _canonicalize_dict,
    compute_execution_spec_id,
)


class TestComputeExecutionSpecId:
    """Tests for compute_execution_spec_id()."""

    def test_deterministic_output(self):
        """Same inputs produce same output."""
        spec1 = compute_execution_spec_id(
            operation_name="relax",
            inputs={"data": ["abc123" + "0" * 26, "def456" + "0" * 26]},
            params={"tolerance": 0.1},
        )
        spec2 = compute_execution_spec_id(
            operation_name="relax",
            inputs={"data": ["abc123" + "0" * 26, "def456" + "0" * 26]},
            params={"tolerance": 0.1},
        )
        assert spec1 == spec2
        assert len(spec1) == 32  # xxh3_128 hex length

    def test_artifact_ids_sorted(self):
        """Artifact IDs are sorted before hashing (order in list doesn't matter)."""
        spec1 = compute_execution_spec_id(
            operation_name="relax",
            inputs={"data": ["bbb" + "0" * 29, "aaa" + "0" * 29]},  # Unsorted
        )
        spec2 = compute_execution_spec_id(
            operation_name="relax",
            inputs={"data": ["aaa" + "0" * 29, "bbb" + "0" * 29]},  # Sorted
        )
        assert spec1 == spec2

    def test_multi_role_inputs_combined(self):
        """Artifact IDs from all roles are combined and sorted."""
        spec1 = compute_execution_spec_id(
            operation_name="transform",
            inputs={
                "primary": ["aaa" + "0" * 29],
                "reference": ["bbb" + "0" * 29],
            },
        )
        spec2 = compute_execution_spec_id(
            operation_name="transform",
            inputs={
                "reference": ["bbb" + "0" * 29],
                "primary": ["aaa" + "0" * 29],
            },
        )
        assert spec1 == spec2  # Role order doesn't matter

    def test_params_key_order_irrelevant(self):
        """Params dict key order doesn't affect hash."""
        spec1 = compute_execution_spec_id(
            operation_name="relax",
            inputs={},
            params={"a": 1, "b": 2},
        )
        spec2 = compute_execution_spec_id(
            operation_name="relax",
            inputs={},
            params={"b": 2, "a": 1},  # Different order
        )
        assert spec1 == spec2

    def test_different_operation_different_hash(self):
        """Different operation names produce different hashes."""
        spec1 = compute_execution_spec_id(
            operation_name="relax",
            inputs={"data": ["a" * 32]},
        )
        spec2 = compute_execution_spec_id(
            operation_name="minimize",
            inputs={"data": ["a" * 32]},
        )
        assert spec1 != spec2

    def test_different_artifacts_different_hash(self):
        """Different artifact IDs produce different hashes."""
        spec1 = compute_execution_spec_id(
            operation_name="relax",
            inputs={"data": ["a" * 32]},
        )
        spec2 = compute_execution_spec_id(
            operation_name="relax",
            inputs={"data": ["b" * 32]},
        )
        assert spec1 != spec2

    def test_empty_inputs(self):
        """Handles empty inputs gracefully (generative ops)."""
        spec = compute_execution_spec_id(
            operation_name="generate",
            inputs={},
            params=None,
        )
        assert len(spec) == 32

    def test_different_params_different_hash(self):
        """Different merged params produce different spec_id."""
        spec1 = compute_execution_spec_id(
            operation_name="relax",
            inputs={},
            params={"tolerance": 0.1},
        )
        spec2 = compute_execution_spec_id(
            operation_name="relax",
            inputs={},
            params={"tolerance": 0.2},
        )
        assert spec1 != spec2

    def test_duplicate_artifact_ids_deduplicated(self):
        """Duplicate artifact IDs are deduplicated before hashing."""
        spec1 = compute_execution_spec_id(
            operation_name="relax",
            inputs={"data": ["a" * 32, "a" * 32]},  # Duplicate
        )
        spec2 = compute_execution_spec_id(
            operation_name="relax",
            inputs={"data": ["a" * 32]},  # Single
        )
        assert spec1 == spec2

    def test_command_overrides_none_same_as_empty(self):
        """command_overrides=None produces same spec_id as empty dict."""
        spec_none = compute_execution_spec_id(
            operation_name="relax",
            inputs={},
            command_overrides=None,
        )
        spec_empty = compute_execution_spec_id(
            operation_name="relax",
            inputs={},
            command_overrides={},
        )
        assert spec_none == spec_empty

    def test_command_overrides_changes_hash(self):
        """Different command_overrides produce different spec_id."""
        spec1 = compute_execution_spec_id(
            operation_name="relax",
            inputs={"data": ["a" * 32]},
            command_overrides=None,
        )
        spec2 = compute_execution_spec_id(
            operation_name="relax",
            inputs={"data": ["a" * 32]},
            command_overrides={"image": "/path/to/image.sif"},
        )
        assert spec1 != spec2

    def test_command_overrides_deterministic(self):
        """Same command_overrides produce same spec_id."""
        kwargs = {
            "operation_name": "relax",
            "inputs": {"data": ["a" * 32]},
            "command_overrides": {"image": "/opt/image.sif", "gpu": True},
        }
        assert compute_execution_spec_id(**kwargs) == compute_execution_spec_id(
            **kwargs
        )

    def test_command_overrides_with_path_objects(self):
        """Path objects in command_overrides serialize correctly."""
        from pathlib import Path

        spec = compute_execution_spec_id(
            operation_name="relax",
            inputs={},
            command_overrides={"image": Path("/opt/containers/relax.sif")},
        )
        assert len(spec) == 32


class TestCanonicalizeDict:
    """Tests for _canonicalize_dict()."""

    def test_none_returns_empty(self):
        assert _canonicalize_dict(None) == ""

    def test_empty_dict_returns_empty(self):
        assert _canonicalize_dict({}) == ""

    def test_sorted_keys(self):
        result = _canonicalize_dict({"b": 1, "a": 2})
        assert result == '{"a":2,"b":1}'

    def test_nested_dict_sorted(self):
        result = _canonicalize_dict({"outer": {"b": 1, "a": 2}})
        assert result == '{"outer":{"a":2,"b":1}}'

    def test_no_whitespace(self):
        result = _canonicalize_dict({"key": "value"})
        assert " " not in result

    def test_set_values_serialized_as_sorted_list(self):
        """Sets should be serialized as sorted lists for determinism."""
        result = _canonicalize_dict({"names": {"cherry", "apple", "banana"}})
        assert result == '{"names":["apple","banana","cherry"]}'

    def test_set_values_deterministic(self):
        """Same set produces same hash regardless of iteration order."""
        r1 = _canonicalize_dict({"s": {"b", "a", "c"}})
        r2 = _canonicalize_dict({"s": {"c", "a", "b"}})
        assert r1 == r2

    def test_path_values_serialized_as_strings(self):
        """Path objects should be serialized as strings."""
        from pathlib import Path

        result = _canonicalize_dict({"image": Path("/opt/containers/tool_c.sif")})
        assert result == '{"image":"/opt/containers/tool_c.sif"}'

    def test_nested_path_in_list(self):
        """Path objects nested in lists should be serialized as strings."""
        from pathlib import Path

        result = _canonicalize_dict({"binds": [[Path("/src"), Path("/dst")]]})
        assert '"/src"' in result
        assert '"/dst"' in result
