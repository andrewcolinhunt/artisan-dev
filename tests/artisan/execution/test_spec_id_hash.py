"""Tests for execution_spec_id computation."""

from __future__ import annotations

from artisan.utils.hashing import CacheInputIdentity
from artisan.utils.hashing import (
    compute_execution_spec_id as _compute_execution_spec_id,
)


def _typed_inputs(inputs: dict[str, list[str]]) -> dict[str, list[CacheInputIdentity]]:
    """Add the concrete type, role, group, and position cache dimensions."""
    return {
        role: [
            CacheInputIdentity(role, None, position, "metric", artifact_id)
            for position, artifact_id in enumerate(artifact_ids)
        ]
        for role, artifact_ids in inputs.items()
    }


def compute_execution_spec_id(
    *,
    operation_name: str,
    inputs: dict[str, list[str]],
    params=None,
    config_overrides=None,
) -> str:
    """Call the production cache hash with typed test identities."""
    return _compute_execution_spec_id(
        operation_name,
        _typed_inputs(inputs),
        params,
        config_overrides,
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

    def test_artifact_order_is_preserved(self):
        """Occurrence order within a role remains part of the cache identity."""
        spec1 = compute_execution_spec_id(
            operation_name="relax",
            inputs={"data": ["bbb" + "0" * 29, "aaa" + "0" * 29]},  # Unsorted
        )
        spec2 = compute_execution_spec_id(
            operation_name="relax",
            inputs={"data": ["aaa" + "0" * 29, "bbb" + "0" * 29]},  # Sorted
        )
        assert spec1 != spec2

    def test_multi_role_inputs_role_order_irrelevant(self):
        """Role-key ordering of the inputs dict doesn't affect the hash.

        Roles are sorted before hashing, so the dict-construction order
        is irrelevant. Role *assignment* of each artifact_id still matters
        — see ``test_execution_spec_id_differs_per_role_assignment``.
        """
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
        assert spec1 == spec2

    def test_execution_spec_id_differs_per_role_assignment(self):
        """Swapping which role an artifact_id belongs to changes the spec_id.

        Equal ID multisets with different role assignments represent
        different execution units.
        """
        a, b = "a" * 32, "b" * 32
        spec_a_primary = compute_execution_spec_id(
            operation_name="transform",
            inputs={"primary": [a], "reference": [b]},
        )
        spec_b_primary = compute_execution_spec_id(
            operation_name="transform",
            inputs={"primary": [b], "reference": [a]},
        )
        assert spec_a_primary != spec_b_primary

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

    def test_duplicate_artifact_ids_preserve_multiplicity(self):
        """Multiplicity within a role is preserved (not deduped) in the hash.

        ``[A, A]`` and ``[A]`` produce different spec_ids — repeated
        artifacts reflect the actual unit shape (e.g. aligned CROSS_PRODUCT
        primaries) rather than collapsing into a content-only fingerprint.
        """
        spec1 = compute_execution_spec_id(
            operation_name="relax",
            inputs={"data": ["a" * 32, "a" * 32]},
        )
        spec2 = compute_execution_spec_id(
            operation_name="relax",
            inputs={"data": ["a" * 32]},
        )
        assert spec1 != spec2

    def test_config_overrides_none_same_as_empty(self):
        """config_overrides=None produces same spec_id as empty dict."""
        spec_none = compute_execution_spec_id(
            operation_name="relax",
            inputs={},
            config_overrides=None,
        )
        spec_empty = compute_execution_spec_id(
            operation_name="relax",
            inputs={},
            config_overrides={},
        )
        assert spec_none == spec_empty

    def test_config_overrides_changes_hash(self):
        """Different config_overrides produce different spec_id."""
        spec1 = compute_execution_spec_id(
            operation_name="relax",
            inputs={"data": ["a" * 32]},
            config_overrides=None,
        )
        spec2 = compute_execution_spec_id(
            operation_name="relax",
            inputs={"data": ["a" * 32]},
            config_overrides={"image": "/path/to/image.sif"},
        )
        assert spec1 != spec2

    def test_config_overrides_deterministic(self):
        """Same config_overrides produce same spec_id."""
        kwargs = {
            "operation_name": "relax",
            "inputs": {"data": ["a" * 32]},
            "config_overrides": {"image": "/opt/image.sif", "gpu": True},
        }
        assert compute_execution_spec_id(**kwargs) == compute_execution_spec_id(
            **kwargs
        )

    def test_config_overrides_with_path_objects(self):
        """Path objects in config_overrides serialize correctly."""
        from pathlib import Path

        spec = compute_execution_spec_id(
            operation_name="relax",
            inputs={},
            config_overrides={"image": Path("/opt/containers/relax.sif")},
        )
        assert len(spec) == 32
