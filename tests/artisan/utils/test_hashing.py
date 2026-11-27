"""Tests for hashing utilities."""

from __future__ import annotations

import tracemalloc
from io import BytesIO
from typing import Any

import pytest

from artisan.utils.hashing import (
    AssociatedInputIdentity,
    CacheInputIdentity,
    compute_artifact_id,
    compute_content_digest,
    compute_execution_spec_id,
    compute_step_spec_id,
    compute_stream_digest,
    serialize_params,
)


def test_association_types_and_empty_membership_change_both_cache_keys() -> None:
    """An empty captured type differs from no declaration or another type."""
    memberships = [
        (),
        (AssociatedInputIdentity("metric", ()),),
        (AssociatedInputIdentity("config", ()),),
        (AssociatedInputIdentity("metric", ("b" * 32,)),),
    ]
    inputs = [
        {"data": [CacheInputIdentity("data", None, 0, "csv", "a" * 32, membership)]}
        for membership in memberships
    ]
    assert len({compute_execution_spec_id("consumer", item) for item in inputs}) == 4
    assert len({compute_step_spec_id("consumer", 1, {}, item) for item in inputs}) == 4


class TestComputeArtifactId:
    """Tests for compute_artifact_id."""

    def test_deterministic(self) -> None:
        data = b"same input"
        assert compute_artifact_id("csv", data, {}) == compute_artifact_id(
            "csv", data, {}
        )

    def test_different_content_different_hash(self) -> None:
        assert compute_artifact_id("csv", b"a", {}) != compute_artifact_id(
            "csv", b"b", {}
        )

    def test_returns_32_char_hex(self) -> None:
        result = compute_artifact_id("csv", b"anything", {})
        assert len(result) == 32
        int(result, 16)

    def test_type_domain_separates_identical_content(self) -> None:
        assert compute_artifact_id("csv", b"same", {}) != compute_artifact_id(
            "metric", b"same", {}
        )

    def test_length_framing_separates_component_boundaries(self) -> None:
        assert compute_artifact_id("ab", b"c", {}) != compute_artifact_id(
            "a", b"bc", {}
        )

    @pytest.mark.parametrize(
        ("artifact_type", "content", "metadata", "expected"),
        [
            ("csv", b"", {}, "36f46d6a3d5e00b9a17e4f98053b92f4"),
            (
                "csv",
                b"value\n1\n",
                {
                    "metadata": {"source": "fixture"},
                    "original_name": "values",
                    "extension": ".csv",
                },
                "c3004cb2ebbd5f5fd372860faf5253c8",
            ),
            (
                "custom",
                bytes(range(256)),
                {
                    "metadata": {
                        "unicode": "雪☃",
                        "nested": [None, True, False, 1, -0.0],
                    },
                    "original_name": None,
                },
                "cef87236c30dccfae8de25c17eb6cfa3",
            ),
        ],
    )
    def test_incremental_hash_matches_original_framed_vectors(
        self, artifact_type: str, content: bytes, metadata: dict, expected: str
    ) -> None:
        """Values were captured before replacing the complete v1 preimage."""
        assert compute_artifact_id(artifact_type, content, metadata) == expected

    def test_hash_does_not_allocate_a_copy_of_the_content(self) -> None:
        content = b"x" * (8 * 1024 * 1024)
        tracemalloc.start()
        try:
            compute_artifact_id("csv", content, {})
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        assert peak < 1024 * 1024


class TestContentDigests:
    def test_content_and_stream_digest_match(self) -> None:
        content = b"a" * (1024 * 1024 + 17)
        assert compute_stream_digest(BytesIO(content)) == (
            compute_content_digest(content),
            len(content),
        )

    def test_content_digest_returns_32_char_hex(self):
        """Content digests use 32 lowercase hex characters."""
        content = b"test content"
        artifact_id = compute_content_digest(content)

        assert len(artifact_id) == 32
        assert all(c in "0123456789abcdef" for c in artifact_id)

    def test_content_digest_deterministic(self):
        """Same content should always produce same ID."""
        content = b"deterministic test"
        id1 = compute_content_digest(content)
        id2 = compute_content_digest(content)

        assert id1 == id2

    def test_content_digest_different_content_different_id(self):
        """Different content should produce different IDs."""
        id1 = compute_content_digest(b"content A")
        id2 = compute_content_digest(b"content B")

        assert id1 != id2


def _cache_input(
    role: str,
    artifact_id: str,
    *,
    position: int = 0,
    artifact_type: str = "csv",
    group_id: str | None = None,
) -> CacheInputIdentity:
    return CacheInputIdentity(
        role=role,
        group_id=group_id,
        position=position,
        artifact_type=artifact_type,
        artifact_id=artifact_id,
    )


class TestCacheIdentityStructure:
    def test_step_and_execution_domains_are_distinct(self) -> None:
        inputs = {"data": [_cache_input("data", "a" * 32)]}
        assert compute_step_spec_id("op", 0, {}, inputs) != compute_execution_spec_id(
            "op", inputs, {}
        )

    def test_role_mapping_order_is_irrelevant(self) -> None:
        first = {
            "data": [_cache_input("data", "a" * 32)],
            "config": [_cache_input("config", "b" * 32, artifact_type="config")],
        }
        second = {"config": first["config"], "data": first["data"]}
        assert compute_execution_spec_id("op", first) == compute_execution_spec_id(
            "op", second
        )

    def test_item_order_and_multiplicity_affect_identity(self) -> None:
        a = _cache_input("data", "a" * 32)
        b = _cache_input("data", "b" * 32, position=1)
        reversed_items = [
            _cache_input("data", "b" * 32),
            _cache_input("data", "a" * 32, position=1),
        ]
        assert compute_execution_spec_id("op", {"data": [a, b]}) != (
            compute_execution_spec_id("op", {"data": reversed_items})
        )
        assert compute_execution_spec_id("op", {"data": [a]}) != (
            compute_execution_spec_id(
                "op",
                {"data": [a, _cache_input("data", "a" * 32, position=1)]},
            )
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


def _execution_spec_from_ids(
    *,
    operation_name: str,
    inputs: dict[str, list[str]],
    params=None,
    config_overrides=None,
) -> str:
    """Call the production cache hash with typed test identities."""
    return compute_execution_spec_id(
        operation_name,
        _typed_inputs(inputs),
        params,
        config_overrides,
    )


class TestComputeExecutionSpecId:
    """Tests for _execution_spec_from_ids()."""

    def test_deterministic_output(self):
        """Same inputs produce same output."""
        spec1 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={"data": ["abc123" + "0" * 26, "def456" + "0" * 26]},
            params={"tolerance": 0.1},
        )
        spec2 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={"data": ["abc123" + "0" * 26, "def456" + "0" * 26]},
            params={"tolerance": 0.1},
        )
        assert spec1 == spec2
        assert len(spec1) == 32

    def test_artifact_order_is_preserved(self):
        """Occurrence order within a role remains part of the cache identity."""
        spec1 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={"data": ["bbb" + "0" * 29, "aaa" + "0" * 29]},
        )
        spec2 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={"data": ["aaa" + "0" * 29, "bbb" + "0" * 29]},
        )
        assert spec1 != spec2

    def test_multi_role_inputs_role_order_irrelevant(self):
        """Role-key ordering of the inputs dict doesn't affect the hash.

        Roles are sorted before hashing, so the dict-construction order
        is irrelevant. Role *assignment* of each artifact_id still matters
        — see ``test_execution_spec_id_differs_per_role_assignment``.
        """
        spec1 = _execution_spec_from_ids(
            operation_name="transform",
            inputs={
                "primary": ["aaa" + "0" * 29],
                "reference": ["bbb" + "0" * 29],
            },
        )
        spec2 = _execution_spec_from_ids(
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
        spec_a_primary = _execution_spec_from_ids(
            operation_name="transform",
            inputs={"primary": [a], "reference": [b]},
        )
        spec_b_primary = _execution_spec_from_ids(
            operation_name="transform",
            inputs={"primary": [b], "reference": [a]},
        )
        assert spec_a_primary != spec_b_primary

    def test_params_key_order_irrelevant(self):
        """Params dict key order doesn't affect hash."""
        spec1 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={},
            params={"a": 1, "b": 2},
        )
        spec2 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={},
            params={"b": 2, "a": 1},
        )
        assert spec1 == spec2

    def test_different_operation_different_hash(self):
        """Different operation names produce different hashes."""
        spec1 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={"data": ["a" * 32]},
        )
        spec2 = _execution_spec_from_ids(
            operation_name="minimize",
            inputs={"data": ["a" * 32]},
        )
        assert spec1 != spec2

    def test_different_artifacts_different_hash(self):
        """Different artifact IDs produce different hashes."""
        spec1 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={"data": ["a" * 32]},
        )
        spec2 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={"data": ["b" * 32]},
        )
        assert spec1 != spec2

    def test_empty_inputs(self):
        """Handles empty inputs gracefully (generative ops)."""
        spec = _execution_spec_from_ids(
            operation_name="generate",
            inputs={},
            params=None,
        )
        assert len(spec) == 32

    def test_different_params_different_hash(self):
        """Different merged params produce different spec_id."""
        spec1 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={},
            params={"tolerance": 0.1},
        )
        spec2 = _execution_spec_from_ids(
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
        spec1 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={"data": ["a" * 32, "a" * 32]},
        )
        spec2 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={"data": ["a" * 32]},
        )
        assert spec1 != spec2

    def test_config_overrides_none_same_as_empty(self):
        """config_overrides=None produces same spec_id as empty dict."""
        spec_none = _execution_spec_from_ids(
            operation_name="relax",
            inputs={},
            config_overrides=None,
        )
        spec_empty = _execution_spec_from_ids(
            operation_name="relax",
            inputs={},
            config_overrides={},
        )
        assert spec_none == spec_empty

    def test_config_overrides_changes_hash(self):
        """Different config_overrides produce different spec_id."""
        spec1 = _execution_spec_from_ids(
            operation_name="relax",
            inputs={"data": ["a" * 32]},
            config_overrides=None,
        )
        spec2 = _execution_spec_from_ids(
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
        assert _execution_spec_from_ids(**kwargs) == _execution_spec_from_ids(**kwargs)

    def test_config_overrides_with_path_objects(self):
        """Path objects in config_overrides serialize correctly."""
        from pathlib import Path

        spec = _execution_spec_from_ids(
            operation_name="relax",
            inputs={},
            config_overrides={"image": Path("/opt/containers/relax.sif")},
        )
        assert len(spec) == 32


def _step_spec_from_ids(
    *,
    operation_name: str,
    step_number: int,
    params: dict[str, Any] | None,
    inputs: dict[str, tuple[str, str]],
    config_overrides: dict[str, Any] | None = None,
) -> str:
    """Call the production step hash with concrete typed test identities."""
    typed = {
        role: [CacheInputIdentity(role, None, 0, artifact_type, artifact_id)]
        for role, (artifact_id, artifact_type) in inputs.items()
    }
    return compute_step_spec_id(
        operation_name,
        step_number,
        params,
        typed,
        config_overrides,
    )


class TestComputeStepSpecId:
    """Tests for step_spec_id computation."""

    def test_deterministic(self):
        """Same inputs produce same spec_id."""
        spec1 = _step_spec_from_ids(
            operation_name="ToolC",
            step_number=1,
            params={"model": "v2"},
            inputs={"data": ("abc123", "csv")},
        )
        spec2 = _step_spec_from_ids(
            operation_name="ToolC",
            step_number=1,
            params={"model": "v2"},
            inputs={"data": ("abc123", "csv")},
        )
        assert spec1 == spec2
        assert len(spec1) == 32

    def test_upstream_change_cascades(self):
        """Different concrete input artifact identities change the step hash."""
        spec1 = _step_spec_from_ids(
            operation_name="ToolB",
            step_number=2,
            params=None,
            inputs={"data": ("upstream_v1", "csv")},
        )
        spec2 = _step_spec_from_ids(
            operation_name="ToolB",
            step_number=2,
            params=None,
            inputs={"data": ("upstream_v2", "csv")},
        )
        assert spec1 != spec2

    def test_param_change(self):
        """Different params produce different spec_id."""
        spec1 = _step_spec_from_ids(
            operation_name="ToolC",
            step_number=1,
            params={"model": "v1"},
            inputs={"data": ("abc123", "csv")},
        )
        spec2 = _step_spec_from_ids(
            operation_name="ToolC",
            step_number=1,
            params={"model": "v2"},
            inputs={"data": ("abc123", "csv")},
        )
        assert spec1 != spec2

    def test_role_matters(self):
        """Same upstream but different role produces different spec_id."""
        spec1 = _step_spec_from_ids(
            operation_name="Score",
            step_number=2,
            params=None,
            inputs={"data": ("abc123", "csv")},
        )
        spec2 = _step_spec_from_ids(
            operation_name="Score",
            step_number=2,
            params=None,
            inputs={"scored": ("abc123", "csv")},
        )
        assert spec1 != spec2

    def test_step_number_matters(self):
        """Same operation at different positions produces different spec_id."""
        spec1 = _step_spec_from_ids(
            operation_name="Score",
            step_number=1,
            params=None,
            inputs={"data": ("abc123", "csv")},
        )
        spec2 = _step_spec_from_ids(
            operation_name="Score",
            step_number=3,
            params=None,
            inputs={"data": ("abc123", "csv")},
        )
        assert spec1 != spec2

    def test_empty_inputs(self):
        """Generative ops with no inputs produce valid spec_id."""
        spec = _step_spec_from_ids(
            operation_name="Generate",
            step_number=0,
            params={"count": 10},
            inputs={},
        )
        assert len(spec) == 32

    def test_none_params(self):
        """None params produces same spec_id as empty dict."""
        spec_none = _step_spec_from_ids(
            operation_name="Op",
            step_number=0,
            params=None,
            inputs={},
        )
        spec_empty = _step_spec_from_ids(
            operation_name="Op",
            step_number=0,
            params={},
            inputs={},
        )
        assert spec_none == spec_empty

    def test_config_overrides_none_same_as_empty(self):
        """config_overrides=None produces same spec_id as empty dict."""
        spec_none = _step_spec_from_ids(
            operation_name="Op",
            step_number=0,
            params=None,
            inputs={},
            config_overrides=None,
        )
        spec_empty = _step_spec_from_ids(
            operation_name="Op",
            step_number=0,
            params=None,
            inputs={},
            config_overrides={},
        )
        assert spec_none == spec_empty

    def test_config_overrides_changes_hash(self):
        """Different config_overrides produce different spec_id."""
        spec1 = _step_spec_from_ids(
            operation_name="ToolC",
            step_number=1,
            params=None,
            inputs={"data": ("abc123", "csv")},
            config_overrides=None,
        )
        spec2 = _step_spec_from_ids(
            operation_name="ToolC",
            step_number=1,
            params=None,
            inputs={"data": ("abc123", "csv")},
            config_overrides={"image": "/path/to/image.sif"},
        )
        assert spec1 != spec2

    def test_config_overrides_deterministic(self):
        """Same config_overrides produce same spec_id."""
        kwargs = {
            "operation_name": "ToolC",
            "step_number": 1,
            "params": None,
            "inputs": {"data": ("abc123", "csv")},
            "config_overrides": {"image": "/path/to/image.sif", "gpu": True},
        }
        assert _step_spec_from_ids(**kwargs) == _step_spec_from_ids(**kwargs)

    def test_config_overrides_with_path_objects(self):
        """Path objects in config_overrides serialize correctly."""
        from pathlib import Path

        spec1 = _step_spec_from_ids(
            operation_name="ToolC",
            step_number=1,
            params=None,
            inputs={},
            config_overrides={"image": Path("/opt/containers/tool_c.sif")},
        )
        spec2 = _step_spec_from_ids(
            operation_name="ToolC",
            step_number=1,
            params=None,
            inputs={},
            config_overrides={"image": Path("/opt/containers/tool_c.sif")},
        )
        assert spec1 == spec2
        assert len(spec1) == 32


def test_public_env_named_parameter_remains_cache_affecting():
    from pydantic import Field

    from artisan.operations.examples.data_generator import DataGenerator

    class AnnotationParameter(DataGenerator):
        name = "hash_annotation_parameter"

        class Params(DataGenerator.Params):
            public_env: str = Field(
                default="first",
                description="An execution parameter, not an annotation.",
            )

        params: Params = Params()

    one = AnnotationParameter()
    two = AnnotationParameter(params={"public_env": "second"})
    assert serialize_params(one)["public_env"] == "first"
    assert compute_execution_spec_id(
        one.name, {}, serialize_params(one)
    ) != compute_execution_spec_id(two.name, {}, serialize_params(two))
