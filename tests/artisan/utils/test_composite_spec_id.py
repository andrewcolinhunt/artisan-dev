"""Tests for compute_composite_spec_id."""

from __future__ import annotations

from artisan.utils.hashing import compute_composite_spec_id


class TestComputeCompositeSpecId:
    def test_deterministic(self) -> None:
        id1 = compute_composite_spec_id(
            "my_composite",
            {"threshold": 0.5},
            {"data": ("upstream_spec", "data")},
        )
        id2 = compute_composite_spec_id(
            "my_composite",
            {"threshold": 0.5},
            {"data": ("upstream_spec", "data")},
        )
        assert id1 == id2
        assert len(id1) == 32

    def test_param_sensitivity(self) -> None:
        id1 = compute_composite_spec_id("my_composite", {"threshold": 0.5}, {})
        id2 = compute_composite_spec_id("my_composite", {"threshold": 0.9}, {})
        assert id1 != id2

    def test_input_sensitivity(self) -> None:
        id1 = compute_composite_spec_id(
            "my_composite", None, {"data": ("spec_a", "data")}
        )
        id2 = compute_composite_spec_id(
            "my_composite", None, {"data": ("spec_b", "data")}
        )
        assert id1 != id2

    def test_name_sensitivity(self) -> None:
        id1 = compute_composite_spec_id("comp_a", None, {})
        id2 = compute_composite_spec_id("comp_b", None, {})
        assert id1 != id2

    def test_empty_inputs(self) -> None:
        result = compute_composite_spec_id("gen_comp", {"n": 5}, {})
        assert len(result) == 32

    def test_none_params(self) -> None:
        result = compute_composite_spec_id("gen_comp", None, {})
        assert len(result) == 32

    def test_input_role_order_independent(self) -> None:
        """Input roles are sorted, so order doesn't matter."""
        id1 = compute_composite_spec_id(
            "comp", None, {"a": ("s1", "r1"), "b": ("s2", "r2")}
        )
        id2 = compute_composite_spec_id(
            "comp", None, {"b": ("s2", "r2"), "a": ("s1", "r1")}
        )
        assert id1 == id2
