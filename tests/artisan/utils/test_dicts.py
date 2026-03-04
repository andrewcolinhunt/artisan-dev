"""Tests for artisan.utils.dicts."""

from __future__ import annotations

from artisan.utils.dicts import flatten_dict


class TestFlattenDict:
    """Tests for flatten_dict."""

    def test_flat_input_passthrough(self) -> None:
        """Already-flat dict is returned unchanged."""
        d = {"a": 1, "b": 2.0, "c": "hello"}
        assert flatten_dict(d) == d

    def test_single_nesting(self) -> None:
        """Single level of nesting produces dot-separated keys."""
        d = {"a": {"b": 1, "c": 2}}
        assert flatten_dict(d) == {"a.b": 1, "a.c": 2}

    def test_deep_nesting(self) -> None:
        """Multiple nesting levels produce multi-dot keys."""
        d = {"a": {"b": {"c": {"d": 42}}}}
        assert flatten_dict(d) == {"a.b.c.d": 42}

    def test_empty_dict(self) -> None:
        """Empty dict returns empty dict."""
        assert flatten_dict({}) == {}

    def test_type_preservation(self) -> None:
        """Values retain their original types (no casting)."""
        d = {"int": 1, "float": 2.5, "str": "x", "bool": True, "none": None}
        result = flatten_dict(d)
        assert result["int"] == 1
        assert isinstance(result["int"], int)
        assert result["float"] == 2.5
        assert isinstance(result["float"], float)
        assert result["str"] == "x"
        assert result["bool"] is True
        assert result["none"] is None

    def test_mixed_nesting(self) -> None:
        """Mix of flat and nested keys."""
        d = {"flat": 1, "nested": {"a": 2, "b": {"c": 3}}, "also_flat": 4}
        assert flatten_dict(d) == {
            "flat": 1,
            "nested.a": 2,
            "nested.b.c": 3,
            "also_flat": 4,
        }

    def test_custom_separator(self) -> None:
        """Custom separator is used instead of dot."""
        d = {"a": {"b": 1}}
        assert flatten_dict(d, separator="/") == {"a/b": 1}

    def test_empty_nested_dict(self) -> None:
        """Empty nested dict produces no keys for that branch."""
        d = {"a": {}, "b": 1}
        assert flatten_dict(d) == {"b": 1}

    def test_list_values_preserved(self) -> None:
        """Non-dict collection values are treated as leaves."""
        d = {"a": [1, 2, 3], "b": {"c": [4, 5]}}
        assert flatten_dict(d) == {"a": [1, 2, 3], "b.c": [4, 5]}
