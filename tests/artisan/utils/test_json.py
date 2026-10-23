"""Tests for artisan.utils.json."""

from __future__ import annotations

import json
from enum import Enum
from pathlib import Path

import pytest

from artisan.schemas.enums import GroupByStrategy
from artisan.utils.json import artisan_json_default


class _Color(str, Enum):
    RED = "red"
    BLUE = "blue"


class _Code(int, Enum):
    OK = 200
    ERR = 500


class TestArtisanJsonDefault:
    def test_set_becomes_sorted_list(self) -> None:
        """Sets serialize as sorted lists for deterministic output."""
        assert artisan_json_default({"c", "a", "b"}) == ["a", "b", "c"]

    def test_empty_set_becomes_empty_list(self) -> None:
        assert artisan_json_default(set()) == []

    def test_path_becomes_string(self) -> None:
        assert artisan_json_default(Path("/tmp/foo")) == "/tmp/foo"

    def test_str_enum_value(self) -> None:
        assert artisan_json_default(_Color.RED) == "red"

    def test_int_enum_value(self) -> None:
        assert artisan_json_default(_Code.OK) == 200

    def test_unknown_type_raises(self) -> None:
        with pytest.raises(TypeError, match="not JSON serializable"):
            artisan_json_default(object())

    def test_enum_in_params_dump(self) -> None:
        dumped = json.dumps(
            {"pairing": GroupByStrategy.CROSS_PRODUCT},
            default=artisan_json_default,
        )
        assert dumped == '{"pairing": "cross_product"}'
