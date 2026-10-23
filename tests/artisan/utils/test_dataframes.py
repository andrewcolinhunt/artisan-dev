"""Tests for artisan.utils.dataframes — metric value encoding."""

from __future__ import annotations

import json

import pytest

from artisan.utils.dataframes import encode_metric_value

# ---------------------------------------------------------------------------
# encode_metric_value
# ---------------------------------------------------------------------------


class TestEncodeMetricValue:
    def test_int(self):
        assert encode_metric_value(5) == ("5", None)

    def test_float(self):
        assert encode_metric_value(3.14) == ("3.14", None)

    def test_bool_true(self):
        assert encode_metric_value(True) == ("true", None)

    def test_bool_false(self):
        assert encode_metric_value(False) == ("false", None)

    def test_str(self):
        assert encode_metric_value("high") == ('"high"', None)

    def test_none(self):
        assert encode_metric_value(None) == (None, None)

    def test_nan(self):
        assert encode_metric_value(float("nan")) == (None, None)

    def test_inf(self):
        assert encode_metric_value(float("inf")) == (None, None)

    def test_neg_inf(self):
        assert encode_metric_value(float("-inf")) == (None, None)

    def test_list(self):
        scalar, compound = encode_metric_value([1, 2, 3])
        assert scalar is None
        assert json.loads(compound) == [1, 2, 3]

    def test_dict(self):
        scalar, compound = encode_metric_value({"a": 1})
        assert scalar is None
        assert json.loads(compound) == {"a": 1}

    def test_unsupported_type_raises(self):
        with pytest.raises(TypeError, match="Unsupported metric value type"):
            encode_metric_value(object())
