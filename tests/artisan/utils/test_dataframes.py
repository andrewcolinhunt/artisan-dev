"""Tests for artisan.utils.dataframes — shared metric pivot utilities."""

from __future__ import annotations

import json

import polars as pl
import pytest

from artisan.utils.dataframes import (
    encode_metric_value,
    is_scalar_metric,
    pivot_metrics_wide,
)

# ---------------------------------------------------------------------------
# is_scalar_metric
# ---------------------------------------------------------------------------


class TestIsScalarMetric:
    def test_int(self):
        assert is_scalar_metric(5) is True

    def test_float(self):
        assert is_scalar_metric(3.14) is True

    def test_bool(self):
        assert is_scalar_metric(True) is True

    def test_str(self):
        assert is_scalar_metric("hello") is True

    def test_none(self):
        assert is_scalar_metric(None) is True

    def test_list(self):
        assert is_scalar_metric([1, 2]) is False

    def test_dict(self):
        assert is_scalar_metric({"a": 1}) is False


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


# ---------------------------------------------------------------------------
# pivot_metrics_wide
# ---------------------------------------------------------------------------


def _tidy(rows: list[dict]) -> pl.DataFrame:
    """Build a tidy DataFrame from row dicts."""
    return pl.DataFrame(
        rows,
        schema={
            "artifact_id": pl.String,
            "step_number": pl.Int32,
            "step_name": pl.String,
            "metric_name": pl.String,
            "metric_value": pl.String,
            "metric_compound": pl.String,
        },
    )


class TestPivotMetricsWide:
    def test_basic_pivot(self):
        tidy = _tidy(
            [
                {
                    "artifact_id": "a1",
                    "step_number": 1,
                    "step_name": "calc",
                    "metric_name": "score",
                    "metric_value": "0.9",
                    "metric_compound": None,
                },
                {
                    "artifact_id": "a1",
                    "step_number": 2,
                    "step_name": "extra",
                    "metric_name": "accuracy",
                    "metric_value": "1.5",
                    "metric_compound": None,
                },
            ]
        )
        wide = pivot_metrics_wide(tidy)
        assert wide.height == 1
        assert "calc.score" in wide.columns
        assert "extra.accuracy" in wide.columns
        row = wide.row(0, named=True)
        assert row["calc.score"] == pytest.approx(0.9)
        assert row["extra.accuracy"] == pytest.approx(1.5)

    def test_disambiguation(self):
        """Same step_name at different step_numbers gets {step_number}. prefix."""
        tidy = _tidy(
            [
                {
                    "artifact_id": "a1",
                    "step_number": 1,
                    "step_name": "repeat",
                    "metric_name": "val",
                    "metric_value": "1",
                    "metric_compound": None,
                },
                {
                    "artifact_id": "a1",
                    "step_number": 2,
                    "step_name": "repeat",
                    "metric_name": "val",
                    "metric_value": "2",
                    "metric_compound": None,
                },
            ]
        )
        wide = pivot_metrics_wide(tidy)
        assert "1.repeat.val" in wide.columns
        assert "2.repeat.val" in wide.columns
        row = wide.row(0, named=True)
        assert row["1.repeat.val"] == 1
        assert row["2.repeat.val"] == 2

    def test_mixed_ambiguous_and_unique(self):
        """Ambiguous names get prefix; unique names stay as-is."""
        tidy = _tidy(
            [
                {
                    "artifact_id": "a1",
                    "step_number": 1,
                    "step_name": "repeat",
                    "metric_name": "val",
                    "metric_value": "1",
                    "metric_compound": None,
                },
                {
                    "artifact_id": "a1",
                    "step_number": 2,
                    "step_name": "repeat",
                    "metric_name": "val",
                    "metric_value": "2",
                    "metric_compound": None,
                },
                {
                    "artifact_id": "a1",
                    "step_number": 3,
                    "step_name": "unique",
                    "metric_name": "score",
                    "metric_value": "0.5",
                    "metric_compound": None,
                },
            ]
        )
        wide = pivot_metrics_wide(tidy)
        assert "1.repeat.val" in wide.columns
        assert "2.repeat.val" in wide.columns
        assert "unique.score" in wide.columns

    def test_custom_index_cols(self):
        """Extra index columns are preserved in wide output."""
        tidy = pl.DataFrame(
            [
                {
                    "artifact_id": "a1",
                    "data_name": "s1",
                    "data_file": "/f1",
                    "step_number": 1,
                    "step_name": "calc",
                    "metric_name": "score",
                    "metric_value": "0.9",
                    "metric_compound": None,
                },
            ],
            schema={
                "artifact_id": pl.String,
                "data_name": pl.String,
                "data_file": pl.String,
                "step_number": pl.Int32,
                "step_name": pl.String,
                "metric_name": pl.String,
                "metric_value": pl.String,
                "metric_compound": pl.String,
            },
        )
        wide = pivot_metrics_wide(
            tidy, index_cols=["artifact_id", "data_name", "data_file"]
        )
        assert "data_name" in wide.columns
        assert "data_file" in wide.columns
        assert wide.height == 1

    def test_missing_columns_raises(self):
        bad = pl.DataFrame({"artifact_id": ["a1"], "other": [1]})
        with pytest.raises(ValueError, match="Missing required columns"):
            pivot_metrics_wide(bad)

    def test_empty_dataframe(self):
        tidy = _tidy([])
        wide = pivot_metrics_wide(tidy)
        assert wide.height == 0
        assert "artifact_id" in wide.columns

    def test_compounds_excluded_from_wide(self):
        """Compound values (list/dict) in metric_compound don't appear in wide."""
        tidy = _tidy(
            [
                {
                    "artifact_id": "a1",
                    "step_number": 1,
                    "step_name": "calc",
                    "metric_name": "score",
                    "metric_value": "0.9",
                    "metric_compound": None,
                },
                {
                    "artifact_id": "a1",
                    "step_number": 1,
                    "step_name": "calc",
                    "metric_name": "bins",
                    "metric_value": None,
                    "metric_compound": "[1, 2, 3]",
                },
            ]
        )
        wide = pivot_metrics_wide(tidy)
        assert "calc.score" in wide.columns
        assert "calc.bins" not in wide.columns

    def test_all_compounds_returns_empty(self):
        """If all rows are compounds, wide should be empty."""
        tidy = _tidy(
            [
                {
                    "artifact_id": "a1",
                    "step_number": 1,
                    "step_name": "calc",
                    "metric_name": "bins",
                    "metric_value": None,
                    "metric_compound": "[1, 2, 3]",
                },
            ]
        )
        wide = pivot_metrics_wide(tidy)
        assert wide.height == 0
        assert "artifact_id" in wide.columns


# ---------------------------------------------------------------------------
# Type inference
# ---------------------------------------------------------------------------


class TestInferColumnTypes:
    def test_all_int_becomes_int64(self):
        tidy = _tidy(
            [
                {
                    "artifact_id": "a1",
                    "step_number": 1,
                    "step_name": "s",
                    "metric_name": "count",
                    "metric_value": "5",
                    "metric_compound": None,
                },
                {
                    "artifact_id": "a2",
                    "step_number": 1,
                    "step_name": "s",
                    "metric_name": "count",
                    "metric_value": "10",
                    "metric_compound": None,
                },
            ]
        )
        wide = pivot_metrics_wide(tidy)
        assert wide["s.count"].dtype == pl.Int64
        assert wide["s.count"].to_list() == [5, 10]

    def test_all_float_becomes_float64(self):
        tidy = _tidy(
            [
                {
                    "artifact_id": "a1",
                    "step_number": 1,
                    "step_name": "s",
                    "metric_name": "score",
                    "metric_value": "0.9",
                    "metric_compound": None,
                },
                {
                    "artifact_id": "a2",
                    "step_number": 1,
                    "step_name": "s",
                    "metric_name": "score",
                    "metric_value": "0.7",
                    "metric_compound": None,
                },
            ]
        )
        wide = pivot_metrics_wide(tidy)
        assert wide["s.score"].dtype == pl.Float64

    def test_mixed_int_float_becomes_float64(self):
        tidy = _tidy(
            [
                {
                    "artifact_id": "a1",
                    "step_number": 1,
                    "step_name": "s",
                    "metric_name": "val",
                    "metric_value": "5",
                    "metric_compound": None,
                },
                {
                    "artifact_id": "a2",
                    "step_number": 1,
                    "step_name": "s",
                    "metric_name": "val",
                    "metric_value": "3.14",
                    "metric_compound": None,
                },
            ]
        )
        wide = pivot_metrics_wide(tidy)
        assert wide["s.val"].dtype == pl.Float64

    def test_all_bool_becomes_boolean(self):
        tidy = _tidy(
            [
                {
                    "artifact_id": "a1",
                    "step_number": 1,
                    "step_name": "s",
                    "metric_name": "flag",
                    "metric_value": "true",
                    "metric_compound": None,
                },
                {
                    "artifact_id": "a2",
                    "step_number": 1,
                    "step_name": "s",
                    "metric_name": "flag",
                    "metric_value": "false",
                    "metric_compound": None,
                },
            ]
        )
        wide = pivot_metrics_wide(tidy)
        assert wide["s.flag"].dtype == pl.Boolean
        assert wide["s.flag"].to_list() == [True, False]

    def test_all_string_becomes_string(self):
        tidy = _tidy(
            [
                {
                    "artifact_id": "a1",
                    "step_number": 1,
                    "step_name": "s",
                    "metric_name": "label",
                    "metric_value": '"high"',
                    "metric_compound": None,
                },
                {
                    "artifact_id": "a2",
                    "step_number": 1,
                    "step_name": "s",
                    "metric_name": "label",
                    "metric_value": '"low"',
                    "metric_compound": None,
                },
            ]
        )
        wide = pivot_metrics_wide(tidy)
        assert wide["s.label"].dtype == pl.String
        assert wide["s.label"].to_list() == ["high", "low"]

    def test_mixed_types_stays_string(self):
        tidy = _tidy(
            [
                {
                    "artifact_id": "a1",
                    "step_number": 1,
                    "step_name": "s",
                    "metric_name": "mixed",
                    "metric_value": "5",
                    "metric_compound": None,
                },
                {
                    "artifact_id": "a2",
                    "step_number": 1,
                    "step_name": "s",
                    "metric_name": "mixed",
                    "metric_value": '"high"',
                    "metric_compound": None,
                },
            ]
        )
        wide = pivot_metrics_wide(tidy)
        assert wide["s.mixed"].dtype == pl.String

    def test_nulls_preserved_with_type_inference(self):
        tidy = _tidy(
            [
                {
                    "artifact_id": "a1",
                    "step_number": 1,
                    "step_name": "s",
                    "metric_name": "count",
                    "metric_value": "5",
                    "metric_compound": None,
                },
                {
                    "artifact_id": "a2",
                    "step_number": 1,
                    "step_name": "s",
                    "metric_name": "count",
                    "metric_value": "10",
                    "metric_compound": None,
                },
                {
                    "artifact_id": "a3",
                    "step_number": 1,
                    "step_name": "s",
                    "metric_name": "other",
                    "metric_value": "99",
                    "metric_compound": None,
                },
            ]
        )
        wide = pivot_metrics_wide(tidy)
        assert wide["s.count"].dtype == pl.Int64
        # a3 has no "count" metric, so it should be null
        vals = wide.sort("artifact_id")["s.count"].to_list()
        assert vals[0] == 5
        assert vals[1] == 10
        assert vals[2] is None
