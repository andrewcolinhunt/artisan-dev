"""Tests for artisan.utils.dataframes — shared metric pivot utilities."""

from __future__ import annotations

import polars as pl
import pytest

from artisan.utils.dataframes import pivot_metrics_wide, to_float

# ---------------------------------------------------------------------------
# to_float
# ---------------------------------------------------------------------------


class TestToFloat:
    def test_float_passthrough(self):
        assert to_float(1.5) == 1.5

    def test_int_to_float(self):
        assert to_float(5) == 5.0

    def test_bool_true(self):
        assert to_float(True) == 1.0

    def test_bool_false(self):
        assert to_float(False) == 0.0

    def test_string_returns_none(self):
        assert to_float("test") is None

    def test_none_returns_none(self):
        assert to_float(None) is None

    def test_dict_returns_none(self):
        assert to_float({"a": 1}) is None


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
            "metric_value": pl.Float64,
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
                    "metric_value": 0.9,
                },
                {
                    "artifact_id": "a1",
                    "step_number": 2,
                    "step_name": "extra",
                    "metric_name": "accuracy",
                    "metric_value": 1.5,
                },
            ]
        )
        wide = pivot_metrics_wide(tidy)
        assert wide.height == 1
        assert "calc.score" in wide.columns
        assert "extra.accuracy" in wide.columns
        row = wide.row(0, named=True)
        assert row["calc.score"] == 0.9
        assert row["extra.accuracy"] == 1.5

    def test_disambiguation(self):
        """Same step_name at different step_numbers gets {step_number}. prefix."""
        tidy = _tidy(
            [
                {
                    "artifact_id": "a1",
                    "step_number": 1,
                    "step_name": "repeat",
                    "metric_name": "val",
                    "metric_value": 1.0,
                },
                {
                    "artifact_id": "a1",
                    "step_number": 2,
                    "step_name": "repeat",
                    "metric_name": "val",
                    "metric_value": 2.0,
                },
            ]
        )
        wide = pivot_metrics_wide(tidy)
        assert "1.repeat.val" in wide.columns
        assert "2.repeat.val" in wide.columns
        row = wide.row(0, named=True)
        assert row["1.repeat.val"] == 1.0
        assert row["2.repeat.val"] == 2.0

    def test_mixed_ambiguous_and_unique(self):
        """Ambiguous names get prefix; unique names stay as-is."""
        tidy = _tidy(
            [
                {
                    "artifact_id": "a1",
                    "step_number": 1,
                    "step_name": "repeat",
                    "metric_name": "val",
                    "metric_value": 1.0,
                },
                {
                    "artifact_id": "a1",
                    "step_number": 2,
                    "step_name": "repeat",
                    "metric_name": "val",
                    "metric_value": 2.0,
                },
                {
                    "artifact_id": "a1",
                    "step_number": 3,
                    "step_name": "unique",
                    "metric_name": "score",
                    "metric_value": 0.5,
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
                    "metric_value": 0.9,
                },
            ],
            schema={
                "artifact_id": pl.String,
                "data_name": pl.String,
                "data_file": pl.String,
                "step_number": pl.Int32,
                "step_name": pl.String,
                "metric_name": pl.String,
                "metric_value": pl.Float64,
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
