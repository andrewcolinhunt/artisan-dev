"""Unit tests for the Filter operation (direct-input with structured criteria).

Tests cover:
1. Class attributes (runtime_defined_inputs, hydrate_inputs, etc.)
2. Single and multi-criterion evaluation (all operators)
3. Multi-role multi-criteria filtering
4. Lineage matching integration (passthrough <-> metrics pairing)
5. Error handling (missing passthrough, no metric roles, invalid criteria)
6. Edge cases (empty criteria, missing field, type mismatch, empty passthrough)
7. Chunked evaluation (chunk_size parameter, boundary tests, diagnostics)
"""

from __future__ import annotations

import json
import logging
from unittest.mock import Mock

import polars as pl
import pytest

from artisan.operations.curator.filter import (
    Criterion,
    Filter,
    _criterion_to_expr,
    _flatten_struct_columns,
)
from artisan.schemas.artifact.types import ArtifactTypes


def pad_id(short_id: str) -> str:
    """Pad a short ID to 32 characters for valid artifact_id."""
    return short_id.ljust(32, "0")[:32]


def _df(ids: list[str]) -> pl.DataFrame:
    """Create a DataFrame with artifact_id column."""
    return pl.DataFrame({"artifact_id": ids})


def _make_inputs(roles: dict[str, list[str]]) -> dict[str, pl.DataFrame]:
    """Build dict[str, pl.DataFrame] from {role: [short_ids]}."""
    return {role: _df([pad_id(sid) for sid in ids]) for role, ids in roles.items()}


def _build_edges_df(edges: list[tuple[str, str]]) -> pl.DataFrame:
    """Build provenance edges DataFrame from (source, target) pairs."""
    if not edges:
        return pl.DataFrame(
            schema={"source_artifact_id": pl.String, "target_artifact_id": pl.String}
        )
    return pl.DataFrame(
        {
            "source_artifact_id": [e[0] for e in edges],
            "target_artifact_id": [e[1] for e in edges],
        }
    )


def _make_metrics_df(
    metric_map: dict[str, dict],
) -> pl.DataFrame:
    """Build a load_metrics_df-style DataFrame from {metric_id: values_dict}.

    Returns DataFrame with artifact_id (Utf8) and content (Binary).
    """
    if not metric_map:
        return pl.DataFrame(schema={"artifact_id": pl.String, "content": pl.Binary})
    rows = [
        {"artifact_id": mid, "content": json.dumps(vals).encode("utf-8")}
        for mid, vals in metric_map.items()
    ]
    return pl.DataFrame(rows, schema={"artifact_id": pl.String, "content": pl.Binary})


def _build_lineage_store(
    step_map: dict[str, int],
    edges: list[tuple[str, str]],
    metric_values: dict[str, dict],
) -> Mock:
    """Build a mock artifact store for provenance-based matching.

    Args:
        step_map: {artifact_id: step_number}.
        edges: List of (source, target) provenance edges.
        metric_values: {metric_artifact_id: values_dict} for load_metrics_df.
    """
    store = Mock()
    edges_df = _build_edges_df(edges)

    # get_step_range returns (min, max) of step numbers for given IDs
    steps = list(step_map.values())
    if steps:
        store.get_step_range.return_value = (min(steps), max(steps))
    else:
        store.get_step_range.return_value = None

    store.load_provenance_edges_df.return_value = edges_df

    metrics_df = _make_metrics_df(metric_values)
    store.load_metrics_df.side_effect = lambda ids: (
        metrics_df.filter(pl.col("artifact_id").is_in(ids))
        if ids
        else pl.DataFrame(schema={"artifact_id": pl.String, "content": pl.Binary})
    )
    return store


@pytest.fixture
def lineage_store():
    """Create a mock artifact store with a standard provenance graph.

    Provenance graph::

        Step 0: root_a --------- root_b
                |                |
        Step 1: pt_a             pt_b
               /    \\          /    \\
        Step 2: m_qa  m_la    m_qb  m_lb

    pt_a matches m_qa and m_la (walk backward: m_qa->pt_a)
    pt_b matches m_qb and m_lb (walk backward: m_qb->pt_b)
    """
    ids = {
        "root_a": pad_id("root_a"),
        "root_b": pad_id("root_b"),
        "pt_a": pad_id("pt_a"),
        "pt_b": pad_id("pt_b"),
        "m_qa": pad_id("m_qa"),
        "m_la": pad_id("m_la"),
        "m_qb": pad_id("m_qb"),
        "m_lb": pad_id("m_lb"),
    }

    step_map = {
        ids["root_a"]: 0,
        ids["root_b"]: 0,
        ids["pt_a"]: 1,
        ids["pt_b"]: 1,
        ids["m_qa"]: 2,
        ids["m_la"]: 2,
        ids["m_qb"]: 2,
        ids["m_lb"]: 2,
    }

    # Edges: source produced target
    edges = [
        (ids["root_a"], ids["pt_a"]),
        (ids["root_b"], ids["pt_b"]),
        (ids["pt_a"], ids["m_qa"]),
        (ids["pt_a"], ids["m_la"]),
        (ids["pt_b"], ids["m_qb"]),
        (ids["pt_b"], ids["m_lb"]),
    ]

    metric_values = {
        ids["m_qa"]: {"score": 0.9, "n_chainbreaks": 0},
        ids["m_la"]: {"component_score": 0.08},
        ids["m_qb"]: {"score": 0.3, "n_chainbreaks": 2},
        ids["m_lb"]: {"component_score": 0.04},
    }

    store = _build_lineage_store(step_map, edges, metric_values)
    store._ids = ids

    return store


class TestFilterClassAttributes:
    """Tests for Filter class attributes."""

    def test_has_correct_name(self):
        assert Filter.name == "filter"

    def test_has_description(self):
        assert Filter.description is not None

    def test_runtime_defined_inputs(self):
        assert Filter.runtime_defined_inputs is True

    def test_hydrate_inputs_false(self):
        assert Filter.hydrate_inputs is False

    def test_independent_input_streams(self):
        assert Filter.independent_input_streams is True

    def test_passthrough_output_spec(self):
        assert "passthrough" in Filter.outputs
        assert Filter.outputs["passthrough"].artifact_type == ArtifactTypes.ANY
        assert Filter.outputs["passthrough"].required is False

    def test_passthrough_input_declared(self):
        assert "passthrough" in Filter.inputs
        assert Filter.inputs["passthrough"].artifact_type == ArtifactTypes.ANY
        assert Filter.inputs["passthrough"].required is True


class TestFilterSingleCriterion:
    """Tests for single criterion evaluation."""

    def test_single_criterion_pass(self, lineage_store):
        """One metric, one criterion that passes."""
        ids = lineage_store._ids
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="quality.score", operator="gt", value=0.5)]
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a"], "quality": ["m_qa"]}),
            step_number=0,
            artifact_store=lineage_store,
        )

        assert result.success
        assert ids["pt_a"] in result.passthrough["passthrough"]

    def test_single_criterion_fail(self, lineage_store):
        """One metric, one criterion that fails -> empty."""
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="quality.score", operator="gt", value=0.95)]
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a"], "quality": ["m_qa"]}),
            step_number=0,
            artifact_store=lineage_store,
        )

        assert result.success
        assert len(result.passthrough["passthrough"]) == 0


class TestFilterMultipleArtifacts:
    """Tests for filtering multiple artifacts."""

    def test_mixed_pass_fail(self, lineage_store):
        """Some pass, some fail -> correct subset."""
        ids = lineage_store._ids
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="quality.score", operator="gt", value=0.5)]
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs(
                {
                    "passthrough": ["pt_a", "pt_b"],
                    "quality": ["m_qa", "m_qb"],
                }
            ),
            step_number=0,
            artifact_store=lineage_store,
        )

        assert result.success
        passed = result.passthrough["passthrough"]
        # pt_a has score=0.9 (pass), pt_b has score=0.3 (fail)
        assert ids["pt_a"] in passed
        assert ids["pt_b"] not in passed


class TestFilterMultiRoleMultiCriteria:
    """Tests for N metric roles with criteria referencing all."""

    def test_multi_role_all_pass(self, lineage_store):
        """Artifact passes when all criteria across multiple roles pass."""
        ids = lineage_store._ids
        op = Filter(
            params=Filter.Params(
                criteria=[
                    Criterion(metric="quality.score", operator="ge", value=0.9),
                    Criterion(
                        metric="component.component_score", operator="ge", value=0.06
                    ),
                ]
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs(
                {
                    "passthrough": ["pt_a"],
                    "quality": ["m_qa"],
                    "component": ["m_la"],
                }
            ),
            step_number=0,
            artifact_store=lineage_store,
        )

        assert result.success
        assert ids["pt_a"] in result.passthrough["passthrough"]

    def test_multi_role_one_fails(self, lineage_store):
        """Artifact fails when one criterion fails (AND semantics)."""
        ids = lineage_store._ids
        op = Filter(
            params=Filter.Params(
                criteria=[
                    Criterion(metric="quality.score", operator="ge", value=0.5),
                    Criterion(
                        metric="component.component_score", operator="ge", value=0.06
                    ),
                ]
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs(
                {
                    "passthrough": ["pt_a", "pt_b"],
                    "quality": ["m_qa", "m_qb"],
                    "component": ["m_la", "m_lb"],
                }
            ),
            step_number=0,
            artifact_store=lineage_store,
        )

        assert result.success
        passed = result.passthrough["passthrough"]
        # pt_a: score=0.9 >=0.5, component=0.08 >=0.06 -> pass
        # pt_b: score=0.3 >=0.5 -> fail
        assert ids["pt_a"] in passed
        assert ids["pt_b"] not in passed


class TestFilterAllOperators:
    """Tests for all comparison operators."""

    @pytest.fixture
    def simple_store(self):
        """Simple store with one passthrough and one metric."""
        pt_id = pad_id("pt")
        m_id = pad_id("met")

        # pt produced met (edge: pt -> met)
        step_map = {pt_id: 0, m_id: 1}
        edges = [(pt_id, m_id)]
        metric_values = {m_id: {"val": 10}}

        store = _build_lineage_store(step_map, edges, metric_values)
        store._pt_id = pt_id
        store._m_id = m_id
        return store

    @pytest.mark.parametrize(
        ("op_name", "threshold", "expected_pass"),
        [
            ("gt", 9, True),
            ("gt", 10, False),
            ("ge", 10, True),
            ("ge", 11, False),
            ("lt", 11, True),
            ("lt", 10, False),
            ("le", 10, True),
            ("le", 9, False),
            ("eq", 10, True),
            ("eq", 9, False),
            ("ne", 9, True),
            ("ne", 10, False),
        ],
    )
    def test_operator(self, simple_store, op_name, threshold, expected_pass):
        op = Filter(
            params=Filter.Params(
                criteria=[
                    Criterion(metric="metrics.val", operator=op_name, value=threshold)
                ]
            )
        )
        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt"], "metrics": ["met"]}),
            step_number=0,
            artifact_store=simple_store,
        )
        passed = result.passthrough["passthrough"]
        if expected_pass:
            assert len(passed) == 1
        else:
            assert len(passed) == 0


class TestFilterEdgeCases:
    """Tests for edge cases."""

    def test_empty_criteria_passes_all(self, lineage_store):
        """No criteria -> all matched artifacts pass."""
        ids = lineage_store._ids
        op = Filter(params=Filter.Params(criteria=[]))

        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a"], "quality": ["m_qa"]}),
            step_number=0,
            artifact_store=lineage_store,
        )

        assert result.success
        assert ids["pt_a"] in result.passthrough["passthrough"]

    def test_missing_field_fails(self, lineage_store):
        """Criterion references nonexistent field -> artifact fails."""
        op = Filter(
            params=Filter.Params(
                criteria=[
                    Criterion(metric="quality.nonexistent", operator="gt", value=0.5)
                ]
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a"], "quality": ["m_qa"]}),
            step_number=0,
            artifact_store=lineage_store,
        )

        assert result.success
        assert len(result.passthrough["passthrough"]) == 0

    def test_empty_passthrough_returns_empty(self, lineage_store):
        """Zero passthrough inputs -> empty result."""
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="quality.score", operator="gt", value=0.5)]
            )
        )

        result = op.execute_curator(
            inputs={"passthrough": _df([]), "quality": _df([pad_id("m_qa")])},
            step_number=0,
            artifact_store=lineage_store,
        )

        assert result.success
        assert len(result.passthrough["passthrough"]) == 0

    def test_unmatched_passthrough_dropped(self):
        """Passthrough with no lineage match is not in output."""
        # Empty edges -> no lineage match
        store = _build_lineage_store(
            step_map={pad_id("pt"): 0, pad_id("met"): 1},
            edges=[],
            metric_values={},
        )

        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="quality.score", operator="gt", value=0)]
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt"], "quality": ["met"]}),
            step_number=0,
            artifact_store=store,
        )

        assert result.success
        assert len(result.passthrough["passthrough"]) == 0

    def test_partial_match_dropped(self, lineage_store):
        """Matched in some metric streams but not all -> dropped."""
        # pt_a has lineage to quality/m_qa and component/m_la
        # but if we provide m_qb as quality metric, it won't match pt_a
        op = Filter(
            params=Filter.Params(
                criteria=[
                    Criterion(metric="quality.score", operator="gt", value=0),
                    Criterion(
                        metric="component.component_score", operator="gt", value=0
                    ),
                ]
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs(
                {
                    "passthrough": ["pt_a"],
                    "quality": ["m_qb"],  # Wrong lineage for pt_a
                    "component": ["m_la"],
                }
            ),
            step_number=0,
            artifact_store=lineage_store,
        )

        assert result.success
        assert len(result.passthrough["passthrough"]) == 0


class TestFilterValidationErrors:
    """Tests for input validation errors."""

    def test_missing_passthrough_raises(self, lineage_store):
        """No passthrough role -> ValueError."""
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="quality.score", operator="gt", value=0)]
            )
        )

        with pytest.raises(ValueError, match="passthrough"):
            op.execute_curator(
                inputs=_make_inputs({"quality": ["m_qa"]}),
                step_number=0,
                artifact_store=lineage_store,
            )

    def test_no_metric_roles_explicit_criteria_raises(self, lineage_store):
        """Only passthrough with explicit criteria but no metric roles -> ValueError."""
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="quality.score", operator="gt", value=0)]
            )
        )

        with pytest.raises(ValueError, match="at least one metric input role"):
            op.execute_curator(
                inputs=_make_inputs({"passthrough": ["pt_a"]}),
                step_number=0,
                artifact_store=lineage_store,
            )

    def test_bare_field_treated_as_implicit(self, lineage_store):
        """metric="nodot" with metric roles -> treated as implicit criterion."""
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="nodot", operator="gt", value=0)]
            )
        )

        # Mock get_descendant_ids_df to return empty
        lineage_store.get_descendant_ids_df.return_value = pl.DataFrame(
            schema={"source_artifact_id": pl.String, "target_artifact_id": pl.String}
        )

        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a"], "quality": ["m_qa"]}),
            step_number=0,
            artifact_store=lineage_store,
        )

        assert result.success
        assert len(result.passthrough["passthrough"]) == 0

    def test_unknown_role_raises(self, lineage_store):
        """Criterion references role not in inputs -> ValueError."""
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="unknown.score", operator="gt", value=0)]
            )
        )

        with pytest.raises(ValueError, match="unknown role"):
            op.execute_curator(
                inputs=_make_inputs({"passthrough": ["pt_a"], "quality": ["m_qa"]}),
                step_number=0,
                artifact_store=lineage_store,
            )


class TestCriterionToExpr:
    """Tests for _criterion_to_expr vectorized evaluation."""

    @pytest.mark.parametrize(
        ("op_name", "threshold", "values", "expected"),
        [
            ("gt", 5, [3, 5, 7], [False, False, True]),
            ("ge", 5, [3, 5, 7], [False, True, True]),
            ("lt", 5, [3, 5, 7], [True, False, False]),
            ("le", 5, [3, 5, 7], [True, True, False]),
            ("eq", 5, [3, 5, 7], [False, True, False]),
            ("ne", 5, [3, 5, 7], [True, False, True]),
        ],
    )
    def test_operator_expr(self, op_name, threshold, values, expected):
        """Each operator produces correct boolean column."""
        c = Criterion(metric="x", operator=op_name, value=threshold)
        df = pl.DataFrame({"x": values})
        result = df.select(_criterion_to_expr(c).alias("result"))
        assert result["result"].to_list() == expected

    def test_null_handling(self):
        """Null values produce null (not True/False)."""
        c = Criterion(metric="x", operator="gt", value=5)
        df = pl.DataFrame({"x": [None, 7, None]})
        result = df.select(_criterion_to_expr(c).fill_null(False).alias("result"))
        assert result["result"].to_list() == [False, True, False]


class TestFlattenStructColumns:
    """Tests for _flatten_struct_columns helper."""

    def test_flat_columns_unchanged(self):
        """Non-struct columns pass through unchanged."""
        df = pl.DataFrame({"a": [1], "b": ["x"]})
        result = _flatten_struct_columns(df)
        assert result.columns == ["a", "b"]

    def test_nested_struct_flattened(self):
        """Struct columns get flattened to dot-separated names."""
        df = pl.DataFrame({"a": [{"x": 1, "y": 2}]})
        result = _flatten_struct_columns(df)
        assert set(result.columns) == {"a.x", "a.y"}
        assert result["a.x"][0] == 1


class TestFilterDiagnosticsMetadata:
    """Tests for diagnostics populated in PassthroughResult.metadata."""

    def test_diagnostics_populated_in_metadata(self, lineage_store):
        """'diagnostics' key in result.metadata, version 3."""
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="quality.score", operator="gt", value=0.5)]
            )
        )
        result = op.execute_curator(
            inputs=_make_inputs(
                {
                    "passthrough": ["pt_a", "pt_b"],
                    "quality": ["m_qa", "m_qb"],
                }
            ),
            step_number=0,
            artifact_store=lineage_store,
        )
        assert "diagnostics" in result.metadata
        assert result.metadata["diagnostics"]["version"] == 3

    def test_diagnostics_counts(self, lineage_store):
        """total_input=2, total_explicit_matched=2, total_passed=1."""
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="quality.score", operator="gt", value=0.5)]
            )
        )
        result = op.execute_curator(
            inputs=_make_inputs(
                {
                    "passthrough": ["pt_a", "pt_b"],
                    "quality": ["m_qa", "m_qb"],
                }
            ),
            step_number=0,
            artifact_store=lineage_store,
        )
        diag = result.metadata["diagnostics"]
        assert diag["total_input"] == 2
        assert diag["total_explicit_matched"] == 2
        assert diag["total_passed"] == 1

    def test_diagnostics_criteria_stats(self, lineage_store):
        """Check metric, pass_count, and stats."""
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="quality.score", operator="gt", value=0.5)]
            )
        )
        result = op.execute_curator(
            inputs=_make_inputs(
                {
                    "passthrough": ["pt_a", "pt_b"],
                    "quality": ["m_qa", "m_qb"],
                }
            ),
            step_number=0,
            artifact_store=lineage_store,
        )
        crit = result.metadata["diagnostics"]["criteria"][0]
        assert crit["metric"] == "quality.score"
        assert crit["pass_count"] == 1
        assert crit["stats"]["min"] == pytest.approx(0.3)
        assert crit["stats"]["max"] == pytest.approx(0.9)

    def test_diagnostics_funnel(self, lineage_store):
        """Funnel entries: All matched, then one per criterion."""
        op = Filter(
            params=Filter.Params(
                criteria=[
                    Criterion(metric="quality.score", operator="ge", value=0.3),
                    Criterion(metric="quality.n_chainbreaks", operator="eq", value=0),
                ]
            )
        )
        result = op.execute_curator(
            inputs=_make_inputs(
                {
                    "passthrough": ["pt_a", "pt_b"],
                    "quality": ["m_qa", "m_qb"],
                }
            ),
            step_number=0,
            artifact_store=lineage_store,
        )
        funnel = result.metadata["diagnostics"]["funnel"]
        assert len(funnel) == 3
        assert funnel[0]["label"] == "All evaluated"
        assert funnel[0]["count"] == 2
        # Both pass score >= 0.3
        assert funnel[1]["count"] == 2
        assert funnel[1]["eliminated"] == 0
        # Only pt_a has n_chainbreaks == 0
        assert funnel[2]["count"] == 1
        assert funnel[2]["eliminated"] == 1

    def test_diagnostics_empty_criteria(self, lineage_store):
        """Empty criteria -> all passthrough pass, empty criteria list."""
        op = Filter(params=Filter.Params(criteria=[]))
        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a"], "quality": ["m_qa"]}),
            step_number=0,
            artifact_store=lineage_store,
        )
        diag = result.metadata["diagnostics"]
        assert diag["criteria"] == []
        assert diag["total_passed"] == 1

    def test_diagnostics_all_filtered_out(self, lineage_store):
        """Impossible threshold -> total_passed=0, pass_count=0."""
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="quality.score", operator="gt", value=999)]
            )
        )
        result = op.execute_curator(
            inputs=_make_inputs(
                {
                    "passthrough": ["pt_a", "pt_b"],
                    "quality": ["m_qa", "m_qb"],
                }
            ),
            step_number=0,
            artifact_store=lineage_store,
        )
        diag = result.metadata["diagnostics"]
        assert diag["total_passed"] == 0
        assert diag["criteria"][0]["pass_count"] == 0

    def test_diagnostics_missing_field_pass_count_zero(self, lineage_store):
        """Nonexistent field -> pass_count=0."""
        op = Filter(
            params=Filter.Params(
                criteria=[
                    Criterion(metric="quality.nonexistent", operator="gt", value=0)
                ]
            )
        )
        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a"], "quality": ["m_qa"]}),
            step_number=0,
            artifact_store=lineage_store,
        )
        diag = result.metadata["diagnostics"]
        assert diag["criteria"][0]["pass_count"] == 0

    def test_diagnostics_total_evaluated(self, lineage_store):
        """total_evaluated reflects number of evaluable rows in wide_df."""
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="quality.score", operator="gt", value=0.5)]
            )
        )
        result = op.execute_curator(
            inputs=_make_inputs(
                {
                    "passthrough": ["pt_a", "pt_b"],
                    "quality": ["m_qa", "m_qb"],
                }
            ),
            step_number=0,
            artifact_store=lineage_store,
        )
        diag = result.metadata["diagnostics"]
        assert diag["total_evaluated"] == 2


def _build_implicit_store(
    pt_id: str,
    metric_entries: list[tuple[str, dict]],
) -> Mock:
    """Build a mock store for implicit metric resolution.

    Args:
        pt_id: Passthrough artifact ID.
        metric_entries: List of (metric_id, values_dict) tuples.
    """
    store = Mock()
    metric_ids = [m_id for m_id, _ in metric_entries]

    # get_descendant_ids_df returns DataFrame
    if metric_ids:
        store.get_descendant_ids_df.return_value = pl.DataFrame(
            {
                "source_artifact_id": [pt_id] * len(metric_ids),
                "target_artifact_id": metric_ids,
            }
        )
    else:
        store.get_descendant_ids_df.return_value = pl.DataFrame(
            schema={
                "source_artifact_id": pl.String,
                "target_artifact_id": pl.String,
            }
        )

    metric_values = {m_id: vals for m_id, vals in metric_entries}
    metrics_df = _make_metrics_df(metric_values)
    store.load_metrics_df.side_effect = lambda ids: (
        metrics_df.filter(pl.col("artifact_id").is_in(ids))
        if ids
        else pl.DataFrame(schema={"artifact_id": pl.String, "content": pl.Binary})
    )
    return store


class TestFilterImplicitMetrics:
    """Tests for implicit metric resolution via forward provenance."""

    def test_implicit_basic_pass(self):
        """Bare field criterion passes when descendant metric has the field."""
        pt_id = pad_id("pt")
        m_id = pad_id("met")

        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score", operator="gt", value=0.5)]
            )
        )

        store = _build_implicit_store(pt_id, [(m_id, {"score": 0.9})])
        result = op.execute_curator(
            inputs={"passthrough": _df([pt_id])},
            step_number=0,
            artifact_store=store,
        )

        assert result.success
        assert pt_id in result.passthrough["passthrough"]

    def test_implicit_basic_fail(self):
        """Bare field criterion fails when metric value doesn't meet threshold."""
        pt_id = pad_id("pt")
        m_id = pad_id("met")

        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score", operator="gt", value=0.5)]
            )
        )

        store = _build_implicit_store(pt_id, [(m_id, {"score": 0.3})])
        result = op.execute_curator(
            inputs={"passthrough": _df([pt_id])},
            step_number=0,
            artifact_store=store,
        )

        assert result.success
        assert len(result.passthrough["passthrough"]) == 0

    def test_implicit_multi_metric_merge(self):
        """Multiple MetricArtifacts with different fields merge correctly."""
        pt_id = pad_id("pt")
        ma_id = pad_id("ma")
        mb_id = pad_id("mb")

        op = Filter(
            params=Filter.Params(
                criteria=[
                    Criterion(metric="score", operator="gt", value=0.5),
                    Criterion(metric="accuracy", operator="lt", value=2.0),
                ]
            )
        )

        store = _build_implicit_store(
            pt_id, [(ma_id, {"score": 0.9}), (mb_id, {"accuracy": 1.5})]
        )
        result = op.execute_curator(
            inputs={"passthrough": _df([pt_id])},
            step_number=0,
            artifact_store=store,
        )

        assert result.success
        assert pt_id in result.passthrough["passthrough"]

    def test_implicit_field_collision_raises(self):
        """Same field in two metrics raises ValueError."""
        pt_id = pad_id("pt")
        ma_id = pad_id("ma")
        mb_id = pad_id("mb")

        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score", operator="gt", value=0.5)]
            )
        )

        store = _build_implicit_store(
            pt_id, [(ma_id, {"score": 0.9}), (mb_id, {"score": 0.8})]
        )
        with pytest.raises(ValueError, match="field collision"):
            op.execute_curator(
                inputs={"passthrough": _df([pt_id])},
                step_number=0,
                artifact_store=store,
            )

    def test_implicit_missing_field_fails(self):
        """Bare field not in any descendant metric -> artifact fails."""
        pt_id = pad_id("pt")
        m_id = pad_id("met")

        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score", operator="gt", value=0.5)]
            )
        )

        store = _build_implicit_store(pt_id, [(m_id, {"other_field": 0.9})])
        result = op.execute_curator(
            inputs={"passthrough": _df([pt_id])},
            step_number=0,
            artifact_store=store,
        )

        assert result.success
        assert len(result.passthrough["passthrough"]) == 0

    def test_implicit_no_associated_metrics(self):
        """No descendant metrics -> artifact fails implicit criteria."""
        pt_id = pad_id("pt")

        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score", operator="gt", value=0.5)]
            )
        )

        store = _build_implicit_store(pt_id, [])
        result = op.execute_curator(
            inputs={"passthrough": _df([pt_id])},
            step_number=0,
            artifact_store=store,
        )

        assert result.success
        assert len(result.passthrough["passthrough"]) == 0

    def test_implicit_diagnostics_v3(self):
        """Diagnostics version 3 with resolution='implicit'."""
        pt_id = pad_id("pt")
        m_id = pad_id("met")

        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score", operator="gt", value=0.5)]
            )
        )

        store = _build_implicit_store(pt_id, [(m_id, {"score": 0.9})])
        result = op.execute_curator(
            inputs={"passthrough": _df([pt_id])},
            step_number=0,
            artifact_store=store,
        )

        diag = result.metadata["diagnostics"]
        assert diag["version"] == 3
        assert diag["total_implicit_resolved"] == 1
        assert diag["total_explicit_matched"] == 0
        assert diag["criteria"][0]["resolution"] == "implicit"
        assert diag["criteria"][0]["metric"] == "score"

    def test_no_criteria_no_roles_error(self):
        """No metric roles and no implicit criteria -> ValueError."""
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="quality.score", operator="gt", value=0.5)]
            )
        )

        store = Mock()
        with pytest.raises(ValueError, match="at least one metric input role"):
            op.execute_curator(
                inputs=_make_inputs({"passthrough": ["pt"]}),
                step_number=0,
                artifact_store=store,
            )


class TestFilterHybridMode:
    """Tests for hybrid mode: both implicit and explicit criteria."""

    @pytest.fixture
    def hybrid_setup(self, lineage_store):
        """Setup with both explicit lineage-matched metrics and descendant metrics.

        pt_a has quality metric m_qa via explicit lineage (score=0.9).
        pt_a also has implicit descendant metric with accuracy=1.5.
        """
        ids = lineage_store._ids
        implicit_id = pad_id("imp")

        # Add get_descendant_ids_df for implicit resolution
        lineage_store.get_descendant_ids_df.return_value = pl.DataFrame(
            {
                "source_artifact_id": [ids["pt_a"]],
                "target_artifact_id": [implicit_id],
            }
        )

        # Extend load_metrics_df to also return the implicit metric
        original_side_effect = lineage_store.load_metrics_df.side_effect
        implicit_df = _make_metrics_df({implicit_id: {"accuracy": 1.5}})

        def combined_load_metrics(art_ids):
            result = original_side_effect(art_ids)
            extra = implicit_df.filter(pl.col("artifact_id").is_in(art_ids))
            if not extra.is_empty():
                result = pl.concat([result, extra])
            return result

        lineage_store.load_metrics_df.side_effect = combined_load_metrics

        return {
            "store": lineage_store,
            "ids": ids,
            "implicit_id": implicit_id,
        }

    def test_hybrid_both_pass(self, hybrid_setup):
        """Passes both implicit and explicit criteria."""
        setup = hybrid_setup
        op = Filter(
            params=Filter.Params(
                criteria=[
                    Criterion(metric="quality.score", operator="gt", value=0.5),
                    Criterion(metric="accuracy", operator="lt", value=2.0),
                ]
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a"], "quality": ["m_qa"]}),
            step_number=0,
            artifact_store=setup["store"],
        )

        assert result.success
        assert setup["ids"]["pt_a"] in result.passthrough["passthrough"]

    def test_hybrid_implicit_fails(self, hybrid_setup):
        """Passes explicit, fails implicit -> overall fail."""
        setup = hybrid_setup
        op = Filter(
            params=Filter.Params(
                criteria=[
                    Criterion(metric="quality.score", operator="gt", value=0.5),
                    Criterion(metric="accuracy", operator="lt", value=1.0),
                ]
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a"], "quality": ["m_qa"]}),
            step_number=0,
            artifact_store=setup["store"],
        )

        assert result.success
        assert len(result.passthrough["passthrough"]) == 0

    def test_hybrid_explicit_fails(self, hybrid_setup):
        """Passes implicit, fails explicit -> overall fail."""
        setup = hybrid_setup
        op = Filter(
            params=Filter.Params(
                criteria=[
                    Criterion(metric="quality.score", operator="gt", value=0.95),
                    Criterion(metric="accuracy", operator="lt", value=2.0),
                ]
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a"], "quality": ["m_qa"]}),
            step_number=0,
            artifact_store=setup["store"],
        )

        assert result.success
        assert len(result.passthrough["passthrough"]) == 0

    def test_hybrid_diagnostics(self, hybrid_setup):
        """Resolution mode per criterion in diagnostics."""
        setup = hybrid_setup
        op = Filter(
            params=Filter.Params(
                criteria=[
                    Criterion(metric="quality.score", operator="gt", value=0.5),
                    Criterion(metric="accuracy", operator="lt", value=2.0),
                ]
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a"], "quality": ["m_qa"]}),
            step_number=0,
            artifact_store=setup["store"],
        )

        diag = result.metadata["diagnostics"]
        assert diag["version"] == 3
        assert diag["criteria"][0]["resolution"] == "explicit"
        assert diag["criteria"][0]["metric"] == "quality.score"
        assert diag["criteria"][1]["resolution"] == "implicit"
        assert diag["criteria"][1]["metric"] == "accuracy"


class TestFilterPassthroughFailures:
    """Tests for passthrough_failures mode."""

    def test_all_pass_through(self, lineage_store):
        """Mixed pass/fail with passthrough_failures=True -> all in output."""
        ids = lineage_store._ids
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="quality.score", operator="gt", value=0.5)],
                passthrough_failures=True,
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs(
                {
                    "passthrough": ["pt_a", "pt_b"],
                    "quality": ["m_qa", "m_qb"],
                }
            ),
            step_number=0,
            artifact_store=lineage_store,
        )

        assert result.success
        passed = result.passthrough["passthrough"]
        assert ids["pt_a"] in passed
        assert ids["pt_b"] in passed
        # Diagnostics still reflect true evaluation
        diag = result.metadata["diagnostics"]
        assert diag["total_passed"] == 1  # Only pt_a truly passed
        assert diag["passthrough_failures"] is True

    def test_non_evaluable_included(self):
        """Artifact with no lineage match IS in output when passthrough_failures."""
        pt_id = pad_id("pt")
        store = _build_lineage_store(
            step_map={pt_id: 0, pad_id("met"): 1},
            edges=[],
            metric_values={},
        )

        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="quality.score", operator="gt", value=0)],
                passthrough_failures=True,
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt"], "quality": ["met"]}),
            step_number=0,
            artifact_store=store,
        )

        assert result.success
        assert pt_id in result.passthrough["passthrough"]

    def test_default_false_unchanged(self, lineage_store):
        """Default passthrough_failures=False preserves normal filtering."""
        ids = lineage_store._ids
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="quality.score", operator="gt", value=0.5)],
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs(
                {
                    "passthrough": ["pt_a", "pt_b"],
                    "quality": ["m_qa", "m_qb"],
                }
            ),
            step_number=0,
            artifact_store=lineage_store,
        )

        passed = result.passthrough["passthrough"]
        assert ids["pt_a"] in passed
        assert ids["pt_b"] not in passed
        assert "passthrough_failures" not in result.metadata["diagnostics"]

    def test_empty_criteria(self, lineage_store):
        """passthrough_failures + no criteria -> flag in diagnostics."""
        op = Filter(params=Filter.Params(criteria=[], passthrough_failures=True))

        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a"], "quality": ["m_qa"]}),
            step_number=0,
            artifact_store=lineage_store,
        )

        assert result.success
        diag = result.metadata["diagnostics"]
        assert diag["passthrough_failures"] is True

    def test_all_pass_naturally(self, lineage_store):
        """All artifacts pass criteria + passthrough_failures -> flag set, same output."""
        ids = lineage_store._ids
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="quality.score", operator="gt", value=0.0)],
                passthrough_failures=True,
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs(
                {
                    "passthrough": ["pt_a", "pt_b"],
                    "quality": ["m_qa", "m_qb"],
                }
            ),
            step_number=0,
            artifact_store=lineage_store,
        )

        assert result.success
        passed = result.passthrough["passthrough"]
        assert ids["pt_a"] in passed
        assert ids["pt_b"] in passed
        assert result.metadata["diagnostics"]["passthrough_failures"] is True

    def test_with_implicit_criteria(self):
        """Implicit criteria + passthrough_failures mode."""
        pt_id = pad_id("pt")
        m_id = pad_id("met")

        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score", operator="gt", value=0.5)],
                passthrough_failures=True,
            )
        )

        store = _build_implicit_store(pt_id, [(m_id, {"score": 0.3})])
        result = op.execute_curator(
            inputs={"passthrough": _df([pt_id])},
            step_number=0,
            artifact_store=store,
        )

        assert result.success
        # Fails criterion but still passed through
        assert pt_id in result.passthrough["passthrough"]
        assert result.metadata["diagnostics"]["total_passed"] == 0

    def test_logging(self, lineage_store, caplog):
        """Warning logged when failures are passed through."""
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="quality.score", operator="gt", value=0.5)],
                passthrough_failures=True,
            )
        )

        with caplog.at_level(
            logging.WARNING, logger="artisan.operations.curator.filter"
        ):
            op.execute_curator(
                inputs=_make_inputs(
                    {
                        "passthrough": ["pt_a", "pt_b"],
                        "quality": ["m_qa", "m_qb"],
                    }
                ),
                step_number=0,
                artifact_store=lineage_store,
            )

        assert any("passthrough_failures mode" in msg for msg in caplog.messages)


class TestFilterNestedMetricValues:
    """Tests for flattening nested metric dicts at hydration."""

    def test_filter_execute_nested_metrics(self, lineage_store):
        """Full execute_curator with nested MetricArtifacts."""
        ids = lineage_store._ids

        # Replace metric m_qa with nested values
        nested_values = {"scores": {"confidence": 0.85, "similarity": 0.7}}
        nested_df = _make_metrics_df({ids["m_qa"]: nested_values})

        # Update load_metrics_df to return nested metric for m_qa
        original_side_effect = lineage_store.load_metrics_df.side_effect

        def patched_load(art_ids):
            result = original_side_effect(art_ids)
            if ids["m_qa"] in art_ids:
                result = result.filter(pl.col("artifact_id") != ids["m_qa"])
                override = nested_df.filter(pl.col("artifact_id").is_in(art_ids))
                result = pl.concat([result, override])
            return result

        lineage_store.load_metrics_df.side_effect = patched_load

        op = Filter(
            params=Filter.Params(
                criteria=[
                    Criterion(
                        metric="quality.scores.confidence", operator="gt", value=0.5
                    ),
                ]
            )
        )
        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a"], "quality": ["m_qa"]}),
            step_number=0,
            artifact_store=lineage_store,
        )
        assert result.success
        assert ids["pt_a"] in result.passthrough["passthrough"]


# ---------------------------------------------------------------------------
# Chunked evaluation tests
# ---------------------------------------------------------------------------


def _build_multi_artifact_store(
    n_artifacts: int,
    score_fn,
) -> tuple[Mock, list[str], list[str]]:
    """Build a mock store with n passthrough artifacts and n metrics.

    Each passthrough pt_i has one metric m_i with {"score": score_fn(i)}.

    Returns:
        (store, pt_ids, metric_ids)
    """
    pt_ids = [pad_id(f"pt_{i}") for i in range(n_artifacts)]
    m_ids = [pad_id(f"m_{i}") for i in range(n_artifacts)]

    step_map = {}
    edges = []
    metric_values = {}
    for i in range(n_artifacts):
        step_map[pt_ids[i]] = 0
        step_map[m_ids[i]] = 1
        edges.append((pt_ids[i], m_ids[i]))
        metric_values[m_ids[i]] = {"score": score_fn(i)}

    store = _build_lineage_store(step_map, edges, metric_values)
    return store, pt_ids, m_ids


class TestFilterChunkedEvaluation:
    """Tests for chunked hydration + evaluation."""

    def test_chunk_size_param_default(self):
        """Default chunk_size is 100_000."""
        op = Filter()
        assert op.params.chunk_size == 100_000

    def test_chunk_size_param_custom(self):
        """Custom chunk_size via Params."""
        op = Filter(params=Filter.Params(chunk_size=50))
        assert op.params.chunk_size == 50

    def test_chunk_boundary_correct_results(self):
        """5 artifacts with chunk_size=2 gives same results as unchunked."""
        # Scores: 0.1, 0.6, 0.2, 0.8, 0.4 -> threshold 0.5 -> indices 1, 3 pass
        scores = [0.1, 0.6, 0.2, 0.8, 0.4]
        store, pt_ids, m_ids = _build_multi_artifact_store(5, lambda i: scores[i])

        criteria = [Criterion(metric="quality.score", operator="gt", value=0.5)]

        # Run with chunk_size=2
        op_chunked = Filter(params=Filter.Params(criteria=criteria, chunk_size=2))
        result_chunked = op_chunked.execute_curator(
            inputs={
                "passthrough": _df(pt_ids),
                "quality": _df(m_ids),
            },
            step_number=0,
            artifact_store=store,
        )

        # Run with chunk_size > total (single chunk)
        op_full = Filter(params=Filter.Params(criteria=criteria, chunk_size=100))
        result_full = op_full.execute_curator(
            inputs={
                "passthrough": _df(pt_ids),
                "quality": _df(m_ids),
            },
            step_number=0,
            artifact_store=store,
        )

        assert set(result_chunked.passthrough["passthrough"]) == set(
            result_full.passthrough["passthrough"]
        )
        assert set(result_chunked.passthrough["passthrough"]) == {
            pt_ids[1],
            pt_ids[3],
        }

    def test_chunk_size_1_correct(self):
        """chunk_size=1 processes each artifact individually, same result."""
        scores = [0.9, 0.3, 0.7]
        store, pt_ids, m_ids = _build_multi_artifact_store(3, lambda i: scores[i])

        criteria = [Criterion(metric="quality.score", operator="ge", value=0.5)]

        op = Filter(params=Filter.Params(criteria=criteria, chunk_size=1))
        result = op.execute_curator(
            inputs={
                "passthrough": _df(pt_ids),
                "quality": _df(m_ids),
            },
            step_number=0,
            artifact_store=store,
        )

        assert set(result.passthrough["passthrough"]) == {pt_ids[0], pt_ids[2]}

    def test_diagnostics_equivalence_chunked_vs_full(self):
        """Diagnostics (pass_count, min, max, mean, funnel) match across chunk sizes."""
        scores = [0.1, 0.6, 0.2, 0.8, 0.4]
        store, pt_ids, m_ids = _build_multi_artifact_store(5, lambda i: scores[i])

        criteria = [Criterion(metric="quality.score", operator="gt", value=0.3)]

        # Run chunked (chunk_size=2) and full
        op_chunked = Filter(params=Filter.Params(criteria=criteria, chunk_size=2))
        result_chunked = op_chunked.execute_curator(
            inputs={"passthrough": _df(pt_ids), "quality": _df(m_ids)},
            step_number=0,
            artifact_store=store,
        )

        op_full = Filter(params=Filter.Params(criteria=criteria, chunk_size=1000))
        result_full = op_full.execute_curator(
            inputs={"passthrough": _df(pt_ids), "quality": _df(m_ids)},
            step_number=0,
            artifact_store=store,
        )

        diag_c = result_chunked.metadata["diagnostics"]
        diag_f = result_full.metadata["diagnostics"]

        assert diag_c["total_input"] == diag_f["total_input"]
        assert diag_c["total_evaluated"] == diag_f["total_evaluated"]
        assert diag_c["total_passed"] == diag_f["total_passed"]

        # Per-criterion stats
        crit_c = diag_c["criteria"][0]
        crit_f = diag_f["criteria"][0]
        assert crit_c["pass_count"] == crit_f["pass_count"]
        assert crit_c["stats"]["min"] == pytest.approx(crit_f["stats"]["min"])
        assert crit_c["stats"]["max"] == pytest.approx(crit_f["stats"]["max"])
        assert crit_c["stats"]["mean"] == pytest.approx(crit_f["stats"]["mean"])

        # Funnel
        assert len(diag_c["funnel"]) == len(diag_f["funnel"])
        for fc, ff in zip(diag_c["funnel"], diag_f["funnel"], strict=True):
            assert fc["count"] == ff["count"]

    def test_single_chunk_degenerate_case(self):
        """chunk_size > total -> behaves exactly like non-chunked."""
        store, pt_ids, m_ids = _build_multi_artifact_store(
            3, lambda i: [0.9, 0.3, 0.7][i]
        )

        criteria = [Criterion(metric="quality.score", operator="gt", value=0.5)]
        op = Filter(params=Filter.Params(criteria=criteria, chunk_size=999_999))

        result = op.execute_curator(
            inputs={"passthrough": _df(pt_ids), "quality": _df(m_ids)},
            step_number=0,
            artifact_store=store,
        )

        assert set(result.passthrough["passthrough"]) == {pt_ids[0], pt_ids[2]}

    def test_passthrough_failures_with_chunking(self):
        """passthrough_failures=True + chunking -> all IDs pass through."""
        scores = [0.9, 0.3, 0.7]
        store, pt_ids, m_ids = _build_multi_artifact_store(3, lambda i: scores[i])

        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="quality.score", operator="gt", value=0.5)],
                passthrough_failures=True,
                chunk_size=2,
            )
        )

        result = op.execute_curator(
            inputs={"passthrough": _df(pt_ids), "quality": _df(m_ids)},
            step_number=0,
            artifact_store=store,
        )

        assert result.success
        # All 3 pass through
        assert set(result.passthrough["passthrough"]) == set(pt_ids)
        # But diagnostics reflect true pass count
        diag = result.metadata["diagnostics"]
        assert diag["total_passed"] == 2  # Only indices 0, 2 truly passed
        assert diag["passthrough_failures"] is True

    def test_chunk_preserves_input_order(self):
        """Passed IDs maintain input order across chunks."""
        scores = [0.9, 0.3, 0.7, 0.8, 0.1]
        store, pt_ids, m_ids = _build_multi_artifact_store(5, lambda i: scores[i])

        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="quality.score", operator="gt", value=0.5)],
                chunk_size=2,
            )
        )

        result = op.execute_curator(
            inputs={"passthrough": _df(pt_ids), "quality": _df(m_ids)},
            step_number=0,
            artifact_store=store,
        )

        passed = result.passthrough["passthrough"]
        # Expected order: pt_0 (chunk 0), pt_2 (chunk 1), pt_3 (chunk 1)
        assert passed == [pt_ids[0], pt_ids[2], pt_ids[3]]

    def test_chunk_implicit_criteria(self):
        """Chunked evaluation with implicit criteria."""
        pt_ids = [pad_id(f"pt_{i}") for i in range(4)]
        m_ids = [pad_id(f"m_{i}") for i in range(4)]
        scores = [0.9, 0.3, 0.7, 0.1]

        store = Mock()
        # get_descendant_ids_df returns DataFrame
        store.get_descendant_ids_df.return_value = pl.DataFrame(
            {
                "source_artifact_id": pt_ids,
                "target_artifact_id": m_ids,
            }
        )

        metric_values = {m_ids[i]: {"score": scores[i]} for i in range(4)}
        metrics_df = _make_metrics_df(metric_values)
        store.load_metrics_df.side_effect = lambda ids: (
            metrics_df.filter(pl.col("artifact_id").is_in(ids))
            if ids
            else pl.DataFrame(schema={"artifact_id": pl.String, "content": pl.Binary})
        )

        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score", operator="gt", value=0.5)],
                chunk_size=2,
            )
        )

        result = op.execute_curator(
            inputs={"passthrough": _df(pt_ids)},
            step_number=0,
            artifact_store=store,
        )

        assert result.success
        assert set(result.passthrough["passthrough"]) == {pt_ids[0], pt_ids[2]}
