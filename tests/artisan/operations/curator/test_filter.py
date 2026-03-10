"""Unit tests for the Filter operation (forward-walk metric discovery).

Tests cover:
- Class attributes (hydrate_inputs, etc.)
- Single and multi-criterion evaluation (all operators)
- Forward metric discovery (descendant metrics via provenance walk)
- Step disambiguation (step/step_number fields on Criterion)
- Collision detection (same field from multiple steps)
- Edge cases (empty criteria, missing field, type mismatch, empty passthrough)
- Chunked evaluation (chunk_size parameter, boundary tests, diagnostics)
- Passthrough failures mode
- Nested metric values
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


def _build_edges_df(
    edges: list[tuple[str, str]],
    *,
    include_target_type: bool = False,
    type_map: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Build provenance edges DataFrame from (source, target) pairs."""
    if not edges:
        schema = {"source_artifact_id": pl.String, "target_artifact_id": pl.String}
        if include_target_type:
            schema["target_artifact_type"] = pl.String
        return pl.DataFrame(schema=schema)

    data: dict[str, list[str]] = {
        "source_artifact_id": [e[0] for e in edges],
        "target_artifact_id": [e[1] for e in edges],
    }
    if include_target_type:
        type_map = type_map or {}
        data["target_artifact_type"] = [type_map.get(e[1], "data") for e in edges]
    return pl.DataFrame(data)


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


def _build_forward_walk_store(
    step_map: dict[str, int],
    edges: list[tuple[str, str]],
    metric_values: dict[str, dict],
    *,
    type_map: dict[str, str] | None = None,
    step_name_map: dict[int, str] | None = None,
) -> Mock:
    """Build a mock artifact store for forward-walk metric discovery.

    Args:
        step_map: {artifact_id: step_number}.
        edges: List of (source, target) provenance edges.
        metric_values: {metric_artifact_id: values_dict} for load_metrics_df.
        type_map: {artifact_id: artifact_type} for edge type annotations.
        step_name_map: {step_number: step_name} for load_step_name_map.
    """
    store = Mock()

    # Auto-build type_map from metric_values if not provided
    if type_map is None:
        type_map = {}
        for mid in metric_values:
            type_map[mid] = "metric"

    edges_df = _build_edges_df(edges, include_target_type=True, type_map=type_map)
    plain_edges_df = _build_edges_df(edges)

    # get_step_range returns (min, max) of step numbers for given IDs
    steps = list(step_map.values())
    if steps:
        store.get_step_range.return_value = (min(steps), max(steps))
    else:
        store.get_step_range.return_value = None

    def _load_edges(step_min, step_max, *, include_target_type=False):
        if include_target_type:
            return edges_df
        return plain_edges_df

    store.load_provenance_edges_df.side_effect = _load_edges

    metrics_df = _make_metrics_df(metric_values)
    store.load_metrics_df.side_effect = lambda ids: (
        metrics_df.filter(pl.col("artifact_id").is_in(ids))
        if ids
        else pl.DataFrame(schema={"artifact_id": pl.String, "content": pl.Binary})
    )

    # Build step_number_map: {artifact_id: step_number}
    store.load_step_number_map.side_effect = lambda ids=None: (
        {aid: sn for aid, sn in step_map.items() if ids is None or aid in ids}
    )

    # Step name map
    store.load_step_name_map.return_value = step_name_map or {}

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

    pt_a -> m_qa (score=0.9, n_chainbreaks=0) and m_la (component_score=0.08)
    pt_b -> m_qb (score=0.3, n_chainbreaks=2) and m_lb (component_score=0.04)
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

    # Edges: source produced target (forward direction)
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

    store = _build_forward_walk_store(
        step_map,
        edges,
        metric_values,
        step_name_map={0: "generator", 1: "passthrough_step", 2: "metric_calc"},
    )
    store._ids = ids

    return store


class TestFilterClassAttributes:
    """Tests for Filter class attributes."""

    def test_has_correct_name(self):
        assert Filter.name == "filter"

    def test_has_description(self):
        assert Filter.description is not None

    def test_hydrate_inputs_false(self):
        assert Filter.hydrate_inputs is False

    def test_passthrough_output_spec(self):
        assert "passthrough" in Filter.outputs
        assert Filter.outputs["passthrough"].artifact_type == ArtifactTypes.ANY
        assert Filter.outputs["passthrough"].required is False

    def test_passthrough_input_declared(self):
        assert "passthrough" in Filter.inputs
        assert Filter.inputs["passthrough"].artifact_type == ArtifactTypes.ANY
        assert Filter.inputs["passthrough"].required is True

    def test_no_runtime_defined_inputs(self):
        assert not getattr(Filter, "runtime_defined_inputs", False)

    def test_no_independent_input_streams(self):
        assert not getattr(Filter, "independent_input_streams", False)


class TestFilterSingleCriterion:
    """Tests for single criterion evaluation."""

    def test_single_criterion_pass(self, lineage_store):
        """One metric, one criterion that passes."""
        ids = lineage_store._ids
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score", operator="gt", value=0.5)]
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a"]}),
            step_number=0,
            artifact_store=lineage_store,
        )

        assert result.success
        assert ids["pt_a"] in result.passthrough["passthrough"]

    def test_single_criterion_fail(self, lineage_store):
        """One metric, one criterion that fails -> empty."""
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score", operator="gt", value=0.95)]
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a"]}),
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
                criteria=[Criterion(metric="score", operator="gt", value=0.5)]
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a", "pt_b"]}),
            step_number=0,
            artifact_store=lineage_store,
        )

        assert result.success
        passed = result.passthrough["passthrough"]
        # pt_a has score=0.9 (pass), pt_b has score=0.3 (fail)
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

        store = _build_forward_walk_store(step_map, edges, metric_values)
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
                criteria=[Criterion(metric="val", operator=op_name, value=threshold)]
            )
        )
        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt"]}),
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
        """No criteria -> all passthrough artifacts pass."""
        ids = lineage_store._ids
        op = Filter(params=Filter.Params(criteria=[]))

        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a"]}),
            step_number=0,
            artifact_store=lineage_store,
        )

        assert result.success
        assert ids["pt_a"] in result.passthrough["passthrough"]

    def test_missing_field_fails(self, lineage_store):
        """Criterion references nonexistent field -> artifact fails."""
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="nonexistent", operator="gt", value=0.5)]
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a"]}),
            step_number=0,
            artifact_store=lineage_store,
        )

        assert result.success
        assert len(result.passthrough["passthrough"]) == 0

    def test_empty_passthrough_returns_empty(self, lineage_store):
        """Zero passthrough inputs -> empty result."""
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score", operator="gt", value=0.5)]
            )
        )

        result = op.execute_curator(
            inputs={"passthrough": _df([])},
            step_number=0,
            artifact_store=lineage_store,
        )

        assert result.success
        assert len(result.passthrough["passthrough"]) == 0

    def test_missing_passthrough_raises(self, lineage_store):
        """No passthrough role -> ValueError."""
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score", operator="gt", value=0)]
            )
        )

        with pytest.raises(ValueError, match="passthrough"):
            op.execute_curator(
                inputs=_make_inputs({"other": ["m_qa"]}),
                step_number=0,
                artifact_store=lineage_store,
            )

    def test_no_descendant_metrics_fails(self):
        """Passthrough with no descendant metrics -> artifact fails criteria."""
        pt_id = pad_id("pt")
        store = _build_forward_walk_store(
            step_map={pt_id: 0},
            edges=[],
            metric_values={},
        )

        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score", operator="gt", value=0)]
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt"]}),
            step_number=0,
            artifact_store=store,
        )

        assert result.success
        assert len(result.passthrough["passthrough"]) == 0


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
        """'diagnostics' key in result.metadata, version 4."""
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score", operator="gt", value=0.5)]
            )
        )
        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a", "pt_b"]}),
            step_number=0,
            artifact_store=lineage_store,
        )
        assert "diagnostics" in result.metadata
        assert result.metadata["diagnostics"]["version"] == 4

    def test_diagnostics_counts(self, lineage_store):
        """total_input=2, total_metrics_discovered>0, total_passed=1."""
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score", operator="gt", value=0.5)]
            )
        )
        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a", "pt_b"]}),
            step_number=0,
            artifact_store=lineage_store,
        )
        diag = result.metadata["diagnostics"]
        assert diag["total_input"] == 2
        assert diag["total_metrics_discovered"] > 0
        assert diag["total_passed"] == 1

    def test_diagnostics_criteria_stats(self, lineage_store):
        """Check metric, pass_count, and stats."""
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score", operator="gt", value=0.5)]
            )
        )
        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a", "pt_b"]}),
            step_number=0,
            artifact_store=lineage_store,
        )
        crit = result.metadata["diagnostics"]["criteria"][0]
        assert crit["metric"] == "score"
        assert crit["pass_count"] == 1
        assert crit["stats"]["min"] == pytest.approx(0.3)
        assert crit["stats"]["max"] == pytest.approx(0.9)

    def test_diagnostics_funnel(self, lineage_store):
        """Funnel entries: All matched, then one per criterion."""
        op = Filter(
            params=Filter.Params(
                criteria=[
                    Criterion(metric="score", operator="ge", value=0.3),
                    Criterion(metric="n_chainbreaks", operator="eq", value=0),
                ]
            )
        )
        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a", "pt_b"]}),
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
            inputs=_make_inputs({"passthrough": ["pt_a"]}),
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
                criteria=[Criterion(metric="score", operator="gt", value=999)]
            )
        )
        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a", "pt_b"]}),
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
                criteria=[Criterion(metric="nonexistent", operator="gt", value=0)]
            )
        )
        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a"]}),
            step_number=0,
            artifact_store=lineage_store,
        )
        diag = result.metadata["diagnostics"]
        assert diag["criteria"][0]["pass_count"] == 0

    def test_diagnostics_total_evaluated(self, lineage_store):
        """total_evaluated reflects number of evaluable rows in wide_df."""
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score", operator="gt", value=0.5)]
            )
        )
        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a", "pt_b"]}),
            step_number=0,
            artifact_store=lineage_store,
        )
        diag = result.metadata["diagnostics"]
        assert diag["total_evaluated"] == 2


class TestForwardMetricDiscovery:
    """Tests for forward walk metric discovery."""

    def test_single_hop_discovery(self):
        """Forward walk finds metrics one hop from passthrough."""
        pt_id = pad_id("pt")
        m_id = pad_id("met")

        store = _build_forward_walk_store(
            step_map={pt_id: 0, m_id: 1},
            edges=[(pt_id, m_id)],
            metric_values={m_id: {"score": 0.9}},
        )

        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score", operator="gt", value=0.5)]
            )
        )
        result = op.execute_curator(
            inputs={"passthrough": _df([pt_id])},
            step_number=0,
            artifact_store=store,
        )

        assert result.success
        assert pt_id in result.passthrough["passthrough"]

    def test_multi_hop_discovery(self):
        """Forward walk finds metrics multiple hops from passthrough."""
        pt_id = pad_id("pt")
        mid_id = pad_id("mid")
        m_id = pad_id("met")

        store = _build_forward_walk_store(
            step_map={pt_id: 0, mid_id: 1, m_id: 2},
            edges=[(pt_id, mid_id), (mid_id, m_id)],
            metric_values={m_id: {"score": 0.9}},
            type_map={mid_id: "data", m_id: "metric"},
        )

        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score", operator="gt", value=0.5)]
            )
        )
        result = op.execute_curator(
            inputs={"passthrough": _df([pt_id])},
            step_number=0,
            artifact_store=store,
        )

        assert result.success
        assert pt_id in result.passthrough["passthrough"]

    def test_multiple_metric_sources_no_collision(self, lineage_store):
        """Multiple metrics with different fields merge without collision."""
        ids = lineage_store._ids
        op = Filter(
            params=Filter.Params(
                criteria=[
                    Criterion(metric="score", operator="ge", value=0.9),
                    Criterion(metric="component_score", operator="ge", value=0.06),
                ]
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a"]}),
            step_number=0,
            artifact_store=lineage_store,
        )

        assert result.success
        assert ids["pt_a"] in result.passthrough["passthrough"]

    def test_no_metrics_reachable(self):
        """Passthrough with no reachable metrics -> fails criteria."""
        pt_id = pad_id("pt")
        store = _build_forward_walk_store(
            step_map={pt_id: 0},
            edges=[],
            metric_values={},
        )

        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score", operator="gt", value=0)]
            )
        )
        result = op.execute_curator(
            inputs={"passthrough": _df([pt_id])},
            step_number=0,
            artifact_store=store,
        )

        assert result.success
        assert len(result.passthrough["passthrough"]) == 0


class TestCollisionDetection:
    """Tests for field collision detection across steps."""

    def test_same_field_two_steps_raises(self):
        """Same field name from two different steps raises ValueError."""
        pt_id = pad_id("pt")
        m1_id = pad_id("m1")
        m2_id = pad_id("m2")

        store = _build_forward_walk_store(
            step_map={pt_id: 0, m1_id: 1, m2_id: 2},
            edges=[(pt_id, m1_id), (pt_id, m2_id)],
            metric_values={
                m1_id: {"score": 0.9},
                m2_id: {"score": 0.8},
            },
            step_name_map={0: "generator", 1: "calc_a", 2: "calc_b"},
        )

        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score", operator="gt", value=0.5)]
            )
        )

        with pytest.raises(ValueError, match="multiple steps"):
            op.execute_curator(
                inputs={"passthrough": _df([pt_id])},
                step_number=0,
                artifact_store=store,
            )

    def test_different_fields_no_collision(self):
        """Different field names from different steps -> no collision."""
        pt_id = pad_id("pt")
        m1_id = pad_id("m1")
        m2_id = pad_id("m2")

        store = _build_forward_walk_store(
            step_map={pt_id: 0, m1_id: 1, m2_id: 2},
            edges=[(pt_id, m1_id), (pt_id, m2_id)],
            metric_values={
                m1_id: {"score_a": 0.9},
                m2_id: {"score_b": 0.8},
            },
        )

        op = Filter(
            params=Filter.Params(
                criteria=[
                    Criterion(metric="score_a", operator="gt", value=0.5),
                    Criterion(metric="score_b", operator="gt", value=0.5),
                ]
            )
        )

        result = op.execute_curator(
            inputs={"passthrough": _df([pt_id])},
            step_number=0,
            artifact_store=store,
        )

        assert result.success
        assert pt_id in result.passthrough["passthrough"]


class TestStepDisambiguation:
    """Tests for step/step_number disambiguation on Criterion."""

    def test_step_number_disambiguation(self):
        """step_number on criterion targets specific step's metrics."""
        pt_id = pad_id("pt")
        m1_id = pad_id("m1")
        m2_id = pad_id("m2")

        store = _build_forward_walk_store(
            step_map={pt_id: 0, m1_id: 1, m2_id: 2},
            edges=[(pt_id, m1_id), (pt_id, m2_id)],
            metric_values={
                m1_id: {"score": 0.9},
                m2_id: {"score": 0.3},
            },
            step_name_map={0: "generator", 1: "calc_a", 2: "calc_b"},
        )
        # Add load_artifact_ids_by_type mock
        store.load_artifact_ids_by_type.side_effect = lambda atype, step_numbers=None: (
            {m1_id}
            if step_numbers == [1]
            else {m2_id}
            if step_numbers == [2]
            else set()
        )

        op = Filter(
            params=Filter.Params(
                criteria=[
                    Criterion(metric="score", operator="gt", value=0.5, step_number=1),
                ]
            )
        )

        result = op.execute_curator(
            inputs={"passthrough": _df([pt_id])},
            step_number=0,
            artifact_store=store,
        )

        assert result.success
        # m1 from step 1 has score=0.9 > 0.5
        assert pt_id in result.passthrough["passthrough"]

    def test_step_name_disambiguation(self):
        """step name on criterion targets specific step's metrics."""
        pt_id = pad_id("pt")
        m1_id = pad_id("m1")
        m2_id = pad_id("m2")

        store = _build_forward_walk_store(
            step_map={pt_id: 0, m1_id: 1, m2_id: 2},
            edges=[(pt_id, m1_id), (pt_id, m2_id)],
            metric_values={
                m1_id: {"score": 0.9},
                m2_id: {"score": 0.3},
            },
            step_name_map={0: "generator", 1: "calc_quality", 2: "calc_ligand"},
        )
        store.load_artifact_ids_by_type.side_effect = lambda atype, step_numbers=None: (
            {m1_id}
            if step_numbers == [1]
            else {m2_id}
            if step_numbers == [2]
            else set()
        )

        op = Filter(
            params=Filter.Params(
                criteria=[
                    Criterion(
                        metric="score", operator="gt", value=0.5, step="calc_quality"
                    ),
                ]
            )
        )

        result = op.execute_curator(
            inputs={"passthrough": _df([pt_id])},
            step_number=0,
            artifact_store=store,
        )

        assert result.success
        assert pt_id in result.passthrough["passthrough"]

    def test_step_name_not_found_returns_empty(self):
        """Non-existent step name -> no metrics found, fails criteria."""
        pt_id = pad_id("pt")
        m_id = pad_id("met")

        store = _build_forward_walk_store(
            step_map={pt_id: 0, m_id: 1},
            edges=[(pt_id, m_id)],
            metric_values={m_id: {"score": 0.9}},
            step_name_map={0: "generator", 1: "calc"},
        )
        store.load_artifact_ids_by_type.return_value = set()

        op = Filter(
            params=Filter.Params(
                criteria=[
                    Criterion(
                        metric="score", operator="gt", value=0.5, step="nonexistent"
                    ),
                ]
            )
        )

        result = op.execute_curator(
            inputs={"passthrough": _df([pt_id])},
            step_number=0,
            artifact_store=store,
        )

        assert result.success
        assert len(result.passthrough["passthrough"]) == 0


class TestFilterPassthroughFailures:
    """Tests for passthrough_failures mode."""

    def test_all_pass_through(self, lineage_store):
        """Mixed pass/fail with passthrough_failures=True -> all in output."""
        ids = lineage_store._ids
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score", operator="gt", value=0.5)],
                passthrough_failures=True,
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a", "pt_b"]}),
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
        """Artifact with no descendant metrics IS in output when passthrough_failures."""
        pt_id = pad_id("pt")
        store = _build_forward_walk_store(
            step_map={pt_id: 0},
            edges=[],
            metric_values={},
        )

        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score", operator="gt", value=0)],
                passthrough_failures=True,
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt"]}),
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
                criteria=[Criterion(metric="score", operator="gt", value=0.5)],
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a", "pt_b"]}),
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
            inputs=_make_inputs({"passthrough": ["pt_a"]}),
            step_number=0,
            artifact_store=lineage_store,
        )

        assert result.success
        diag = result.metadata["diagnostics"]
        assert diag["passthrough_failures"] is True

    def test_all_pass_naturally(self, lineage_store):
        """All artifacts pass criteria + passthrough_failures -> flag set."""
        ids = lineage_store._ids
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score", operator="gt", value=0.0)],
                passthrough_failures=True,
            )
        )

        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a", "pt_b"]}),
            step_number=0,
            artifact_store=lineage_store,
        )

        assert result.success
        passed = result.passthrough["passthrough"]
        assert ids["pt_a"] in passed
        assert ids["pt_b"] in passed
        assert result.metadata["diagnostics"]["passthrough_failures"] is True

    def test_logging(self, lineage_store, caplog):
        """Warning logged when failures are passed through."""
        op = Filter(
            params=Filter.Params(
                criteria=[Criterion(metric="score", operator="gt", value=0.5)],
                passthrough_failures=True,
            )
        )

        with caplog.at_level(
            logging.WARNING, logger="artisan.operations.curator.filter"
        ):
            op.execute_curator(
                inputs=_make_inputs({"passthrough": ["pt_a", "pt_b"]}),
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
                    Criterion(metric="scores.confidence", operator="gt", value=0.5),
                ]
            )
        )
        result = op.execute_curator(
            inputs=_make_inputs({"passthrough": ["pt_a"]}),
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

    store = _build_forward_walk_store(step_map, edges, metric_values)
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

        criteria = [Criterion(metric="score", operator="gt", value=0.5)]

        # Run with chunk_size=2
        op_chunked = Filter(params=Filter.Params(criteria=criteria, chunk_size=2))
        result_chunked = op_chunked.execute_curator(
            inputs={"passthrough": _df(pt_ids)},
            step_number=0,
            artifact_store=store,
        )

        # Run with chunk_size > total (single chunk)
        op_full = Filter(params=Filter.Params(criteria=criteria, chunk_size=100))
        result_full = op_full.execute_curator(
            inputs={"passthrough": _df(pt_ids)},
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

        criteria = [Criterion(metric="score", operator="ge", value=0.5)]

        op = Filter(params=Filter.Params(criteria=criteria, chunk_size=1))
        result = op.execute_curator(
            inputs={"passthrough": _df(pt_ids)},
            step_number=0,
            artifact_store=store,
        )

        assert set(result.passthrough["passthrough"]) == {pt_ids[0], pt_ids[2]}

    def test_diagnostics_equivalence_chunked_vs_full(self):
        """Diagnostics (pass_count, min, max, mean, funnel) match across chunk sizes."""
        scores = [0.1, 0.6, 0.2, 0.8, 0.4]
        store, pt_ids, m_ids = _build_multi_artifact_store(5, lambda i: scores[i])

        criteria = [Criterion(metric="score", operator="gt", value=0.3)]

        # Run chunked (chunk_size=2) and full
        op_chunked = Filter(params=Filter.Params(criteria=criteria, chunk_size=2))
        result_chunked = op_chunked.execute_curator(
            inputs={"passthrough": _df(pt_ids)},
            step_number=0,
            artifact_store=store,
        )

        op_full = Filter(params=Filter.Params(criteria=criteria, chunk_size=1000))
        result_full = op_full.execute_curator(
            inputs={"passthrough": _df(pt_ids)},
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

        criteria = [Criterion(metric="score", operator="gt", value=0.5)]
        op = Filter(params=Filter.Params(criteria=criteria, chunk_size=999_999))

        result = op.execute_curator(
            inputs={"passthrough": _df(pt_ids)},
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
                criteria=[Criterion(metric="score", operator="gt", value=0.5)],
                passthrough_failures=True,
                chunk_size=2,
            )
        )

        result = op.execute_curator(
            inputs={"passthrough": _df(pt_ids)},
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
                criteria=[Criterion(metric="score", operator="gt", value=0.5)],
                chunk_size=2,
            )
        )

        result = op.execute_curator(
            inputs={"passthrough": _df(pt_ids)},
            step_number=0,
            artifact_store=store,
        )

        passed = result.passthrough["passthrough"]
        # Expected order: pt_0 (chunk 0), pt_2 (chunk 1), pt_3 (chunk 1)
        assert passed == [pt_ids[0], pt_ids[2], pt_ids[3]]
