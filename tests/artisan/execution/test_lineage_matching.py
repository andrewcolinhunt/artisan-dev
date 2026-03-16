"""Unit tests for provenance graph walking utilities.

Tests cover:
- match_by_ancestry — end-to-end matching with filtering
- walk_forward — forward provenance walk for metric discovery
"""

from __future__ import annotations

import polars as pl
import pytest

from artisan.execution.inputs.lineage_matching import match_by_ancestry
from artisan.provenance.traversal import walk_forward


def _provenance_map_to_edges(
    provenance_map: dict[str, list[str]],
) -> pl.DataFrame:
    """Convert {target: [sources]} dict to edges DataFrame."""
    sources = []
    targets = []
    for target_id, source_ids in provenance_map.items():
        for source_id in source_ids:
            sources.append(source_id)
            targets.append(target_id)
    if not sources:
        return pl.DataFrame(
            schema={
                "source_artifact_id": pl.String,
                "target_artifact_id": pl.String,
            }
        )
    return pl.DataFrame(
        {
            "source_artifact_id": sources,
            "target_artifact_id": targets,
        }
    )


@pytest.fixture
def provenance_graph():
    """Create provenance data for a standard provenance graph.

    Simulates provenance graph::

        Step 0: A ------------------- B
                |                     |
        Step 1: A1                    B1
               /  \\                 /  \\
        Step 2: A1a  M_A1           B1a  M_B1

    A1a and M_A1 share ancestor A1 (step 1)
    B1a and M_B1 share ancestor B1 (step 1)
    """
    A = "a" * 32
    B = "b" * 32
    A1 = "a1" + "0" * 30
    B1 = "b1" + "0" * 30
    A1a = "a1a" + "0" * 29
    B1a = "b1a" + "0" * 29
    M_A1 = "ma1" + "0" * 29
    M_B1 = "mb1" + "0" * 29

    # provenance_map: {target_id: [source_ids]}
    provenance_map = {
        A1: [A],
        B1: [B],
        A1a: [A1],
        B1a: [B1],
        M_A1: [A1],
        M_B1: [B1],
    }

    edges = _provenance_map_to_edges(provenance_map)

    ids = {
        "A": A,
        "B": B,
        "A1": A1,
        "B1": B1,
        "A1a": A1a,
        "B1a": B1a,
        "M_A1": M_A1,
        "M_B1": M_B1,
    }

    return edges, ids


class TestMatchByAncestry:
    """Tests for match_by_ancestry."""

    def test_direct_ancestry_matching(self, provenance_graph):
        """Candidates are descendants of targets (different step numbers)."""
        edges, ids = provenance_graph
        # Targets at step 1 (A1, B1), candidates at step 2 (A1a, B1a)
        result = match_by_ancestry(
            target_ids={ids["A1"], ids["B1"]},
            candidate_ids_by_role={"results": [ids["A1a"], ids["B1a"]]},
            edges=edges,
        )

        assert ids["A1"] in result
        assert result[ids["A1"]]["results"] == [ids["A1a"]]
        assert ids["B1"] in result
        assert result[ids["B1"]]["results"] == [ids["B1a"]]

    def test_sibling_matching(self, provenance_graph):
        """Candidates share common ancestor with targets (same step case)."""
        edges, ids = provenance_graph
        # A1a and M_A1 are siblings (both step 2, share ancestor A1)
        result = match_by_ancestry(
            target_ids={ids["A1a"], ids["B1a"]},
            candidate_ids_by_role={"metrics": [ids["M_A1"], ids["M_B1"]]},
            edges=edges,
        )

        assert ids["A1a"] in result
        assert result[ids["A1a"]]["metrics"] == [ids["M_A1"]]
        assert ids["B1a"] in result
        assert result[ids["B1a"]]["metrics"] == [ids["M_B1"]]

    def test_multi_role_complete_matches(self, provenance_graph):
        """All roles match for all targets."""
        edges, ids = provenance_graph
        result = match_by_ancestry(
            target_ids={ids["A1"], ids["B1"]},
            candidate_ids_by_role={
                "results": [ids["A1a"], ids["B1a"]],
                "metrics": [ids["M_A1"], ids["M_B1"]],
            },
            edges=edges,
        )

        assert len(result) == 2
        assert result[ids["A1"]]["results"] == [ids["A1a"]]
        assert result[ids["A1"]]["metrics"] == [ids["M_A1"]]

    def test_multi_role_incomplete_excluded(self, provenance_graph):
        """Target matched in role A but not role B is excluded."""
        edges, ids = provenance_graph
        # A1 has child A1a in results, but M_B1 in metrics has no link to A1
        result = match_by_ancestry(
            target_ids={ids["A1"]},
            candidate_ids_by_role={
                "results": [ids["A1a"]],
                "metrics": [ids["M_B1"]],  # wrong lineage
            },
            edges=edges,
        )

        assert len(result) == 0

    def test_unmatched_candidate_warns(self, provenance_graph, caplog):
        """Warning logged with 'no common ancestor' for unmatched candidate."""
        edges, ids = provenance_graph
        match_by_ancestry(
            target_ids={ids["A1a"]},
            candidate_ids_by_role={"metrics": [ids["M_B1"]]},
            edges=edges,
        )

        assert "no common ancestor" in caplog.text.lower()

    def test_multiple_candidates_same_target_accumulates(self):
        """Two candidates resolving to same target are both accumulated."""
        # Both C1 and C2 descend from T
        t = "t" * 32
        c1 = "c1" + "0" * 30
        c2 = "c2" + "0" * 30
        edges = _provenance_map_to_edges({c1: [t], c2: [t]})

        result = match_by_ancestry(
            target_ids={t},
            candidate_ids_by_role={"role": [c1, c2]},
            edges=edges,
        )

        assert t in result
        assert set(result[t]["role"]) == {c1, c2}

    def test_one_to_n_matching(self):
        """1 target with N candidates produces list of N in result.

        Simulates parameter sweep: 1 dataset -> N configs via a shared root.
        """
        root = "root" + "0" * 28
        dataset = "ds" + "0" * 30
        c1 = "c1" + "0" * 30
        c2 = "c2" + "0" * 30
        c3 = "c3" + "0" * 30
        edges = _provenance_map_to_edges(
            {
                dataset: [root],
                c1: [root],
                c2: [root],
                c3: [root],
            }
        )

        result = match_by_ancestry(
            target_ids={dataset},
            candidate_ids_by_role={"configs": [c1, c2, c3]},
            edges=edges,
        )

        assert dataset in result
        assert set(result[dataset]["configs"]) == {c1, c2, c3}

    def test_empty_targets_returns_empty(self, provenance_graph):
        """Empty target set produces empty result."""
        edges, ids = provenance_graph
        result = match_by_ancestry(
            target_ids=set(),
            candidate_ids_by_role={"metrics": [ids["M_A1"]]},
            edges=edges,
        )

        assert result == {}

    def test_empty_candidates_returns_empty(self, provenance_graph):
        """Empty candidate dict produces empty result."""
        edges, ids = provenance_graph
        result = match_by_ancestry(
            target_ids={ids["A1a"]},
            candidate_ids_by_role={},
            edges=edges,
        )

        assert result == {}


def _edges_df(
    edges: list[tuple[str, str]],
    *,
    type_map: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Build edges DataFrame with optional target_artifact_type column."""
    if not edges:
        schema = {
            "source_artifact_id": pl.String,
            "target_artifact_id": pl.String,
        }
        if type_map is not None:
            schema["target_artifact_type"] = pl.String
        return pl.DataFrame(schema=schema)

    data: dict[str, list[str]] = {
        "source_artifact_id": [e[0] for e in edges],
        "target_artifact_id": [e[1] for e in edges],
    }
    if type_map is not None:
        data["target_artifact_type"] = [type_map.get(e[1], "data") for e in edges]
    return pl.DataFrame(data)


class TestWalkForwardToTargets:
    """Tests for walk_forward."""

    def test_single_hop(self):
        """Source finds target one hop forward."""
        edges = _edges_df([("S", "T")], type_map={"T": "metric"})
        sources = pl.DataFrame({"artifact_id": ["S"]})

        result = walk_forward(sources, edges, target_type="metric")

        assert result.height == 1
        assert result["source_id"][0] == "S"
        assert result["target_id"][0] == "T"

    def test_multi_hop(self):
        """Source finds target two hops forward."""
        edges = _edges_df(
            [("S", "M"), ("M", "T")],
            type_map={"M": "data", "T": "metric"},
        )
        sources = pl.DataFrame({"artifact_id": ["S"]})

        result = walk_forward(sources, edges, target_type="metric")

        assert result.height == 1
        assert result["source_id"][0] == "S"
        assert result["target_id"][0] == "T"

    def test_no_targets_reachable(self):
        """No targets of the requested type -> empty result."""
        edges = _edges_df([("S", "M")], type_map={"M": "data"})
        sources = pl.DataFrame({"artifact_id": ["S"]})

        result = walk_forward(sources, edges, target_type="metric")

        assert result.is_empty()

    def test_type_filtering(self):
        """Only nodes matching target_type count as targets."""
        edges = _edges_df(
            [("S", "D"), ("S", "M")],
            type_map={"D": "data", "M": "metric"},
        )
        sources = pl.DataFrame({"artifact_id": ["S"]})

        result = walk_forward(sources, edges, target_type="metric")

        assert result.height == 1
        assert result["target_id"][0] == "M"

    def test_all_match_semantics(self):
        """One source can match multiple targets."""
        edges = _edges_df(
            [("S", "M1"), ("S", "M2")],
            type_map={"M1": "metric", "M2": "metric"},
        )
        sources = pl.DataFrame({"artifact_id": ["S"]})

        result = walk_forward(sources, edges, target_type="metric")

        assert result.height == 2
        target_ids = set(result["target_id"].to_list())
        assert target_ids == {"M1", "M2"}

    def test_diamond_graph(self):
        """Diamond: S -> A, S -> B, A -> T, B -> T still finds T once."""
        edges = _edges_df(
            [("S", "A"), ("S", "B"), ("A", "T"), ("B", "T")],
            type_map={"A": "data", "B": "data", "T": "metric"},
        )
        sources = pl.DataFrame({"artifact_id": ["S"]})

        result = walk_forward(sources, edges, target_type="metric")

        assert result.height == 1
        assert result["target_id"][0] == "T"

    def test_empty_sources(self):
        """Empty sources -> empty result."""
        edges = _edges_df([("S", "T")], type_map={"T": "metric"})
        sources = pl.DataFrame(schema={"artifact_id": pl.String})

        result = walk_forward(sources, edges, target_type="metric")

        assert result.is_empty()

    def test_empty_edges(self):
        """Empty edges -> empty result."""
        sources = pl.DataFrame({"artifact_id": ["S"]})
        edges = _edges_df([], type_map={})

        result = walk_forward(sources, edges, target_type="metric")

        assert result.is_empty()

    def test_no_type_filter_returns_all(self):
        """Without target_type, all reachable nodes are targets."""
        edges = _edges_df(
            [("S", "A"), ("A", "B")],
            type_map={"A": "data", "B": "metric"},
        )
        sources = pl.DataFrame({"artifact_id": ["S"]})

        result = walk_forward(sources, edges, target_type=None)

        target_ids = set(result["target_id"].to_list())
        assert target_ids == {"A", "B"}

    def test_multiple_sources(self):
        """Multiple sources each find their own targets."""
        edges = _edges_df(
            [("S1", "M1"), ("S2", "M2")],
            type_map={"M1": "metric", "M2": "metric"},
        )
        sources = pl.DataFrame({"artifact_id": ["S1", "S2"]})

        result = walk_forward(sources, edges, target_type="metric")

        assert result.height == 2
        pairs = set(
            zip(
                result["source_id"].to_list(),
                result["target_id"].to_list(),
                strict=False,
            )
        )
        assert pairs == {("S1", "M1"), ("S2", "M2")}
