"""Test forward and backward provenance traversal independently of input matching."""

from __future__ import annotations

import polars as pl
import pytest

from artisan.provenance.traversal import walk_backward, walk_forward


def _df(ids: list[str]) -> pl.DataFrame:
    return pl.DataFrame({"artifact_id": ids}, schema={"artifact_id": pl.String})


def _edges(
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


class TestWalkProvenanceBasic:
    """Tests for basic provenance walking."""

    def test_one_hop_walk(self):
        """Candidate is a direct child of target: A -> B."""
        candidates = _df(["B"])
        targets = _df(["A"])
        edges = _edges([("A", "B")])

        result = walk_backward(candidates, targets, edges)

        assert len(result) == 1
        assert result["candidate_id"][0] == "B"
        assert result["target_id"][0] == "A"

    def test_multi_hop_walk(self):
        """Candidate is 3 hops from target: A -> B -> C -> D."""
        candidates = _df(["D"])
        targets = _df(["A"])
        edges = _edges([("A", "B"), ("B", "C"), ("C", "D")])

        result = walk_backward(candidates, targets, edges)

        assert len(result) == 1
        assert result["candidate_id"][0] == "D"
        assert result["target_id"][0] == "A"

    def test_candidate_is_target(self):
        """Candidate that is itself a target matches immediately."""
        candidates = _df(["A"])
        targets = _df(["A"])
        edges = _edges([("X", "Y")])

        result = walk_backward(candidates, targets, edges)

        assert len(result) == 1
        assert result["candidate_id"][0] == "A"
        assert result["target_id"][0] == "A"

    def test_no_match(self):
        """Candidate with no path to any target returns empty."""
        candidates = _df(["X"])
        targets = _df(["A"])
        edges = _edges([("A", "B")])

        result = walk_backward(candidates, targets, edges)

        assert result.is_empty()
        assert result.columns == ["candidate_id", "target_id"]


class TestWalkProvenanceBranching:
    """Tests for branching DAG structures."""

    def test_branching_dag(self):
        """Multiple candidates descend from different targets.

        DAG:  T1 -> B -> C1
              T2 -> B -> C2
        """
        candidates = _df(["C1", "C2"])
        targets = _df(["T1", "T2"])
        edges = _edges([("T1", "B"), ("T2", "B"), ("B", "C1"), ("B", "C2")])

        result = walk_backward(candidates, targets, edges)

        # Both candidates should find a target through B
        assert len(result) == 2
        matched = dict(
            zip(
                result["candidate_id"].to_list(),
                result["target_id"].to_list(),
                strict=True,
            )
        )
        # Both should match (first-claim for B means both get T1 or T2)
        assert "C1" in matched
        assert "C2" in matched

    def test_mixed_match_no_match(self):
        """Some candidates match, others don't.

        DAG: T1 -> B -> C1    (C1 matches T1)
             X is isolated     (C2 has no path)
        """
        candidates = _df(["C1", "C2"])
        targets = _df(["T1"])
        edges = _edges([("T1", "B"), ("B", "C1")])

        result = walk_backward(candidates, targets, edges)

        assert len(result) == 1
        assert result["candidate_id"][0] == "C1"
        assert result["target_id"][0] == "T1"


class TestWalkProvenanceEdgeCases:
    """Tests for edge cases."""

    def test_empty_candidates(self):
        """Empty candidates returns empty result."""
        result = walk_backward(_df([]), _df(["A"]), _edges([("A", "B")]))
        assert result.is_empty()

    def test_empty_targets(self):
        """Empty targets returns empty result."""
        result = walk_backward(_df(["B"]), _df([]), _edges([("A", "B")]))
        assert result.is_empty()

    def test_empty_edges(self):
        """Empty edges returns empty result (unless candidate is target)."""
        result = walk_backward(_df(["B"]), _df(["A"]), _edges([]))
        assert result.is_empty()

    def test_empty_edges_preserve_self_matches(self):
        result = walk_backward(_df(["A", "B"]), _df(["A", "C"]), _edges([]))

        assert result.rows() == [("A", "A")]

    def test_cycle_in_edges_terminates(self):
        """Walk terminates even if edges form a cycle.

        Cycle: A -> B -> A (but target is T elsewhere)
        """
        candidates = _df(["B"])
        targets = _df(["T"])
        edges = _edges([("A", "B"), ("B", "A")])

        result = walk_backward(candidates, targets, edges)
        # No path to T, should terminate without infinite loop
        assert result.is_empty()

    def test_first_match_semantics(self):
        """Multiple paths to different targets: first match wins.

        DAG: T1 -> M -> C
             T2 -> M -> C
        """
        candidates = _df(["C"])
        targets = _df(["T1", "T2"])
        edges = _edges([("T1", "M"), ("T2", "M"), ("M", "C")])

        result = walk_backward(candidates, targets, edges)

        assert len(result) == 1
        assert result["candidate_id"][0] == "C"
        # Should match one of the targets
        assert result["target_id"][0] in ("T1", "T2")


class TestWalkForwardToTargets:
    """Tests for walk_forward."""

    def test_single_hop(self):
        """Source finds target one hop forward."""
        edges = _edges([("S", "T")], type_map={"T": "metric"})
        sources = pl.DataFrame({"artifact_id": ["S"]})

        result = walk_forward(sources, edges, target_type="metric")

        assert result.height == 1
        assert result["source_id"][0] == "S"
        assert result["target_id"][0] == "T"

    def test_multi_hop(self):
        """Source finds target two hops forward."""
        edges = _edges(
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
        edges = _edges([("S", "M")], type_map={"M": "data"})
        sources = pl.DataFrame({"artifact_id": ["S"]})

        result = walk_forward(sources, edges, target_type="metric")

        assert result.is_empty()

    def test_type_filtering(self):
        """Only nodes matching target_type count as targets."""
        edges = _edges(
            [("S", "D"), ("S", "M")],
            type_map={"D": "data", "M": "metric"},
        )
        sources = pl.DataFrame({"artifact_id": ["S"]})

        result = walk_forward(sources, edges, target_type="metric")

        assert result.height == 1
        assert result["target_id"][0] == "M"

    def test_all_match_semantics(self):
        """One source can match multiple targets."""
        edges = _edges(
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
        edges = _edges(
            [("S", "A"), ("S", "B"), ("A", "T"), ("B", "T")],
            type_map={"A": "data", "B": "data", "T": "metric"},
        )
        sources = pl.DataFrame({"artifact_id": ["S"]})

        result = walk_forward(sources, edges, target_type="metric")

        assert result.height == 1
        assert result["target_id"][0] == "T"

    def test_empty_sources(self):
        """Empty sources -> empty result."""
        edges = _edges([("S", "T")], type_map={"T": "metric"})
        sources = pl.DataFrame(schema={"artifact_id": pl.String})

        result = walk_forward(sources, edges, target_type="metric")

        assert result.is_empty()

    def test_empty_edges(self):
        """Empty edges -> empty result."""
        sources = pl.DataFrame({"artifact_id": ["S"]})
        edges = _edges([], type_map={})

        result = walk_forward(sources, edges, target_type="metric")

        assert result.is_empty()

    def test_no_type_filter_returns_all(self):
        """Without target_type, all reachable nodes are targets."""
        edges = _edges(
            [("S", "A"), ("A", "B")],
            type_map={"A": "data", "B": "metric"},
        )
        sources = pl.DataFrame({"artifact_id": ["S"]})

        result = walk_forward(sources, edges, target_type=None)

        target_ids = set(result["target_id"].to_list())
        assert target_ids == {"A", "B"}

    def test_multiple_sources(self):
        """Multiple sources each find their own targets."""
        edges = _edges(
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

    @pytest.mark.parametrize("source_ids", [["S"], []], ids=["sources", "no_sources"])
    @pytest.mark.parametrize("pairs", [[("S", "T")], []], ids=["edges", "no_edges"])
    def test_type_filter_requires_type_column(self, source_ids, pairs):
        with pytest.raises(ValueError, match="target_artifact_type"):
            walk_forward(_df(source_ids), _edges(pairs), target_type="metric")

    @pytest.mark.parametrize("pairs", [[("S", "A"), ("A", "B")], []])
    def test_untyped_edges_supported_without_filter(self, pairs):
        result = walk_forward(_df(["S"]), _edges(pairs))

        assert set(result.rows()) == ({("S", "A"), ("S", "B")} if pairs else set())
