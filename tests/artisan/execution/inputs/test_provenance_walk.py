"""Tests for backward provenance walk (artisan.provenance.traversal)."""

from __future__ import annotations

import polars as pl

from artisan.provenance.traversal import walk_backward


def _df(ids: list[str]) -> pl.DataFrame:
    return pl.DataFrame({"artifact_id": ids})


def _edges(pairs: list[tuple[str, str]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "source_artifact_id": [p[0] for p in pairs],
            "target_artifact_id": [p[1] for p in pairs],
        }
    )


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
