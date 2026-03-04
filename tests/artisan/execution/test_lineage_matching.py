"""Unit tests for backward-walk ancestry matching utilities.

Tests cover:
1. _build_target_ancestry_index — BFS index construction
2. _find_target — candidate-to-target resolution
3. match_by_ancestry — end-to-end matching with filtering
"""

from __future__ import annotations

import pytest

from artisan.execution.inputs.lineage_matching import (
    _build_target_ancestry_index,
    _find_target,
    match_by_ancestry,
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

    return provenance_map, ids


class TestBuildTargetAncestryIndex:
    """Tests for _build_target_ancestry_index."""

    def test_target_maps_to_self(self, provenance_graph):
        """Target ID in index maps to itself."""
        provenance_map, ids = provenance_graph
        index = _build_target_ancestry_index({ids["A1a"]}, provenance_map)

        assert index[ids["A1a"]] == ids["A1a"]

    def test_ancestors_map_to_target(self, provenance_graph):
        """Ancestors of target resolve to that target."""
        provenance_map, ids = provenance_graph
        index = _build_target_ancestry_index({ids["A1a"]}, provenance_map)

        # A1a -> A1 -> A
        assert index[ids["A1"]] == ids["A1a"]
        assert index[ids["A"]] == ids["A1a"]

    def test_multiple_targets_independent(self, provenance_graph):
        """Two targets from different lineages build independent index entries."""
        provenance_map, ids = provenance_graph
        index = _build_target_ancestry_index({ids["A1a"], ids["B1a"]}, provenance_map)

        assert index[ids["A1a"]] == ids["A1a"]
        assert index[ids["A1"]] == ids["A1a"]
        assert index[ids["A"]] == ids["A1a"]
        assert index[ids["B1a"]] == ids["B1a"]
        assert index[ids["B1"]] == ids["B1a"]
        assert index[ids["B"]] == ids["B1a"]

    def test_empty_targets(self, provenance_graph):
        """Empty target set produces empty index."""
        provenance_map, _ = provenance_graph
        index = _build_target_ancestry_index(set(), provenance_map)

        assert index == {}

    def test_shared_ancestor_first_claim_wins(self):
        """Ancestor reachable from 2 targets is claimed by first BFS."""
        # shared_root -> T1, shared_root -> T2
        shared_root = "root" + "0" * 28
        t1 = "t1" + "0" * 30
        t2 = "t2" + "0" * 30
        provenance_map = {t1: [shared_root], t2: [shared_root]}

        index = _build_target_ancestry_index({t1, t2}, provenance_map)

        # shared_root claimed by one of the two targets
        assert index[shared_root] in (t1, t2)
        # Both targets map to themselves
        assert index[t1] == t1
        assert index[t2] == t2


class TestFindTarget:
    """Tests for _find_target."""

    def test_candidate_in_index_directly(self, provenance_graph):
        """Candidate that IS a target resolves immediately."""
        provenance_map, ids = provenance_graph
        index = _build_target_ancestry_index({ids["A1a"]}, provenance_map)

        result = _find_target(ids["A1a"], index, provenance_map)
        assert result == ids["A1a"]

    def test_walks_backward_to_find_target(self, provenance_graph):
        """Candidate BFS backward finds ancestor in index (sibling case)."""
        provenance_map, ids = provenance_graph
        # Targets are A1a and B1a; candidates are M_A1 and M_B1
        index = _build_target_ancestry_index({ids["A1a"], ids["B1a"]}, provenance_map)

        # M_A1 walks back to A1, which is in index -> resolves to A1a
        result = _find_target(ids["M_A1"], index, provenance_map)
        assert result == ids["A1a"]

    def test_no_connection_returns_none(self, provenance_graph):
        """Unrelated candidate returns None."""
        provenance_map, ids = provenance_graph
        # Only index A-lineage
        index = _build_target_ancestry_index({ids["A1a"]}, provenance_map)

        # M_B1 has no connection to A-lineage
        result = _find_target(ids["M_B1"], index, provenance_map)
        assert result is None

    def test_candidate_one_hop_from_target(self, provenance_graph):
        """Direct child of target finds it in one step."""
        provenance_map, ids = provenance_graph
        # Target is A1 (step 1); candidate is A1a (step 2, child of A1)
        index = _build_target_ancestry_index({ids["A1"]}, provenance_map)

        result = _find_target(ids["A1a"], index, provenance_map)
        assert result == ids["A1"]


class TestMatchByAncestry:
    """Tests for match_by_ancestry."""

    def test_direct_ancestry_matching(self, provenance_graph):
        """Candidates are descendants of targets (different step numbers)."""
        provenance_map, ids = provenance_graph
        # Targets at step 1 (A1, B1), candidates at step 2 (A1a, B1a)
        result = match_by_ancestry(
            target_ids={ids["A1"], ids["B1"]},
            candidate_ids_by_role={"results": [ids["A1a"], ids["B1a"]]},
            provenance_map=provenance_map,
        )

        assert ids["A1"] in result
        assert result[ids["A1"]]["results"] == [ids["A1a"]]
        assert ids["B1"] in result
        assert result[ids["B1"]]["results"] == [ids["B1a"]]

    def test_sibling_matching(self, provenance_graph):
        """Candidates share common ancestor with targets (same step case)."""
        provenance_map, ids = provenance_graph
        # A1a and M_A1 are siblings (both step 2, share ancestor A1)
        result = match_by_ancestry(
            target_ids={ids["A1a"], ids["B1a"]},
            candidate_ids_by_role={"metrics": [ids["M_A1"], ids["M_B1"]]},
            provenance_map=provenance_map,
        )

        assert ids["A1a"] in result
        assert result[ids["A1a"]]["metrics"] == [ids["M_A1"]]
        assert ids["B1a"] in result
        assert result[ids["B1a"]]["metrics"] == [ids["M_B1"]]

    def test_multi_role_complete_matches(self, provenance_graph):
        """All roles match for all targets."""
        provenance_map, ids = provenance_graph
        result = match_by_ancestry(
            target_ids={ids["A1"], ids["B1"]},
            candidate_ids_by_role={
                "results": [ids["A1a"], ids["B1a"]],
                "metrics": [ids["M_A1"], ids["M_B1"]],
            },
            provenance_map=provenance_map,
        )

        assert len(result) == 2
        assert result[ids["A1"]]["results"] == [ids["A1a"]]
        assert result[ids["A1"]]["metrics"] == [ids["M_A1"]]

    def test_multi_role_incomplete_excluded(self, provenance_graph):
        """Target matched in role A but not role B is excluded."""
        provenance_map, ids = provenance_graph
        # A1 has child A1a in results, but M_B1 in metrics has no link to A1
        result = match_by_ancestry(
            target_ids={ids["A1"]},
            candidate_ids_by_role={
                "results": [ids["A1a"]],
                "metrics": [ids["M_B1"]],  # wrong lineage
            },
            provenance_map=provenance_map,
        )

        assert len(result) == 0

    def test_unmatched_candidate_warns(self, provenance_graph, caplog):
        """Warning logged with 'no common ancestor' for unmatched candidate."""
        provenance_map, ids = provenance_graph
        match_by_ancestry(
            target_ids={ids["A1a"]},
            candidate_ids_by_role={"metrics": [ids["M_B1"]]},
            provenance_map=provenance_map,
        )

        assert "no common ancestor" in caplog.text.lower()

    def test_multiple_candidates_same_target_accumulates(self):
        """Two candidates resolving to same target are both accumulated."""
        # Both C1 and C2 descend from T
        t = "t" * 32
        c1 = "c1" + "0" * 30
        c2 = "c2" + "0" * 30
        provenance_map = {c1: [t], c2: [t]}

        result = match_by_ancestry(
            target_ids={t},
            candidate_ids_by_role={"role": [c1, c2]},
            provenance_map=provenance_map,
        )

        assert t in result
        assert result[t]["role"] == [c1, c2]

    def test_one_to_n_matching(self):
        """1 target with N candidates produces list of N in result.

        Simulates parameter sweep: 1 dataset -> N configs via a shared root.
        """
        root = "root" + "0" * 28
        dataset = "ds" + "0" * 30
        c1 = "c1" + "0" * 30
        c2 = "c2" + "0" * 30
        c3 = "c3" + "0" * 30
        provenance_map = {
            dataset: [root],
            c1: [root],
            c2: [root],
            c3: [root],
        }

        result = match_by_ancestry(
            target_ids={dataset},
            candidate_ids_by_role={"configs": [c1, c2, c3]},
            provenance_map=provenance_map,
        )

        assert dataset in result
        assert result[dataset]["configs"] == [c1, c2, c3]

    def test_empty_targets_returns_empty(self, provenance_graph):
        """Empty target set produces empty result."""
        provenance_map, ids = provenance_graph
        result = match_by_ancestry(
            target_ids=set(),
            candidate_ids_by_role={"metrics": [ids["M_A1"]]},
            provenance_map=provenance_map,
        )

        assert result == {}

    def test_empty_candidates_returns_empty(self, provenance_graph):
        """Empty candidate dict produces empty result."""
        provenance_map, ids = provenance_graph
        result = match_by_ancestry(
            target_ids={ids["A1a"]},
            candidate_ids_by_role={},
            provenance_map=provenance_map,
        )

        assert result == {}
