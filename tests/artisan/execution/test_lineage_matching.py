"""Test strict ancestry matching, including role completeness and ambiguity."""

from __future__ import annotations

import logging

import polars as pl
import pytest

from artisan.execution.inputs.lineage_matching import match_by_ancestry


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

        Step 0: A         B
                |         |
        Step 1: A1        B1
                |         |
        Step 2: A1a       B1a
                |         |
        Step 3: M_A1      M_B1

    M_A1 is a direct descendant of A1a; M_B1 of B1a.
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
        M_A1: [A1a],
        M_B1: [B1a],
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

    def test_descendant_matching(self, provenance_graph):
        """Candidates are direct descendants of targets via the directed graph."""
        edges, ids = provenance_graph
        # M_A1 walks back through A1a (target); M_B1 through B1a.
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

    def test_unmatched_candidate_warns(self, provenance_graph, caplog, monkeypatch):
        """Warning logged with 'no directed path' for unmatched candidate."""
        # Pipeline logging can disable propagation before this test runs.
        monkeypatch.setattr(logging.getLogger("artisan"), "propagate", True)
        edges, ids = provenance_graph
        match_by_ancestry(
            target_ids={ids["A1a"]},
            candidate_ids_by_role={"metrics": [ids["M_B1"]]},
            edges=edges,
        )

        assert "no directed path" in caplog.text.lower()

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

        Simulates parameter sweep: 1 dataset -> N configs, each a direct
        descendant of the dataset.
        """
        root = "root" + "0" * 28
        dataset = "ds" + "0" * 30
        c1 = "c1" + "0" * 30
        c2 = "c2" + "0" * 30
        c3 = "c3" + "0" * 30
        edges = _provenance_map_to_edges(
            {
                dataset: [root],
                c1: [dataset],
                c2: [dataset],
                c3: [dataset],
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

    def test_multi_target_sibling_collision_resolved(self):
        """N sibling targets sharing one root, each with K descendants.

        Reproduces the bug case the directional matcher fixes: every
        descendant shares the root with every sibling, so the symmetric
        matcher non-deterministically picks among the N targets. The
        directional matcher pairs each descendant with its direct-parent
        sibling deterministically.
        """
        root = "root" + "0" * 28
        targets = [f"t{i}" + "0" * 30 for i in range(1, 4)]
        # 3 candidates per target = 9 total
        candidates = {
            t: [f"c{i}_{j}" + "0" * (32 - 4) for j in range(3)]
            for i, t in enumerate(targets, 1)
        }
        provenance_map: dict[str, list[str]] = {t: [root] for t in targets}
        for target, cs in candidates.items():
            for c in cs:
                provenance_map[c] = [target]
        edges = _provenance_map_to_edges(provenance_map)

        all_candidates = [c for cs in candidates.values() for c in cs]

        # Run the matcher 5 times; results must be identical every time.
        prior_result: dict[str, dict[str, list[str]]] | None = None
        for _ in range(5):
            result = match_by_ancestry(
                target_ids=set(targets),
                candidate_ids_by_role={"items": all_candidates},
                edges=edges,
            )
            if prior_result is not None:
                assert result == prior_result
            prior_result = result

        # Each target paired only with its own direct-descendant candidates.
        assert set(result.keys()) == set(targets)
        for target, expected_cs in candidates.items():
            assert set(result[target]["items"]) == set(expected_cs)

    def test_tie_at_same_hop_raises(self):
        """Candidate with two distinct targets at hop 1 on different branches."""
        t1 = "t1" + "0" * 30
        t2 = "t2" + "0" * 30
        c = "c1" + "0" * 30
        # c has both t1 and t2 as parents at hop 1.
        edges = _provenance_map_to_edges({c: [t1, t2]})

        with pytest.raises(RuntimeError, match="multiple targets at hop depth 1"):
            match_by_ancestry(
                target_ids={t1, t2},
                candidate_ids_by_role={"items": [c]},
                edges=edges,
            )

    def test_closer_target_wins(self):
        """Two targets in candidate's directed lineage at different depths."""
        # Chain: C ← A ← B. Both A and B are targets; A is closer to C.
        c = "c1" + "0" * 30
        a = "a1" + "0" * 30
        b = "b1" + "0" * 30
        edges = _provenance_map_to_edges({c: [a], a: [b]})

        result = match_by_ancestry(
            target_ids={a, b},
            candidate_ids_by_role={"items": [c]},
            edges=edges,
        )

        # Closer target (A at hop 1) wins; B (hop 2) is never visited.
        assert a in result
        assert b not in result
        assert result[a]["items"] == [c]

    def test_self_match_at_depth_zero(self):
        """Candidate that IS a target self-matches at depth 0."""
        t = "t1" + "0" * 30
        # No edges needed; self-match should resolve before any walk.
        edges = _provenance_map_to_edges({})

        result = match_by_ancestry(
            target_ids={t},
            candidate_ids_by_role={"items": [t]},
            edges=edges,
        )

        assert t in result
        assert result[t]["items"] == [t]
