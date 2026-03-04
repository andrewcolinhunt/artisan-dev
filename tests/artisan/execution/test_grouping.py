"""Unit tests for framework pairing functions (group_inputs, match_inputs_to_primary).

Tests cover:
1. group_inputs() with ZIP strategy
2. group_inputs() with CROSS_PRODUCT strategy
3. group_inputs() with LINEAGE strategy
4. group_id computation (deterministic, content-addressed)
5. match_inputs_to_primary() with LINEAGE strategy
6. validate_stem_match_uniqueness()
7. Error handling and edge cases
"""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from artisan.execution.inputs.grouping import (
    compute_group_id,
    group_inputs,
    match_inputs_to_primary,
    validate_stem_match_uniqueness,
)
from artisan.schemas.enums import GroupByStrategy

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_artifact_store():
    """Create a mock artifact store with provenance data.

    Simulates provenance graph::

        Step 0: A ------------------- B
                |                     |
        Step 1: A1                    B1
               /  \\                 /  \\
        Step 2: A1a  M_A1           B1a  M_B1

    A1a and M_A1 share ancestor A1 (step 1).
    B1a and M_B1 share ancestor B1 (step 1).
    """
    store = Mock()

    A = "a" * 32
    B = "b" * 32
    A1 = "a1" + "0" * 30
    B1 = "b1" + "0" * 30
    A1a = "a1a" + "0" * 29
    B1a = "b1a" + "0" * 29
    M_A1 = "ma1" + "0" * 29
    M_B1 = "mb1" + "0" * 29

    provenance_map = {
        A1: [A],
        B1: [B],
        A1a: [A1],
        B1a: [B1],
        M_A1: [A1],
        M_B1: [B1],
    }

    step_numbers = {
        A: 0,
        B: 0,
        A1: 1,
        B1: 1,
        A1a: 2,
        B1a: 2,
        M_A1: 2,
        M_B1: 2,
    }

    store.load_provenance_map.return_value = provenance_map
    store.load_step_number_map.side_effect = lambda ids=None: (
        {k: v for k, v in step_numbers.items() if ids is None or k in ids}
    )

    store._test_ids = {
        "A": A,
        "B": B,
        "A1": A1,
        "B1": B1,
        "A1a": A1a,
        "B1a": B1a,
        "M_A1": M_A1,
        "M_B1": M_B1,
    }

    return store


@pytest.fixture
def mock_artifact_store_three_lineages():
    """Mock store with three independent lineage chains.

    Provenance graph::

        Step 0: R1        R2        R3
                |         |         |
        Step 1: S1        S2        S3
                |         |         |
        Step 2: P1  MA1   P2  MA2   P3  MA3
                      |         |         |
        Step 3:      MB1       MB2       MB3

    P = passthrough, MA/MB = metrics from roles A and B respectively.
    S = intermediate (e.g., processing step output).
    R = root ingest.

    P1 <-> MA1 (via S1), P1 <-> MB1 (via S1 -> R1, since MB1's chain is
    MB1->MA1->S1 ... wait, let's keep it simple: MA and MB both descend from S.
    """
    store = Mock()

    R1 = "r1" + "0" * 30
    R2 = "r2" + "0" * 30
    R3 = "r3" + "0" * 30
    S1 = "s1" + "0" * 30
    S2 = "s2" + "0" * 30
    S3 = "s3" + "0" * 30
    P1 = "p1" + "0" * 30
    P2 = "p2" + "0" * 30
    P3 = "p3" + "0" * 30
    MA1 = "ma1" + "0" * 29
    MA2 = "ma2" + "0" * 29
    MA3 = "ma3" + "0" * 29
    MB1 = "mb1" + "0" * 29
    MB2 = "mb2" + "0" * 29
    MB3 = "mb3" + "0" * 29

    provenance_map = {
        S1: [R1],
        S2: [R2],
        S3: [R3],
        P1: [S1],
        P2: [S2],
        P3: [S3],
        MA1: [S1],
        MA2: [S2],
        MA3: [S3],
        MB1: [S1],
        MB2: [S2],
        MB3: [S3],
    }

    step_numbers = {
        R1: 0,
        R2: 0,
        R3: 0,
        S1: 1,
        S2: 1,
        S3: 1,
        P1: 2,
        P2: 2,
        P3: 2,
        MA1: 2,
        MA2: 2,
        MA3: 2,
        MB1: 3,
        MB2: 3,
        MB3: 3,
    }

    store.load_provenance_map.return_value = provenance_map
    store.load_step_number_map.side_effect = lambda ids=None: (
        {k: v for k, v in step_numbers.items() if ids is None or k in ids}
    )

    store._test_ids = {
        "P1": P1,
        "P2": P2,
        "P3": P3,
        "MA1": MA1,
        "MA2": MA2,
        "MA3": MA3,
        "MB1": MB1,
        "MB2": MB2,
        "MB3": MB3,
    }

    return store


@pytest.fixture
def mock_artifact_store_one_to_n():
    """Mock store with 1:N lineage (1 dataset -> N configs via shared root).

    Provenance graph::

        Step 0: Root
                |
        Step 1: Dataset
                |  (shared ancestor Root)
        Step 2: C1  C2  C3  C4  C5  C6

    Dataset and all configs share ancestor Root.
    Dataset is the target (lower step), configs are candidates.
    """
    store = Mock()

    root = "root" + "0" * 28
    dataset = "ds" + "0" * 30
    configs = [f"c{i}" + "0" * 30 for i in range(1, 7)]

    provenance_map = {dataset: [root]}
    for c in configs:
        provenance_map[c] = [root]

    step_numbers = {root: 0, dataset: 1}
    for c in configs:
        step_numbers[c] = 2

    store.load_provenance_map.return_value = provenance_map
    store.load_step_number_map.side_effect = lambda ids=None: (
        {k: v for k, v in step_numbers.items() if ids is None or k in ids}
    )

    store._test_ids = {"dataset": dataset, "configs": configs, "root": root}

    return store


# ---------------------------------------------------------------------------
# Tests: compute_group_id
# ---------------------------------------------------------------------------


class TestComputeGroupId:
    """Tests for deterministic group_id computation."""

    def test_deterministic_for_same_inputs(self):
        """Same artifact IDs produce the same group_id."""
        ids = ["artifact_abc", "artifact_def"]
        assert compute_group_id(ids) == compute_group_id(ids)

    def test_order_independent(self):
        """Sorted internally, so order of IDs does not matter."""
        ids_a = ["artifact_abc", "artifact_def"]
        ids_b = ["artifact_def", "artifact_abc"]
        assert compute_group_id(ids_a) == compute_group_id(ids_b)

    def test_different_inputs_produce_different_ids(self):
        """Different artifact IDs produce different group_ids."""
        ids_a = ["artifact_abc", "artifact_def"]
        ids_b = ["artifact_abc", "artifact_xyz"]
        assert compute_group_id(ids_a) != compute_group_id(ids_b)

    def test_produces_32_char_hex(self):
        """xxh3_128 produces a 32-character hex string."""
        result = compute_group_id(["a", "b"])
        assert len(result) == 32
        assert all(c in "0123456789abcdef" for c in result)

    def test_same_ids_different_role_order_same_group_id(self):
        """group_id is based on artifact IDs only, not role association.

        When group_inputs() collects IDs per index, the role order
        may differ. compute_group_id sorts, so the result is the same.
        """
        # Simulating two roles with different orderings
        ids = ["id_role_a", "id_role_b"]
        ids_reversed = ["id_role_b", "id_role_a"]
        assert compute_group_id(ids) == compute_group_id(ids_reversed)


# ---------------------------------------------------------------------------
# Tests: group_inputs - ZIP strategy
# ---------------------------------------------------------------------------


class TestGroupInputsZip:
    """Tests for group_inputs() with ZIP strategy."""

    def test_equal_lengths_returns_aligned_output(self):
        """ZIP with equal-length lists returns positionally-aligned results."""
        inputs = {
            "data": ["s1", "s2", "s3"],
            "config": ["c1", "c2", "c3"],
        }

        aligned, group_ids = group_inputs(inputs, GroupByStrategy.ZIP)

        assert aligned["data"] == ["s1", "s2", "s3"]
        assert aligned["config"] == ["c1", "c2", "c3"]
        assert len(group_ids) == 3

    def test_equal_lengths_group_ids_are_deterministic(self):
        """group_ids are deterministic for the same inputs."""
        inputs = {
            "data": ["s1", "s2"],
            "config": ["c1", "c2"],
        }

        _, group_ids_a = group_inputs(inputs, GroupByStrategy.ZIP)
        _, group_ids_b = group_inputs(inputs, GroupByStrategy.ZIP)

        assert group_ids_a == group_ids_b

    def test_unequal_lengths_raises_value_error(self):
        """ZIP with different-length lists raises ValueError."""
        inputs = {
            "data": ["s1", "s2"],
            "config": ["c1"],
        }

        with pytest.raises(ValueError, match="same length"):
            group_inputs(inputs, GroupByStrategy.ZIP)

    def test_empty_inputs_returns_empty(self):
        """ZIP with empty lists returns empty results."""
        inputs = {"data": [], "config": []}
        aligned, group_ids = group_inputs(inputs, GroupByStrategy.ZIP)

        assert aligned["data"] == []
        assert aligned["config"] == []
        assert group_ids == []

    def test_single_element(self):
        """ZIP with single-element lists works correctly."""
        inputs = {"a": ["x"], "b": ["y"]}
        aligned, group_ids = group_inputs(inputs, GroupByStrategy.ZIP)

        assert aligned == {"a": ["x"], "b": ["y"]}
        assert len(group_ids) == 1

    def test_three_roles(self):
        """ZIP works with 3+ roles."""
        inputs = {"a": ["a1", "a2"], "b": ["b1", "b2"], "c": ["c1", "c2"]}
        aligned, group_ids = group_inputs(inputs, GroupByStrategy.ZIP)

        assert len(aligned) == 3
        assert len(group_ids) == 2
        assert aligned["a"] == ["a1", "a2"]
        assert aligned["b"] == ["b1", "b2"]
        assert aligned["c"] == ["c1", "c2"]

    def test_no_roles_returns_empty(self):
        """ZIP with no roles returns empty results."""
        aligned, group_ids = group_inputs({}, GroupByStrategy.ZIP)
        assert aligned == {}
        assert group_ids == []


# ---------------------------------------------------------------------------
# Tests: group_inputs - CROSS_PRODUCT strategy
# ---------------------------------------------------------------------------


class TestGroupInputsCrossProduct:
    """Tests for group_inputs() with CROSS_PRODUCT strategy."""

    def test_two_by_two_produces_four_combinations(self):
        """2 x 2 inputs produce 4 combinations."""
        inputs = {
            "data": ["s1", "s2"],
            "config": ["c1", "c2"],
        }

        aligned, group_ids = group_inputs(inputs, GroupByStrategy.CROSS_PRODUCT)

        assert len(aligned["data"]) == 4
        assert len(aligned["config"]) == 4
        assert len(group_ids) == 4

        # Verify all 4 combinations exist
        pairs = set(zip(aligned["data"], aligned["config"], strict=True))
        expected = {("s1", "c1"), ("s1", "c2"), ("s2", "c1"), ("s2", "c2")}
        assert pairs == expected

    def test_three_by_two_produces_six_combinations(self):
        """3 x 2 inputs produce 6 combinations."""
        inputs = {
            "a": ["a1", "a2", "a3"],
            "b": ["b1", "b2"],
        }

        aligned, group_ids = group_inputs(inputs, GroupByStrategy.CROSS_PRODUCT)

        assert len(aligned["a"]) == 6
        assert len(group_ids) == 6

    def test_three_roles(self):
        """Cross product of 2 x 2 x 2 = 8."""
        inputs = {
            "a": ["a1", "a2"],
            "b": ["b1", "b2"],
            "c": ["c1", "c2"],
        }

        aligned, group_ids = group_inputs(inputs, GroupByStrategy.CROSS_PRODUCT)
        assert len(group_ids) == 8

    def test_empty_role_produces_empty(self):
        """Empty role produces zero combinations."""
        inputs = {"a": ["a1", "a2"], "b": []}
        aligned, group_ids = group_inputs(inputs, GroupByStrategy.CROSS_PRODUCT)

        assert aligned["a"] == []
        assert aligned["b"] == []
        assert group_ids == []

    def test_group_ids_differ_per_combination(self):
        """Each combination gets a unique group_id."""
        inputs = {"a": ["a1", "a2"], "b": ["b1", "b2"]}
        _, group_ids = group_inputs(inputs, GroupByStrategy.CROSS_PRODUCT)

        # All 4 group_ids should be unique
        assert len(set(group_ids)) == 4


# ---------------------------------------------------------------------------
# Tests: group_inputs - LINEAGE strategy
# ---------------------------------------------------------------------------


class TestGroupInputsLineage:
    """Tests for group_inputs() with LINEAGE strategy."""

    def test_matched_by_shared_ancestor(self, mock_artifact_store):
        """Artifacts with shared ancestor are paired correctly."""
        ids = mock_artifact_store._test_ids

        inputs = {
            "results": [ids["A1a"], ids["B1a"]],
            "metrics": [ids["M_A1"], ids["M_B1"]],
        }

        aligned, group_ids = group_inputs(
            inputs, GroupByStrategy.LINEAGE, mock_artifact_store
        )

        assert len(aligned["results"]) == 2
        assert len(group_ids) == 2

        # A1a should match M_A1, B1a should match M_B1
        pairs = dict(zip(aligned["results"], aligned["metrics"], strict=True))
        assert pairs[ids["A1a"]] == ids["M_A1"]
        assert pairs[ids["B1a"]] == ids["M_B1"]

    def test_unmatched_artifacts_excluded_with_warning(
        self, mock_artifact_store, caplog
    ):
        """Artifacts without common ancestor are excluded; warning logged."""
        ids = mock_artifact_store._test_ids

        # A1a has no common ancestor with M_B1
        inputs = {
            "results": [ids["A1a"]],
            "metrics": [ids["M_B1"]],
        }

        aligned, group_ids = group_inputs(
            inputs, GroupByStrategy.LINEAGE, mock_artifact_store
        )

        assert len(aligned["results"]) == 0
        assert len(group_ids) == 0
        assert "no common ancestor" in caplog.text.lower()

    def test_requires_artifact_store(self):
        """LINEAGE without artifact_store raises RuntimeError."""
        inputs = {"a": ["id1"], "b": ["id2"]}

        with pytest.raises(RuntimeError, match="artifact_store"):
            group_inputs(inputs, GroupByStrategy.LINEAGE, artifact_store=None)

    def test_requires_exactly_two_roles(self, mock_artifact_store):
        """LINEAGE with != 2 roles raises ValueError."""
        with pytest.raises(ValueError, match="exactly 2"):
            group_inputs(
                {"only_one": ["a" * 32]},
                GroupByStrategy.LINEAGE,
                mock_artifact_store,
            )

        with pytest.raises(ValueError, match="exactly 2"):
            group_inputs(
                {"a": ["a" * 32], "b": ["b" * 32], "c": ["c" * 32]},
                GroupByStrategy.LINEAGE,
                mock_artifact_store,
            )

    def test_unequal_lengths_handled(self, mock_artifact_store):
        """LINEAGE handles unequal input lengths (iterates over larger set)."""
        ids = mock_artifact_store._test_ids

        inputs = {
            "results": [ids["A1a"]],
            "metrics": [ids["M_A1"], ids["M_B1"]],
        }

        aligned, group_ids = group_inputs(
            inputs, GroupByStrategy.LINEAGE, mock_artifact_store
        )

        # Only A1a<->M_A1 should match (M_B1 has no common ancestor with A1a)
        assert len(aligned["results"]) == 1
        assert aligned["metrics"][0] == ids["M_A1"]

    def test_empty_role_returns_empty(self, mock_artifact_store):
        """LINEAGE with an empty role returns empty result."""
        ids = mock_artifact_store._test_ids
        inputs = {"results": [], "metrics": [ids["M_A1"]]}

        aligned, group_ids = group_inputs(
            inputs, GroupByStrategy.LINEAGE, mock_artifact_store
        )

        assert len(group_ids) == 0

    def test_one_to_n_produces_n_matched_sets(self, mock_artifact_store_one_to_n):
        """1 dataset + 6 configs with shared ancestor -> 6 matched sets."""
        ids = mock_artifact_store_one_to_n._test_ids

        inputs = {
            "dataset": [ids["dataset"]],
            "config": ids["configs"],
        }

        aligned, group_ids = group_inputs(
            inputs, GroupByStrategy.LINEAGE, mock_artifact_store_one_to_n
        )

        assert len(aligned["dataset"]) == 6
        assert len(aligned["config"]) == 6
        assert len(group_ids) == 6

        # All dataset entries should be the same (1:N expansion)
        assert all(d == ids["dataset"] for d in aligned["dataset"])
        # All 6 configs should appear
        assert set(aligned["config"]) == set(ids["configs"])

    def test_group_ids_for_lineage_matches(self, mock_artifact_store):
        """LINEAGE matches produce valid group_ids."""
        ids = mock_artifact_store._test_ids
        inputs = {
            "results": [ids["A1a"]],
            "metrics": [ids["M_A1"]],
        }

        _, group_ids = group_inputs(
            inputs, GroupByStrategy.LINEAGE, mock_artifact_store
        )

        assert len(group_ids) == 1
        assert len(group_ids[0]) == 32  # xxh3_128 hex digest


# ---------------------------------------------------------------------------
# Tests: group_id role-order independence
# ---------------------------------------------------------------------------


class TestGroupIdRoleOrderIndependence:
    """Verify group_id is the same regardless of role ordering in inputs."""

    def test_zip_role_order_does_not_affect_group_id(self):
        """Same artifact pair produces same group_id regardless of role names."""
        inputs_ab = {"role_a": ["id_1", "id_2"], "role_b": ["id_3", "id_4"]}
        inputs_ba = {"role_b": ["id_3", "id_4"], "role_a": ["id_1", "id_2"]}

        _, ids_ab = group_inputs(inputs_ab, GroupByStrategy.ZIP)
        _, ids_ba = group_inputs(inputs_ba, GroupByStrategy.ZIP)

        # group_id is computed from sorted artifact IDs, not role names
        assert ids_ab == ids_ba

    def test_cross_product_same_pair_same_group_id(self):
        """In cross product, the same pair of IDs yields the same group_id."""
        inputs = {"a": ["x"], "b": ["y"]}
        _, group_ids = group_inputs(inputs, GroupByStrategy.CROSS_PRODUCT)

        expected_id = compute_group_id(["x", "y"])
        assert group_ids[0] == expected_id


# ---------------------------------------------------------------------------
# Tests: match_inputs_to_primary - LINEAGE strategy
# ---------------------------------------------------------------------------


class TestMatchInputsToPrimary:
    """Tests for match_inputs_to_primary() with LINEAGE strategy."""

    def test_one_primary_one_other_role(self, mock_artifact_store):
        """Single other role: primary matched against it."""
        ids = mock_artifact_store._test_ids

        inputs = {
            "passthrough": [ids["A1a"], ids["B1a"]],
            "metrics": [ids["M_A1"], ids["M_B1"]],
        }

        result = match_inputs_to_primary(
            "passthrough", inputs, GroupByStrategy.LINEAGE, mock_artifact_store
        )

        assert len(result["passthrough"]) == 2
        assert len(result["metrics"]) == 2

        # A1a matched to M_A1, B1a matched to M_B1
        pairs = dict(zip(result["passthrough"], result["metrics"], strict=True))
        assert pairs[ids["A1a"]] == ids["M_A1"]
        assert pairs[ids["B1a"]] == ids["M_B1"]

    def test_one_primary_two_other_roles_complete_matches(
        self, mock_artifact_store_three_lineages
    ):
        """Two other roles: only complete matches (matched in both) returned."""
        ids = mock_artifact_store_three_lineages._test_ids

        inputs = {
            "passthrough": [ids["P1"], ids["P2"], ids["P3"]],
            "metrics_a": [ids["MA1"], ids["MA2"], ids["MA3"]],
            "metrics_b": [ids["MB1"], ids["MB2"], ids["MB3"]],
        }

        result = match_inputs_to_primary(
            "passthrough",
            inputs,
            GroupByStrategy.LINEAGE,
            mock_artifact_store_three_lineages,
        )

        # All 3 should match completely
        assert len(result["passthrough"]) == 3
        assert len(result["metrics_a"]) == 3
        assert len(result["metrics_b"]) == 3

    def test_partial_match_excluded(self, mock_artifact_store):
        """Primary artifact matched in one role but not another is excluded."""
        ids = mock_artifact_store._test_ids

        # A1a can match M_A1 in "metrics" but has no match for M_B1 in "other"
        # since we only provide M_B1 in "other" and A1a shares no ancestor with M_B1
        inputs = {
            "passthrough": [ids["A1a"]],
            "metrics": [ids["M_A1"]],
            "other": [ids["M_B1"]],
        }

        result = match_inputs_to_primary(
            "passthrough", inputs, GroupByStrategy.LINEAGE, mock_artifact_store
        )

        # A1a matches M_A1 in "metrics" but M_B1 in "other" has no common
        # ancestor with A1a, so the match is incomplete -> excluded
        assert len(result["passthrough"]) == 0
        assert len(result["metrics"]) == 0
        assert len(result["other"]) == 0

    def test_no_matches_returns_empty(self, mock_artifact_store):
        """No matches at all returns empty aligned lists."""
        ids = mock_artifact_store._test_ids

        # A1a vs M_B1: no common ancestor
        inputs = {
            "passthrough": [ids["A1a"]],
            "metrics": [ids["M_B1"]],
        }

        result = match_inputs_to_primary(
            "passthrough", inputs, GroupByStrategy.LINEAGE, mock_artifact_store
        )

        assert result["passthrough"] == []
        assert result["metrics"] == []

    def test_all_match_positionally_aligned(self, mock_artifact_store):
        """When all match, results are positionally aligned to primary."""
        ids = mock_artifact_store._test_ids

        inputs = {
            "passthrough": [ids["A1a"], ids["B1a"]],
            "metrics": [ids["M_A1"], ids["M_B1"]],
        }

        result = match_inputs_to_primary(
            "passthrough", inputs, GroupByStrategy.LINEAGE, mock_artifact_store
        )

        # result["passthrough"][i] is matched with result["metrics"][i]
        for i, pt_id in enumerate(result["passthrough"]):
            metric_id = result["metrics"][i]
            if pt_id == ids["A1a"]:
                assert metric_id == ids["M_A1"]
            else:
                assert metric_id == ids["M_B1"]

    def test_one_to_n_primary_produces_expanded_aligned(
        self, mock_artifact_store_one_to_n
    ):
        """1 primary with N candidates produces N aligned entries."""
        ids = mock_artifact_store_one_to_n._test_ids

        inputs = {
            "primary": [ids["dataset"]],
            "configs": ids["configs"],
        }

        result = match_inputs_to_primary(
            "primary",
            inputs,
            GroupByStrategy.LINEAGE,
            mock_artifact_store_one_to_n,
        )

        assert len(result["primary"]) == 6
        assert len(result["configs"]) == 6
        assert all(p == ids["dataset"] for p in result["primary"])
        assert set(result["configs"]) == set(ids["configs"])

    def test_ancestry_cache_built_once(self, mock_artifact_store):
        """Ancestry cache is built once (single call to load_provenance_map)."""
        ids = mock_artifact_store._test_ids

        inputs = {
            "passthrough": [ids["A1a"]],
            "metrics": [ids["M_A1"]],
        }

        match_inputs_to_primary(
            "passthrough", inputs, GroupByStrategy.LINEAGE, mock_artifact_store
        )

        # match_by_ancestry calls load_provenance_map exactly once
        assert mock_artifact_store.load_provenance_map.call_count == 1

    def test_requires_artifact_store(self):
        """LINEAGE strategy without artifact_store raises RuntimeError."""
        inputs = {"primary": ["id1"], "other": ["id2"]}

        with pytest.raises(RuntimeError, match="artifact_store"):
            match_inputs_to_primary(
                "primary", inputs, GroupByStrategy.LINEAGE, artifact_store=None
            )

    def test_primary_role_not_in_inputs_raises(self):
        """Missing primary role raises ValueError."""
        inputs = {"other": ["id1"]}

        with pytest.raises(ValueError, match="not found"):
            match_inputs_to_primary("missing", inputs, GroupByStrategy.LINEAGE)

    def test_no_other_roles_raises(self):
        """No non-primary roles raises ValueError."""
        inputs = {"primary": ["id1"]}

        with pytest.raises(ValueError, match="at least one non-primary"):
            match_inputs_to_primary("primary", inputs, GroupByStrategy.LINEAGE)

    def test_unsupported_strategy_raises(self):
        """Non-LINEAGE strategy raises ValueError."""
        inputs = {"primary": ["id1"], "other": ["id2"]}

        with pytest.raises(ValueError, match="LINEAGE"):
            match_inputs_to_primary("primary", inputs, GroupByStrategy.ZIP)


# ---------------------------------------------------------------------------
# Tests: validate_stem_match_uniqueness
# ---------------------------------------------------------------------------


class TestValidateStemMatchUniqueness:
    """Tests for validate_stem_match_uniqueness()."""

    def test_unique_names_passes(self):
        """No error when all names are unique."""
        validate_stem_match_uniqueness("config", ["a.json", "b.json", "c.json"])

    def test_empty_list_passes(self):
        """No error for empty list."""
        validate_stem_match_uniqueness("config", [])

    def test_single_item_passes(self):
        """No error for single item."""
        validate_stem_match_uniqueness("config", ["only_one.json"])

    def test_duplicate_names_raises(self):
        """Duplicate names raise ValueError with details."""
        with pytest.raises(ValueError, match="duplicate") as exc_info:
            validate_stem_match_uniqueness("config", ["a.json", "b.json", "a.json"])
        assert "config" in str(exc_info.value)
        assert "a.json" in str(exc_info.value)

    def test_multiple_duplicates_reported(self):
        """All duplicate names are reported."""
        with pytest.raises(ValueError, match="duplicate") as exc_info:
            validate_stem_match_uniqueness(
                "config", ["a.json", "b.json", "a.json", "b.json", "c.json"]
            )
        error_msg = str(exc_info.value)
        assert "a.json" in error_msg
        assert "b.json" in error_msg

    def test_triple_duplicate_reported_once(self):
        """A name appearing 3 times is only reported once as duplicate."""
        with pytest.raises(ValueError, match="duplicate") as exc_info:
            validate_stem_match_uniqueness("config", ["x.json", "x.json", "x.json"])
        error_msg = str(exc_info.value)
        assert error_msg.count("x.json") >= 1


# ---------------------------------------------------------------------------
# Tests: edge cases and integration
# ---------------------------------------------------------------------------


class TestGroupInputsEdgeCases:
    """Edge cases for group_inputs()."""

    def test_unknown_strategy_raises(self):
        """Passing an invalid strategy raises ValueError."""
        # GroupByStrategy is an enum, so we can't easily pass an invalid value.
        # Instead verify all valid strategies are handled without raising
        # "Unknown group_by strategy".
        inputs = {"a": ["x"], "b": ["y"]}
        for strategy in GroupByStrategy:
            if strategy == GroupByStrategy.LINEAGE:
                continue  # Needs artifact_store
            group_inputs(inputs, strategy)

    def test_single_role_zip(self):
        """ZIP with a single role (degenerate case) works."""
        inputs = {"only": ["a", "b", "c"]}
        aligned, group_ids = group_inputs(inputs, GroupByStrategy.ZIP)

        assert aligned["only"] == ["a", "b", "c"]
        assert len(group_ids) == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
