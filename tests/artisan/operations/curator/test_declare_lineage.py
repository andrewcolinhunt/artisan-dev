"""Unit tests for the DeclareLineage operation.

Tests cover:

1. Pairing strategies (ZIP, NAME, CROSS_PRODUCT) and their validation.
2. Source-existence checks via artifact_index lookup.
3. Self-loop rejection.
4. LINEAGE-as-pairing rejection.
5. Idempotence via (source, target, role, role, group_id) dedup.
6. Passthrough semantics — children IDs preserved unchanged.
"""

from __future__ import annotations

from unittest.mock import Mock

import polars as pl
import pytest

from artisan.operations.curator.declare_lineage import DeclareLineage
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import GroupByStrategy


# A 32-char hex-like ID is what ArtifactProvenanceEdge expects.
def _id(seed: str) -> str:
    """Build a deterministic 32-char ID for tests."""
    return (seed * 32)[:32]


def _df(ids: list[str]) -> pl.DataFrame:
    """Helper to wrap an ID list in a polars frame with the artifact_id column."""
    return pl.DataFrame({"artifact_id": ids})


def _make_op(pairing: GroupByStrategy) -> DeclareLineage:
    """Construct a DeclareLineage with the given pairing strategy."""
    return DeclareLineage(params=DeclareLineage.Params(pairing=pairing))


def _make_store(
    *,
    types: dict[str, str] | None = None,
    original_names: dict[str, str] | None = None,
    existing_edges: pl.DataFrame | None = None,
    step_range: tuple[int, int] | None = None,
) -> Mock:
    """Build a Mock ArtifactStore with the provided fixtures."""
    store = Mock()
    store.provenance.load_type_map.return_value = types or {}
    store.load_original_names.return_value = original_names or {}
    store.provenance.get_step_range.return_value = step_range or (0, 0)
    store.provenance.load_edges_df.return_value = (
        existing_edges
        if existing_edges is not None
        else pl.DataFrame(
            schema={
                "source_artifact_id": pl.String,
                "target_artifact_id": pl.String,
                "source_role": pl.String,
                "target_role": pl.String,
                "group_id": pl.String,
            }
        )
    )
    return store


# ---------------------------------------------------------------------------
# ZIP pairing
# ---------------------------------------------------------------------------


class TestDeclareLineageZIP:
    """Positional 1:1 pairing tests."""

    def test_should_emit_one_edge_per_pair(self):
        """3 parents + 3 children + ZIP → 3 edges, in input order."""
        parents = [_id(c) for c in "abc"]
        children = [_id(c) for c in "xyz"]
        store = _make_store(
            types=dict.fromkeys(parents, "data") | dict.fromkeys(children, "data"),
        )

        op = _make_op(GroupByStrategy.ZIP)
        result = op.execute_curator(
            inputs={"parents": _df(parents), "children": _df(children)},
            step_number=0,
            artifact_store=store,
        )

        assert result.success
        assert result.passthrough["children"] == children
        assert result.lineage_edges is not None
        assert len(result.lineage_edges) == 3
        pairs = [
            (e.source_artifact_id, e.target_artifact_id) for e in result.lineage_edges
        ]
        assert pairs == list(zip(parents, children, strict=True))

    def test_should_set_role_columns_and_types(self):
        """ZIP edges use source_role='parents', target_role='children', looked-up types."""
        parents = [_id("a")]
        children = [_id("x")]
        store = _make_store(
            types={parents[0]: "structure", children[0]: "msa"},
        )

        op = _make_op(GroupByStrategy.ZIP)
        result = op.execute_curator(
            inputs={"parents": _df(parents), "children": _df(children)},
            step_number=0,
            artifact_store=store,
        )

        edge = result.lineage_edges[0]
        assert edge.source_role == "parents"
        assert edge.target_role == "children"
        assert edge.source_artifact_type == "structure"
        assert edge.target_artifact_type == "msa"
        assert edge.group_id is None
        assert edge.step_boundary is True

    def test_should_raise_on_length_mismatch(self):
        """ZIP with 3 parents and 2 children raises before any edge is built."""
        parents = [_id(c) for c in "abc"]
        children = [_id(c) for c in "xy"]
        store = _make_store(
            types=dict.fromkeys(parents, "data") | dict.fromkeys(children, "data"),
        )

        op = _make_op(GroupByStrategy.ZIP)
        with pytest.raises(ValueError, match="ZIP matching requires"):
            op.execute_curator(
                inputs={"parents": _df(parents), "children": _df(children)},
                step_number=0,
                artifact_store=store,
            )


# ---------------------------------------------------------------------------
# NAME pairing
# ---------------------------------------------------------------------------


class TestDeclareLineageNAME:
    """Stem-matching pairing tests."""

    def test_should_pair_by_stem(self):
        """Three parents and three children whose stems match → 3 edges."""
        parents = [_id(c) for c in "abc"]
        children = [_id(c) for c in "xyz"]
        # Map parent IDs to names "a.dat", "b.dat", "c.dat"
        # and child IDs to names "a_out.dat", "b_out.dat", "c_out.dat".
        # _match_by_name uses strip_extensions; the stems will not match
        # ("a" vs "a_out"). Use identical stems with different extensions.
        names = {
            parents[0]: "a.json",
            parents[1]: "b.json",
            parents[2]: "c.json",
            children[0]: "a.tsv",
            children[1]: "b.tsv",
            children[2]: "c.tsv",
        }
        store = _make_store(
            types=dict.fromkeys(parents, "data") | dict.fromkeys(children, "data"),
            original_names=names,
        )

        op = _make_op(GroupByStrategy.NAME)
        result = op.execute_curator(
            inputs={"parents": _df(parents), "children": _df(children)},
            step_number=0,
            artifact_store=store,
        )

        # Result should have 3 edges, one per matching stem.
        assert result.lineage_edges is not None
        assert len(result.lineage_edges) == 3
        pairs = {
            (e.source_artifact_id, e.target_artifact_id) for e in result.lineage_edges
        }
        expected = {(parents[i], children[i]) for i in range(3)}
        assert pairs == expected

    def test_should_raise_on_duplicate_stem_in_parents(self):
        """Two parents sharing the same stem raise."""
        parents = [_id("a"), _id("b")]
        children = [_id("x")]
        names = {
            parents[0]: "shared.json",
            parents[1]: "shared.tsv",  # same stem 'shared'
            children[0]: "shared.csv",
        }
        store = _make_store(
            types=dict.fromkeys(parents, "data") | dict.fromkeys(children, "data"),
            original_names=names,
        )

        op = _make_op(GroupByStrategy.NAME)
        with pytest.raises(ValueError, match="duplicate"):
            op.execute_curator(
                inputs={"parents": _df(parents), "children": _df(children)},
                step_number=0,
                artifact_store=store,
            )


# ---------------------------------------------------------------------------
# CROSS_PRODUCT pairing
# ---------------------------------------------------------------------------


class TestDeclareLineageCrossProduct:
    """Cartesian-product pairing tests."""

    def test_should_emit_cartesian_pairs(self):
        """2 parents x 3 children -> 6 edges."""
        parents = [_id("a"), _id("b")]
        children = [_id(c) for c in "xyz"]
        store = _make_store(
            types=dict.fromkeys(parents, "data") | dict.fromkeys(children, "data"),
        )

        op = _make_op(GroupByStrategy.CROSS_PRODUCT)
        result = op.execute_curator(
            inputs={"parents": _df(parents), "children": _df(children)},
            step_number=0,
            artifact_store=store,
        )

        assert result.lineage_edges is not None
        assert len(result.lineage_edges) == 6
        pairs = {
            (e.source_artifact_id, e.target_artifact_id) for e in result.lineage_edges
        }
        expected = {(p, c) for p in parents for c in children}
        assert pairs == expected

    def test_should_handle_unequal_input_lengths(self):
        """CROSS_PRODUCT does not require equal lengths."""
        parents = [_id("a")]
        children = [_id(c) for c in "xyz"]
        store = _make_store(
            types=dict.fromkeys(parents, "data") | dict.fromkeys(children, "data"),
        )

        op = _make_op(GroupByStrategy.CROSS_PRODUCT)
        result = op.execute_curator(
            inputs={"parents": _df(parents), "children": _df(children)},
            step_number=0,
            artifact_store=store,
        )

        assert len(result.lineage_edges) == 3


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestDeclareLineageValidation:
    """Validation tests: pairing strategy, source existence, self-loops."""

    def test_should_reject_lineage_strategy(self):
        """pairing=LINEAGE raises — DeclareLineage creates lineage, not consumes it."""
        parents = [_id("a")]
        children = [_id("x")]
        store = _make_store(
            types={parents[0]: "data", children[0]: "data"},
        )

        op = _make_op(GroupByStrategy.LINEAGE)
        with pytest.raises(ValueError, match="does not support pairing=LINEAGE"):
            op.execute_curator(
                inputs={"parents": _df(parents), "children": _df(children)},
                step_number=0,
                artifact_store=store,
            )

    def test_should_raise_on_missing_source(self):
        """A parent ID absent from artifact_index raises with a clear message."""
        parents = [_id("a"), _id("b")]
        children = [_id("x"), _id("y")]
        # Only one parent is in the index; the other is missing.
        store = _make_store(
            types={
                parents[0]: "data",
                children[0]: "data",
                children[1]: "data",
                # parents[1] is intentionally missing
            },
        )

        op = _make_op(GroupByStrategy.ZIP)
        with pytest.raises(ValueError, match="not found in artifact_index"):
            op.execute_curator(
                inputs={"parents": _df(parents), "children": _df(children)},
                step_number=0,
                artifact_store=store,
            )

    def test_should_raise_on_self_loop(self):
        """A pairing that emits source == target raises."""
        same = _id("a")
        # Same ID used as both parent and child → self-loop under ZIP.
        store = _make_store(types={same: "data"})

        op = _make_op(GroupByStrategy.ZIP)
        with pytest.raises(ValueError, match="self-loop"):
            op.execute_curator(
                inputs={"parents": _df([same]), "children": _df([same])},
                step_number=0,
                artifact_store=store,
            )

    def test_should_raise_when_role_missing(self):
        """Missing 'parents' or 'children' input role raises."""
        store = _make_store(types={_id("a"): "data"})
        op = _make_op(GroupByStrategy.ZIP)
        with pytest.raises(ValueError, match="requires 'parents'"):
            op.execute_curator(
                inputs={"children": _df([_id("a")])},
                step_number=0,
                artifact_store=store,
            )


# ---------------------------------------------------------------------------
# Passthrough semantics
# ---------------------------------------------------------------------------


class TestDeclareLineagePassthrough:
    """Passthrough output tests."""

    def test_output_children_ids_equal_input(self):
        """Output's children IDs exactly equal input's children IDs."""
        parents = [_id("a")]
        children = [_id("x"), _id("y"), _id("z")]
        store = _make_store(
            types={parents[0]: "data"} | dict.fromkeys(children, "data"),
        )

        op = _make_op(GroupByStrategy.CROSS_PRODUCT)
        result = op.execute_curator(
            inputs={"parents": _df(parents), "children": _df(children)},
            step_number=0,
            artifact_store=store,
        )

        assert result.passthrough["children"] == children

    def test_parents_role_not_in_output(self):
        """Output dict contains only the 'children' role."""
        parents = [_id("a")]
        children = [_id("x")]
        store = _make_store(
            types={parents[0]: "data", children[0]: "data"},
        )

        op = _make_op(GroupByStrategy.ZIP)
        result = op.execute_curator(
            inputs={"parents": _df(parents), "children": _df(children)},
            step_number=0,
            artifact_store=store,
        )

        assert set(result.passthrough.keys()) == {"children"}

    def test_empty_inputs_produces_empty_passthrough(self):
        """Empty parents and children yields an empty passthrough and no edges."""
        store = _make_store()
        op = _make_op(GroupByStrategy.ZIP)
        result = op.execute_curator(
            inputs={"parents": _df([]), "children": _df([])},
            step_number=0,
            artifact_store=store,
        )

        assert result.success
        assert result.passthrough["children"] == []
        assert result.lineage_edges is None


# ---------------------------------------------------------------------------
# Idempotence / dedup
# ---------------------------------------------------------------------------


class TestDeclareLineageIdempotence:
    """Dedup against existing edges in artifact_edges."""

    def test_rerun_emits_empty_edge_list(self):
        """A re-run where all candidate edges already exist returns lineage_edges=None."""
        parents = [_id("a"), _id("b"), _id("c")]
        children = [_id("x"), _id("y"), _id("z")]

        # Pre-populate existing edges matching the ZIP pairing.
        existing = pl.DataFrame(
            {
                "source_artifact_id": parents,
                "target_artifact_id": children,
                "source_role": ["parents"] * 3,
                "target_role": ["children"] * 3,
                "group_id": [None, None, None],
            },
            schema={
                "source_artifact_id": pl.String,
                "target_artifact_id": pl.String,
                "source_role": pl.String,
                "target_role": pl.String,
                "group_id": pl.String,
            },
        )
        store = _make_store(
            types=dict.fromkeys(parents, "data") | dict.fromkeys(children, "data"),
            existing_edges=existing,
            step_range=(0, 5),
        )

        op = _make_op(GroupByStrategy.ZIP)
        result = op.execute_curator(
            inputs={"parents": _df(parents), "children": _df(children)},
            step_number=0,
            artifact_store=store,
        )

        assert result.success
        assert result.lineage_edges is None
        assert result.passthrough["children"] == children

    def test_partial_overlap_emits_only_new_edges(self):
        """When only some candidate edges exist, the rest are emitted."""
        parents = [_id("a"), _id("b"), _id("c")]
        children = [_id("x"), _id("y"), _id("z")]

        # Only the first pair already exists.
        existing = pl.DataFrame(
            {
                "source_artifact_id": [parents[0]],
                "target_artifact_id": [children[0]],
                "source_role": ["parents"],
                "target_role": ["children"],
                "group_id": [None],
            },
            schema={
                "source_artifact_id": pl.String,
                "target_artifact_id": pl.String,
                "source_role": pl.String,
                "target_role": pl.String,
                "group_id": pl.String,
            },
        )
        store = _make_store(
            types=dict.fromkeys(parents, "data") | dict.fromkeys(children, "data"),
            existing_edges=existing,
            step_range=(0, 5),
        )

        op = _make_op(GroupByStrategy.ZIP)
        result = op.execute_curator(
            inputs={"parents": _df(parents), "children": _df(children)},
            step_number=0,
            artifact_store=store,
        )

        assert result.lineage_edges is not None
        assert len(result.lineage_edges) == 2
        pairs = {
            (e.source_artifact_id, e.target_artifact_id) for e in result.lineage_edges
        }
        assert pairs == {(parents[1], children[1]), (parents[2], children[2])}

    def test_different_role_edges_are_not_deduped(self):
        """Existing edges with different roles or non-null group_id are ignored by dedup."""
        parents = [_id("a")]
        children = [_id("x")]

        # The "existing" edge looks identical except its source_role
        # differs — it must NOT be considered a duplicate.
        existing = pl.DataFrame(
            {
                "source_artifact_id": parents,
                "target_artifact_id": children,
                "source_role": ["inputs"],  # different role
                "target_role": ["children"],
                "group_id": [None],
            },
            schema={
                "source_artifact_id": pl.String,
                "target_artifact_id": pl.String,
                "source_role": pl.String,
                "target_role": pl.String,
                "group_id": pl.String,
            },
        )
        store = _make_store(
            types={parents[0]: "data", children[0]: "data"},
            existing_edges=existing,
            step_range=(0, 5),
        )

        op = _make_op(GroupByStrategy.ZIP)
        result = op.execute_curator(
            inputs={"parents": _df(parents), "children": _df(children)},
            step_number=0,
            artifact_store=store,
        )

        # Existing edge had source_role='inputs'; our new edge has
        # source_role='parents'. Should be emitted as a fresh edge.
        assert result.lineage_edges is not None
        assert len(result.lineage_edges) == 1


# ---------------------------------------------------------------------------
# Op shape / static metadata
# ---------------------------------------------------------------------------


class TestDeclareLineageShape:
    """Static class-level properties."""

    def test_output_is_any_type(self):
        """The children output accepts any artifact type."""
        spec = DeclareLineage.outputs["children"]
        assert spec.artifact_type == ArtifactTypes.ANY

    def test_no_infer_lineage_from(self):
        """Children output doesn't trigger framework lineage capture."""
        spec = DeclareLineage.outputs["children"]
        assert spec.infer_lineage_from is None

    def test_declared_input_roles(self):
        """Inputs are declared (not runtime-defined) with parents + children."""
        assert DeclareLineage.runtime_defined_inputs is False
        assert set(DeclareLineage.inputs.keys()) == {"parents", "children"}

    def test_hydrate_inputs_false(self):
        """The op needs only IDs, not artifact content."""
        assert DeclareLineage.hydrate_inputs is False

    def test_independent_input_streams_true(self):
        """Required for CROSS_PRODUCT / NAME with unequal cardinalities."""
        assert DeclareLineage.independent_input_streams is True

    def test_pairing_is_required(self):
        """Constructing DeclareLineage without pairing raises."""
        with pytest.raises(Exception):
            DeclareLineage(params=DeclareLineage.Params())  # type: ignore[call-arg]
