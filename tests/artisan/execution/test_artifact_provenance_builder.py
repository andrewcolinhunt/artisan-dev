"""Typed enrichment resolves every declared endpoint without adding edges."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from artisan.execution.exceptions import LineageIntegrityError
from artisan.execution.lineage.enrich import (
    build_artifact_edges_from_store,
    build_artifact_edges_from_types,
)
from artisan.schemas.provenance.source_target_pair import SourceTargetPair


def pair() -> SourceTargetPair:
    return SourceTargetPair(
        source="a" * 32,
        target="b" * 32,
        source_role="input",
        target_role="output",
        group_id="c" * 32,
    )


def test_enrichment_preserves_exact_pair_and_metadata() -> None:
    edges = build_artifact_edges_from_types(
        [pair()], "d" * 32, {"a" * 32: "data", "b" * 32: "metric"}
    )
    assert len(edges) == 1
    edge = edges[0]
    assert edge.source_artifact_type == "data"
    assert edge.target_artifact_type == "metric"
    assert (edge.source_artifact_id, edge.target_artifact_id) == ("a" * 32, "b" * 32)
    assert (
        edge.source_role,
        edge.target_role,
        edge.group_id,
        edge.execution_run_id,
    ) == ("input", "output", "c" * 32, "d" * 32)


@pytest.mark.parametrize("endpoint", ["a" * 32, "b" * 32])
@pytest.mark.parametrize("missing_type", [None, "UNKNOWN", "any", ""])
def test_missing_endpoint_types_fail(endpoint: str, missing_type: str | None) -> None:
    types = {"a" * 32: "data", "b" * 32: "metric"}
    if missing_type is None:
        del types[endpoint]
    else:
        types[endpoint] = missing_type
    with pytest.raises(LineageIntegrityError, match="Missing concrete artifact type"):
        build_artifact_edges_from_types([pair()], "d" * 32, types)


def test_store_wrapper_does_one_deduplicated_bulk_lookup() -> None:
    store = MagicMock()
    types = {"a" * 32: "data", "b" * 32: "metric"}
    store.provenance.load_type_map.return_value = types
    pairs = [pair(), pair()]
    actual = build_artifact_edges_from_store(pairs, "d" * 32, store)
    assert actual == build_artifact_edges_from_types(pairs, "d" * 32, types)
    store.provenance.load_type_map.assert_called_once_with(["a" * 32, "b" * 32])


def test_empty_pairs_do_not_read_store() -> None:
    store = MagicMock()
    assert build_artifact_edges_from_store([], "d" * 32, store) == []
    store.provenance.load_type_map.assert_not_called()


def test_store_missing_endpoint_raises() -> None:
    store = MagicMock()
    store.provenance.load_type_map.return_value = {"a" * 32: "data"}
    with pytest.raises(LineageIntegrityError):
        build_artifact_edges_from_store([pair()], "d" * 32, store)
