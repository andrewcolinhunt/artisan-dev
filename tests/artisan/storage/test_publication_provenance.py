"""Portable curator provenance preserves original typed relation columns."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

import polars as pl
import pytest
from fixtures.retained_acceptance import admit_case, open_committer
from fixtures.retained_curators import admit_curator, stage_curator

from artisan.errors import StoreIntegrityError
from artisan.operations.curator.declare_lineage import DeclareLineage
from artisan.operations.curator.merge import Merge
from artisan.schemas.enums import GroupByStrategy, TablePath
from artisan.schemas.execution.curator_result import PassthroughResult
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.storage import _retained_publication as portable
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_view import CuratorView, capture_curator_view
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.core.table_schemas import ARTIFACT_EDGES_SCHEMA
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest


class GroupedLineage(DeclareLineage):
    """Declare explicit jointly necessary parents with non-default edge metadata."""

    name = "portable_grouped_lineage"

    def execute_curator(
        self,
        inputs: Mapping[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> PassthroughResult:
        result = super().execute_curator(inputs, step_number, artifact_store)
        edges = result.lineage_edges or []
        group = compute_content_digest(
            canonical_json_bytes(
                sorted(
                    {
                        (
                            edge.source_artifact_id,
                            edge.source_artifact_type,
                            edge.source_role,
                        )
                        for edge in edges
                    }
                )
            )
        )
        return result.model_copy(
            update={
                "lineage_edges": [
                    edge.model_copy(update={"group_id": group, "step_boundary": False})
                    for edge in edges
                ]
            }
        )


class FailedMerge(Merge):
    name = "portable_failed_merge"

    def execute_curator(
        self,
        inputs: Mapping[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> PassthroughResult:
        return PassthroughResult(success=False, error="authored failure")


def _source(
    tmp_path: Path,
    *,
    grouped: bool = False,
    diagnostic: bool = False,
) -> tuple[RetainedReader, str]:
    storage = StorageConfig()
    committer = open_committer(
        str(tmp_path / "source"), storage.filesystem(), storage, initialize=True
    )
    _, receipt = admit_case(committer, tmp_path, kind="file")
    committer.accept(receipt)
    reader = RetainedReader(committer)
    original = reader.read(receipt)
    cls = GroupedLineage if grouped else DeclareLineage
    operation = cls(params=cls.Params(pairing=GroupByStrategy.CROSS_PRODUCT))
    parents = original.support
    assert parents
    curator, admission, reference = admit_curator(
        committer,
        operation,
        {"parents": (*parents, parents[0]), "children": original.output("config")},
        step_run_id="b" * 32,
    )
    if diagnostic:
        admission = admission.model_copy(
            update={
                "record": admission.record.model_copy(
                    update={
                        "step_run_id": "c" * 32,
                        "replay_of_execution_run_id": "f" * 32,
                    }
                )
            }
        )
        reference = curator.admit(admission)
    staged, terminal = stage_curator(curator, admission, operation)
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    curator.finish(reference, staged.execution_run_id, terminal)
    return reader, staged.execution_run_id


@pytest.mark.parametrize("grouped", [False, True])
def test_original_curator_edge_columns_and_deduplicated_execution_membership_roundtrip(
    tmp_path: Path,
    grouped: bool,
) -> None:
    reader, execution = _source(tmp_path, grouped=grouped)
    committer = reader.committer
    view = CuratorView(committer, capture_curator_view(committer))
    expected = (
        view.artifact_edges(direct=True)
        .filter(pl.col("execution_run_id") == execution)
        .collect()
    )
    memberships = (
        view.scan(TablePath.EXECUTION_EDGES)
        .filter(pl.col("execution_run_id") == execution)
        .collect()
    )
    assert expected.height
    assert expected["source_role"].unique().to_list() == ["parents"]
    assert expected["target_role"].unique().to_list() == ["children"]
    assert expected["step_boundary"].unique().to_list() == [not grouped]
    assert expected["group_id"].null_count() == (0 if grouped else expected.height)
    publication = portable.export_retained_publication(
        reader, tmp_path / "bundle", redact=()
    )
    assert (
        publication.curator_artifact_edges()
        .sort(list(expected.columns))
        .equals(expected.sort(list(expected.columns)))
    )
    assert (
        publication.curator_execution_edges()
        .sort(list(memberships.columns))
        .equals(memberships.sort(list(memberships.columns)))
    )
    store = publication.open_store()
    facade_edges = store.provenance.load_edges_df(
        0,
        100,
        include_target_type=True,
        include_roles=True,
        execution_ids={execution},
    )
    assert facade_edges.sort(facade_edges.columns).equals(
        expected.select(facade_edges.columns).sort(facade_edges.columns)
    )
    assert store._read_scope is not None
    with store._read_scope.activate():
        assert store._view is not None
        facade_memberships = (
            store._view.scan(TablePath.EXECUTION_EDGES)
            .filter(pl.col("execution_run_id") == execution)
            .collect()
        )
    assert facade_memberships.sort(list(memberships.columns)).equals(
        memberships.sort(list(memberships.columns))
    )
    assert "logical_commit_id" not in publication.curator_artifact_edges().columns
    assert (
        publication.metadata().curator_artifact_edges.table_path
        == TablePath.ARTIFACT_EDGES.value
    )


def test_diagnostic_relations_are_preserved_but_excluded_from_ordinary_graph(
    tmp_path: Path,
) -> None:
    reader, execution = _source(tmp_path, diagnostic=True)
    publication = portable.export_retained_publication(
        reader, tmp_path / "bundle", redact=()
    )
    assert publication.curator_artifact_edges().is_empty()
    assert publication.curator_execution_edges().is_empty()
    store = publication.open_store()
    assert store.provenance.load_edges_df(
        0, 100, include_roles=True, execution_ids={execution}
    ).is_empty()
    assert store._read_scope is not None
    with store._read_scope.activate():
        assert store._view is not None
        assert (
            store._view.scan(TablePath.EXECUTION_EDGES)
            .filter(pl.col("execution_run_id") == execution)
            .collect()
            .is_empty()
        )
    assert publication.curator_artifact_edges(ordinary=False)[
        "execution_run_id"
    ].unique().to_list() == [execution]
    assert publication.curator_execution_edges(ordinary=False)[
        "execution_run_id"
    ].unique().to_list() == [execution]


def test_failed_curator_execution_is_input_only_and_empty_edges_remain_typed(
    tmp_path: Path,
) -> None:
    storage = StorageConfig()
    committer = open_committer(
        str(tmp_path / "source"), storage.filesystem(), storage, initialize=True
    )
    _, receipt = admit_case(committer, tmp_path, kind="alias")
    committer.accept(receipt)
    reader = RetainedReader(committer)
    original = reader.read(receipt).output("data")
    operation = FailedMerge()
    curator, admission, reference = admit_curator(
        committer, operation, {"selected": original}, step_run_id="b" * 32
    )
    staged, terminal = stage_curator(curator, admission, operation)
    curator.finish(reference, staged.execution_run_id, terminal)
    publication = portable.export_retained_publication(
        reader, tmp_path / "bundle", redact=()
    )
    assert publication.curator_artifact_edges().schema == ARTIFACT_EDGES_SCHEMA
    assert publication.curator_artifact_edges().is_empty()
    assert publication.curator_execution_edges().to_dicts() == [
        {
            "execution_run_id": staged.execution_run_id,
            "direction": "input",
            "role": "selected",
            "artifact_id": original[0].artifact.artifact_id,
        }
    ]


@pytest.mark.parametrize("field", ["source_role", "group_id"])
def test_original_role_and_group_values_are_protected_from_redaction(
    tmp_path: Path,
    field: str,
) -> None:
    reader, _ = _source(tmp_path, grouped=True)
    view = CuratorView(reader.committer, capture_curator_view(reader.committer))
    value = view._visible(TablePath.ARTIFACT_EDGES.value).collect()[field][0]
    with pytest.raises(ValueError, match="protected evidence"):
        portable.export_retained_publication(
            reader, tmp_path / "bundle", redact=(value,)
        )
    assert not (tmp_path / "bundle").exists()


@pytest.mark.parametrize("tamper", ["bytes", "owner", "missing", "type"])
def test_copied_curator_relations_reject_file_and_semantic_mismatch(
    tmp_path: Path,
    tamper: str,
) -> None:
    reader, _ = _source(tmp_path)
    publication = portable.export_retained_publication(
        reader, tmp_path / "bundle", redact=()
    )
    payload = publication.metadata().model_dump(mode="json")
    descriptor = payload["curator_artifact_edges"]
    path = publication.directory / descriptor["filename"]
    if tamper == "bytes":
        path.write_bytes(path.read_bytes() + b"changed")
    else:
        rows = pl.read_parquet(path)
        if tamper == "owner":
            rows = rows.with_columns(pl.lit("f" * 32).alias("execution_run_id"))
        elif tamper == "type":
            rows = rows.with_columns(pl.lit("config").alias("source_artifact_type"))
        else:
            rows = rows.head(0)
        rows.write_parquet(path)
        encoded = path.read_bytes()
        descriptor.update(
            digest=compute_content_digest(encoded),
            size_bytes=len(encoded),
            row_count=rows.height,
        )
        payload.pop("inspection_digest")
        payload["inspection_digest"] = portable._inspection_digest(payload)
        (publication.directory / "inspection.json").write_bytes(
            canonical_json_bytes(payload)
        )
    with pytest.raises(StoreIntegrityError):
        publication.curator_artifact_edges()


def test_export_checks_derived_execution_edges_against_original_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader, _ = _source(tmp_path)
    derive = portable.curator_execution_edges

    def omit(
        sources: Sequence[portable.InspectedCurator], *, ordinary: bool = True
    ) -> pl.DataFrame:
        return derive(sources, ordinary=ordinary).head(0)

    monkeypatch.setattr(portable, "curator_execution_edges", omit)
    with pytest.raises(StoreIntegrityError, match="original execution edges"):
        portable.export_retained_publication(reader, tmp_path / "bundle", redact=())
    assert not (tmp_path / "bundle").exists()


@pytest.mark.parametrize("field", ["artifact_id", "artifact_type"])
def test_query_execution_edges_require_catalogued_curator_inputs(
    tmp_path: Path, field: str
) -> None:
    reader, _ = _source(tmp_path)
    publication = portable.export_retained_publication(
        reader, tmp_path / "bundle", redact=()
    )
    payload = publication.metadata().model_dump(mode="json")
    member = payload["curator_sources"][0]["inputs"][0]["members"][0]
    member[field] = "f" * 32 if field == "artifact_id" else "metric"
    payload.pop("inspection_digest")
    payload["inspection_digest"] = portable._inspection_digest(payload)
    (publication.directory / "inspection.json").write_bytes(
        canonical_json_bytes(payload)
    )
    store = publication.open_store()
    assert store._read_scope is not None
    with store._read_scope.activate():
        assert store._view is not None
        with pytest.raises(StoreIntegrityError, match="protected content types"):
            store._view.scan(TablePath.EXECUTION_EDGES).collect()
