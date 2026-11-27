"""Existing artifact queries over confined, relocated inspection bundles."""

from __future__ import annotations

import io
import json
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, BinaryIO

import polars as pl
import pytest
from fixtures.candidate_outputs import external_case, prepare_scientific
from fixtures.retained_results import (
    RetainedCase,
    _derive,
    boundary_artifact,
    retained_case,
)

from artisan.errors import (
    ArtifactIntegrityError,
    IncompatibleStoreError,
    StoreIntegrityError,
)
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage._retained_publication import (
    PublicationExport,
    _inspection_digest,
)
from artisan.storage.core import publication_view
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.publication_view import PublicationReadScope
from artisan.storage.io.retained_store import create_retained_store
from artisan.storage.publication import export_publication
from artisan.utils.hashing import canonical_json_bytes, compute_stream_digest


def _publish(tmp_path: Path, case: RetainedCase) -> PublicationExport:
    committer = create_retained_store(
        str(tmp_path / "store"),
        str(tmp_path / "files"),
        fs=StorageConfig().filesystem(),
    )
    results, admitted, receipts = prepare_scientific(committer, case)
    committer.accept(receipts[0])
    results.finish(admitted.step_run_id)
    publication = export_publication(
        ArtifactStore(committer.delta_root, files_root=committer.files_root),
        tmp_path / "bundle",
        redact=(),
    )
    shutil.rmtree(tmp_path / "store")
    if (tmp_path / "files").exists():
        shutil.rmtree(tmp_path / "files")
    moved = tmp_path / "relocated"
    publication.directory.rename(moved)
    return PublicationExport(moved)


@pytest.fixture
def published(tmp_path: Path) -> tuple[PublicationExport, RetainedCase]:
    case = retained_case(tmp_path / "original", kind="file")
    publication = _publish(tmp_path, case)
    shutil.rmtree(tmp_path / "original")
    return publication, case


def _id(artifact: Artifact) -> str:
    assert artifact.artifact_id is not None
    return artifact.artifact_id


def test_metadata_queries_need_no_models_content_or_carriers(
    published: tuple[PublicationExport, RetainedCase], monkeypatch: pytest.MonkeyPatch
) -> None:
    publication, case = published
    scope = PublicationReadScope(publication)
    captured: list[str] = []
    capture = publication_view.capture_table_payload

    def observe(item: Any, **kwargs: Any) -> Any:
        assert item.table_path in {TablePath.ARTIFACT_INDEX, TablePath.STEPS}
        captured.append(item.table_path)
        return capture(item, **kwargs)

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        msg = "Metadata query attempted model/content access"
        raise AssertionError(msg)

    monkeypatch.setattr(publication_view, "capture_table_payload", observe)
    monkeypatch.setattr(ArtifactTypeDef, "get_all", forbidden)
    monkeypatch.setattr(ArtifactTypeDef, "get_model", forbidden)
    store = scope.open_store()
    primary = _id(next(iter(case.boundary.values())))
    assert store.get_artifact_type(primary) == "csv"
    assert store.provenance.load_step_name_map() == {7: "produce"}
    assert store.provenance.load_step_map({primary}) == {primary: 1}
    assert scope.view is None
    assert captured.count(TablePath.STEPS) == 1


def test_selected_artifacts_keep_origin_content_and_confined_locations(
    published: tuple[PublicationExport, RetainedCase],
) -> None:
    publication, case = published
    scope = PublicationReadScope(publication)
    store = scope.open_store()
    for expected in [*case.boundary.values(), *case.artifacts.values()]:
        actual = store.get_artifact(_id(expected))
        assert actual is not None
        assert actual.to_row() == expected.to_row()
        if actual.EXTERNALLY_BACKED:
            actual.verify_external_content(fs=store.filesystem)
            locator = getattr(actual, next(iter(actual.LOCATOR_FIELDS)))
            assert Path(locator).is_relative_to(publication.directory)
    assert scope.view is None


def test_unrelated_corrupt_carrier_does_not_prevent_inline_queries(
    published: tuple[PublicationExport, RetainedCase],
) -> None:
    publication, case = published
    scope = PublicationReadScope(publication)
    store = scope.open_store()
    carrier = publication.metadata().carriers[0]
    (publication.directory / carrier.relative_path).write_bytes(b"corrupt")
    primary = next(iter(case.boundary.values()))
    assert store.get_artifact(_id(primary)) is not None
    external = next(item for item in case.artifacts.values() if item.EXTERNALLY_BACKED)
    with pytest.raises(ArtifactIntegrityError, match="carrier bytes changed"):
        store.get_artifact(_id(external))


def test_appendable_checks_whole_carrier_after_hydration(tmp_path: Path) -> None:
    case = external_case(tmp_path / "original", appendable=True)
    publication = _publish(tmp_path, case)
    store = publication.open_store()
    expected = next(iter(case.artifacts.values()))
    artifact = store.get_artifact(_id(expected))
    assert artifact is not None
    carrier = publication.metadata().carriers[0]
    path = publication.directory / carrier.relative_path
    path.write_bytes(path.read_bytes().replace(b'"value": 2', b'"value": 3'))
    with pytest.raises(ArtifactIntegrityError, match="carrier bytes changed"):
        artifact.verify_external_content(fs=store.filesystem)
    with pytest.raises(ArtifactIntegrityError, match="carrier bytes changed"):
        store.get_artifact(_id(expected))


def test_long_lived_store_rejects_coherent_manifest_replacement(
    published: tuple[PublicationExport, RetainedCase],
) -> None:
    publication, case = published
    store = publication.open_store()
    payload = json.loads((publication.directory / "inspection.json").read_bytes())
    payload["runs"]["lifecycle"]["digest"] = "f" * 32
    payload.pop("inspection_digest")
    payload["inspection_digest"] = _inspection_digest(payload)
    (publication.directory / "inspection.json").write_bytes(
        canonical_json_bytes(payload)
    )
    publication.metadata()
    with pytest.raises(StoreIntegrityError, match="identity changed"):
        store.get_artifact_type(_id(next(iter(case.boundary.values()))))


def test_later_content_and_index_changes_are_detected(
    published: tuple[PublicationExport, RetainedCase],
) -> None:
    publication, case = published
    store = publication.open_store()
    primary = _id(next(iter(case.boundary.values())))
    assert store.get_artifact(primary) is not None
    manifest = publication.metadata()
    table = next(
        item for item in manifest.tables if item.table_path == "artifacts/csvs"
    )
    path = publication.directory / table.filename
    path.write_bytes(path.read_bytes() + b"changed")
    with pytest.raises(StoreIntegrityError):
        store.get_artifact(primary)
    assert store.get_artifact_type(primary) == "csv"
    index = publication.directory / manifest.artifact_index.filename
    index.write_bytes(index.read_bytes() + b"changed")
    with pytest.raises(StoreIntegrityError):
        store.get_artifact_type(primary)


def test_scope_captures_once_per_nested_query_and_releases_on_error(
    published: tuple[PublicationExport, RetainedCase], monkeypatch: pytest.MonkeyPatch
) -> None:
    publication, case = published
    scope = PublicationReadScope(publication)
    store = scope.open_store()
    captures: list[Path] = []
    capture = publication_view.capture_table_payload

    def observe(item: Any, **kwargs: Any) -> Any:
        result = capture(item, **kwargs)
        captures.append(Path(result.path))
        return result

    monkeypatch.setattr(publication_view, "capture_table_payload", observe)
    primary = _id(next(iter(case.boundary.values())))
    assert store.get_artifact(primary) is not None
    assert len(captures) == 2
    assert len({item.parent for item in captures}) == 1
    assert all(not item.exists() for item in captures)
    assert scope.view is None
    with pytest.raises(RuntimeError, match="injected"), scope.activate():
        assert store.get_artifact(primary) is not None
        msg = "injected"
        raise RuntimeError(msg)
    assert scope.view is None
    assert all(not item.exists() for item in captures)


def test_concurrent_queries_do_not_share_capture_lifetimes(
    published: tuple[PublicationExport, RetainedCase], monkeypatch: pytest.MonkeyPatch
) -> None:
    publication, case = published
    scope = PublicationReadScope(publication)
    store = scope.open_store()
    barrier = threading.Barrier(2)
    capture = publication_view.capture_table_payload
    scratch: list[Path] = []

    def observe(item: Any, **kwargs: Any) -> Any:
        result = capture(item, **kwargs)
        scratch.append(Path(result.path).parent)
        barrier.wait(timeout=5)
        return result

    monkeypatch.setattr(publication_view, "capture_table_payload", observe)
    primary = _id(next(iter(case.boundary.values())))
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(store.get_artifact_type, primary) for _ in range(2)]
        assert [future.result(timeout=10) for future in futures] == ["csv", "csv"]
    assert len(set(scratch)) == 2
    assert all(not path.exists() for path in scratch)
    assert scope.view is None


def test_filesystem_and_write_preparation_cannot_escape_inspection(
    published: tuple[PublicationExport, RetainedCase], tmp_path: Path
) -> None:
    publication, _ = published
    store = publication.open_store()
    outside = tmp_path / "outside"
    outside.write_bytes(b"private")
    with pytest.raises(PermissionError, match="outside"):
        store.filesystem.open(str(outside), "rb")
    with pytest.raises(PermissionError, match="read-only"):
        store.filesystem.open(str(publication.directory / "new"), "wb")
    with pytest.raises(PermissionError, match="read-only"):
        store.filesystem.rm(str(publication.directory / "inspection.json"))
    with pytest.raises(ValueError, match="staged writes"):
        store.validate_staged_artifacts({}, {}, per_execution_artifact_ids={})
    with pytest.raises(IncompatibleStoreError):
        ArtifactStore(str(publication.directory), fs=store.filesystem)
    carrier = publication.metadata().carriers[0]
    path = publication.directory / carrier.relative_path
    path.unlink()
    path.symlink_to(outside)
    with pytest.raises((PermissionError, StoreIntegrityError)):
        store.filesystem.open(str(path), "rb")


@pytest.mark.parametrize("hidden", [False, True])
def test_metrics_preserve_direct_associations_and_collapsed_ancestry(
    tmp_path: Path, hidden: bool
) -> None:
    graph = OccurrenceGraph()
    primary = boundary_artifact()
    source = graph.add_input("data", primary)
    parent = _derive(graph, "hidden", primary, [source]) if hidden else source
    metric = MetricArtifact.draft({"score": 2}, "score.json", 7).finalize()
    measured = _derive(graph, "measure", metric, [parent])
    case = RetainedCase(
        graph.project(
            {"data": [source], "metric": [measured]},
            {"data": OutputSpec(), "metric": OutputSpec()},
        ),
        {source: primary},
        {measured: metric},
    )
    publication = _publish(tmp_path, case)
    store = publication.open_store()
    expected = () if hidden else (_id(metric),)
    assert store.capture_associated({_id(primary): ("metric",)}) == {
        (_id(primary), "metric"): expected
    }
    assert store.provenance.get_descendant_ids(_id(primary)) == [_id(metric)]
    assert store.load_metrics_df([_id(metric)]).height == 1
    associated = store.get_associated({_id(primary)}, "metric")
    assert bool(associated) is not hidden


def test_selected_content_compares_all_protected_index_fields(
    published: tuple[PublicationExport, RetainedCase],
) -> None:
    publication, case = published
    payload = json.loads((publication.directory / "inspection.json").read_bytes())
    table = next(
        item for item in payload["tables"] if item["table_path"] == "artifacts/csvs"
    )
    path = publication.directory / table["filename"]
    pl.read_parquet(path).with_columns(
        (pl.col("origin_step_number") + 1).alias("origin_step_number")
    ).write_parquet(path)
    with path.open("rb") as stream:
        table["digest"], table["size_bytes"] = compute_stream_digest(stream)
    payload.pop("inspection_digest")
    payload["inspection_digest"] = _inspection_digest(payload)
    (publication.directory / "inspection.json").write_bytes(
        canonical_json_bytes(payload)
    )
    store = publication.open_store()
    primary = _id(next(iter(case.boundary.values())))
    assert store.provenance.get_artifact_step_number(primary) == 1
    with pytest.raises(StoreIntegrityError, match="protected index metadata"):
        store.get_artifact(primary)


def test_selected_inline_type_does_not_require_other_registered_types(
    published: tuple[PublicationExport, RetainedCase], monkeypatch: pytest.MonkeyPatch
) -> None:
    publication, case = published
    definitions = ArtifactTypeDef.get_all()
    monkeypatch.setattr(ArtifactTypeDef, "get_all", lambda: {"csv": definitions["csv"]})
    store = publication.open_store()
    primary = _id(next(iter(case.boundary.values())))
    assert store.get_artifact(primary) is not None
    missing = next(
        item for item in case.artifacts.values() if item.artifact_type == "config"
    )
    with pytest.raises(ValueError, match="registered by the caller"):
        store.get_artifact(_id(missing))


def test_carrier_stream_is_private_read_only_and_closed_with_its_capture(
    published: tuple[PublicationExport, RetainedCase], monkeypatch: pytest.MonkeyPatch
) -> None:
    publication, _ = published
    store = publication.open_store()
    carrier = publication.metadata().carriers[0]
    path = publication.directory / carrier.relative_path
    original = path.read_bytes()
    captured: list[BinaryIO] = []
    digest = publication_view.compute_stream_digest

    def mutate_after_copy(
        source: BinaryIO, *, destination: BinaryIO | None = None
    ) -> tuple[str, int]:
        result = digest(source, destination=destination)
        assert destination is not None
        captured.append(destination)
        path.write_bytes(b"changed after verified copy")
        return result

    monkeypatch.setattr(publication_view, "compute_stream_digest", mutate_after_copy)
    with store.filesystem.open(str(path), "rb") as stream:
        assert stream.name == str(path)
        assert stream.mode == "rb"
        assert stream.read() == original
        with pytest.raises(io.UnsupportedOperation):
            stream.write(b"not writable")
        assert stream.raw is not None
        assert not stream.raw.writable()
        with pytest.raises(io.UnsupportedOperation):
            stream.raw.write(b"not writable either")
        assert captured[0].closed
        assert not Path(captured[0].name).exists()
    assert captured[0].closed
    assert not Path(captured[0].name).exists()
    with pytest.raises(ArtifactIntegrityError, match="carrier bytes changed"):
        store.filesystem.open(str(path), "rb")
    assert all(item.closed for item in captured)


def test_failed_carrier_copy_closes_source_and_private_capture(
    published: tuple[PublicationExport, RetainedCase], monkeypatch: pytest.MonkeyPatch
) -> None:
    publication, _ = published
    store = publication.open_store()
    path = publication.directory / publication.metadata().carriers[0].relative_path
    streams: list[BinaryIO] = []

    def fail_copy(
        source: BinaryIO, *, destination: BinaryIO | None = None
    ) -> tuple[str, int]:
        assert destination is not None
        streams.extend((source, destination))
        destination.write(source.read(3))
        msg = "injected copy failure"
        raise ValueError(msg)

    monkeypatch.setattr(publication_view, "compute_stream_digest", fail_copy)
    with pytest.raises(ValueError, match="injected copy failure"):
        store.filesystem.open(str(path), "rb")
    assert len(streams) == 2
    assert all(item.closed for item in streams)


def test_captured_readonly_descriptor_can_transfer_to_its_caller(
    published: tuple[PublicationExport, RetainedCase],
) -> None:
    publication, _ = published
    store = publication.open_store()
    path = publication.directory / publication.metadata().carriers[0].relative_path
    expected = path.read_bytes()
    stream = store.filesystem.open(str(path), "rb")
    raw = stream.detach()
    try:
        assert not raw.writable()
        assert raw.read() == expected
        with pytest.raises(io.UnsupportedOperation):
            raw.write(b"cannot mutate")
    finally:
        raw.close()
    assert raw.closed
