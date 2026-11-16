"""Public export from actual worker results to a portable read-only inspection."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path

import pytest

from artisan.composites.examples import TransformAndScore
from artisan.errors import IncompatibleStoreError
from artisan.operations.curator import IngestCSV
from artisan.orchestration import PipelineManager, StepStatus
from artisan.storage import ArtifactStore, PublicationExport, export_publication

pytestmark = pytest.mark.integration


@dataclass(frozen=True)
class PublicationSource:
    root: Path
    store: ArtifactStore


@pytest.fixture(scope="module")
def publication_source(tmp_path_factory: pytest.TempPathFactory) -> PublicationSource:
    root = tmp_path_factory.mktemp("publication") / "private-workspace"
    root.mkdir()
    incoming = root / "input.csv"
    incoming.write_text("id,x,y,score\n0,1,2,0.4\n1,3,4,0.8\n")
    with PipelineManager.create(
        "publication-example",
        delta_root=str(root / "delta"),
        staging_root=str(root / "staging"),
        working_root=str(root / "work"),
    ) as manager:
        ingested = manager.run(IngestCSV, inputs=[str(incoming)])
        assert ingested.status is StepStatus.SUCCEEDED, ingested.error
        transformed = manager.run(
            TransformAndScore,
            inputs={"dataset": ingested.output("csv")},
            params={"seed": 3},
        )
        assert transformed.status is StepStatus.SUCCEEDED, transformed.error
    return PublicationSource(root, ArtifactStore(str(root / "delta")))


def _inventory(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*")
        if path.is_file()
    }


def _assert_content_and_lineage(
    original: ArtifactStore, published: ArtifactStore
) -> None:
    types = original.provenance.load_type_map()
    assert types
    assert published.provenance.load_type_map() == types
    for artifact_id in types:
        source = original.get_artifact(artifact_id)
        copied = published.get_artifact(artifact_id)
        assert source is not None
        assert copied is not None
        assert copied.to_row() == source.to_row()
        if copied.EXTERNALLY_BACKED:
            copied.verify_external_content(fs=published.filesystem)
    edges = original.provenance.load_edges_df(0, 10, include_roles=True)
    assert not edges.is_empty()
    assert (
        published.provenance.load_edges_df(0, 10, include_roles=True)
        .sort(edges.columns)
        .equals(edges.sort(edges.columns))
    )


def test_export_publication_preserves_actual_worker_results_and_source(
    publication_source: PublicationSource, tmp_path: Path
) -> None:
    before = _inventory(publication_source.root)
    publication = export_publication(
        publication_source.store,
        tmp_path / "bundle",
        redact=(str(publication_source.root),),
    )
    _assert_content_and_lineage(publication_source.store, publication.open_store())
    assert _inventory(publication_source.root) == before
    assert not list(publication.directory.rglob("_delta_log"))
    assert not list(publication.directory.rglob("retained-result.json"))
    assert not any(
        hasattr(publication, name) for name in ("accept", "recover", "resume", "cache")
    )


def test_export_reopens_after_original_store_and_external_file_removal(
    tmp_path: Path,
) -> None:
    original = tmp_path / "original"
    original.mkdir()
    path = original / "input.csv"
    path.write_text("id,x\n0,7\n")
    with PipelineManager.create(
        "portable",
        str(original / "delta"),
        str(original / "staging"),
        working_root=str(original / "work"),
    ) as manager:
        step = manager.run(IngestCSV, inputs=[str(path)])
        assert step.status is StepStatus.SUCCEEDED, step.error
    source = ArtifactStore(str(original / "delta"))
    expected = {
        artifact_id: source.get_artifact(artifact_id).to_row()
        for artifact_id in source.provenance.load_type_map()
    }
    publication = export_publication(source, tmp_path / "bundle", redact=())
    shutil.rmtree(original)
    moved = tmp_path / "moved"
    publication.directory.rename(moved)
    reopened = PublicationExport(moved).open_store()
    for artifact_id, row in expected.items():
        artifact = reopened.get_artifact(artifact_id)
        assert artifact is not None
        assert artifact.to_row() == row
        if artifact.EXTERNALLY_BACKED:
            artifact.verify_external_content(fs=reopened.filesystem)
    with pytest.raises(IncompatibleStoreError):
        ArtifactStore(str(moved))


@pytest.mark.parametrize("location", ["delta", "files"])
def test_export_rejects_source_overlap_without_mutation(
    publication_source: PublicationSource, location: str
) -> None:
    before = _inventory(publication_source.root)
    destination = publication_source.root / location / "bundle"
    with pytest.raises(ValueError, match="overlap"):
        export_publication(publication_source.store, destination, redact=())
    assert _inventory(publication_source.root) == before


def test_export_rejects_nonempty_destination_without_mutation(
    publication_source: PublicationSource, tmp_path: Path
) -> None:
    destination = tmp_path / "bundle"
    destination.mkdir()
    (destination / "unrelated.txt").write_text("keep this file")
    before = _inventory(destination)
    with pytest.raises(ValueError):
        export_publication(publication_source.store, destination, redact=())
    assert _inventory(destination) == before
