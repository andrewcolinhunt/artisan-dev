"""Public artifact hydration over accepted retained results and managed carriers."""

from __future__ import annotations

import polars as pl
import pytest
from fixtures.retained_inspection import commit_inspection_attempt

from artisan.errors import ArtifactIntegrityError
from artisan.schemas import CSVArtifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.utils.hashing import compute_content_digest


def _store(committer: RetainedCommitter) -> ArtifactStore:
    return ArtifactStore(
        committer.delta_root,
        fs=committer.fs,
        storage_options=committer.storage_options,
        files_root=committer.files_root,
    )


def test_prepare_entries_and_explicit_binding(
    backend_committer: RetainedCommitter,
) -> None:
    store = _store(backend_committer)
    assert store.base_path == backend_committer.delta_root
    assert store.files_root == backend_committer.files_root
    assert store.filesystem is backend_committer.fs
    row = store.prepare_artifact_index_entry("a" * 32, "metric", 1)
    assert row.to_dicts() == [
        {
            "artifact_id": "a" * 32,
            "artifact_type": "metric",
            "origin_step_number": 1,
            "metadata": "{}",
        }
    ]
    location = store.prepare_artifact_location_entry("a" * 32, "memory://payload")
    assert location.to_dicts() == [{"artifact_id": "a" * 32, "uri": "memory://payload"}]


def test_hydrates_typed_inline_artifacts_and_id_only_models(
    backend_committer: RetainedCommitter,
) -> None:
    metric = MetricArtifact.draft({"score": 0.95}, "score.json", 1).finalize()
    config = ExecutionConfigArtifact.draft({"seed": 7}, "settings.json", 1).finalize()
    commit_inspection_attempt(
        backend_committer.delta_root,
        committer=backend_committer,
        number=1,
        outputs={"metrics": [metric], "config": [config]},
    )
    store = _store(backend_committer)
    for artifact in (metric, config):
        loaded = store.get_artifact(artifact.artifact_id)
        assert type(loaded) is type(artifact)
        assert loaded.to_row() == artifact.to_row()
        assert (
            store.get_artifact(artifact.artifact_id, artifact.artifact_type) == loaded
        )
        minimal = store.get_artifact(artifact.artifact_id, hydrate=False)
        assert minimal.artifact_id == artifact.artifact_id
        assert minimal.content is None
        assert store.artifact_exists(artifact.artifact_id)
    with pytest.raises(ArtifactIntegrityError, match="indexed as"):
        store.get_artifact(metric.artifact_id, "csv")
    assert store.get_artifact("f" * 32) is None
    assert not store.artifact_exists("f" * 32)
    assert store.get_artifact_type("f" * 32) is None


def test_bulk_hydration_and_names_keep_type_and_requested_id_boundaries(
    backend_committer: RetainedCommitter,
) -> None:
    metrics = [
        MetricArtifact.draft({"value": i}, f"score-{i}.json", 2).finalize()
        for i in range(2)
    ]
    metrics.append(
        MetricArtifact(content=b'{"value": 3}', origin_step_number=2).finalize()
    )
    config = ExecutionConfigArtifact.draft({"seed": 3}, "settings.json", 2).finalize()
    commit_inspection_attempt(
        backend_committer.delta_root,
        committer=backend_committer,
        number=2,
        outputs={"metrics": metrics, "config": [config]},
    )
    store = _store(backend_committer)
    ids = [item.artifact_id for item in (*metrics, config)]
    loaded = store.get_artifacts_by_type([*ids, "f" * 32], "metric")
    assert set(loaded) == {item.artifact_id for item in metrics}
    assert {key: value.to_row() for key, value in loaded.items()} == {
        item.artifact_id: item.to_row() for item in metrics
    }
    assert store.get_artifacts_by_type([], "metric") == {}
    assert (
        store.get_artifacts_by_type([config.artifact_id], "config")[
            config.artifact_id
        ].to_row()
        == config.to_row()
    )
    assert store.load_type_map(ids) == {
        item.artifact_id: item.artifact_type for item in (*metrics, config)
    }
    assert store.load_original_names([*ids, "f" * 32]) == {
        item.artifact_id: item.original_name
        for item in (*metrics, config)
        if item.original_name is not None
    }
    assert store.load_original_names([]) == {}


def test_associated_metrics_use_real_direct_lineage(
    backend_committer: RetainedCommitter,
) -> None:
    source = CSVArtifact.draft(b"x\n1\n", "source.csv", 0).finalize()
    metric = MetricArtifact.draft({"score": 4}, "score.json", 1).finalize()
    commit_inspection_attempt(
        backend_committer.delta_root,
        committer=backend_committer,
        outputs={"data": [source]},
    )
    commit_inspection_attempt(
        backend_committer.delta_root,
        committer=backend_committer,
        number=1,
        inputs={"data": [source]},
        outputs={"metrics": [metric]},
    )
    store = _store(backend_committer)
    associated = store.get_associated({source.artifact_id}, "metric")
    assert [item.to_row() for item in associated[source.artifact_id]] == [
        metric.to_row()
    ]
    assert store.get_associated({"f" * 32}, "metric") == {}


def test_external_payload_uses_managed_copy_and_detects_later_mutation(
    backend_committer: RetainedCommitter,
) -> None:
    committer = backend_committer
    source = f"{committer.delta_root}/input.bin"
    with committer.fs.open(source, "wb") as stream:
        stream.write(b"verified payload")
    artifact = FileRefArtifact.draft(
        path=source,
        content_hash=compute_content_digest(b"verified payload"),
        size_bytes=16,
        step_number=0,
        original_name="payload",
        extension=".bin",
    ).finalize()
    commit_inspection_attempt(
        committer.delta_root, committer=committer, outputs={"files": [artifact]}
    )
    committer.fs.rm(source)
    store = _store(committer)
    loaded = store.get_artifact(artifact.artifact_id)
    assert loaded.path.startswith(committer.files_root + "/")
    assert committer.fs.cat(loaded.path) == b"verified payload"
    with committer.fs.open(loaded.path, "wb") as stream:
        stream.write(b"changed payload!")
    with pytest.raises(ArtifactIntegrityError, match="integrity|carrier|digest"):
        store.get_artifact(artifact.artifact_id)


def test_empty_store_returns_typed_empty_queries(
    backend_committer: RetainedCommitter,
) -> None:
    store = _store(backend_committer)
    assert store.get_artifacts_by_type(["a" * 32], "metric") == {}
    assert store.load_metrics_df([]).schema == {
        "artifact_id": pl.String,
        "content": pl.Binary,
    }
    assert store.load_original_names(["a" * 32]) == {}
    assert store.load_type_map([]) == {}
