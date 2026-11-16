"""DataFrame queries preserve filtering, binary content and empty schemas."""

from __future__ import annotations

import polars as pl
from fixtures.retained_inspection import commit_inspection_attempt

from artisan.schemas import CSVArtifact, MetricArtifact
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.io.retained_commit import RetainedCommitter


def _store(committer: RetainedCommitter) -> ArtifactStore:
    return ArtifactStore(
        committer.delta_root,
        fs=committer.fs,
        storage_options=committer.storage_options,
        files_root=committer.files_root,
    )


def test_load_edges_df_filters_both_endpoints_and_exact_execution(
    backend_committer: RetainedCommitter,
) -> None:
    root = backend_committer.delta_root
    source = CSVArtifact.draft(b"x\n1\n", "source.csv", 1).finalize()
    metric = MetricArtifact.draft({"score": 0.95}, "score.json", 5).finalize()
    commit_inspection_attempt(
        root, committer=backend_committer, number=1, outputs={"data": [source]}
    )
    _, execution = commit_inspection_attempt(
        root,
        committer=backend_committer,
        number=5,
        inputs={"data": [source]},
        outputs={"metric": [metric]},
    )
    provenance = _store(backend_committer).provenance
    result = provenance.load_edges_df(
        1, 5, include_target_type=True, include_roles=True
    )
    assert result.select(
        "source_artifact_id", "target_artifact_id", "target_artifact_type"
    ).to_dicts() == [
        {
            "source_artifact_id": source.artifact_id,
            "target_artifact_id": metric.artifact_id,
            "target_artifact_type": "metric",
        }
    ]
    assert result["source_role"].to_list() == ["data"]
    assert result["target_role"].to_list() == ["metric"]
    assert provenance.load_edges_df(1, 3).is_empty()
    assert provenance.load_edges_df(2, 5).is_empty()
    assert provenance.load_edges_df(1, 5, execution_ids={execution}).height == 1
    assert provenance.load_edges_df(1, 5, execution_ids=set()).is_empty()


def test_load_metrics_df_returns_only_requested_binary_payloads(
    backend_committer: RetainedCommitter,
) -> None:
    metrics = [
        MetricArtifact.draft({"value": index}, f"metric-{index}.json", 1).finalize()
        for index in range(2)
    ]
    commit_inspection_attempt(
        backend_committer.delta_root,
        committer=backend_committer,
        number=1,
        outputs={"metrics": metrics},
    )
    store = _store(backend_committer)
    result = store.load_metrics_df([metrics[0].artifact_id, "f" * 32])
    assert result.to_dicts() == [
        {"artifact_id": metrics[0].artifact_id, "content": metrics[0].content}
    ]
    assert result.schema == {"artifact_id": pl.String, "content": pl.Binary}
    assert store.load_metrics_df([item.artifact_id for item in metrics]).height == 2
    assert store.load_metrics_df([]).is_empty()


def test_empty_dataframe_queries_keep_declared_columns(
    backend_committer: RetainedCommitter,
) -> None:
    store = _store(backend_committer)
    assert store.load_metrics_df(["f" * 32]).schema == {
        "artifact_id": pl.String,
        "content": pl.Binary,
    }
    edges = store.provenance.load_edges_df(
        0, 10, include_roles=True, include_target_type=True
    )
    assert edges.is_empty()
    assert edges.columns == [
        "source_artifact_id",
        "target_artifact_id",
        "target_artifact_type",
        "source_role",
        "target_role",
        "group_id",
    ]
