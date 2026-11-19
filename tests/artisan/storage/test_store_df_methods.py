"""DataFrame queries preserve filtering, binary content and empty schemas."""

from __future__ import annotations

import polars as pl
import pytest
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


def test_direct_edges_include_null_origins_and_preserve_exact_roles(
    backend_committer: RetainedCommitter,
) -> None:
    parent = MetricArtifact.draft({"value": 7}, "shared.json").finalize()
    source = CSVArtifact.draft(b"x\n1\n", "source.csv", 1).finalize()
    target = MetricArtifact.draft({"value": 8}, "target.json", 2).finalize()
    unrelated = MetricArtifact.draft({"value": 9}, "other.json", 2).finalize()
    commit_inspection_attempt(
        backend_committer.delta_root,
        committer=backend_committer,
        number=2,
        inputs={"source": [source], "shared": [parent]},
        outputs={"result": [target], "other": [unrelated]},
    )
    provenance = _store(backend_committer).provenance
    assert provenance.get_artifact_step_number(parent.artifact_id) is None
    result = provenance.load_direct_edges_df(
        [target.artifact_id, target.artifact_id], include_roles=True
    )
    assert result.schema == dict.fromkeys(
        [
            "source_artifact_id",
            "target_artifact_id",
            "source_role",
            "target_role",
            "group_id",
        ],
        pl.String,
    )
    assert result.select("source_artifact_id", "source_role").sort(
        "source_role"
    ).rows() == [(parent.artifact_id, "shared"), (source.artifact_id, "source")]
    assert result["target_artifact_id"].to_list() == [target.artifact_id] * 2
    assert result["target_role"].to_list() == ["result"] * 2
    assert result["group_id"].to_list() == [None, None]
    assert set(result["source_artifact_id"]) == set(
        provenance.get_direct_ancestors(target.artifact_id)
    )
    # Numeric origin ranges retain their existing exclusion of null origins.
    ranged = provenance.load_edges_df(0, 2)
    assert parent.artifact_id not in ranged["source_artifact_id"]


def test_direct_edges_filter_executions_without_inferring_current_membership(
    backend_committer: RetainedCommitter,
) -> None:
    root = backend_committer.delta_root
    first = MetricArtifact.draft({"value": 1}, "first.json").finalize()
    second = MetricArtifact.draft({"value": 2}, "second.json").finalize()
    target = MetricArtifact.draft({"value": 3}, "target.json", 1).finalize()
    _, first_execution = commit_inspection_attempt(
        root,
        committer=backend_committer,
        number=1,
        inputs={"first": [first]},
        outputs={"result": [target]},
    )
    provenance = _store(backend_committer).provenance
    before = provenance.load_direct_edges_df({target.artifact_id}, include_roles=True)
    assert before["source_role"].to_list() == ["first"]
    _, second_execution = commit_inspection_attempt(
        root,
        committer=backend_committer,
        number=2,
        inputs={"second": [second]},
        outputs={"result": [target]},
    )
    _, repeated_execution = commit_inspection_attempt(
        root,
        committer=backend_committer,
        number=3,
        inputs={"second": [second]},
        outputs={"result": [target]},
    )
    _, diagnostic = commit_inspection_attempt(
        root,
        committer=backend_committer,
        number=4,
        replay_of=first_execution,
        inputs={"first": [first]},
        outputs={"result": [target]},
    )
    _, failed = commit_inspection_attempt(
        root,
        committer=backend_committer,
        number=5,
        status="failed",
        inputs={"first": [first]},
        outputs={"result": [target]},
    )
    assert before.height == 1
    all_edges = provenance.load_direct_edges_df(
        {target.artifact_id}, include_roles=True
    )
    assert sorted(all_edges["source_role"].to_list()) == ["first", "second", "second"]
    assert provenance.load_direct_edges_df(
        {target.artifact_id}, execution_ids=[first_execution]
    ).rows() == [(first.artifact_id, target.artifact_id)]
    assert provenance.load_direct_edges_df(
        {target.artifact_id},
        include_roles=True,
        execution_ids={second_execution, repeated_execution},
    )["source_role"].to_list() == ["second", "second"]
    assert provenance.load_direct_edges_df(
        {target.artifact_id}, execution_ids={diagnostic, failed}
    ).is_empty()


@pytest.mark.parametrize("include_roles", [False, True])
def test_direct_edges_empty_selections_and_unknown_ids_keep_string_schema(
    backend_committer: RetainedCommitter,
    monkeypatch: pytest.MonkeyPatch,
    include_roles: bool,
) -> None:
    provenance = _store(backend_committer).provenance
    columns = ["source_artifact_id", "target_artifact_id"]
    if include_roles:
        columns.extend(["source_role", "target_role", "group_id"])
    expected = dict.fromkeys(columns, pl.String)
    unknown = provenance.load_direct_edges_df(["f" * 32], include_roles=include_roles)
    assert unknown.is_empty()
    assert unknown.schema == expected

    def no_scan(*args: object, **kwargs: object) -> None:
        pytest.fail("An explicitly empty selection must not scan edge or origin tables")

    monkeypatch.setattr(provenance, "_scan", no_scan)
    for targets, executions in (([], None), (["f" * 32], [])):
        empty = provenance.load_direct_edges_df(
            targets, include_roles=include_roles, execution_ids=executions
        )
        assert empty.is_empty()
        assert empty.schema == expected
