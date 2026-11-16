"""Store-bound curator reads keep their admitted complete-owner visibility."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from fixtures.retained_acceptance import open_committer
from fixtures.retained_readers import Backend, admit_read_case
from fixtures.retained_results import (
    RetainedCase,
    _derive,
    boundary_artifact,
    retained_case,
)

from artisan.errors import StoreIntegrityError
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.operations.curator.filter import Criterion, Filter
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.curator_admission import CuratorReadSnapshot
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_view import CuratorView, capture_curator_view
from artisan.storage.io.retained_commit import RetainedCommitter


def _store(committer: RetainedCommitter) -> ArtifactStore:
    return ArtifactStore(
        committer.delta_root,
        fs=committer.fs,
        storage_options=committer.storage_options,
        files_root=committer.files_root,
        _view=CuratorView(committer, capture_curator_view(committer)),
    )


def _measurement_case(field: str, *, hidden: bool = False) -> RetainedCase:
    graph = OccurrenceGraph(boundary_invocation=f"input-{field}")
    artifact = boundary_artifact()
    source = graph.add_input("data", artifact)
    parent = _derive(graph, f"hidden-{field}", artifact, [source]) if hidden else source
    metric = MetricArtifact.draft({field: 1}, f"{field}.json", 7).finalize()
    measured = _derive(graph, f"measure-{field}", metric, [parent])
    projection = graph.project(
        {"data": [source], "metric": [measured]},
        {"data": OutputSpec(), "metric": OutputSpec()},
    )
    return RetainedCase(projection, {source: artifact}, {measured: metric})


def test_curator_queries_preserve_global_metrics_for_equal_content_producers(
    raw_backend_fs: Backend,
) -> None:
    fs, storage, root = raw_backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    first = _measurement_case("score_a")
    _, receipt = admit_read_case(committer, first, step_run_id="a" * 32)
    committer.accept(receipt)
    before = _store(committer)
    second = _measurement_case("score_b")
    _, receipt = admit_read_case(committer, second, step_run_id="b" * 32)
    committer.accept(receipt)
    current = _store(committer)
    primary = next(iter(first.boundary.values())).artifact_id
    assert primary == next(iter(second.boundary.values())).artifact_id
    criterion = Filter(
        params=Filter.Params(
            criteria=[Criterion(metric="score_b", operator="gt", value=0)]
        )
    )
    inputs = {"passthrough": pl.DataFrame({"artifact_id": [primary, primary]})}
    assert criterion.execute_curator(inputs, 8, before).passthrough == {
        "passthrough": []
    }
    assert criterion.execute_curator(inputs, 8, current).passthrough == {
        "passthrough": [primary, primary]
    }
    expected = tuple(
        sorted(
            item.artifact_id
            for case in (first, second)
            for item in case.artifacts.values()
        )
    )
    assert current.capture_associated({primary: ("metric",)}) == {
        (primary, "metric"): expected
    }


def test_curator_direct_queries_exclude_collapsed_parent_shortcuts(
    raw_backend_fs: Backend,
) -> None:
    fs, storage, root = raw_backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    case = _measurement_case("hidden_score", hidden=True)
    _, receipt = admit_read_case(committer, case, step_run_id="a" * 32)
    committer.accept(receipt)
    store = _store(committer)
    primary = next(iter(case.boundary.values())).artifact_id
    metric = next(iter(case.artifacts.values())).artifact_id
    assert store.capture_associated({primary: ("metric",)}) == {(primary, "metric"): ()}
    assert store.provenance.get_direct_descendants({primary}, "metric") == {}
    assert store.provenance.get_direct_ancestors(metric) == []
    assert store.provenance.get_descendant_ids(primary) == [metric]


def test_curator_view_excludes_later_complete_content(
    raw_backend_fs: Backend, tmp_path: Path
) -> None:
    fs, storage, root = raw_backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    empty = capture_curator_view(committer)
    case = retained_case(tmp_path, kind="alias")
    _, receipt = admit_read_case(committer, case, step_run_id="a" * 32)
    committer.accept(receipt)
    populated = capture_curator_view(committer)
    assert empty.digest != populated.digest
    assert (
        CuratorView(committer, empty)
        .scan(TablePath.ARTIFACT_INDEX)
        .collect()
        .is_empty()
    )
    assert (
        CuratorView(committer, populated)
        .scan(TablePath.ARTIFACT_INDEX)
        .collect()
        .height
        == 1
    )
    reopened = CuratorReadSnapshot.model_validate_json(populated.model_dump_json())
    assert reopened == populated

    later = retained_case(tmp_path / "later", kind="file")
    _, second = admit_read_case(committer, later, step_run_id="b" * 32)
    committer.accept(second)
    frozen_ids = (
        CuratorView(committer, reopened)
        .scan(TablePath.ARTIFACT_INDEX)
        .select("artifact_id")
        .collect()["artifact_id"]
        .to_list()
    )
    assert frozen_ids == [next(iter(case.boundary.values())).artifact_id]
    assert CuratorView(committer, capture_curator_view(committer)).scan(
        TablePath.ARTIFACT_INDEX
    ).collect().height > len(frozen_ids)


def test_curator_view_rejects_changed_complete_owner(
    raw_backend_fs: Backend, tmp_path: Path
) -> None:
    fs, storage, root = raw_backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    _, receipt = admit_read_case(
        committer, retained_case(tmp_path, kind="alias"), step_run_id="a" * 32
    )
    committer.accept(receipt)
    snapshot = capture_curator_view(committer)
    changed = snapshot.model_copy(
        update={
            "controls": (
                snapshot.controls[0].model_copy(update={"plan_digest": "f" * 32}),
            )
        }
    )
    with pytest.raises(StoreIntegrityError, match="differs from its completed plan"):
        CuratorView(committer, changed)
