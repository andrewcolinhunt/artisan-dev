"""Ordinary store queries refresh visibility without changing admitted curator views."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import polars as pl
import pytest
from fixtures.candidate_outputs import prepare_scientific
from fixtures.retained_readers import admit_read_case
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
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_view import CuratorView, capture_curator_view
from artisan.storage.core.provenance_store import ProvenanceStore
from artisan.storage.core.store_read_scope import RetainedStoreReadScope
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_read import RetainedReadSnapshot
from artisan.storage.io.retained_store import create_retained_store
from artisan.utils.path import uri_join


@pytest.fixture
def committer(tmp_path: Path) -> RetainedCommitter:
    return create_retained_store(
        str(tmp_path / "store"),
        str(tmp_path / "files"),
        fs=StorageConfig().filesystem(),
    )


def _store(committer: RetainedCommitter) -> ArtifactStore:
    return ArtifactStore(
        committer.delta_root,
        files_root=committer.files_root,
        fs=committer.fs,
        storage_options=committer.storage_options,
    )


def test_default_stores_bind_local_carriers_and_reject_historical_manifest(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    store = ArtifactStore(committer.delta_root)
    provenance = ProvenanceStore(committer.delta_root, fs=committer.fs)
    assert store.files_root == provenance.files_root == committer.files_root
    assert provenance.load_backward_map() == {}
    historical = tmp_path / "historical"
    manifest = historical / "_artisan/store.json"
    manifest.parent.mkdir(parents=True)
    original = b'{"store_format":9,"artifact_identity":1,"cache_identity":3}'
    manifest.write_bytes(original)
    for reader in (
        lambda: ArtifactStore(str(historical)),
        lambda: ProvenanceStore(str(historical), fs=committer.fs),
    ):
        with pytest.raises(IncompatibleStoreError, match="outside"):
            reader()
    assert manifest.read_bytes() == original
    assert set(historical.rglob("*")) == {manifest.parent, manifest}


def _measurement(name: str, *, hidden: bool = False) -> RetainedCase:
    graph = OccurrenceGraph(boundary_invocation=f"input-{name}")
    artifact = boundary_artifact()
    source = graph.add_input("data", artifact)
    parent = _derive(graph, f"hidden-{name}", artifact, [source]) if hidden else source
    metric = MetricArtifact.draft({name: 1}, f"{name}.json", 7).finalize()
    result = _derive(graph, f"measure-{name}", metric, [parent])
    return RetainedCase(
        graph.project(
            {"data": [source], "metric": [result]},
            {"data": OutputSpec(), "metric": OutputSpec()},
        ),
        {source: artifact},
        {result: metric},
    )


def _accept(committer: RetainedCommitter, case: RetainedCase, step: str) -> None:
    _, receipt = admit_read_case(committer, case, step_run_id=step * 32)
    committer.accept(receipt)


def _artifact_id(artifact: Artifact) -> str:
    assert artifact.artifact_id is not None
    return artifact.artifact_id


def test_queries_refresh_while_curator_keeps_admitted_view(
    committer: RetainedCommitter,
) -> None:
    store = _store(committer)
    provenance = store.provenance
    primary = boundary_artifact().artifact_id
    assert primary is not None
    assert store.get_artifact(primary) is None
    first = _measurement("first")
    _accept(committer, first, "a")
    frozen = ArtifactStore(
        committer.delta_root,
        files_root=committer.files_root,
        fs=committer.fs,
        storage_options=committer.storage_options,
        _view=CuratorView(committer, capture_curator_view(committer)),
    )
    second = _measurement("second")
    _accept(committer, second, "b")
    expected = sorted(
        _artifact_id(artifact)
        for case in (first, second)
        for artifact in case.artifacts.values()
    )
    assert (
        sorted(provenance.get_direct_descendants({primary}, "metric")[primary])
        == expected
    )
    assert (
        sorted(
            _artifact_id(item)
            for item in store.get_associated({primary}, "metric")[primary]
        )
        == expected
    )
    assert len(frozen.get_associated({primary}, "metric")[primary]) == 1
    assert store.capture_associated({primary: ("metric",)}) == {
        (primary, "metric"): tuple(expected)
    }
    assert store._read_scope is not None
    assert store._read_scope.view is None


def test_nested_queries_share_one_snapshot_and_release_after_failure(
    committer: RetainedCommitter, monkeypatch: pytest.MonkeyPatch
) -> None:
    case = _measurement("score")
    _accept(committer, case, "a")
    primary = next(iter(case.boundary.values())).artifact_id
    assert primary is not None
    store = _store(committer)
    assert isinstance(store._read_scope, RetainedStoreReadScope)
    reader = store._read_scope.committer
    original = reader._read_snapshot
    snapshots = 0

    @contextmanager
    def count() -> Iterator[RetainedReadSnapshot]:
        nonlocal snapshots
        snapshots += 1
        with original() as read:
            yield read

    monkeypatch.setattr(reader, "_read_snapshot", count)
    assert store.get_associated({primary}, "metric")[primary]
    assert snapshots == 1
    with pytest.raises(ArtifactIntegrityError, match="not 'metric'"):
        store.get_artifact(primary, "metric")
    assert snapshots == 2
    assert store._read_scope is not None
    assert store._read_scope.view is None
    assert store.get_artifact(primary) is not None
    assert snapshots == 3


def test_acceptance_during_nested_read_is_visible_only_to_next_query(
    committer: RetainedCommitter, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = _measurement("first")
    _accept(committer, first, "a")
    second = _measurement("second")
    store = _store(committer)
    primary = next(iter(first.boundary.values())).artifact_id
    assert primary is not None
    original = store.provenance.get_direct_descendants
    changed = False

    def accept_after_edges(
        artifact_ids: set[str], target_artifact_type: str | None = None
    ) -> dict[str, list[str]]:
        nonlocal changed
        result = original(artifact_ids, target_artifact_type)
        if not changed:
            _accept(committer, second, "b")
            changed = True
        return result

    monkeypatch.setattr(store.provenance, "get_direct_descendants", accept_after_edges)
    assert len(store.get_associated({primary}, "metric")[primary]) == 1
    assert len(store.get_associated({primary}, "metric")[primary]) == 2


def test_same_store_concurrent_queries_have_distinct_views(
    committer: RetainedCommitter, monkeypatch: pytest.MonkeyPatch
) -> None:
    case = _measurement("score")
    _accept(committer, case, "a")
    primary = next(iter(case.boundary.values())).artifact_id
    assert primary is not None
    store = _store(committer)
    entered = threading.Event()
    release = threading.Event()
    second_entered = threading.Event()
    views: list[CuratorView] = []
    original = store._scan

    def scan(
        table: str | TablePath,
        *,
        direct: bool = False,
    ) -> pl.LazyFrame:
        assert store._view is not None
        views.append(store._view)
        if len(views) == 1:
            entered.set()
            assert release.wait(10)
        return original(table, direct=direct)

    def second_query() -> str | None:
        assert store._view is None
        second_entered.set()
        return store.get_artifact_type(primary)

    monkeypatch.setattr(store, "_scan", scan)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(store.get_artifact_type, primary)
        try:
            assert entered.wait(10)
            second = pool.submit(second_query)
            assert second_entered.wait(10)
            assert not second.done()
        finally:
            release.set()
        assert first.result(timeout=15) == "csv"
        assert second.result(timeout=15) == "csv"
    assert len(views) == 2
    assert views[0] is not views[1]
    assert store._view is None


def test_direct_associations_exclude_collapsed_edges(
    committer: RetainedCommitter,
) -> None:
    case = _measurement("score", hidden=True)
    _accept(committer, case, "a")
    primary = next(iter(case.boundary.values())).artifact_id
    metric = next(iter(case.artifacts.values())).artifact_id
    assert primary is not None
    assert metric is not None
    store = _store(committer)
    assert store.get_associated({primary}, "metric") == {}
    assert store.provenance.get_direct_ancestors(metric) == []
    assert store.provenance.get_descendant_ids(primary) == [metric]


def test_fresh_store_hydrates_managed_files_after_original_disappears(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    case = retained_case(tmp_path / "source", kind="file")
    _accept(committer, case, "a")
    (tmp_path / "source" / "structure.bin").unlink()
    store = _store(committer)
    files = [
        item for item in case.artifacts.values() if item.artifact_type == "file_ref"
    ]
    loaded = store.get_artifacts_by_type(
        [_artifact_id(item) for item in files], "file_ref"
    )
    assert len(loaded) == 2
    for item in loaded.values():
        item.verify_external_content(fs=committer.fs)


def test_read_scope_rejects_conflicting_store_binding(
    committer: RetainedCommitter,
) -> None:
    scope = RetainedStoreReadScope(committer)
    with pytest.raises(StoreIntegrityError, match="another store binding"):
        ArtifactStore(
            committer.delta_root + "-other", fs=committer.fs, _read_scope=scope
        )
    with pytest.raises(ArtifactIntegrityError, match="another managed files root"):
        ArtifactStore(committer.delta_root, fs=committer.fs, _read_scope=scope)
    with pytest.raises(ValueError, match="fixed and fresh"):
        ArtifactStore(
            committer.delta_root,
            fs=committer.fs,
            _read_scope=scope,
            _view=CuratorView(committer, capture_curator_view(committer)),
        )


@pytest.mark.parametrize("changed", ["timestamp", "removed"])
def test_fresh_provenance_names_require_verified_current_terminal(
    committer: RetainedCommitter, tmp_path: Path, changed: str
) -> None:
    results, admitted, receipts = prepare_scientific(
        committer, retained_case(tmp_path, kind="alias")
    )
    committer.accept(receipts[0])
    results.finish(admitted.step_run_id)
    store = _store(committer)
    assert store.provenance.load_step_name_map()
    path = uri_join(committer.delta_root, TablePath.STEPS.value)
    rows = pl.read_delta(path)
    if changed == "removed":
        rows = rows.filter(pl.col("status") != "succeeded")
    else:
        rows = rows.with_columns(
            pl.when(pl.col("status") == "succeeded")
            .then(pl.col("timestamp") + pl.duration(days=1))
            .otherwise(pl.col("timestamp"))
            .alias("timestamp")
        )
    rows.write_delta(path, mode="overwrite")
    with pytest.raises(StoreIntegrityError):
        store.provenance.load_step_name_map()
