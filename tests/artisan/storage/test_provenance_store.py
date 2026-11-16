"""Provenance queries walk real accepted chain, branch and diamond relations."""

from __future__ import annotations

from fixtures.retained_inspection import commit_inspection_attempt

from artisan.schemas import CSVArtifact, MetricArtifact
from artisan.storage.core.provenance_store import ProvenanceStore
from artisan.storage.io.retained_commit import RetainedCommitter


def _store(committer: RetainedCommitter) -> ProvenanceStore:
    return ProvenanceStore(
        committer.delta_root,
        fs=committer.fs,
        storage_options=committer.storage_options,
        files_root=committer.files_root,
    )


def test_diamond_walks_deduplicate_and_filter_actual_artifact_types(
    backend_committer: RetainedCommitter,
) -> None:
    root = backend_committer.delta_root
    a, b, c = [
        CSVArtifact.draft(f"x\n{i}\n".encode(), f"data-{i}.csv", i).finalize()
        for i in range(3)
    ]
    d = MetricArtifact.draft({"score": 4}, "score.json", 3).finalize()
    for number, item, parents in ((0, a, []), (1, b, [a]), (2, c, [a]), (3, d, [b, c])):
        commit_inspection_attempt(
            root,
            committer=backend_committer,
            number=number,
            inputs={"data": parents} if parents else {},
            outputs={"output": [item]},
        )
    store = _store(backend_committer)
    assert set(store.get_ancestor_ids(d.artifact_id)) == {
        a.artifact_id,
        b.artifact_id,
        c.artifact_id,
    }
    assert set(store.get_descendant_ids(a.artifact_id)) == {
        b.artifact_id,
        c.artifact_id,
        d.artifact_id,
    }
    assert set(store.get_ancestor_ids(d.artifact_id, ancestor_type="csv")) == {
        a.artifact_id,
        b.artifact_id,
        c.artifact_id,
    }
    assert store.get_ancestor_ids(d.artifact_id, ancestor_type="metric") == []
    assert store.get_descendant_ids(a.artifact_id, descendant_type="metric") == [
        d.artifact_id
    ]
    assert set(store.get_direct_ancestors(d.artifact_id)) == {
        b.artifact_id,
        c.artifact_id,
    }
    assert set(store.get_direct_descendants({a.artifact_id})[a.artifact_id]) == {
        b.artifact_id,
        c.artifact_id,
    }
    assert {key: set(value) for key, value in store.load_backward_map().items()} == {
        b.artifact_id: {a.artifact_id},
        c.artifact_id: {a.artifact_id},
        d.artifact_id: {b.artifact_id, c.artifact_id},
    }
    forward = store.load_forward_map()
    assert set(forward[a.artifact_id]) == {b.artifact_id, c.artifact_id}
    assert forward[b.artifact_id] == forward[c.artifact_id] == [d.artifact_id]
    assert store.get_ancestor_ids("f" * 32) == []
    assert store.get_descendant_ids("f" * 32) == []
    assert store.get_ancestor_ids(a.artifact_id) == []
    assert store.get_descendant_ids(d.artifact_id) == []


def test_index_maps_filter_ids_types_steps_and_use_latest_authoritative_names(
    backend_committer: RetainedCommitter,
) -> None:
    root = backend_committer.delta_root
    a = CSVArtifact.draft(b"x\n1\n", "data.csv", 1).finalize()
    b = MetricArtifact.draft({"score": 4}, "score.json", 2).finalize()
    commit_inspection_attempt(
        root,
        committer=backend_committer,
        number=1,
        name="import",
        outputs={"data": [a]},
    )
    commit_inspection_attempt(
        root,
        committer=backend_committer,
        number=2,
        name="measure",
        outputs={"metric": [b]},
    )
    store = _store(backend_committer)
    assert store.load_type_map() == {a.artifact_id: "csv", b.artifact_id: "metric"}
    assert store.load_type_map([a.artifact_id, "f" * 32]) == {a.artifact_id: "csv"}
    assert store.load_step_map() == {a.artifact_id: 1, b.artifact_id: 2}
    assert store.get_artifact_step_number(b.artifact_id) == 2
    assert store.get_artifact_step_number("f" * 32) is None
    assert store.get_step_range([a.artifact_id, b.artifact_id, "f" * 32]) == (1, 2)
    assert store.get_step_range([]) is None
    assert store.load_artifact_ids_by_type("csv", step_numbers=[1]) == {a.artifact_id}
    assert store.load_artifact_ids_by_type("csv", step_numbers=[2]) == set()
    assert (
        store.load_artifact_ids_by_type("metric", artifact_ids=[a.artifact_id]) == set()
    )
    assert store.load_step_name_map("source-run") == {1: "import", 2: "measure"}
    commit_inspection_attempt(
        root, committer=backend_committer, number=2, name="retry", status="pending"
    )
    assert store.load_step_name_map("source-run") == {1: "import", 2: "retry"}


def test_empty_provenance_queries_are_empty(
    backend_committer: RetainedCommitter,
) -> None:
    store = _store(backend_committer)
    assert store.load_backward_map() == store.load_forward_map() == {}
    assert store.load_type_map() == store.load_step_map() == {}
    assert store.get_direct_ancestors("f" * 32) == []
    assert store.get_direct_descendants({"f" * 32}) == {}
    assert store.load_step_name_map() == {}
    assert store.load_artifact_ids_by_type("metric") == set()
