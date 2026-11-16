"""Both cache layers require identical admitted science and complete winners."""

from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.retained_acceptance import open_committer
from fixtures.retained_readers import Backend, admit_read_case
from fixtures.retained_results import RetainedCase, _derive, retained_case

from artisan.errors import StoreIntegrityError
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.schemas.artifact.csv import CSVArtifact
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.cache.retained_lookup import lookup_assignment, lookup_step
from artisan.storage.core.retained_reader import RetainedReader


def _case(value: int = 1, *, owner: str = "boundary") -> RetainedCase:
    artifact = CSVArtifact.draft(f"x\n{value}\n".encode(), "input.csv", 1).finalize()
    graph = OccurrenceGraph(boundary_invocation=owner)
    source = graph.add_input("data", artifact)
    first = _derive(graph, "first", artifact, [source])
    second = _derive(graph, "second", artifact, [first])
    return RetainedCase(
        graph.project({"data": [second, first, second]}, {"data": OutputSpec()}),
        {source: artifact},
        {first: artifact, second: artifact},
    )


def test_both_retained_caches_preserve_current_order_without_claiming_reuse(
    backend_fs: Backend, tmp_path: Path
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    reader = RetainedReader(committer)
    _, first = admit_read_case(committer, _case(1), step_run_id="a" * 32)
    # A pending sibling does not revoke the first unit's accepted output.
    admit_read_case(committer, _case(9), step_run_id="a" * 32, unit_index=1)
    _, second = admit_read_case(committer, _case(2), step_run_id="b" * 32)
    committer.accept(first)
    committer.accept(second)
    committer.journal.cancel(first.delivery.attempt)
    current = [
        admit_read_case(
            committer,
            _case(value, owner="fresh"),
            step_run_id="c" * 32,
            unit_index=index,
        )[0]
        for index, value in enumerate((2, 1))
    ]
    hits = lookup_step(current, reader=reader)
    assert hits is not None
    assert tuple(hit.receipt for hit in hits) == (second, first)
    assert lookup_assignment(current[0], reader=reader) == hits[0]
    outputs = reader.step_outputs([hit.selection for hit in hits])
    assert len(outputs) == 1
    assert len(outputs[0].occurrences) == 6
    for index, source in enumerate((second, first)):
        expected = reader.read(source).output("data")
        assert outputs[0].occurrences[index * 3 : (index + 1) * 3] == expected
        assert expected[0].reference == expected[2].reference
        assert expected[0].reference != expected[1].reference
    assert all(
        committer.journal.state(item.attempt.assignment).winner is None
        for item in current
    )
    with pytest.raises(ValueError, match="complete order"):
        lookup_step(list(reversed(current)), reader=reader)
    assert lookup_step([], reader=reader) is None

    missing = admit_read_case(committer, _case(3), step_run_id="c" * 32, unit_index=2)[
        0
    ]
    assert lookup_step([*current, missing], reader=reader) is None
    assert lookup_assignment(current[0], reader=reader) is not None


@pytest.mark.parametrize(
    "change", ["parameters", "software", "noncacheable", "diagnostic", "cancelled"]
)
def test_both_retained_caches_reject_changed_or_ineligible_current_work(
    backend_fs: Backend, change: str
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    reader = RetainedReader(committer)
    case = _case()
    _, source = admit_read_case(committer, case, step_run_id="a" * 32)
    committer.accept(source)
    recipe = case.recipe
    if change == "parameters":
        recipe = recipe.model_copy(update={"params_json": '{"scale":2}'})
    if change == "noncacheable":
        recipe = recipe.model_copy(update={"cacheable": False})
    current, _ = admit_read_case(
        committer,
        case,
        step_run_id="b" * 32,
        recipe=recipe,
        software_digest=("d" if change == "software" else "c") * 64,
        replay_of="e" * 32 if change == "diagnostic" else None,
    )
    if change == "cancelled":
        committer.journal.cancel(current.attempt)
    assert lookup_assignment(current, reader=reader) is None
    assert lookup_step([current], reader=reader) is None


@pytest.mark.parametrize("source_kind", ["unclaimed", "diagnostic", "noncacheable"])
def test_both_retained_caches_exclude_non_success_sources(
    backend_fs: Backend, source_kind: str
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    reader = RetainedReader(committer)
    case = _case()
    _, source = admit_read_case(
        committer,
        case,
        step_run_id="a" * 32,
        recipe=case.recipe.model_copy(update={"cacheable": False})
        if source_kind == "noncacheable"
        else None,
        replay_of="e" * 32 if source_kind == "diagnostic" else None,
    )
    if source_kind != "unclaimed":
        committer.accept(source)
    if source_kind == "diagnostic":
        assert reader.read(source).receipt == source
        with reader.capture_inputs(source) as captured:
            assert captured.reference.attempt.replay_of_execution_run_id == "e" * 32
    current, _ = admit_read_case(committer, case, step_run_id="b" * 32)
    assert lookup_assignment(current, reader=reader) is None
    assert lookup_step([current], reader=reader) is None


def test_retained_cache_requires_current_journal_bound_snapshot(
    backend_fs: Backend,
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    current, _ = admit_read_case(committer, _case(), step_run_id="a" * 32)
    changed = current.model_copy(update={"record_digest": "0" * 32})
    with pytest.raises(StoreIntegrityError, match="journal-bound"):
        lookup_assignment(changed, reader=RetainedReader(committer))


def test_both_retained_caches_recheck_accepted_managed_carriers(
    backend_fs: Backend, tmp_path: Path
) -> None:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    reader = RetainedReader(committer)
    case = retained_case(tmp_path / "worker", kind="file")
    _, receipt = admit_read_case(committer, case, step_run_id="a" * 32)
    plan = committer.accept(receipt)
    current, _ = admit_read_case(committer, case, step_run_id="b" * 32)
    assert lookup_assignment(current, reader=reader) is not None
    with fs.open(
        f"{committer.files_root}/{plan.carriers[0].relative_path}", "wb"
    ) as stream:
        stream.write(b"changed")
    with pytest.raises(StoreIntegrityError, match="carrier"):
        lookup_assignment(current, reader=reader)
    with pytest.raises(StoreIntegrityError, match="carrier"):
        lookup_step([current], reader=reader)
