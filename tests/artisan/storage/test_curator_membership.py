"""Curator selection uses frozen accepted membership rather than content origins."""

from __future__ import annotations

from functools import partial
from pathlib import Path

import polars as pl
import pytest
from fixtures.candidate_outputs import prepare_scientific
from fixtures.curator_cache import STAMPS, DynamicCurator, stage_observed
from fixtures.retained_curators import admit_curator, stage_curator
from fixtures.retained_results import retained_case

from artisan.errors import StoreIntegrityError
from artisan.operations.curator.merge import Merge
from artisan.orchestration.engine.retained_curator import CuratorResults
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.enums import CachePolicy, TablePath
from artisan.schemas.execution.curator_admission import CuratorAdmission
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.storage.cache.curator_dependencies import (
    curator_views_match,
    observe_curator_reads,
    validate_curator_cache_proof,
)
from artisan.storage.cache.curator_lookup import lookup_curator
from artisan.storage.cache.retained_reuse import verify_cache_source
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_membership import ACCEPTED_STEP_VIEW
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.core.curator_view import CuratorView, capture_curator_view
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_store import create_retained_store


@pytest.fixture
def committer(tmp_path: Path) -> RetainedCommitter:
    return create_retained_store(
        str(tmp_path / "store"),
        str(tmp_path / "files"),
        fs=StorageConfig().filesystem(),
    )


def _produce(
    committer: RetainedCommitter,
    owner: str,
    run: str,
    number: int,
) -> tuple[str, str, CuratorAdmission]:
    operation = DynamicCurator()
    results, admission, reference = admit_curator(
        committer,
        operation,
        {},
        step_run_id=owner * 32,
        pipeline_run_id=run,
        step_number=number,
        **STAMPS,
    )
    staged, terminal, _ = stage_observed(results, admission, operation)
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    results.finish(reference, staged.execution_run_id, terminal)
    return staged.artifact_ids[0], staged.execution_run_id, admission


def _view(
    committer: RetainedCommitter, owner: str, run: str
) -> tuple[CuratorView, CuratorAdmission]:
    _, admission, _ = admit_curator(
        committer,
        Merge(),
        {},
        step_run_id=owner * 32,
        pipeline_run_id=run,
        step_number=9,
        **STAMPS,
    )
    view = CuratorView(committer, admission.view)
    view.bind_admission(admission)
    return view, admission


def _store(view: CuratorView) -> ArtifactStore:
    committer = view.committer
    return ArtifactStore(
        committer.delta_root,
        fs=committer.fs,
        storage_options=committer.storage_options,
        files_root=committer.files_root,
        _view=view,
    )


def test_membership_uses_owning_run_and_frozen_accepted_steps(
    committer: RetainedCommitter,
) -> None:
    original, _, _ = _produce(committer, "a", "historical", 0)
    reused, _, _ = _produce(committer, "b", "current", 1)
    assert original == reused
    _produce(committer, "c", "unrelated", 4)
    view, _ = _view(committer, "d", "current")
    store = _store(view)
    assert store.provenance.load_step_map([reused]) == {reused: 0}
    expected = {1: {reused}}
    assert store.provenance.load_accepted_artifact_ids_by_step("csv") == expected
    assert store.provenance.load_step_name_map() == {1: "observed_dynamic"}
    _produce(committer, "e", "current", 2)
    _produce(committer, "f", "later", 1)
    assert store.provenance.load_accepted_artifact_ids_by_step("csv") == expected
    assert store.provenance.load_step_name_map() == {1: "observed_dynamic"}
    fresh, _ = _view(committer, "1", "current")
    assert _store(fresh).provenance.load_accepted_artifact_ids_by_step("csv") == {
        1: {reused},
        2: {reused},
    }


def test_membership_and_empty_reads_are_bound_cache_dependencies(
    committer: RetainedCommitter,
) -> None:
    original, _, _ = _produce(committer, "a", "current", 1)
    view, admission = _view(committer, "b", "current")
    with observe_curator_reads(view, admission) as reads:
        assert (
            _store(view).provenance.load_accepted_artifact_ids_by_step("metric") == {}
        )
        assert _store(view).provenance.load_step_name_map() == {1: "observed_dynamic"}
    assert [item.table_path for item in reads.proof.views] == [ACCEPTED_STEP_VIEW]
    validate_curator_cache_proof(
        reads.proof, CuratorView(committer, admission.view), admission=admission
    )
    _produce(committer, "c", "unrelated", 4)
    same = CuratorView(committer, capture_curator_view(committer))
    same._pipeline_run_id = "current"
    assert not curator_views_match(reads.proof, same)
    other = CuratorView(committer, admission.view)
    other._pipeline_run_id = "unrelated"
    assert not curator_views_match(reads.proof, other)
    assert _store(view).provenance.load_accepted_artifact_ids_by_step("csv") == {
        1: {original}
    }


def test_replay_membership_keeps_source_run_with_new_frozen_visibility(
    committer: RetainedCommitter,
) -> None:
    artifact_id, execution_id, source = _produce(committer, "a", "source", 1)
    _produce(committer, "b", "source", 2)
    _produce(committer, "c", "later", 5)
    replay = source.model_copy(
        update={
            "record": source.record.model_copy(
                update={
                    "step_run_id": "d" * 32,
                    "replay_of_execution_run_id": execution_id,
                }
            ),
            "view": capture_curator_view(committer),
        }
    )
    results = CuratorResults(
        RetainedStepTracker(committer, "diagnostic"),
        f"{committer.delta_root}/curator-staging",
    )
    results.admit(replay)
    view = CuratorView(committer, replay.view)
    with observe_curator_reads(view, replay) as reads:
        assert _store(view).provenance.load_accepted_artifact_ids_by_step("csv") == {
            1: {artifact_id},
            2: {artifact_id},
        }
        assert _store(view).provenance.load_step_name_map() == {
            1: "observed_dynamic",
            2: "observed_dynamic",
        }
    validate_curator_cache_proof(
        reads.proof, CuratorView(committer, replay.view), admission=replay
    )
    staged, terminal = stage_curator(results, replay, DynamicCurator())
    terminal = terminal.model_copy(
        update={
            "output_roles": frozenset({"found"}),
            "output_types": {"found": "csv"},
        }
    )
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    results.finish(results.admit(replay), staged.execution_run_id, terminal)
    nested = replay.model_copy(
        update={
            "record": replay.record.model_copy(
                update={
                    "step_run_id": "e" * 32,
                    "replay_of_execution_run_id": staged.execution_run_id,
                }
            ),
            "view": capture_curator_view(committer),
        }
    )
    nested_results = CuratorResults(
        RetainedStepTracker(committer, "nested-diagnostic"),
        f"{committer.delta_root}/curator-staging",
    )
    nested_results.admit(nested)
    nested_view = CuratorView(committer, nested.view)
    with observe_curator_reads(nested_view, nested):
        assert _store(nested_view).provenance.load_accepted_artifact_ids_by_step(
            "csv"
        ) == {
            1: {artifact_id},
            2: {artifact_id},
        }


@pytest.mark.parametrize("removed", ["terminal", "whole_attempt"])
def test_membership_rejects_missing_completed_lifecycle(
    committer: RetainedCommitter,
    removed: str,
) -> None:
    _produce(committer, "a", "source", 1)
    path = f"{committer.delta_root}/{TablePath.STEPS.value}"
    rows = pl.read_delta(path)
    rows.filter(
        pl.col("status") != "succeeded"
        if removed == "terminal"
        else pl.col("step_run_id") != "a" * 32
    ).write_delta(path, mode="overwrite")
    view = CuratorView(committer, capture_curator_view(committer))
    with pytest.raises(StoreIntegrityError):
        _store(view).provenance.load_accepted_artifact_ids_by_step("csv")


@pytest.mark.parametrize("reuse", ["unit", "whole_step"])
def test_frozen_membership_reads_scientific_reuse_exports(
    committer: RetainedCommitter,
    tmp_path: Path,
    reuse: str,
) -> None:
    case = retained_case(tmp_path, kind="alias")
    results, admission, receipts = prepare_scientific(committer, case)
    committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    current, admitted, _ = prepare_scientific(committer, case, step_run_id="b" * 32)
    if reuse == "unit":
        committer.journal.reuse(
            admitted.assignments[0],
            receipts[0],
            verify_source=partial(
                verify_cache_source, reader=RetainedReader(committer)
            ),
        )
        current.finish(admitted.step_run_id)
    else:
        hit = current.lookup(admitted.step_run_id, CachePolicy.STEP_COMPLETED)
        assert hit is not None
        current.reuse(hit)
    view, _ = _view(committer, "c", "synthetic-pipeline")
    artifact = next(iter(case.boundary.values()))
    assert _store(view).provenance.load_accepted_artifact_ids_by_step(
        artifact.artifact_type
    ) == {
        7: {artifact.artifact_id},
    }


def test_frozen_membership_reads_curator_reuse_without_hydration(
    committer: RetainedCommitter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_id, _, _ = _produce(committer, "a", "synthetic-pipeline", 1)
    results, _, reference = admit_curator(
        committer,
        DynamicCurator(),
        {},
        step_run_id="b" * 32,
        step_number=2,
        **STAMPS,
    )
    hit = lookup_curator(reference, reader=CuratorReader(committer))
    assert hit is not None
    results.reuse(hit)
    view, _ = _view(committer, "c", "synthetic-pipeline")
    monkeypatch.setattr(
        ArtifactStore,
        "get_artifacts_by_type",
        lambda *_: pytest.fail("Membership must not hydrate payloads"),
    )
    assert _store(view).provenance.load_accepted_artifact_ids_by_step("csv") == {
        1: {artifact_id},
        2: {artifact_id},
    }
