"""A finite curator read shares evidence without retaining it across calls."""

from __future__ import annotations

from collections import Counter

import polars as pl
import pytest
from fixtures.curator_cache import STAMPS, DynamicCurator, stage_observed
from fixtures.retained_acceptance import open_committer
from fixtures.retained_curators import admit_curator
from fixtures.retained_readers import Backend

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.storage.cache.curator_lookup import lookup_curator
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.core.curator_view import CuratorView, capture_curator_view
from artisan.storage.io import retained_read
from artisan.storage.io.curator_control import CandidatePlan
from artisan.storage.io.curator_plan import CuratorPlan, read_curator_plan
from artisan.storage.io.delta_files import DeltaSnapshot
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_read import RetainedControlProof
from artisan.utils.path import uri_join


def _completed(
    backend: Backend, *, reused: bool = False
) -> tuple[RetainedCommitter, CuratorPlan]:
    fs, storage, root = backend
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    operation = DynamicCurator()
    coordinator, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="a" * 32, **STAMPS
    )
    staged, terminal, _ = stage_observed(
        coordinator, admission, operation, storage=storage
    )
    assert staged.success
    assert staged.execution_run_id is not None
    coordinator.finish(reference, staged.execution_run_id, terminal)
    if reused:
        _, _, reference = admit_curator(
            committer, operation, {}, step_run_id="b" * 32, **STAMPS
        )
        hit = lookup_curator(reference, reader=CuratorReader(committer))
        assert hit is not None
        coordinator.reuse(hit)
    plan = read_curator_plan(committer.delta_root, reference.step_run_id, fs)
    assert plan is not None
    return committer, plan


@pytest.mark.parametrize("reused", [False, True])
def test_curator_read_shares_one_control_proof_and_fixed_snapshots(
    backend_fs: Backend, monkeypatch: pytest.MonkeyPatch, reused: bool
) -> None:
    committer, plan = _completed(backend_fs, reused=reused)
    reader = CuratorReader(committer)
    expected = reader.read_result(plan.step_run_id, plan.plan_digest)
    proofs = 0
    opened: Counter[str] = Counter()
    control_proof = committer._control_proof
    snapshot = committer._snapshot
    effects: Counter[tuple[str, str, int]] = Counter()
    verify_effect = retained_read.verify_snapshot_effect

    def count_proof() -> RetainedControlProof:
        nonlocal proofs
        proofs += 1
        return control_proof()

    def count_snapshot(table_path: str) -> DeltaSnapshot:
        opened[table_path] += 1
        return snapshot(table_path)

    def count_effect(
        selected: CandidatePlan,
        table_path: str,
        captured: DeltaSnapshot,
        controls: pl.DataFrame,
    ) -> None:
        effects[selected.logical_commit_id, table_path, captured.delta.version()] += 1
        verify_effect(selected, table_path, captured, controls)

    monkeypatch.setattr(committer, "_control_proof", count_proof)
    monkeypatch.setattr(committer, "_snapshot", count_snapshot)
    monkeypatch.setattr(retained_read, "verify_snapshot_effect", count_effect)
    assert reader.read_result(plan.step_run_id, plan.plan_digest) == expected
    assert proofs == 1
    assert opened
    assert set(opened.values()) == {1}
    assert effects
    assert set(effects.values()) == {1}
    assert len({owner for owner, _, _ in effects}) == (2 if reused else 1)
    opened.clear()
    effects.clear()
    assert reader.read_result(plan.step_run_id, plan.plan_digest) == expected
    assert proofs == 2
    assert opened
    assert set(opened.values()) == {1}
    assert set(effects.values()) == {1}


def test_curator_read_keeps_captured_content_version_then_rechecks_latest(
    backend_fs: Backend, monkeypatch: pytest.MonkeyPatch
) -> None:
    committer, plan = _completed(backend_fs)
    reader = CuratorReader(committer)
    expected = reader.read_result(plan.step_run_id, plan.plan_digest)
    snapshot = committer._snapshot
    changed = False

    def change_after_capture(table_path: str) -> DeltaSnapshot:
        nonlocal changed
        captured = snapshot(table_path)
        if table_path == ArtifactTypeDef.get_table_path("csv") and not changed:
            changed = True
            rows = pl.read_delta(
                captured.path, storage_options=committer.storage_options
            )
            rows.with_columns(pl.col("origin_step_number") + 1).write_delta(
                captured.path,
                mode="overwrite",
                storage_options=committer.storage_options,
            )
        return captured

    monkeypatch.setattr(committer, "_snapshot", change_after_capture)
    assert reader.read_result(plan.step_run_id, plan.plan_digest) == expected
    assert changed
    with pytest.raises(StoreIntegrityError, match="origin"):
        reader.read_result(plan.step_run_id, plan.plan_digest)


@pytest.mark.parametrize("change", ["content", "origin", "duplicate"])
def test_curator_metadata_does_not_authorize_changed_full_content(
    backend_fs: Backend, change: str
) -> None:
    committer, plan = _completed(backend_fs)
    reader = CuratorReader(committer)
    assert reader.metadata(plan.step_run_id, plan.plan_digest) == plan
    path = uri_join(committer.delta_root, ArtifactTypeDef.get_table_path("csv"))
    rows = pl.read_delta(path, storage_options=committer.storage_options)
    if change == "origin":
        changed = rows.with_columns(pl.col("origin_step_number") + 1)
    elif change == "duplicate":
        changed = pl.concat([rows, rows])
    else:
        changed = rows.with_columns(pl.lit(b"value\n999\n").alias("content"))
    changed.write_delta(
        path, mode="overwrite", storage_options=committer.storage_options
    )
    assert reader.metadata(plan.step_run_id, plan.plan_digest) == plan
    with pytest.raises(StoreIntegrityError):
        reader.read_result(plan.step_run_id, plan.plan_digest)


def test_historical_effect_failure_is_not_reused_or_hidden_by_latest_repair(
    backend_fs: Backend, monkeypatch: pytest.MonkeyPatch
) -> None:
    committer, plan = _completed(backend_fs)
    path = uri_join(committer.delta_root, ArtifactTypeDef.get_table_path("csv"))
    original = pl.read_delta(path, storage_options=committer.storage_options)
    original.with_columns(pl.col("origin_step_number") + 1).write_delta(
        path, mode="overwrite", storage_options=committer.storage_options
    )
    corrupted = committer._snapshot(
        ArtifactTypeDef.get_table_path("csv")
    ).delta.version()
    original.write_delta(
        path, mode="overwrite", storage_options=committer.storage_options
    )
    verify = retained_read.verify_snapshot_effect
    checked: Counter[int] = Counter()

    def count_effect(
        selected: CandidatePlan,
        table_path: str,
        snapshot: DeltaSnapshot,
        controls: pl.DataFrame,
    ) -> None:
        checked[snapshot.delta.version()] += 1
        verify(selected, table_path, snapshot, controls)

    monkeypatch.setattr(retained_read, "verify_snapshot_effect", count_effect)
    with committer._read_snapshot() as read:
        current = read.verify_effect(plan, ArtifactTypeDef.get_table_path("csv"))
        assert (
            read.verify_effect(plan, ArtifactTypeDef.get_table_path("csv")) is current
        )
        assert (
            read.snapshot(
                ArtifactTypeDef.get_table_path("csv"), version=current.delta.version()
            )
            is current
        )
        for _ in range(2):
            with pytest.raises(StoreIntegrityError, match="origin"):
                read.verify_effect(
                    plan, ArtifactTypeDef.get_table_path("csv"), version=corrupted
                )
        assert checked[current.delta.version()] == 1
        assert checked[corrupted] == 2
        assert (
            read.snapshot(ArtifactTypeDef.get_table_path("csv")).delta.version()
            != corrupted
        )


def test_curator_view_private_proof_requires_the_same_store_binding(
    backend_fs: Backend,
) -> None:
    committer, _ = _completed(backend_fs)
    fs, storage, _ = backend_fs
    other = open_committer(committer.delta_root, fs, storage)
    with committer._read_snapshot() as read:
        captured = capture_curator_view(committer, _read=read)
        assert (
            not CuratorView(committer, captured, _read=read)
            .scan(TablePath.ARTIFACT_INDEX)
            .collect()
            .is_empty()
        )
        with pytest.raises(StoreIntegrityError, match="another store binding"):
            capture_curator_view(other, _read=read)
        with pytest.raises(StoreIntegrityError, match="another store binding"):
            CuratorView(other, captured, _read=read)
