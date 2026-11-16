"""Batch reads share fixed evidence only until the public read returns."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import polars as pl
import pytest
from fixtures.retained_acceptance import admit_case, open_committer
from fixtures.retained_readers import Backend, admit_read_case
from fixtures.retained_results import retained_case

from artisan.errors import StoreIntegrityError
from artisan.schemas.execution.assignment import AssignmentIdentity
from artisan.storage.core.assignment_schema import ASSIGNMENT_DECISIONS_SCHEMA
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.delta_files import DeltaSnapshot, append_table_files
from artisan.storage.io.retained_read import RetainedControlProof
from artisan.utils.path import uri_join


def test_batch_reads_open_each_effect_table_once_and_keep_duplicate_order(
    raw_backend_fs: Backend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fs, storage, root = raw_backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    case = retained_case(tmp_path, kind="alias")
    receipts = tuple(
        admit_read_case(committer, case, step_run_id=owner * 32)[1]
        for owner in ("a", "b")
    )
    for receipt in receipts:
        committer.accept(receipt)
    reader = RetainedReader(committer)
    expected = tuple(reader.read(receipt) for receipt in receipts)
    opened: Counter[str] = Counter()
    snapshot = committer._snapshot
    control_proof = committer._control_proof
    proofs = 0

    def count_proof() -> RetainedControlProof:
        nonlocal proofs
        proofs += 1
        return control_proof()

    def count_snapshot(table_path: str) -> DeltaSnapshot:
        opened[table_path] += 1
        return snapshot(table_path)

    monkeypatch.setattr(committer, "_snapshot", count_snapshot)
    monkeypatch.setattr(committer, "_control_proof", count_proof)
    selected = reader.read_many((*receipts, receipts[0]))
    assert selected == (*expected, expected[0])
    assert opened
    assert set(opened.values()) == {1}
    assert proofs == 1
    opened.clear()
    assert reader.read_many(receipts) == expected
    assert opened
    assert set(opened.values()) == {1}
    assert proofs == 2


def test_one_read_uses_the_verified_table_version_and_next_read_checks_latest(
    raw_backend_fs: Backend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fs, storage, root = raw_backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    _, receipt = admit_case(committer, tmp_path, kind="alias")
    plan = committer.accept(receipt)
    reader = RetainedReader(committer)
    expected = reader.read(receipt)
    table = next(
        item for item in plan.tables if item.table_path.startswith("artifacts/")
    )
    snapshot = committer._snapshot
    changed = False

    def change_after_snapshot(table_path: str) -> DeltaSnapshot:
        nonlocal changed
        captured = snapshot(table_path)
        if table_path == table.table_path and not changed:
            changed = True
            frame = pl.read_delta(
                captured.path, storage_options=committer.storage_options
            )
            frame.with_columns(pl.col("origin_step_number") + 1).write_delta(
                captured.path,
                mode="overwrite",
                storage_options=committer.storage_options,
            )
        return captured

    monkeypatch.setattr(committer, "_snapshot", change_after_snapshot)
    assert reader.read(receipt) == expected
    assert changed
    with pytest.raises(StoreIntegrityError, match="origin"):
        reader.read(receipt)


@pytest.mark.parametrize("changed", ["row", "carrier", "winner"])
def test_fresh_batch_read_rejects_changes_after_successful_read(
    raw_backend_fs: Backend, tmp_path: Path, changed: str
) -> None:
    fs, storage, root = raw_backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    _, receipt = admit_case(committer, tmp_path, kind="file")
    plan = committer.accept(receipt)
    reader = RetainedReader(committer)
    assert reader.read_many((receipt,))[0].support
    if changed == "carrier":
        path = uri_join(committer.files_root, plan.carriers[0].relative_path)
        with fs.open(path, "wb") as stream:
            stream.write(b"changed whole carrier")
    elif changed == "row":
        table = next(
            item for item in plan.tables if item.table_path.startswith("artifacts/")
        )
        path = uri_join(committer.delta_root, table.table_path)
        frame = pl.read_delta(path, storage_options=committer.storage_options)
        frame.with_columns(pl.col("origin_step_number") + 1).write_delta(
            path, mode="overwrite", storage_options=committer.storage_options
        )
    else:
        journal = committer.journal
        other = receipt.delivery.attempt.model_copy(
            update={
                "assignment": AssignmentIdentity(step_run_id="f" * 32, unit_index=0)
            }
        )
        journal.admit(other, admission_digest=plan.admission.record_digest)
        snapshot = journal._snapshot()
        rows = pl.concat(tuple(snapshot.iter_frames()))
        row = rows.filter(pl.col("action") == "claim").row(0, named=True)
        row.update(
            assignment_id=other.assignment.assignment_id,
            step_run_id=other.assignment.step_run_id,
        )
        append_table_files(
            snapshot,
            [pl.DataFrame([row], schema=ASSIGNMENT_DECISIONS_SCHEMA)],
            step_number=0,
            statistics_columns=["assignment_id", "sequence"],
        )
    with pytest.raises(StoreIntegrityError):
        reader.read_many((receipt,))
