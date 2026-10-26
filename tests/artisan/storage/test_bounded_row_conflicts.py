"""Commit integrity when matching physical rows arrive separately."""

from __future__ import annotations

import polars as pl
import pytest
from fsspec import AbstractFileSystem

from artisan.errors import CommitError, StoreIntegrityError
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.storage.core.committed_scan import read_committed
from artisan.storage.core.table_schemas import (
    ARTIFACT_INDEX_SCHEMA,
    EXECUTION_EDGES_SCHEMA,
)
from artisan.storage.io.commit import DeltaCommitter
from artisan.storage.io.commit_plan import CommitPlan, build_commit_plan
from artisan.storage.io.staging import StagingManager


@pytest.fixture
def committer(
    backend_fs: tuple[AbstractFileSystem, StorageConfig, str],
) -> DeltaCommitter:
    fs, storage, root = backend_fs
    return DeltaCommitter(
        root,
        StagingManager(f"{root}/staging", fs),
        fs=fs,
        storage_options=storage.delta_storage_options(),
    )


def _index_rows() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "artifact_id": ["a" * 32, "b" * 32],
            "artifact_type": ["metric", "metric"],
            "origin_step_number": [0, 0],
            "metadata": ["{}", "{}"],
        },
        schema=ARTIFACT_INDEX_SCHEMA,
    )


def _stage_plan(
    committer: DeltaCommitter,
    table_path: str,
    rows: pl.DataFrame,
    step_run_id: str = "c" * 32,
) -> CommitPlan:
    metadata = {
        "commit_kind": "input_registration",
        "step_run_id": step_run_id,
        "step_number": 0,
        "operation_name": "seed",
    }
    committer.staging_manager.stage_orchestrator_dataframe(rows, table_path, **metadata)
    return build_commit_plan(
        delta_root=committer.delta_base_path,
        staging_root=committer.staging_manager.staging_dir,
        fs=committer._fs,
        **metadata,
    )


def _foreign_owner(
    committer: DeltaCommitter,
    table_path: str,
    rows: pl.DataFrame,
    state: str,
) -> CommitPlan:
    plan = _stage_plan(
        committer, table_path, rows, ("d" if state == "pending" else "e") * 32
    )
    committer._insert_planned(plan)
    return plan


@pytest.mark.parametrize(
    ("owners", "conflicting", "error"),
    [
        (("pending",), False, None),
        (("pending", "pending"), False, None),
        (("current", "pending"), False, None),
        (("pending", "current"), False, None),
        (("complete", "pending"), False, None),
        (("current", "current"), False, "Duplicate natural key"),
        (("complete", "complete"), False, "Duplicate natural key"),
        (("current", "complete"), False, "Duplicate natural key"),
        (("pending",), True, "conflicting values"),
        (("current", "pending"), True, "conflicting values"),
        (("complete", "pending"), True, "conflicting values"),
        (("unowned",), False, "unowned rows"),
    ],
)
def test_commit_checks_all_global_copies(
    committer: DeltaCommitter,
    owners: tuple[str, ...],
    conflicting: bool,
    error: str | None,
) -> None:
    table_path = TablePath.ARTIFACT_INDEX.value
    expected = _index_rows()
    plan = _stage_plan(committer, table_path, expected)
    foreign = {
        state: _foreign_owner(committer, table_path, expected.head(1), state)
        for state in set(owners) & {"pending", "complete"}
    }
    owner_ids = {
        "current": plan.logical_commit_id,
        "unowned": None,
        **{state: item.logical_commit_id for state, item in foreign.items()},
    }
    copies = [
        expected.head(1).with_columns(
            pl.lit(owner_ids[owner], dtype=pl.String).alias("logical_commit_id")
        )
        for owner in owners
    ]
    if conflicting:
        copies[-1] = copies[-1].with_columns(
            pl.lit('{"changed":true}').alias("metadata")
        )
    committer._append(pl.concat(copies), table_path)
    if "complete" in foreign:
        committer._complete(foreign["complete"])

    if error is not None:
        with pytest.raises(CommitError) as caught:
            committer.commit_logical(plan)
        assert caught.value.table == table_path
        assert isinstance(caught.value.__cause__, StoreIntegrityError)
        assert error in str(caught.value.__cause__)
        return

    committer.commit_logical(plan)
    visible = read_committed(
        committer.delta_base_path,
        table_path,
        fs=committer._fs,
        storage_options=committer._storage_options,
    )
    assert visible.sort("artifact_id").equals(expected)


@pytest.mark.parametrize("state", ["pending", "complete"])
def test_commit_rejects_foreign_non_global_owner(
    committer: DeltaCommitter, state: str
) -> None:
    table_path = TablePath.EXECUTION_EDGES.value
    expected = pl.DataFrame(
        {
            "execution_run_id": ["f" * 32],
            "direction": ["output"],
            "role": ["metric"],
            "artifact_id": ["a" * 32],
        },
        schema=EXECUTION_EDGES_SCHEMA,
    )
    plan = _stage_plan(committer, table_path, expected)
    foreign = _foreign_owner(committer, table_path, expected, state)
    committer._append(
        expected.with_columns(
            pl.lit(foreign.logical_commit_id).alias("logical_commit_id")
        ),
        table_path,
    )
    if state == "complete":
        committer._complete(foreign)

    with pytest.raises(CommitError) as caught:
        committer.commit_logical(plan)
    assert isinstance(caught.value.__cause__, StoreIntegrityError)
    assert "belongs to another commit" in str(caught.value.__cause__)


def test_completed_cleanup_rejects_identical_global_duplicate(
    committer: DeltaCommitter,
) -> None:
    table_path = TablePath.ARTIFACT_INDEX.value
    expected = _index_rows()
    plan = _stage_plan(committer, table_path, expected)
    committer.commit_logical(plan, preserve_staging=True)
    committer._append(
        expected.head(1).with_columns(
            pl.lit(plan.logical_commit_id).alias("logical_commit_id")
        ),
        table_path,
    )

    with pytest.raises(StoreIntegrityError, match="duplicate"):
        committer.commit_logical(plan)
