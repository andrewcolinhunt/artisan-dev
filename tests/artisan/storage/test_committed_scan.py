"""Tests for completion-authorized Delta scans."""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import pytest
from fixtures.store_format import publish_test_store
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.schemas.enums import TablePath
from artisan.storage.core.committed_scan import (
    audit_table_owners,
    filter_committed_rows,
    read_committed,
    scan_committed,
    verify_plan_effect,
)
from artisan.storage.core.table_schemas import (
    ARTIFACT_INDEX_SCHEMA,
    LOGICAL_COMMITS_SCHEMA,
)
from artisan.storage.io.commit_plan import build_commit_plan
from artisan.storage.io.staging import StagingManager


def test_committed_scan_hides_planned_then_exposes_complete(tmp_path):
    fs = LocalFileSystem()
    delta_root = str(tmp_path / "delta")
    staging_root = str(tmp_path / "staging")
    publish_test_store(delta_root, fs)
    staging = StagingManager(staging_root, fs)
    staged = pl.DataFrame(
        {
            "artifact_id": ["a" * 32],
            "artifact_type": ["metric"],
            "origin_step_number": [0],
            "metadata": ["{}"],
        },
        schema=ARTIFACT_INDEX_SCHEMA,
    )
    staging.stage_orchestrator_dataframe(
        staged,
        TablePath.ARTIFACT_INDEX.value,
        commit_kind="input_registration",
        step_run_id="b" * 32,
        step_number=0,
        operation_name="ingest",
    )
    plan = build_commit_plan(
        delta_root=delta_root,
        staging_root=staging_root,
        fs=fs,
        commit_kind="input_registration",
        step_run_id="b" * 32,
        step_number=0,
        operation_name="ingest",
    )
    control_path = f"{delta_root}/{TablePath.LOGICAL_COMMITS.value}"
    control = pl.DataFrame(
        [
            {
                "logical_commit_id": plan.logical_commit_id,
                "commit_kind": plan.commit_kind,
                "step_run_id": plan.step_run_id,
                "state": "planned",
                "plan_digest": plan.plan_digest,
                "created_at": datetime.now(UTC),
                "completed_at": None,
                "abandon_reason": None,
            }
        ],
        schema=LOGICAL_COMMITS_SCHEMA,
    )
    control.write_delta(control_path, mode="append")
    staged.with_columns(
        pl.lit(plan.logical_commit_id).alias("logical_commit_id")
    ).write_delta(
        f"{delta_root}/{TablePath.ARTIFACT_INDEX.value}",
        mode="append",
    )

    assert read_committed(
        delta_root,
        TablePath.ARTIFACT_INDEX,
        fs=fs,
    ).is_empty()

    control.with_columns(
        pl.lit("complete").alias("state"),
        pl.lit(datetime.now(UTC)).alias("completed_at"),
    ).write_delta(control_path, mode="overwrite")
    visible = read_committed(
        delta_root,
        TablePath.ARTIFACT_INDEX,
        fs=fs,
    )
    assert visible["artifact_id"].to_list() == ["a" * 32]

    staged.with_columns(
        pl.lit("c" * 32).alias("artifact_id"),
        pl.lit(plan.logical_commit_id).alias("logical_commit_id"),
    ).write_delta(
        f"{delta_root}/{TablePath.ARTIFACT_INDEX.value}",
        mode="append",
    )
    assert read_committed(delta_root, TablePath.ARTIFACT_INDEX, fs=fs).height == 2
    controls = pl.read_delta(control_path)
    with pytest.raises(StoreIntegrityError, match="owns unplanned"):
        verify_plan_effect(
            plan,
            TablePath.ARTIFACT_INDEX.value,
            pl.read_delta(f"{delta_root}/{TablePath.ARTIFACT_INDEX.value}"),
            controls,
        )

    pl.DataFrame(
        {
            "artifact_id": ["a" * 32],
            "uri": ["file:///undeclared.json"],
            "logical_commit_id": [plan.logical_commit_id],
        }
    ).write_delta(
        f"{delta_root}/{TablePath.ARTIFACT_LOCATIONS.value}",
        mode="append",
    )
    with pytest.raises(StoreIntegrityError, match="has unplanned"):
        verify_plan_effect(
            plan,
            TablePath.ARTIFACT_LOCATIONS.value,
            pl.read_delta(f"{delta_root}/{TablePath.ARTIFACT_LOCATIONS.value}"),
            controls,
        )


def test_scan_defers_data_collection_and_never_reads_plans(tmp_path, monkeypatch):
    fs = LocalFileSystem()
    root = str(tmp_path / "delta")
    publish_test_store(root, fs)
    pl.DataFrame(
        {
            "artifact_id": ["a" * 32],
            "artifact_type": ["metric"],
            "origin_step_number": [0],
            "metadata": ["{}"],
            "logical_commit_id": ["input_registration:" + "b" * 32],
        },
        schema={**ARTIFACT_INDEX_SCHEMA, "logical_commit_id": pl.String},
    ).write_delta(f"{root}/{TablePath.ARTIFACT_INDEX.value}", mode="append")
    original = pl.LazyFrame.collect
    calls = []

    def collect(frame, *args, **kwargs):
        calls.append(frame.explain())
        return original(frame, *args, **kwargs)

    monkeypatch.setattr(pl.LazyFrame, "collect", collect)
    monkeypatch.setattr(
        "artisan.storage.io.commit_plan.read_commit_plan",
        lambda *_args, **_kwargs: pytest.fail("ordinary scan loaded a historical plan"),
    )
    scan = scan_committed(root, TablePath.ARTIFACT_INDEX, fs=fs)
    # Only small control metadata is collected; requested data remains lazy.
    assert all("artifacts/index" not in description for description in calls)
    assert isinstance(scan, pl.LazyFrame)
    assert "Parquet SCAN" in scan.explain()
    assert scan.filter(pl.col("artifact_id") == "absent").collect().is_empty()
    assert "artifacts/index" in calls[-1]


@pytest.mark.parametrize("owner", [None, "input_registration:unknown"])
def test_scan_hides_invalid_owners_while_explicit_audit_rejects(owner):
    physical = pl.DataFrame(
        {"artifact_id": ["a" * 32], "logical_commit_id": [owner]},
        schema={"artifact_id": pl.String, "logical_commit_id": pl.String},
    )
    controls = pl.DataFrame(schema=LOGICAL_COMMITS_SCHEMA)
    assert filter_committed_rows(
        physical, TablePath.ARTIFACT_INDEX.value, controls
    ).is_empty()
    with pytest.raises(StoreIntegrityError, match="unowned|unknown owners"):
        audit_table_owners(physical, TablePath.ARTIFACT_INDEX.value, controls)


def test_step_visibility_requires_valid_terminal_owner():
    complete_id = "step_result:" + "a" * 32
    controls = pl.DataFrame({"logical_commit_id": [complete_id], "state": ["complete"]})
    physical = pl.DataFrame(
        {
            "step_run_id": ["a" * 32] * 6,
            "status": [
                "running",
                "succeeded",
                "partial",
                "succeeded",
                "cancelled",
                "succeeded",
            ],
            "logical_commit_id": [
                None,
                None,
                None,
                "step_result:unknown",
                None,
                complete_id,
            ],
        }
    )
    assert filter_committed_rows(physical, TablePath.STEPS.value, controls)[
        "status"
    ].to_list() == ["running", "cancelled", "succeeded"]
    with pytest.raises(StoreIntegrityError):
        audit_table_owners(physical, TablePath.STEPS.value, controls)
