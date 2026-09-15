"""Tests for completion-authorized Delta scans."""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import pytest
from fixtures.store_format import publish_test_store
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.schemas.enums import TablePath
from artisan.storage.core.committed_scan import read_committed
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
    with pytest.raises(StoreIntegrityError, match="owns unplanned"):
        read_committed(delta_root, TablePath.ARTIFACT_INDEX, fs=fs)

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
        read_committed(delta_root, TablePath.ARTIFACT_LOCATIONS, fs=fs)
