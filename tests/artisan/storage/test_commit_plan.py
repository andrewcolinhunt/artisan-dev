"""Tests for immutable logical commit plans."""

from __future__ import annotations

import json

import polars as pl
import pytest
from fixtures.store_format import publish_test_store
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import ARTIFACT_INDEX_SCHEMA
from artisan.storage.io.commit_plan import build_commit_plan, read_commit_plan
from artisan.storage.io.staging import StagingManager


def _build_input_plan(tmp_path):
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
    staged_path = staging.stage_orchestrator_dataframe(
        staged,
        TablePath.ARTIFACT_INDEX.value,
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
    return fs, delta_root, staging_root, staged_path, plan


def test_commit_plan_is_stable_and_root_relative(tmp_path):
    fs, delta_root, staging_root, _staged_path, first = _build_input_plan(tmp_path)
    second = build_commit_plan(
        delta_root=delta_root,
        staging_root=staging_root,
        fs=fs,
        commit_kind="input_registration",
        step_run_id="b" * 32,
        step_number=0,
        operation_name="ingest",
    )

    assert first == second
    assert first.logical_commit_id == f"input_registration:{'b' * 32}"
    assert all(
        not planned.relative_path.startswith(staging_root)
        for table in first.tables
        for planned in table.files
    )


def test_commit_plan_rejects_tampered_published_digest(tmp_path):
    fs, delta_root, _staging_root, _staged_path, plan = _build_input_plan(tmp_path)
    plan_path = (
        f"{delta_root}/_artisan/commit_plans/{plan.step_run_id}/input_registration.json"
    )
    with fs.open(plan_path) as stream:
        raw = json.load(stream)
    raw["step_number"] = 4
    with fs.open(plan_path, "w") as stream:
        json.dump(raw, stream)

    with pytest.raises(StoreIntegrityError, match="Unreadable commit plan"):
        read_commit_plan(delta_root, fs, plan.step_run_id, "input_registration")


def test_commit_plan_rejects_unknown_staged_object(tmp_path):
    fs, delta_root, staging_root, staged_path, _plan = _build_input_plan(tmp_path)
    assert staged_path is not None
    fs.touch(f"{staged_path.rsplit('/', 1)[0]}/debug.txt")

    with pytest.raises(StoreIntegrityError, match="Unexpected staged object"):
        build_commit_plan(
            delta_root=delta_root,
            staging_root=staging_root,
            fs=fs,
            commit_kind="input_registration",
            step_run_id="b" * 32,
            step_number=0,
            operation_name="ingest",
        )
