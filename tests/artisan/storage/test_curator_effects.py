"""Unpublished curator inner effects retain exact file, row and owner evidence."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest
from fsspec.implementations.local import LocalFileSystem
from pydantic import ValidationError

from artisan.errors import StoreIntegrityError
from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import ARTIFACT_INDEX_SCHEMA, STEPS_SCHEMA
from artisan.storage.io.curator_effects import (
    CapturedCuratorEffects,
    CuratorEffects,
    capture_curator_effects,
    curator_effects_digest,
    prepare_curator_effects,
)
from artisan.storage.io.plan_evidence import PlannedFile
from artisan.storage.io.staging import StagingManager
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest


def _stage(tmp_path: Path, *, duplicate: bool = False) -> StagingManager:
    staging = StagingManager(str(tmp_path / "staging"), LocalFileSystem())
    terminal = pl.DataFrame(
        [{"step_run_id": "b" * 32, "step_number": 0, "status": "failed"}],
        schema=STEPS_SCHEMA,
    )
    index = pl.DataFrame(
        {
            "artifact_id": ["a" * 32],
            "artifact_type": ["metric"],
            "origin_step_number": [0],
            "metadata": ["{}"],
        },
        schema=ARTIFACT_INDEX_SCHEMA,
    )
    if duplicate:
        index = pl.concat([index, index])
    for table_path, frame in (
        (TablePath.ARTIFACT_INDEX.value, index),
        (TablePath.STEPS.value, terminal),
    ):
        staging.stage_orchestrator_dataframe(
            frame,
            table_path,
            commit_kind="step_result",
            step_run_id="b" * 32,
            step_number=0,
            operation_name="curator",
        )
    return staging


def _prepare(staging: StagingManager) -> CapturedCuratorEffects:
    return prepare_curator_effects(
        staging_root=staging.staging_dir,
        fs=staging._fs,
        step_run_id="b" * 32,
        step_number=0,
        operation_name="curator",
    )


def test_effects_keep_existing_wire_shape_digest_and_root_relative_files(
    tmp_path: Path,
) -> None:
    staging = _stage(tmp_path)
    with _prepare(staging) as captured:
        plan = captured.plan
        encoded = canonical_json_bytes(plan.model_dump(mode="json"))
        assert set(json.loads(encoded)) == {
            "logical_commit_id",
            "commit_kind",
            "step_run_id",
            "recovery_batch_id",
            "step_number",
            "operation_name",
            "tables",
            "plan_digest",
        }
        assert plan.plan_digest == compute_content_digest(
            canonical_json_bytes(plan.model_dump(mode="json", exclude={"plan_digest"}))
        )
        assert str(tmp_path).encode() not in encoded
        assert plan.commit_kind == "step_result"
        assert plan.recovery_batch_id is None
        assert [table.table_path for table in plan.tables] == [
            TablePath.ARTIFACT_INDEX.value,
            TablePath.STEPS.value,
        ]
        assert CuratorEffects.model_validate_json(encoded) == plan
        with capture_curator_effects(plan, staging.staging_dir, staging._fs) as reread:
            assert reread.plan == plan
            assert reread.tables[TablePath.ARTIFACT_INDEX.value].keys == (("a" * 32,),)
    assert not (tmp_path / "_artisan").exists()


@pytest.mark.parametrize(
    "changes",
    [
        {"commit_kind": "input_registration"},
        {"commit_kind": "execution_recovery", "recovery_batch_id": "c" * 32},
        {"recovery_batch_id": "c" * 32},
        {"logical_commit_id": "step_result:" + "c" * 32},
        {"tables": []},
    ],
)
def test_effects_reject_obsolete_authority_and_changed_owner(
    tmp_path: Path, changes: dict
) -> None:
    with _prepare(_stage(tmp_path)) as captured:
        payload = captured.plan.model_dump(mode="json", exclude={"plan_digest"})
    payload.update(changes)
    with pytest.raises(ValidationError):
        CuratorEffects.model_validate(
            {**payload, "plan_digest": curator_effects_digest(payload)}
        )


def test_private_capture_survives_source_mutation_then_reread_rejects(
    tmp_path: Path,
) -> None:
    staging = _stage(tmp_path)
    with _prepare(staging) as captured:
        plan = captured.plan
        scratch = Path(captured._scratch.name)
        table = plan.table(TablePath.ARTIFACT_INDEX.value)
        source = Path(staging.staging_dir) / table.files[0].relative_path
        source.write_bytes(b"replaced staged parquet")
        assert captured.tables[TablePath.ARTIFACT_INDEX.value].keys == (("a" * 32,),)
        with (
            pytest.raises(StoreIntegrityError),
            capture_curator_effects(plan, staging.staging_dir, staging._fs),
        ):
            pytest.fail("Changed staged bytes must not be accepted")
        assert scratch.exists()
    assert not scratch.exists()


def test_identical_global_copies_collapse_but_conflicts_reject(tmp_path: Path) -> None:
    staging = _stage(tmp_path, duplicate=True)
    with _prepare(staging) as captured:
        table = captured.plan.table(TablePath.ARTIFACT_INDEX.value)
        assert table.row_count == 1
        assert sum(item.row_count for item in table.files) == 2
        source = Path(staging.staging_dir) / table.files[0].relative_path
    rows = pl.read_parquet(source)
    pl.concat(
        [
            rows.head(1),
            rows.tail(1).with_columns(pl.lit('{"changed":true}').alias("metadata")),
        ]
    ).write_parquet(source)
    with (
        pytest.raises(StoreIntegrityError, match="Conflicting natural keys"),
        _prepare(staging),
    ):
        pytest.fail("Conflicting copies must reject")


def test_capture_rejects_unknown_object_and_wrong_terminal_owner(
    tmp_path: Path,
) -> None:
    staging = _stage(tmp_path)
    with _prepare(staging) as captured:
        table = captured.plan.table(TablePath.STEPS.value)
        source = Path(staging.staging_dir) / table.files[0].relative_path
    extra = source.parent / "unknown.parquet"
    extra.write_bytes(b"unqualified")
    with (
        pytest.raises(StoreIntegrityError, match="Unexpected staged object"),
        _prepare(staging),
    ):
        pytest.fail("Unknown file must reject")
    extra.unlink()
    pl.read_parquet(source).with_columns(
        pl.lit("c" * 32).alias("step_run_id")
    ).write_parquet(source)
    with (
        pytest.raises(StoreIntegrityError, match="step ownership mismatch"),
        _prepare(staging),
    ):
        pytest.fail("Another terminal owner must reject")


@pytest.mark.parametrize(
    "relative", ["../file.parquet", "/file.parquet", "folder/../file.parquet"]
)
def test_planned_file_rejects_unsafe_relative_paths(relative: str) -> None:
    with pytest.raises(ValidationError, match="relative staged path"):
        PlannedFile(
            relative_path=relative,
            size_bytes=1,
            digest="a" * 32,
            row_count=0,
            schema_signature=(),
        )
