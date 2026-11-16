"""Capture a curator failure/cancellation terminal without a fake execution."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import polars as pl
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.curator_admission import CuratorAdmission
from artisan.storage.core.table_schemas import STEPS_SCHEMA
from artisan.storage.io.bounded_evidence import DiskTable
from artisan.storage.io.bounded_io import capture_parquet
from artisan.storage.io.curator_effects import (
    CapturedCuratorEffects,
    CuratorEffects,
    curator_effects_digest,
)
from artisan.storage.io.plan_evidence import PlannedFile, PlannedTable
from artisan.storage.io.publication import publish_immutable_file
from artisan.storage.io.staging import StagingManager
from artisan.utils.path import uri_join


def prepare_curator_terminal(
    admission: CuratorAdmission, terminal: pl.DataFrame, staging: StagingManager
) -> CapturedCuratorEffects:
    """Retain the existing exact-effect contract for one controller-only outcome."""
    if terminal.height != 1 or dict(terminal.schema) != STEPS_SCHEMA:
        msg = "Curator terminal requires one registered lifecycle snapshot"
        raise StoreIntegrityError(msg)
    row = terminal.row(0, named=True)
    if row["status"] not in {"failed", "cancelled"} or (
        row["step_run_id"],
        row["step_number"],
    ) != (admission.record.step_run_id, admission.record.step_number):
        msg = "Controller-only curator terminal has another owner or usable status"
        raise StoreIntegrityError(msg)
    scratch = TemporaryDirectory(prefix="artisan-curator-terminal-")
    try:
        source = Path(scratch.name) / "source.parquet"
        terminal.write_parquet(source)
        captured = capture_parquet(
            str(source), LocalFileSystem(), str(Path(scratch.name) / "terminal.parquet")
        )
        relative = f"_curator_terminal/{admission.record.step_run_id}/{captured.digest}.parquet"
        publish_immutable_file(
            staging._fs,
            uri_join(staging.staging_dir, relative),
            captured.path,
            source_fs=LocalFileSystem(),
        )
        table = DiskTable(TablePath.STEPS.value, [captured.path], scratch.name)
        owner = f"step_result:{admission.record.step_run_id}"
        evidence = PlannedTable(
            table_path=TablePath.STEPS.value,
            natural_key=table.natural_key,
            row_keys=table.keys,
            row_count=1,
            table_plan_key=table.canonical_key(owner),
            files=(
                PlannedFile(
                    relative_path=relative,
                    size_bytes=captured.size_bytes,
                    digest=captured.digest,
                    row_count=1,
                    schema_signature=tuple(
                        (name, str(dtype)) for name, dtype in captured.schema.items()
                    ),
                ),
            ),
        )
        payload = {
            "logical_commit_id": owner,
            "commit_kind": "step_result",
            "step_run_id": admission.record.step_run_id,
            "recovery_batch_id": None,
            "step_number": admission.record.step_number,
            "operation_name": admission.operation_name,
            "tables": [evidence.model_dump(mode="json")],
        }
        plan = CuratorEffects.model_validate(
            {**payload, "plan_digest": curator_effects_digest(payload)}
        )
        return CapturedCuratorEffects(
            plan, staging.staging_dir, {TablePath.STEPS.value: table}, {}, scratch
        )
    except BaseException:
        scratch.cleanup()
        raise
