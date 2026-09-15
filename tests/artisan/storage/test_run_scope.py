"""Tests for authoritative current-run execution membership."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest
from fixtures.logical_commit_store import commit_test_step
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import (
    IncompatibleStoreError,
    PersistenceIntegrityError,
    StoreIntegrityError,
)
from artisan.schemas.enums import TablePath
from artisan.storage.core.run_scope import (
    load_execution_membership,
    validate_cached_executions,
)
from artisan.storage.core.table_schemas import (
    CACHE_REUSE_SCHEMA,
    EXECUTIONS_SCHEMA,
    STEPS_SCHEMA,
    get_physical_schema,
)
from artisan.storage.io.commit import DeltaCommitter
from artisan.storage.io.staging import StagingManager


def _step_rows(step_id: str, run_id: str, number: int) -> list[dict[str, object]]:
    """Build a complete pending/running/succeeded attempt history."""
    base = {
        "step_run_id": step_id,
        "step_spec_id": None,
        "pipeline_run_id": run_id,
        "step_number": number,
        "step_name": f"step-{number}",
        "status": "pending",
        "state_sequence": 0,
        "disposition": None,
        "cancellation_status": None,
        "logical_commit_id": None,
        "operation_class": "example.Operation",
        "params_json": "{}",
        "input_refs_json": "{}",
        "compute_backend": "local",
        "compute_options_json": "{}",
        "output_roles_json": "[]",
        "output_types_json": "{}",
        "total_count": None,
        "succeeded_count": None,
        "failed_count": None,
        "timestamp": datetime.now(UTC),
        "duration_seconds": None,
        "error": None,
        "metadata": None,
    }
    running = {**base, "status": "running", "state_sequence": 1}
    succeeded = {
        **running,
        "step_spec_id": "f" * 32,
        "status": "succeeded",
        "state_sequence": 2,
        "disposition": "executed",
        "total_count": 1,
        "succeeded_count": 1,
        "failed_count": 0,
        "duration_seconds": 1.0,
    }
    return [base, running, succeeded]


def _execution(
    execution_id: str,
    step_id: str | None,
    step_number: int = 0,
) -> dict[str, object]:
    """Build one successful execution row."""
    now = datetime.now(UTC)
    return {
        "execution_run_id": execution_id,
        "execution_spec_id": "e" * 32,
        "step_run_id": step_id,
        "origin_step_number": step_number,
        "operation_name": "example",
        "params": "{}",
        "user_overrides": "{}",
        "timestamp_start": now,
        "timestamp_end": now,
        "source_worker": 0,
        "compute_backend": "local",
        "success": True,
        "error": None,
        "error_envelope": None,
        "tool_output": None,
        "worker_log": None,
        "metadata": "{}",
    }


@pytest.fixture
def store(tmp_path):
    """Create an empty format-2 store and return its root and filesystem."""
    fs = LocalFileSystem()
    root = str(tmp_path / "delta")
    DeltaCommitter(
        root,
        StagingManager(str(tmp_path / "staging"), fs),
        fs=fs,
    ).initialize_tables()
    return root, fs, tmp_path / "staging"


def _commit(
    root: str,
    staging_root,
    step_rows: list[dict[str, object]],
    *,
    executions: list[dict[str, object]] | None = None,
    reuse: list[dict[str, object]] | None = None,
) -> None:
    """Seed one complete step through the logical commit boundary."""
    tables = {}
    if executions:
        tables[TablePath.EXECUTIONS.value] = pl.DataFrame(
            executions, schema=EXECUTIONS_SCHEMA
        )
    if reuse:
        tables[TablePath.CACHE_REUSE.value] = pl.DataFrame(
            reuse, schema=CACHE_REUSE_SCHEMA
        )
    commit_test_step(Path(root), staging_root, step_rows, tables)


def test_membership_unions_direct_and_cached_executions(store) -> None:
    root, fs, staging = store
    current = "a" * 32
    direct = "b" * 32
    cached = "c" * 32
    source = "d" * 32
    _commit(
        root,
        staging,
        _step_rows(source, "source-run", 0),
        executions=[_execution(cached, source)],
    )
    _commit(
        root,
        staging,
        _step_rows(current, "run-a", 2),
        executions=[_execution(direct, current, 2)],
        reuse=[
            {
                "current_step_run_id": current,
                "cached_execution_run_id": cached,
            }
        ],
    )

    result = load_execution_membership(root, fs=fs, pipeline_run_id="run-a").sort(
        "execution_run_id"
    )

    assert result["execution_run_id"].to_list() == [direct, cached]
    assert result["cache_hit"].to_list() == [False, True]
    assert result["current_step_number"].to_list() == [2, 2]


def test_membership_projects_one_execution_into_multiple_current_steps(store) -> None:
    root, fs, staging = store
    step_a = "a" * 32
    step_b = "b" * 32
    cached = "c" * 32
    source = "d" * 32
    _commit(
        root,
        staging,
        _step_rows(source, "source-run", 0),
        executions=[_execution(cached, source)],
    )
    _commit(
        root,
        staging,
        _step_rows(step_a, "run-a", 0),
        reuse=[{"current_step_run_id": step_a, "cached_execution_run_id": cached}],
    )
    _commit(
        root,
        staging,
        _step_rows(step_b, "run-a", 1),
        reuse=[{"current_step_run_id": step_b, "cached_execution_run_id": cached}],
    )

    result = load_execution_membership(root, fs=fs, pipeline_run_id="run-a")

    assert result.height == 2
    assert set(result["current_step_number"].to_list()) == {0, 1}


def test_reuse_projects_across_runs_without_rewriting_execution_owner(store) -> None:
    root, fs, staging = store
    source_step = "a" * 32
    current_step = "b" * 32
    execution_id = "c" * 32
    _commit(
        root,
        staging,
        _step_rows(source_step, "source-run", 0),
        executions=[_execution(execution_id, source_step)],
    )
    _commit(
        root,
        staging,
        _step_rows(current_step, "current-run", 4),
        reuse=[
            {
                "current_step_run_id": current_step,
                "cached_execution_run_id": execution_id,
            }
        ],
    )

    source = load_execution_membership(root, fs=fs, pipeline_run_id="source-run")
    current = load_execution_membership(root, fs=fs, pipeline_run_id="current-run")
    unrelated = load_execution_membership(root, fs=fs, pipeline_run_id="other-run")
    persisted = pl.read_delta(f"{root}/{TablePath.EXECUTIONS.value}")

    assert source.select("execution_run_id", "cache_hit").row(0) == (
        execution_id,
        False,
    )
    assert current.select("execution_run_id", "cache_hit").row(0) == (
        execution_id,
        True,
    )
    assert current["current_step_number"].item() == 4
    assert unrelated.is_empty()
    assert persisted["step_run_id"].item() == source_step


def test_membership_unknown_run_is_empty_and_typed(store) -> None:
    root, fs, _ = store

    result = load_execution_membership(root, fs=fs, pipeline_run_id="unknown")

    assert result.is_empty()
    assert result.schema["cache_hit"] == pl.Boolean


def test_membership_fails_closed_on_dangling_cached_execution(store) -> None:
    root, fs, staging = store
    current = "a" * 32
    _commit(
        root,
        staging,
        _step_rows(current, "run-a", 0),
        reuse=[
            {
                "current_step_run_id": current,
                "cached_execution_run_id": "b" * 32,
            }
        ],
    )

    with pytest.raises(PersistenceIntegrityError, match="Dangling"):
        load_execution_membership(root, fs=fs, pipeline_run_id="run-a")


def test_membership_rejects_duplicate_complete_pairs(store) -> None:
    root, fs, staging = store
    current = "a" * 32
    cached = "b" * 32
    source = "c" * 32
    _commit(
        root,
        staging,
        _step_rows(source, "source-run", 0),
        executions=[_execution(cached, source)],
    )
    pair = {
        "current_step_run_id": current,
        "cached_execution_run_id": cached,
    }
    _commit(
        root,
        staging,
        _step_rows(current, "run-a", 0),
        reuse=[pair],
    )
    pl.DataFrame([pair], schema=CACHE_REUSE_SCHEMA).write_delta(
        f"{root}/{TablePath.CACHE_REUSE.value}", mode="append"
    )

    with pytest.raises(StoreIntegrityError, match="Duplicate natural key"):
        load_execution_membership(root, fs=fs, pipeline_run_id="run-a")


def test_membership_enforces_store_gate_at_entry(tmp_path) -> None:
    fs = LocalFileSystem()

    with pytest.raises(IncompatibleStoreError, match="missing manifest"):
        load_execution_membership(str(tmp_path / "not-a-store"), fs=fs)


def test_cache_validation_enforces_store_gate_for_empty_input(tmp_path) -> None:
    fs = LocalFileSystem()

    with pytest.raises(IncompatibleStoreError, match="missing manifest"):
        validate_cached_executions(
            str(tmp_path / "not-a-store"),
            "a" * 32,
            set(),
            fs=fs,
        )


def test_membership_rejects_conflicting_step_name(store) -> None:
    root, fs, _ = store
    step_id = "a" * 32
    first, second, *_ = _step_rows(step_id, "run-a", 0)
    second = {**second, "step_name": "different-name"}
    physical = (
        pl.DataFrame([first, second], schema=STEPS_SCHEMA)
        .with_columns(pl.lit(None, dtype=pl.String).alias("logical_commit_id"))
        .select(list(get_physical_schema(TablePath.STEPS)))
    )
    physical.write_delta(f"{root}/{TablePath.STEPS.value}", mode="append")

    with pytest.raises(PersistenceIntegrityError, match="conflicting owners"):
        load_execution_membership(root, fs=fs, pipeline_run_id="run-a")
