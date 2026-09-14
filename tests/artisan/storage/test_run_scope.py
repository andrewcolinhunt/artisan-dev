"""Tests for authoritative current-run execution membership."""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import pytest
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import IncompatibleStoreError, PersistenceIntegrityError
from artisan.schemas.enums import TablePath
from artisan.storage.core.run_scope import (
    load_execution_membership,
    validate_cached_executions,
)
from artisan.storage.core.table_schemas import (
    CACHE_REUSE_SCHEMA,
    EXECUTIONS_SCHEMA,
    STEPS_SCHEMA,
)
from artisan.storage.io.commit import DeltaCommitter
from artisan.storage.io.staging import StagingManager


def _step(step_id: str, run_id: str, number: int) -> dict[str, object]:
    """Build one completed step event row."""
    return {
        "step_run_id": step_id,
        "step_spec_id": "f" * 32,
        "pipeline_run_id": run_id,
        "step_number": number,
        "step_name": f"step-{number}",
        "status": "completed",
        "operation_class": "example.Operation",
        "params_json": "{}",
        "input_refs_json": "{}",
        "compute_backend": "local",
        "compute_options_json": "{}",
        "output_roles_json": "[]",
        "output_types_json": "{}",
        "total_count": 1,
        "succeeded_count": 1,
        "failed_count": 0,
        "timestamp": datetime.now(UTC),
        "duration_seconds": 1.0,
        "error": None,
        "dispatch_error": None,
        "commit_error": None,
        "metadata": "{}",
    }


def _execution(execution_id: str, step_id: str | None) -> dict[str, object]:
    """Build one successful execution row."""
    now = datetime.now(UTC)
    return {
        "execution_run_id": execution_id,
        "execution_spec_id": "e" * 32,
        "step_run_id": step_id,
        "origin_step_number": 0,
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
    return root, fs


def _append(root: str, table: TablePath, rows: list[dict], schema: dict) -> None:
    """Append typed rows to one initialized Delta table."""
    pl.DataFrame(rows, schema=schema).write_delta(
        f"{root}/{table.value}", mode="append"
    )


def test_membership_unions_direct_and_cached_executions(store) -> None:
    root, fs = store
    current = "a" * 32
    direct = "b" * 32
    cached = "c" * 32
    _append(root, TablePath.STEPS, [_step(current, "run-a", 2)], STEPS_SCHEMA)
    _append(
        root,
        TablePath.EXECUTIONS,
        [_execution(direct, current), _execution(cached, "d" * 32)],
        EXECUTIONS_SCHEMA,
    )
    _append(
        root,
        TablePath.CACHE_REUSE,
        [
            {
                "current_step_run_id": current,
                "cached_execution_run_id": cached,
            }
        ],
        CACHE_REUSE_SCHEMA,
    )

    result = load_execution_membership(root, fs=fs, pipeline_run_id="run-a").sort(
        "execution_run_id"
    )

    assert result["execution_run_id"].to_list() == [direct, cached]
    assert result["cache_hit"].to_list() == [False, True]
    assert result["current_step_number"].to_list() == [2, 2]


def test_membership_projects_one_execution_into_multiple_current_steps(store) -> None:
    root, fs = store
    step_a = "a" * 32
    step_b = "b" * 32
    cached = "c" * 32
    _append(
        root,
        TablePath.STEPS,
        [_step(step_a, "run-a", 0), _step(step_b, "run-a", 1)],
        STEPS_SCHEMA,
    )
    _append(
        root,
        TablePath.EXECUTIONS,
        [_execution(cached, "d" * 32)],
        EXECUTIONS_SCHEMA,
    )
    pairs = [
        {"current_step_run_id": step_id, "cached_execution_run_id": cached}
        for step_id in (step_a, step_b)
    ]
    _append(root, TablePath.CACHE_REUSE, pairs, CACHE_REUSE_SCHEMA)

    result = load_execution_membership(root, fs=fs, pipeline_run_id="run-a")

    assert result.height == 2
    assert set(result["current_step_number"].to_list()) == {0, 1}


def test_reuse_projects_across_runs_without_rewriting_execution_owner(store) -> None:
    root, fs = store
    source_step = "a" * 32
    current_step = "b" * 32
    execution_id = "c" * 32
    _append(
        root,
        TablePath.STEPS,
        [
            _step(source_step, "source-run", 0),
            _step(current_step, "current-run", 4),
        ],
        STEPS_SCHEMA,
    )
    _append(
        root,
        TablePath.EXECUTIONS,
        [_execution(execution_id, source_step)],
        EXECUTIONS_SCHEMA,
    )
    _append(
        root,
        TablePath.CACHE_REUSE,
        [
            {
                "current_step_run_id": current_step,
                "cached_execution_run_id": execution_id,
            }
        ],
        CACHE_REUSE_SCHEMA,
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
    root, fs = store

    result = load_execution_membership(root, fs=fs, pipeline_run_id="unknown")

    assert result.is_empty()
    assert result.schema["cache_hit"] == pl.Boolean


def test_membership_fails_closed_on_dangling_cached_execution(store) -> None:
    root, fs = store
    current = "a" * 32
    _append(root, TablePath.STEPS, [_step(current, "run-a", 0)], STEPS_SCHEMA)
    _append(
        root,
        TablePath.CACHE_REUSE,
        [
            {
                "current_step_run_id": current,
                "cached_execution_run_id": "b" * 32,
            }
        ],
        CACHE_REUSE_SCHEMA,
    )

    with pytest.raises(PersistenceIntegrityError, match="Dangling"):
        load_execution_membership(root, fs=fs, pipeline_run_id="run-a")


def test_membership_collapses_duplicate_pairs(store) -> None:
    root, fs = store
    current = "a" * 32
    cached = "b" * 32
    _append(root, TablePath.STEPS, [_step(current, "run-a", 0)], STEPS_SCHEMA)
    _append(
        root,
        TablePath.EXECUTIONS,
        [_execution(cached, "c" * 32)],
        EXECUTIONS_SCHEMA,
    )
    pair = {
        "current_step_run_id": current,
        "cached_execution_run_id": cached,
    }
    _append(root, TablePath.CACHE_REUSE, [pair, pair], CACHE_REUSE_SCHEMA)

    result = load_execution_membership(root, fs=fs, pipeline_run_id="run-a")

    assert result.height == 1


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
    root, fs = store
    step_id = "a" * 32
    first = _step(step_id, "run-a", 0)
    second = {**first, "step_name": "different-name"}
    _append(root, TablePath.STEPS, [first, second], STEPS_SCHEMA)

    with pytest.raises(PersistenceIntegrityError, match="conflicting owners"):
        load_execution_membership(root, fs=fs, pipeline_run_id="run-a")
