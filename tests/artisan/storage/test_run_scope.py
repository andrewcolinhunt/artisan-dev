"""Tests for authoritative current-run execution membership."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from fixtures.logical_commit_store import commit_test_step
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import (
    CommitError,
    IncompatibleStoreError,
    PersistenceIntegrityError,
    StoreIntegrityError,
)
from artisan.schemas.artifact.data import DataArtifact
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
from artisan.storage.io.commit_plan import CommitPlan
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
    """Initialize a supported store and return its root and filesystem."""
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


def _selected(
    root: str,
    fs: AbstractFileSystem,
    *,
    number: int = 0,
    prior: bool = False,
    run_id: str = "source-run",
) -> pl.DataFrame:
    """Read the source selection under test."""
    from artisan.storage.core.run_scope import load_run_step_outputs

    return load_run_step_outputs(
        root,
        fs=fs,
        pipeline_run_id=run_id,
        step_number=number,
        include_prior_steps=prior,
    )


def _artifact(content: bytes, number: int = 0) -> DataArtifact:
    """Create typed committed output content."""
    return DataArtifact.draft(
        content=content, original_name="data.csv", step_number=number
    ).finalize()


def test_run_step_outputs_isolates_runs_latest_attempts_and_cached_origins(
    store: tuple[str, AbstractFileSystem, Path],
) -> None:
    from fixtures.run_outputs import commit_outputs

    root, fs, _ = store
    outside = _artifact(b"outside", 8)
    _, execution = commit_outputs(
        root, run_id="other-run", number=8, artifacts=[outside]
    )
    obsolete, _ = commit_outputs(root, artifacts=[_artifact(b"obsolete")])
    latest, _ = commit_outputs(root, reused_execution_id=execution)
    commit_outputs(root, run_id="other-run", artifacts=[_artifact(b"unrelated")])
    selected = _selected(root, fs)
    assert selected["current_step_run_id"].to_list() == [latest]
    assert obsolete not in selected["current_step_run_id"].to_list()
    assert selected["artifact_id"].to_list() == [outside.artifact_id]
    assert selected["origin_step_number"].to_list() == [8]
    assert selected["cache_hit"].to_list() == [True]


def test_run_step_outputs_inclusive_boundary_allows_gaps_and_later_running(
    store: tuple[str, AbstractFileSystem, Path],
) -> None:
    from fixtures.run_outputs import commit_outputs

    root, fs, _ = store
    first, _ = commit_outputs(root, artifacts=[_artifact(b"first")])
    last, _ = commit_outputs(root, number=2, artifacts=[_artifact(b"last", 2)])
    commit_outputs(root, number=3, status="running")
    assert _selected(root, fs)["current_step_run_id"].to_list() == [first]
    assert _selected(root, fs, number=2)["current_step_run_id"].to_list() == [last]
    assert _selected(root, fs, number=2, prior=True)[
        "current_step_run_id"
    ].to_list() == [first, last]
    with pytest.raises(ValueError, match="has no step 1"):
        _selected(root, fs, number=1, prior=True)


@pytest.mark.parametrize("status", ["pending", "running"])
def test_run_step_outputs_rejects_selected_unresolved_latest_attempt(
    store: tuple[str, AbstractFileSystem, Path], status: str
) -> None:
    from fixtures.run_outputs import commit_outputs

    root, fs, _ = store
    commit_outputs(root, artifacts=[_artifact(b"previous")])
    unresolved, _ = commit_outputs(root, status=status)
    commit_outputs(root, number=2, artifacts=[_artifact(b"last", 2)])
    with pytest.raises(ValueError, match=unresolved):
        _selected(root, fs, number=2, prior=True)
    assert _selected(root, fs, number=2).height == 1


@pytest.mark.parametrize("status", ["failed", "cancelled", "skipped"])
def test_run_step_outputs_terminal_failures_do_not_resurrect_old_outputs(
    store: tuple[str, AbstractFileSystem, Path], status: str
) -> None:
    from fixtures.run_outputs import commit_outputs

    root, fs, _ = store
    commit_outputs(root, artifacts=[_artifact(b"previous")])
    commit_outputs(root, status=status, execution_success=False)
    assert _selected(root, fs).is_empty()


def test_run_step_outputs_partial_excludes_failed_execution_edges(
    store: tuple[str, AbstractFileSystem, Path],
) -> None:
    from fixtures.run_outputs import commit_outputs

    root, fs, _ = store
    good, bad = _artifact(b"good"), _artifact(b"bad")
    commit_outputs(
        root,
        status="partial",
        artifacts=[good, bad],
        output_ids={"accepted": [good.artifact_id]},
        failed_output_ids=[bad.artifact_id],
    )
    assert _selected(root, fs)["artifact_id"].to_list() == [good.artifact_id]
    commit_outputs(root, artifacts=[_artifact(b"replacement")])
    assert _selected(root, fs)["artifact_id"].to_list() != [good.artifact_id]


def test_run_step_outputs_keeps_frozen_attempt_when_retry_commits_during_read(
    store: tuple[str, AbstractFileSystem, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    from fixtures.run_outputs import commit_outputs

    from artisan.storage.core import run_scope

    root, fs, _ = store
    original, _ = commit_outputs(root, artifacts=[_artifact(b"original")])
    reader = run_scope.load_accepted_outputs
    replacement_ids = []

    def commit_then_read(*args: Any, **kwargs: Any) -> pl.DataFrame:
        replacement_ids.append(
            commit_outputs(root, artifacts=[_artifact(b"replacement")])[0]
        )
        return reader(*args, **kwargs)

    monkeypatch.setattr(run_scope, "load_accepted_outputs", commit_then_read)
    assert _selected(root, fs)["current_step_run_id"].to_list() == [original]
    monkeypatch.setattr(run_scope, "load_accepted_outputs", reader)
    assert _selected(root, fs)["current_step_run_id"].to_list() == replacement_ids


def test_run_step_outputs_incomplete_latest_commit_remains_unresolved(
    store: tuple[str, AbstractFileSystem, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    from fixtures.run_outputs import commit_outputs

    root, fs, _ = store
    commit_outputs(root, artifacts=[_artifact(b"original")])

    def fail_completion(self: DeltaCommitter, plan: CommitPlan) -> None:
        msg = "completion unavailable"
        raise OSError(msg)

    monkeypatch.setattr(DeltaCommitter, "_complete", fail_completion)
    with pytest.raises(CommitError, match="logical completion"):
        commit_outputs(root, artifacts=[_artifact(b"unfinished")])
    with pytest.raises(ValueError, match="unresolved attempts"):
        _selected(root, fs)


@pytest.mark.parametrize("damage", ["missing_manifest", "old_format", "missing_table"])
def test_run_step_outputs_gates_store_before_unknown_run(
    store: tuple[str, AbstractFileSystem, Path], damage: str
) -> None:
    import json

    from artisan.storage.core.store_format import STORE_MANIFEST_PATH

    root, fs, _ = store
    manifest = Path(root) / STORE_MANIFEST_PATH
    if damage == "missing_manifest":
        manifest.unlink()
    elif damage == "old_format":
        document = json.loads(manifest.read_text())
        document["store_format"] = 2
        manifest.write_text(json.dumps(document))
    else:
        fs.rm(f"{root}/{TablePath.STEPS.value}", recursive=True)
    with pytest.raises(IncompatibleStoreError):
        _selected(root, fs, run_id="unknown")


def test_run_step_outputs_distinguishes_unknown_run_and_empty_boundary(
    store: tuple[str, AbstractFileSystem, Path],
) -> None:
    from fixtures.run_outputs import commit_outputs

    root, fs, _ = store
    with pytest.raises(ValueError, match="Unknown source run 'unknown'"):
        _selected(root, fs, run_id="unknown")
    commit_outputs(root)
    assert _selected(root, fs).is_empty()


def test_run_step_outputs_propagates_dangling_reuse(
    store: tuple[str, AbstractFileSystem, Path],
) -> None:
    from fixtures.run_outputs import commit_outputs

    root, fs, _ = store
    commit_outputs(root, reused_execution_id="b" * 32)
    with pytest.raises(PersistenceIntegrityError, match="Dangling"):
        _selected(root, fs)
