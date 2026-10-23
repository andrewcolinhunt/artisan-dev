"""Tests for ordered, crash-safe logical commits."""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import pytest
from fixtures.execution_records import executions_df
from fixtures.store_format import publish_test_store

from artisan.errors import CommitError, StoreIntegrityError
from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.core.committed_scan import read_committed, read_logical_commits
from artisan.storage.core.table_schemas import (
    ARTIFACT_EDGES_SCHEMA,
    ARTIFACT_INDEX_SCHEMA,
    ARTIFACT_LOCATIONS_SCHEMA,
    EXECUTION_EDGES_SCHEMA,
    STEPS_SCHEMA,
)
from artisan.storage.io.commit import DeltaCommitter
from artisan.storage.io.commit_plan import (
    CommitPlan,
    build_commit_plan,
    read_plan_evidence,
)
from artisan.storage.io.staging import StagingManager
from artisan.storage.io.worker_seal import (
    STAGING_INVENTORY_KEY,
    build_staging_inventory,
)
from artisan.utils.path import shard_uri

STEP_ID = "b" * 32
ARTIFACT_ID = "a" * 32


@pytest.fixture
def commit_env(backend_fs):
    fs, storage, root = backend_fs
    delta_root = f"{root}/delta"
    staging_root = f"{root}/staging"
    publish_test_store(delta_root, fs, storage.delta_storage_options())
    staging = StagingManager(staging_root, fs)
    committer = DeltaCommitter(
        delta_root,
        staging,
        fs=fs,
        storage_options=storage.delta_storage_options(),
    )
    return committer, fs, storage.delta_storage_options(), delta_root, staging_root


def _stage_registration(
    committer: DeltaCommitter,
    *,
    step_run_id: str = STEP_ID,
    step_number: int = 0,
    content: bytes = b'{"score": 0.5}',
) -> CommitPlan:
    staging = committer.staging_manager
    kwargs = {
        "step_run_id": step_run_id,
        "step_number": step_number,
        "operation_name": "ingest",
    }
    metric = pl.DataFrame(
        {
            "artifact_id": [ARTIFACT_ID],
            "origin_step_number": [step_number],
            "content": [content],
            "original_name": ["metric.json"],
            "extension": [".json"],
            "metadata": ["{}"],
        },
        schema=MetricArtifact.POLARS_SCHEMA,
    )
    index = pl.DataFrame(
        {
            "artifact_id": [ARTIFACT_ID],
            "artifact_type": ["metric"],
            "origin_step_number": [step_number],
            "metadata": ["{}"],
        },
        schema=ARTIFACT_INDEX_SCHEMA,
    )
    locations = pl.DataFrame(
        {"artifact_id": [ARTIFACT_ID], "uri": ["file:///input.json"]},
        schema=ARTIFACT_LOCATIONS_SCHEMA,
    )
    staging.stage_orchestrator_dataframe(
        metric,
        "artifacts/metrics",
        commit_kind="input_registration",
        **kwargs,
    )
    staging.stage_orchestrator_dataframe(
        index,
        TablePath.ARTIFACT_INDEX.value,
        commit_kind="input_registration",
        **kwargs,
    )
    staging.stage_orchestrator_dataframe(
        locations,
        TablePath.ARTIFACT_LOCATIONS.value,
        commit_kind="input_registration",
        **kwargs,
    )
    return build_commit_plan(
        delta_root=committer.delta_base_path,
        staging_root=staging.staging_dir,
        fs=staging._fs,
        commit_kind="input_registration",
        **kwargs,
    )


def _stage_step_result(
    committer: DeltaCommitter, *, cached_execution_ids: list[str] | None = None
) -> CommitPlan:
    staging = committer.staging_manager
    execution_id = "d" * 32
    directory = shard_uri(
        staging.staging_dir,
        execution_id,
        step_number=0,
        operation_name="full",
    )
    staging._fs.makedirs(directory, exist_ok=True)
    metric = pl.DataFrame(
        {
            "artifact_id": [ARTIFACT_ID],
            "origin_step_number": [0],
            "content": [b'{"score": 0.5}'],
            "original_name": ["metric.json"],
            "extension": [".json"],
            "metadata": ["{}"],
        },
        schema=MetricArtifact.POLARS_SCHEMA,
    )
    index = pl.DataFrame(
        {
            "artifact_id": [ARTIFACT_ID],
            "artifact_type": ["metric"],
            "origin_step_number": [0],
            "metadata": ["{}"],
        },
        schema=ARTIFACT_INDEX_SCHEMA,
    )
    locations = pl.DataFrame(
        {"artifact_id": [ARTIFACT_ID], "uri": ["file:///output.json"]},
        schema=ARTIFACT_LOCATIONS_SCHEMA,
    )
    execution_edges = pl.DataFrame(
        {
            "execution_run_id": [execution_id],
            "direction": ["output"],
            "role": ["metric"],
            "artifact_id": [ARTIFACT_ID],
        },
        schema=EXECUTION_EDGES_SCHEMA,
    )
    artifact_edges = pl.DataFrame(
        {
            "execution_run_id": [execution_id],
            "source_artifact_id": ["e" * 32],
            "target_artifact_id": [ARTIFACT_ID],
            "source_artifact_type": ["metric"],
            "target_artifact_type": ["metric"],
            "source_role": ["source"],
            "target_role": ["metric"],
            "group_id": [None],
            "step_boundary": [True],
        },
        schema=ARTIFACT_EDGES_SCHEMA,
    )
    execution = executions_df(
        execution_run_id=[execution_id],
        execution_spec_id=["f" * 32],
        step_run_id=[STEP_ID],
        origin_step_number=[0],
        operation_name=["full"],
        timestamp_start=[datetime.now(UTC)],
        timestamp_end=[datetime.now(UTC)],
        source_worker=[0],
        compute_backend=["local"],
        success=[True],
    )
    for filename, frame in (
        ("metrics.parquet", metric),
        ("index.parquet", index),
        ("locations.parquet", locations),
        ("execution_edges.parquet", execution_edges),
        ("artifact_edges.parquet", artifact_edges),
        ("executions.parquet", execution),
    ):
        metadata = (
            {
                STAGING_INVENTORY_KEY: build_staging_inventory(
                    directory, staging._fs
                ).decode()
            }
            if filename == "executions.parquet"
            else None
        )
        with staging._fs.open(f"{directory}/{filename}", "wb") as stream:
            frame.write_parquet(stream, metadata=metadata)
    staging.stage_cache_reuse(
        STEP_ID,
        cached_execution_ids or ["f" * 32],
        step_number=0,
        operation_name="full",
    )
    terminal = dict.fromkeys(STEPS_SCHEMA)
    terminal.update(
        {
            "step_run_id": STEP_ID,
            "step_spec_id": "1" * 32,
            "pipeline_run_id": "2" * 32,
            "step_number": 0,
            "step_name": "full",
            "status": "succeeded",
            "state_sequence": 2,
            "disposition": "executed",
            "cancellation_status": "none",
            "operation_class": "tests.Full",
            "params_json": "{}",
            "input_refs_json": "[]",
            "compute_backend": "local",
            "compute_options_json": "{}",
            "output_roles_json": '["metric"]',
            "output_types_json": '{"metric":"metric"}',
            "total_count": 1,
            "succeeded_count": 1,
            "failed_count": 0,
            "timestamp": datetime.now(UTC),
            "duration_seconds": 0.1,
            "metadata": "{}",
        }
    )
    staging.stage_orchestrator_dataframe(
        pl.DataFrame([terminal], schema=STEPS_SCHEMA),
        TablePath.STEPS.value,
        commit_kind="step_result",
        step_run_id=STEP_ID,
        step_number=0,
        operation_name="full",
    )
    return build_commit_plan(
        delta_root=committer.delta_base_path,
        staging_root=staging.staging_dir,
        fs=staging._fs,
        commit_kind="step_result",
        step_run_id=STEP_ID,
        step_number=0,
        operation_name="full",
        execution_run_ids=[execution_id],
    )


@pytest.mark.parametrize(
    ("boundary", "table_path"),
    [
        ("planned", None),
        ("table", "artifacts/metrics"),
        ("table", TablePath.ARTIFACT_INDEX.value),
        ("table", TablePath.ARTIFACT_LOCATIONS.value),
        ("complete", None),
    ],
)
def test_crash_after_each_append_boundary_converges_exactly(
    commit_env,
    monkeypatch,
    boundary,
    table_path,
):
    committer, fs, options, delta_root, staging_root = commit_env
    plan = _stage_registration(committer)

    class SimulatedCrash(BaseException):
        pass

    def crash(found_boundary, _plan, found_table):
        if (found_boundary, found_table) == (boundary, table_path):
            raise SimulatedCrash

    monkeypatch.setattr(committer, "_checkpoint", crash)
    with pytest.raises(SimulatedCrash):
        committer.commit_logical(plan)

    monkeypatch.setattr(committer, "_checkpoint", lambda *_: None)
    committer.commit_logical(plan)
    committer.commit_logical(plan)

    controls = read_logical_commits(delta_root, fs=fs, storage_options=options)
    assert controls.select("logical_commit_id", "state").to_dicts() == [
        {"logical_commit_id": plan.logical_commit_id, "state": "complete"}
    ]
    for table in plan.tables:
        rows = read_committed(
            delta_root,
            table.table_path,
            fs=fs,
            storage_options=options,
        )
        assert rows.height == table.row_count
    assert not fs.exists(
        f"{staging_root}/0_ingest/_orchestrator/{STEP_ID}/input_registration"
    )


@pytest.mark.parametrize(
    "table_path",
    [
        TablePath.EXECUTIONS.value,
        TablePath.EXECUTION_EDGES.value,
        TablePath.ARTIFACT_EDGES.value,
        TablePath.CACHE_REUSE.value,
        TablePath.STEPS.value,
    ],
)
def test_crash_after_remaining_ordered_boundaries_converges_exactly(
    commit_env,
    monkeypatch,
    table_path,
):
    committer, fs, options, delta_root, staging_root = commit_env
    plan = _stage_step_result(committer)

    class SimulatedCrash(BaseException):
        pass

    def crash(boundary, _plan, found_table):
        if boundary == "table" and found_table == table_path:
            raise SimulatedCrash

    monkeypatch.setattr(committer, "_checkpoint", crash)
    with pytest.raises(SimulatedCrash):
        committer.commit_logical(plan)

    monkeypatch.setattr(committer, "_checkpoint", lambda *_: None)
    committer.commit_logical(plan)
    committer.commit_logical(plan)

    controls = read_logical_commits(delta_root, fs=fs, storage_options=options)
    assert controls.select("logical_commit_id", "state").to_dicts() == [
        {"logical_commit_id": plan.logical_commit_id, "state": "complete"}
    ]
    for table in plan.tables:
        rows = read_committed(
            delta_root,
            table.table_path,
            fs=fs,
            storage_options=options,
        )
        assert rows.height == table.row_count
    assert not fs.glob(f"{staging_root}/0_full/**/*.parquet")


def test_planned_rows_are_invisible_after_first_table_failure(
    commit_env,
    monkeypatch,
):
    committer, fs, options, delta_root, staging_root = commit_env
    plan = _stage_registration(committer)
    original = committer._append

    def fail_index(frame, table_path):
        if table_path == TablePath.ARTIFACT_INDEX.value:
            msg = "injected storage failure"
            raise OSError(msg)
        original(frame, table_path)

    monkeypatch.setattr(committer, "_append", fail_index)
    with pytest.raises(CommitError) as caught:
        committer.commit_logical(plan)

    assert caught.value.logical_commit_id == plan.logical_commit_id
    assert caught.value.table == TablePath.ARTIFACT_INDEX.value
    assert caught.value.verified_tables == ("artifacts/metrics",)
    assert isinstance(caught.value.__cause__, OSError)
    assert read_committed(
        delta_root,
        "artifacts/metrics",
        fs=fs,
        storage_options=options,
    ).is_empty()
    assert fs.exists(
        f"{staging_root}/0_ingest/_orchestrator/{STEP_ID}/input_registration"
    )


def test_changed_staging_fails_before_planned_marker(commit_env):
    committer, fs, options, delta_root, _ = commit_env
    plan = _stage_registration(committer)
    path = (
        f"{committer.staging_manager.staging_dir}/"
        f"{plan.tables[0].files[0].relative_path}"
    )
    with fs.open(path, "wb") as stream:
        stream.write(b"changed")

    with pytest.raises(StoreIntegrityError, match="Unreadable staged"):
        committer.commit_logical(plan)

    assert read_logical_commits(
        delta_root,
        fs=fs,
        storage_options=options,
    ).is_empty()


def test_terminal_attempt_rejects_retry_before_any_commit_append(
    commit_env,
    monkeypatch,
):
    committer, fs, options, delta_root, staging_root = commit_env
    tracker = StepTracker(delta_root, "run", storage_options=options, fs=fs)
    tracker.create_attempt(
        StepStartRecord(
            step_run_id=STEP_ID,
            step_spec_id="1" * 32,
            step_number=0,
            step_name="full",
            operation_class="tests.Full",
            params_json="{}",
            input_refs_json="[]",
            compute_backend="local",
            compute_options_json="{}",
            output_roles_json='["metric"]',
            output_types_json='{"metric":"metric"}',
        )
    )
    tracker.transition(STEP_ID, StepStatus.PENDING, StepStatus.RUNNING)
    failure = StepResult(
        step_name="full",
        step_number=0,
        step_run_id=STEP_ID,
        status=StepStatus.FAILED,
        error="prior persistence failure",
    )
    tracker.transition(
        STEP_ID,
        StepStatus.RUNNING,
        StepStatus.FAILED,
        result=failure,
        error=failure.error,
    )
    plan = _stage_step_result(committer)
    monkeypatch.setattr(
        committer,
        "_append",
        lambda *_args: pytest.fail("terminal guard allowed a commit append"),
    )

    with pytest.raises(StoreIntegrityError, match="already failed"):
        committer.commit_logical(plan)

    assert read_logical_commits(
        delta_root,
        fs=fs,
        storage_options=options,
    ).is_empty()
    assert fs.exists(f"{staging_root}/0_full")


def test_control_write_failure_writes_no_data_and_preserves_staging(
    commit_env,
    monkeypatch,
):
    committer, fs, options, delta_root, staging_root = commit_env
    plan = _stage_registration(committer)
    original = committer._append

    def fail_control(frame, table_path):
        if table_path == TablePath.LOGICAL_COMMITS.value:
            msg = "control unavailable"
            raise OSError(msg)
        original(frame, table_path)

    monkeypatch.setattr(committer, "_append", fail_control)
    with pytest.raises(CommitError) as caught:
        committer.commit_logical(plan)

    assert caught.value.table == TablePath.LOGICAL_COMMITS.value
    assert read_logical_commits(
        delta_root,
        fs=fs,
        storage_options=options,
    ).is_empty()
    assert (
        pl.scan_delta(
            f"{delta_root}/artifacts/metrics",
            storage_options=options,
        )
        .collect()
        .is_empty()
    )
    assert fs.exists(
        f"{staging_root}/0_ingest/_orchestrator/{STEP_ID}/input_registration"
    )


def test_completion_marker_failure_keeps_all_effects_invisible(
    commit_env,
    monkeypatch,
):
    committer, fs, options, delta_root, staging_root = commit_env
    plan = _stage_registration(committer)

    def fail_completion(_plan):
        msg = "completion unavailable"
        raise OSError(msg)

    monkeypatch.setattr(committer, "_complete", fail_completion)
    with pytest.raises(CommitError) as caught:
        committer.commit_logical(plan)

    assert caught.value.table == TablePath.LOGICAL_COMMITS.value
    assert read_logical_commits(
        delta_root,
        fs=fs,
        storage_options=options,
    )["state"].to_list() == ["planned"]
    assert read_committed(
        delta_root,
        TablePath.ARTIFACT_INDEX,
        fs=fs,
        storage_options=options,
    ).is_empty()
    assert fs.exists(
        f"{staging_root}/0_ingest/_orchestrator/{STEP_ID}/input_registration"
    )


def test_cleanup_failure_does_not_revoke_completed_visibility(
    commit_env,
    monkeypatch,
):
    committer, fs, options, delta_root, staging_root = commit_env
    plan = _stage_registration(committer)

    def fail_cleanup(_paths):
        msg = "cleanup unavailable"
        raise OSError(msg)

    monkeypatch.setattr(committer.staging_manager, "cleanup_plan", fail_cleanup)
    with pytest.raises(OSError, match="cleanup unavailable"):
        committer.commit_logical(plan)

    assert read_logical_commits(
        delta_root,
        fs=fs,
        storage_options=options,
    )["state"].to_list() == ["complete"]
    assert (
        read_committed(
            delta_root,
            TablePath.ARTIFACT_INDEX,
            fs=fs,
            storage_options=options,
        ).height
        == 1
    )
    assert fs.exists(
        f"{staging_root}/0_ingest/_orchestrator/{STEP_ID}/input_registration"
    )


@pytest.mark.parametrize("later_step", [0, 7])
def test_exact_complete_global_artifact_satisfies_later_plan(commit_env, later_step):
    committer, fs, options, delta_root, _ = commit_env
    first = _stage_registration(committer, step_run_id="b" * 32)
    committer.commit_logical(first)
    second = _stage_registration(
        committer, step_run_id="c" * 32, step_number=later_step
    )

    assert committer.commit_logical(second) == {}
    for table in ("artifacts/metrics", TablePath.ARTIFACT_INDEX.value):
        rows = read_committed(delta_root, table, fs=fs, storage_options=options)
        assert rows.height == 1
        assert rows["origin_step_number"].to_list() == [0]
        owners = (
            pl.scan_delta(f"{delta_root}/{table}", storage_options=options)
            .select("logical_commit_id")
            .collect()
        )
        assert owners["logical_commit_id"].to_list() == [first.logical_commit_id]
    assert read_logical_commits(
        delta_root,
        fs=fs,
        storage_options=options,
    )["state"].to_list() == ["complete", "complete"]


def test_conflicting_global_artifact_stops_before_later_tables(commit_env):
    committer, fs, options, delta_root, _ = commit_env
    first = _stage_registration(committer, step_run_id="b" * 32)
    committer.commit_logical(first)
    second = _stage_registration(
        committer,
        step_run_id="c" * 32,
        step_number=7,
        content=b'{"score": 0.9}',
    )

    with pytest.raises(CommitError) as caught:
        committer.commit_logical(second)

    assert caught.value.table == "artifacts/metrics"
    controls = read_logical_commits(delta_root, fs=fs, storage_options=options)
    states = {
        row["logical_commit_id"]: row["state"] for row in controls.iter_rows(named=True)
    }
    assert states == {
        first.logical_commit_id: "complete",
        second.logical_commit_id: "planned",
    }
    assert (
        pl.scan_delta(
            f"{delta_root}/{TablePath.ARTIFACT_INDEX.value}",
            storage_options=options,
        )
        .collect()
        .height
        == 1
    )


@pytest.mark.parametrize("table_path", ["artifacts/metrics", "artifacts/index"])
def test_scoped_verification_rejects_modified_artifact_origin(commit_env, table_path):
    committer, fs, options, delta_root, _ = commit_env
    plan = _stage_registration(committer)
    committer.commit_logical(plan, preserve_staging=True)
    path = f"{delta_root}/{table_path}"
    rows = pl.read_delta(path, storage_options=options)
    rows.with_columns(
        pl.lit(7, dtype=pl.Int32).alias("origin_step_number")
    ).write_delta(path, mode="overwrite", storage_options=options)

    # Durable completion authorizes ordinary visibility; explicit validation
    # and retained-staging cleanup must still reject modified effects.
    assert (
        read_committed(delta_root, table_path, fs=fs, storage_options=options).height
        == 1
    )
    with pytest.raises(StoreIntegrityError, match="origin disagrees"):
        committer._validate_complete(plan)
    with pytest.raises(StoreIntegrityError, match="origin disagrees"):
        committer._cleanup_plan(plan)


def test_control_string_encoding_supports_conditional_completion(commit_env):
    committer, fs, options, delta_root, _ = commit_env
    plan = _stage_registration(committer)

    committer.commit_logical(plan, preserve_staging=True)

    control = read_logical_commits(delta_root, fs=fs, storage_options=options)
    assert control.row(0, named=True)["state"] == "complete"
    assert control.row(0, named=True)["completed_at"] is not None


def test_initialize_and_maintenance_use_exact_tables(commit_env):
    committer, fs, _, delta_root, _ = commit_env
    committer.initialize_tables()

    assert fs.exists(f"{delta_root}/{TablePath.LOGICAL_COMMITS.value}")
    assert committer.compact_table("artifacts/metrics") == {
        "files_added": 0,
        "files_removed": 0,
    }
    committer.vacuum_table("artifacts/metrics")


def test_prepared_evidence_is_writer_bound_and_single_use(commit_env):
    committer, fs, options, delta_root, _ = commit_env
    plan = _stage_registration(committer)
    staged = read_plan_evidence(plan, committer.staging_manager.staging_dir, fs)
    prepared = committer.prepare_logical(plan, staged=staged)
    other = DeltaCommitter(
        delta_root, committer.staging_manager, fs=fs, storage_options=options
    )
    with pytest.raises(StoreIntegrityError, match="another writer/plan/root"):
        other.commit_logical(plan, prepared=prepared)
    committer.commit_logical(plan, prepared=prepared)
    with pytest.raises(StoreIntegrityError, match="stale"):
        committer.commit_logical(plan, prepared=prepared)


def test_fresh_commit_reads_each_table_only_for_write_and_verification(
    commit_env, monkeypatch
):
    committer, _fs, _options, _delta_root, _ = commit_env
    plan = _stage_registration(committer)
    original = committer._read_physical
    calls = []

    def read(table):
        calls.append(table)
        return original(table)

    monkeypatch.setattr(committer, "_read_physical", read)
    committer.commit_logical(plan)
    assert calls == [table.table_path for table in plan.tables for _ in range(2)]
    calls.clear()
    committer.commit_logical(plan)
    assert calls == []


def test_prepared_snapshot_commits_but_changed_staging_is_retained(commit_env):
    committer, fs, _options, _delta_root, staging_root = commit_env
    plan = _stage_registration(committer)
    prepared = committer.prepare_logical(plan)
    path = f"{staging_root}/{plan.tables[0].files[0].relative_path}"
    with fs.open(path, "wb") as stream:
        stream.write(b"changed after preparation")
    committer.commit_logical(plan, prepared=prepared)
    assert fs.exists(path)
    with fs.open(path, "rb") as stream:
        assert stream.read() == b"changed after preparation"


def test_completed_preservation_skips_payload_verification(commit_env, monkeypatch):
    committer, _fs, _options, _delta_root, _ = commit_env
    plan = _stage_registration(committer)
    committer.commit_logical(plan, preserve_staging=True)
    monkeypatch.setattr(
        committer,
        "_read_physical",
        lambda *_: pytest.fail("completed preserved plan was reverified"),
    )
    assert committer.commit_logical(plan, preserve_staging=True) == {}


@pytest.mark.parametrize("mutation", ["partial", "duplicate", "extra"])
def test_retry_rejects_corrupt_non_global_effect_before_completion(
    commit_env, mutation
):
    committer, fs, options, delta_root, staging_root = commit_env
    plan = _stage_step_result(committer, cached_execution_ids=["e" * 32, "f" * 32])
    committer._insert_planned(plan)
    table_path = TablePath.CACHE_REUSE.value
    expected = read_plan_evidence(plan, staging_root, fs).frames[table_path]
    if mutation == "partial":
        corrupt = expected.head(1)
    elif mutation == "duplicate":
        corrupt = pl.concat([expected, expected.head(1)])
    else:
        corrupt = pl.concat(
            [
                expected,
                expected.head(1).with_columns(
                    pl.lit("9" * 32).alias("cached_execution_run_id")
                ),
            ]
        )
    committer._append(corrupt, table_path)
    with pytest.raises(CommitError) as caught:
        committer.commit_logical(plan)
    assert caught.value.table == table_path
    assert isinstance(caught.value.__cause__, StoreIntegrityError)
    assert read_logical_commits(delta_root, fs=fs, storage_options=options)[
        "state"
    ].to_list() == ["planned"]
    assert fs.exists(f"{staging_root}/{plan.table(table_path).files[0].relative_path}")
