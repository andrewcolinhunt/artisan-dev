"""Tests for immutable logical commit plans."""

from __future__ import annotations

import json
import weakref
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import polars as pl
import pytest
from fixtures.store_format import publish_test_store
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import (
    ARTIFACT_INDEX_SCHEMA,
    ARTIFACT_LOCATIONS_SCHEMA,
)
from artisan.storage.io.commit_plan import (
    CommitPlan,
    StagedPlanEvidence,
    _plan_digest,
    build_commit_plan,
    commit_plan_path,
    prepare_commit_evidence,
    prepare_commit_plan,
    publish_commit_plan,
    read_commit_plan,
    verify_plan_files,
)
from artisan.storage.io.staging import StagingManager
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest


def _build_input_plan(tmp_path, *, index: pl.DataFrame | None = None):
    fs = LocalFileSystem()
    delta_root = str(tmp_path / "delta")
    staging_root = str(tmp_path / "staging")
    publish_test_store(delta_root, fs)
    staging = StagingManager(staging_root, fs)
    staged = (
        index
        if index is not None
        else pl.DataFrame(
            {
                "artifact_id": ["a" * 32],
                "artifact_type": ["metric"],
                "origin_step_number": [0],
                "metadata": ["{}"],
            },
            schema=ARTIFACT_INDEX_SCHEMA,
        )
    )
    staged_path = staging.stage_orchestrator_dataframe(
        staged,
        TablePath.ARTIFACT_INDEX.value,
        commit_kind="input_registration",
        step_run_id="b" * 32,
        step_number=0,
        operation_name="ingest",
    )
    staging.stage_orchestrator_dataframe(
        pl.DataFrame(
            {"artifact_id": ["a" * 32], "uri": ["file:///input.json"]},
            schema=ARTIFACT_LOCATIONS_SCHEMA,
        ),
        TablePath.ARTIFACT_LOCATIONS.value,
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
    return fs, delta_root, staging_root, staged_path, plan


def _changed_plan(plan: CommitPlan, **changes) -> CommitPlan:
    payload = plan.model_dump(mode="json", exclude={"plan_digest"})
    payload.update(changes)
    return CommitPlan(**payload, plan_digest=_plan_digest(payload))


def _legacy_plan_bytes(
    plan: CommitPlan, staging_root: str, fs: LocalFileSystem
) -> bytes:
    payload = plan.model_dump(mode="json", exclude={"plan_digest"})
    for table in payload["tables"]:
        table.pop("digest_version")
        frames = []
        for file in table["files"]:
            with fs.open(f"{staging_root}/{file['relative_path']}", "rb") as stream:
                frames.append(pl.read_parquet(stream))
        frame = pl.concat(frames).sort(table["natural_key"])
        if "origin_step_number" in frame:
            frame = frame.drop("origin_step_number")
        table["table_plan_key"] = compute_content_digest(
            canonical_json_bytes(
                {
                    "logical_commit_id": plan.logical_commit_id,
                    "table_path": table["table_path"],
                    "rows": frame.to_dicts(),
                }
            )
        )
    payload["plan_digest"] = compute_content_digest(canonical_json_bytes(payload))
    return canonical_json_bytes(payload)


def _concurrent_publish(delta_root, fs, plans):
    barrier = Barrier(len(plans))

    def publish(plan):
        barrier.wait()
        return publish_commit_plan(delta_root, fs, plan)

    with ThreadPoolExecutor(max_workers=len(plans)) as pool:
        futures = [pool.submit(publish, plan) for plan in plans]
    outcomes = []
    for future in futures:
        try:
            outcomes.append(future.result())
        except Exception as exc:  # Assert exact exception types at the call site.
            outcomes.append(exc)
    return outcomes


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


def test_new_commit_plans_publish_digest_version_two(tmp_path: Path) -> None:
    fs, delta_root, _staging_root, _staged_path, plan = _build_input_plan(tmp_path)
    payload = json.loads(
        fs.cat(commit_plan_path(delta_root, plan.step_run_id, plan.commit_kind))
    )

    assert all(table.digest_version == 2 for table in plan.tables)
    assert all(table["digest_version"] == 2 for table in payload["tables"])


def test_legacy_commit_plan_roundtrip_preserves_original_bytes_and_digest(
    tmp_path: Path,
) -> None:
    fs, delta_root, staging_root, _staged_path, plan = _build_input_plan(tmp_path)
    encoded = _legacy_plan_bytes(plan, staging_root, fs)
    path = commit_plan_path(delta_root, plan.step_run_id, plan.commit_kind)
    fs.pipe(path, encoded)

    legacy = read_commit_plan(delta_root, fs, plan.step_run_id, plan.commit_kind)

    assert all(table.digest_version == 1 for table in legacy.tables)
    assert legacy.plan_digest == json.loads(encoded)["plan_digest"]
    assert canonical_json_bytes(legacy.model_dump(mode="json")) == encoded
    assert all("digest_version" not in table for table in legacy.model_dump()["tables"])
    assert publish_commit_plan(delta_root, fs, legacy) == legacy
    assert fs.cat(path) == encoded
    verified = verify_plan_files(legacy, staging_root, fs)
    assert verified[TablePath.ARTIFACT_INDEX.value]["artifact_id"].to_list() == [
        "a" * 32
    ]


@pytest.mark.parametrize(
    "prepare",
    [build_commit_plan, prepare_commit_plan, prepare_commit_evidence],
    ids=["build", "plan", "evidence"],
)
def test_rebuilding_legacy_commit_plan_preserves_original_identity(
    tmp_path: Path, prepare: Callable[..., CommitPlan | StagedPlanEvidence]
) -> None:
    fs, delta_root, staging_root, _staged_path, plan = _build_input_plan(tmp_path)
    encoded = _legacy_plan_bytes(plan, staging_root, fs)
    path = commit_plan_path(delta_root, plan.step_run_id, plan.commit_kind)
    fs.pipe(path, encoded)
    legacy = read_commit_plan(delta_root, fs, plan.step_run_id, plan.commit_kind)

    result = prepare(
        delta_root=delta_root,
        staging_root=staging_root,
        fs=fs,
        commit_kind=legacy.commit_kind,
        step_run_id=legacy.step_run_id,
        step_number=legacy.step_number,
        operation_name=legacy.operation_name,
    )

    retried = result.plan if isinstance(result, StagedPlanEvidence) else result
    assert retried == legacy
    assert fs.cat(path) == encoded
    if isinstance(result, StagedPlanEvidence):
        assert result.frames[TablePath.ARTIFACT_INDEX.value][
            "artifact_id"
        ].to_list() == ["a" * 32]


@pytest.mark.parametrize(
    "prepare",
    [build_commit_plan, prepare_commit_plan, prepare_commit_evidence],
    ids=["build", "plan", "evidence"],
)
@pytest.mark.parametrize("damage", ["changed", "unknown"])
def test_rebuilding_legacy_commit_plan_revalidates_staging(
    tmp_path: Path,
    prepare: Callable[..., CommitPlan | StagedPlanEvidence],
    damage: str,
) -> None:
    fs, delta_root, staging_root, staged_path, plan = _build_input_plan(tmp_path)
    assert staged_path is not None
    encoded = _legacy_plan_bytes(plan, staging_root, fs)
    path = commit_plan_path(delta_root, plan.step_run_id, plan.commit_kind)
    fs.pipe(path, encoded)
    if damage == "changed":
        with fs.open(staged_path, "rb") as stream:
            changed = pl.read_parquet(stream).with_columns(
                pl.lit('{"changed":true}').alias("metadata")
            )
        with fs.open(staged_path, "wb") as stream:
            changed.write_parquet(stream)
    else:
        fs.touch(f"{staged_path.rsplit('/', 1)[0]}/unknown.txt")

    with pytest.raises(StoreIntegrityError):
        prepare(
            delta_root=delta_root,
            staging_root=staging_root,
            fs=fs,
            commit_kind=plan.commit_kind,
            step_run_id=plan.step_run_id,
            step_number=plan.step_number,
            operation_name=plan.operation_name,
        )
    assert fs.cat(path) == encoded


def test_commit_kinds_use_disjoint_staging_and_cleanup(tmp_path):
    fs, delta_root, staging_root, _staged_path, first = _build_input_plan(tmp_path)
    staging = StagingManager(staging_root, fs)
    step_result_path = staging.stage_orchestrator_dataframe(
        pl.DataFrame(
            {
                "artifact_id": ["c" * 32],
                "artifact_type": ["metric"],
                "origin_step_number": [0],
                "metadata": ["{}"],
            },
            schema=ARTIFACT_INDEX_SCHEMA,
        ),
        TablePath.ARTIFACT_INDEX.value,
        commit_kind="step_result",
        step_run_id=first.step_run_id,
        step_number=0,
        operation_name="ingest",
    )

    rebuilt = build_commit_plan(
        delta_root=delta_root,
        staging_root=staging_root,
        fs=fs,
        commit_kind="input_registration",
        step_run_id=first.step_run_id,
        step_number=0,
        operation_name="ingest",
    )
    staging.cleanup_plan([file for table in rebuilt.tables for file in table.files])

    assert rebuilt == first
    assert step_result_path is not None
    assert fs.exists(step_result_path)


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


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (
            lambda payload: payload.update(logical_commit_id="step_result:" + "b" * 32),
            "does not match its owner",
        ),
        (
            lambda payload: payload["tables"].append(payload["tables"][0]),
            "repeats a table",
        ),
        (
            lambda payload: payload.update(tables=list(reversed(payload["tables"]))),
            "invalid table order",
        ),
    ],
)
def test_commit_plan_model_rejects_invalid_identity_and_structure(
    tmp_path,
    mutate,
    match,
):
    _fs, _delta_root, _staging_root, _staged_path, plan = _build_input_plan(tmp_path)
    payload = plan.model_dump(mode="json", exclude={"plan_digest"})
    mutate(payload)

    with pytest.raises(ValueError, match=match):
        CommitPlan(**payload, plan_digest=_plan_digest(payload))


def test_commit_plan_model_rejects_duplicate_row_keys(tmp_path):
    _fs, _delta_root, _staging_root, _staged_path, plan = _build_input_plan(tmp_path)
    payload = plan.model_dump(mode="json", exclude={"plan_digest"})
    table = payload["tables"][0]
    table["row_keys"] = [table["row_keys"][0], table["row_keys"][0]]
    table["row_count"] = 2
    table["files"][0]["row_count"] = 2

    with pytest.raises(ValueError, match="Invalid row keys"):
        CommitPlan(**payload, plan_digest=_plan_digest(payload))


def test_commit_plan_collapses_identical_global_artifact_rows(tmp_path):
    duplicate_index = pl.DataFrame(
        {
            "artifact_id": ["a" * 32, "a" * 32],
            "artifact_type": ["metric", "metric"],
            "origin_step_number": [0, 0],
            "metadata": ["{}", "{}"],
        },
        schema=ARTIFACT_INDEX_SCHEMA,
    )

    fs, _delta_root, staging_root, _path, plan = _build_input_plan(
        tmp_path,
        index=duplicate_index,
    )

    table = plan.table(TablePath.ARTIFACT_INDEX.value)
    assert table is not None
    assert table.row_count == 1
    assert table.files[0].row_count == 2
    verified = verify_plan_files(plan, staging_root, fs)
    assert verified[TablePath.ARTIFACT_INDEX.value].height == 1


def test_commit_plan_rejects_conflicting_global_artifact_rows(tmp_path):
    conflicting_index = pl.DataFrame(
        {
            "artifact_id": ["a" * 32, "a" * 32],
            "artifact_type": ["metric", "data"],
            "origin_step_number": [0, 0],
            "metadata": ["{}", "{}"],
        },
        schema=ARTIFACT_INDEX_SCHEMA,
    )

    with pytest.raises(StoreIntegrityError, match="Conflicting natural keys"):
        _build_input_plan(tmp_path, index=conflicting_index)


def test_local_plan_publish_is_no_replace_and_cleans_temporary_files(tmp_path):
    fs, delta_root, _staging_root, _staged_path, plan = _build_input_plan(tmp_path)
    path = commit_plan_path(delta_root, plan.step_run_id, plan.commit_kind)
    fs.rm(path)

    outcomes = _concurrent_publish(delta_root, fs, [plan, plan])

    assert outcomes == [plan, plan]
    assert read_commit_plan(delta_root, fs, plan.step_run_id, plan.commit_kind) == plan
    assert not fs.glob(f"{path}.tmp-*")


def test_remote_plan_publish_accepts_competing_same_plan(s3_fs, tmp_path):
    fs, _storage, root = s3_fs
    _local_fs, _local_root, _staging_root, _staged_path, plan = _build_input_plan(
        tmp_path
    )
    delta_root = f"{root}/plan-same"

    outcomes = _concurrent_publish(delta_root, fs, [plan, plan])

    assert outcomes == [plan, plan]
    assert read_commit_plan(delta_root, fs, plan.step_run_id, plan.commit_kind) == plan


def test_remote_plan_publish_rejects_competing_different_plan(s3_fs, tmp_path):
    fs, _storage, root = s3_fs
    _local_fs, _local_root, _staging_root, _staged_path, plan = _build_input_plan(
        tmp_path
    )
    other = _changed_plan(plan, operation_name="different")
    delta_root = f"{root}/plan-conflict"

    outcomes = _concurrent_publish(delta_root, fs, [plan, other])

    assert len([item for item in outcomes if isinstance(item, CommitPlan)]) == 1
    errors = [item for item in outcomes if isinstance(item, StoreIntegrityError)]
    assert len(errors) == 1
    assert "Conflicting immutable object" in str(errors[0])
    published = read_commit_plan(delta_root, fs, plan.step_run_id, plan.commit_kind)
    assert published in {plan, other}


def test_remote_partial_plan_object_is_never_accepted(s3_fs, tmp_path):
    fs, _storage, root = s3_fs
    _local_fs, _local_root, _staging_root, _staged_path, plan = _build_input_plan(
        tmp_path
    )
    delta_root = f"{root}/plan-partial"
    path = commit_plan_path(delta_root, plan.step_run_id, plan.commit_kind)
    fs.makedirs(path.rsplit("/", 1)[0], exist_ok=True)
    with fs.open(path, "wb") as stream:
        stream.write(b'{"logical_commit_id":')

    with pytest.raises(StoreIntegrityError, match="Conflicting immutable object"):
        publish_commit_plan(delta_root, fs, plan)

    with pytest.raises(StoreIntegrityError, match="Unreadable commit plan"):
        read_commit_plan(delta_root, fs, plan.step_run_id, plan.commit_kind)
    with fs.open(path, "rb") as stream:
        assert stream.read() == b'{"logical_commit_id":'


def _recovery_evidence(tmp_path, execution_ids):
    from fixtures.execution_records import executions_df

    from artisan.storage.io.commit_plan import prepare_commit_evidence
    from artisan.storage.io.worker_seal import (
        STAGING_INVENTORY_KEY,
        build_staging_inventory,
    )
    from artisan.utils.path import shard_uri

    fs = LocalFileSystem()
    root = str(tmp_path / "staging")
    for execution_id in execution_ids:
        directory = shard_uri(
            root, execution_id, step_number=3, operation_name="creator"
        )
        fs.makedirs(directory, exist_ok=True)
        frame = executions_df(
            execution_run_id=[execution_id],
            step_run_id=["b" * 32],
            origin_step_number=[3],
            operation_name=["creator"],
            success=[True],
        )
        frame.write_parquet(
            f"{directory}/executions.parquet",
            metadata={
                STAGING_INVENTORY_KEY: build_staging_inventory(directory, fs).decode(),
            },
        )
    return prepare_commit_evidence(
        staging_root=root,
        fs=fs,
        commit_kind="execution_recovery",
        step_run_id="b" * 32,
        step_number=3,
        operation_name="creator",
        execution_run_ids=execution_ids,
    )


def test_recovery_plan_identity_and_effects_cover_sorted_exact_batch(tmp_path):
    first = _recovery_evidence(tmp_path, ["c" * 32, "d" * 32])
    reordered = _recovery_evidence(tmp_path, ["d" * 32, "c" * 32])
    later = _recovery_evidence(tmp_path, ["e" * 32])
    assert first.plan == reordered.plan
    assert first.plan.recovery_batch_id != later.plan.recovery_batch_id
    assert (
        first.plan.logical_commit_id
        == "execution_recovery:" + first.plan.recovery_batch_id
    )
    assert first.plan.table(TablePath.EXECUTIONS.value).row_keys == (
        ("c" * 32,),
        ("d" * 32,),
    )
    assert first.frames[TablePath.EXECUTIONS.value].height == 2
    assert first.per_execution_artifact_ids == {"c" * 32: set(), "d" * 32: set()}


def test_planning_releases_each_worker_payload_before_reading_the_campaign(
    tmp_path, monkeypatch
):
    from artisan.storage.io import commit_plan

    original = commit_plan.read_worker_files
    captured = []

    def read(directory, fs):
        assert sum(reference() is not None for reference in captured) <= 1
        files = original(directory, fs)
        captured.extend(weakref.ref(frame) for _, frame in files.values())
        return files

    monkeypatch.setattr(commit_plan, "read_worker_files", read)
    evidence = _recovery_evidence(tmp_path, [f"{number:032x}" for number in range(25)])
    assert len(captured) == 25
    assert all(reference() is None for reference in captured)
    assert evidence.frames[TablePath.EXECUTIONS.value].height == 25


@pytest.mark.parametrize("ids", [[], ["c" * 32, "c" * 32], ["invalid"]])
def test_recovery_batch_rejects_empty_duplicate_or_invalid_ids(ids):
    from artisan.storage.io.commit_plan import recovery_batch_identity

    with pytest.raises(ValueError):
        recovery_batch_identity("b" * 32, ids)


def test_recovery_model_rejects_batch_identity_changed_independently(tmp_path):
    evidence = _recovery_evidence(tmp_path, ["c" * 32, "d" * 32])
    with pytest.raises(ValueError, match="exact execution batch"):
        _changed_plan(
            evidence.plan,
            recovery_batch_id="f" * 32,
            logical_commit_id="execution_recovery:" + "f" * 32,
        )


@pytest.mark.parametrize(
    "table_path",
    ["artifacts/unregistered_domain", "../bad", "/absolute", "artifacts/../bad"],
)
def test_plan_reader_distinguishes_registration_from_malformed_paths(
    tmp_path, table_path
):
    from artisan.errors import ApplicationRegistrationError

    fs, delta_root, _, _, plan = _build_input_plan(tmp_path)
    payload = plan.model_dump(mode="json")
    payload["tables"][0]["table_path"] = table_path
    payload["plan_digest"] = _plan_digest(
        {key: value for key, value in payload.items() if key != "plan_digest"}
    )
    path = commit_plan_path(delta_root, plan.step_run_id, plan.commit_kind)
    with fs.open(path, "w") as stream:
        json.dump(payload, stream)
    expected = (
        ApplicationRegistrationError
        if table_path == "artifacts/unregistered_domain"
        else StoreIntegrityError
    )
    with pytest.raises(expected):
        read_commit_plan(delta_root, fs, plan.step_run_id, plan.commit_kind)
