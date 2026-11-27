"""Register and recover complete raw imports before curator admission."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast
from uuid import uuid4

import polars as pl
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.admission import ManagedArtifactBinding
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.storage.core.committed_scan import verify_snapshot_effect
from artisan.storage.core.table_schemas import (
    ARTIFACT_INDEX_SCHEMA,
    ARTIFACT_LOCATIONS_SCHEMA,
    FRAMEWORK_SCHEMAS,
)
from artisan.storage.io.bounded_evidence import DiskTable
from artisan.storage.io.bounded_io import write_parquet_rows
from artisan.storage.io.delta_files import DeltaSnapshot, append_table_files
from artisan.storage.io.input_registration_plan import (
    REGISTRATION_TABLES,
    InputRegistrationPlan,
    input_registration_digest,
    read_input_registration,
    registration_directory,
)
from artisan.storage.io.input_registration_sources import (
    CapturedRawInputs,
    capture_raw_inputs,
    normalize_raw_uri,
    resolve_raw_inputs,
)
from artisan.storage.io.logical_control import (
    complete_control,
    control_for,
    planned_control,
)
from artisan.storage.io.publication import (
    publish_immutable_bytes,
    publish_immutable_file,
)
from artisan.storage.io.published_effects import (
    capture_table_effects,
    publish_table_effect,
)
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_plan import retained_table_paths
from artisan.storage.io.retained_read import RetainedReadSnapshot
from artisan.storage.io.table_effect import apply_table_effect, missing_effect_keys
from artisan.utils.hashing import canonical_json_bytes, compute_stream_digest
from artisan.utils.path import uri_join


def register_raw_inputs(
    committer: RetainedCommitter,
    uris: Sequence[str],
    *,
    step_run_id: str,
    step_number: int,
    operation_name: str,
    storage: StorageConfig,
    check_cancelled: Callable[[], None] | None = None,
    checkpoint: Callable[[str], None] | None = None,
) -> InputRegistrationPlan:
    """Capture inputs and complete exact registration before any curator runs.

    Cancellation may raise before plan publication. Once published, the plan
    remains recoverable independently of the curator's cancellation decision.
    The caller must check cancellation again before curator admission.
    """
    directory = registration_directory(committer.delta_root, step_run_id)
    if type(step_number) is not int or step_number < 0 or not operation_name:
        msg = "Registration requires a nonnegative step and operation name"
        raise ValueError(msg)
    requested = tuple(normalize_raw_uri(uri) for uri in uris)
    if not requested:
        msg = "Raw registration requires at least one input"
        raise ValueError(msg)
    check = check_cancelled or _continue
    notify = checkpoint or _ignore_checkpoint
    with committer._lock:
        existing = read_input_registration(
            committer.delta_root, step_run_id, committer.fs
        )
        if existing is not None:
            _verify_retry(existing, requested, step_number, operation_name)
            return recover_input_registration(
                committer, step_run_id, checkpoint=checkpoint
            )
    check()
    sources = resolve_raw_inputs(requested, storage)
    with TemporaryDirectory(prefix="artisan-raw-inputs-") as scratch:
        captured = capture_raw_inputs(
            sources, Path(scratch), step_number=step_number, check_cancelled=check
        )
        notify("captured")
        with committer._lock:
            existing = read_input_registration(
                committer.delta_root, step_run_id, committer.fs
            )
            if existing is not None:
                _verify_retry(existing, requested, step_number, operation_name)
                return recover_input_registration(
                    committer, step_run_id, checkpoint=checkpoint
                )
            plan = _prepare(
                committer,
                captured,
                directory,
                scratch,
                step_run_id=step_run_id,
                step_number=step_number,
                operation_name=operation_name,
            )
            notify("prepared")
            check()
            publish_immutable_bytes(
                committer.fs,
                uri_join(directory, "plan.json"),
                canonical_json_bytes(plan.model_dump(mode="json")),
            )
            notify("plan")
            return recover_input_registration(
                committer, step_run_id, checkpoint=checkpoint
            )


def recover_input_registration(
    committer: RetainedCommitter,
    step_run_id: str,
    *,
    checkpoint: Callable[[str], None] | None = None,
) -> InputRegistrationPlan:
    """Complete only selected immutable evidence, without reopening original inputs."""
    notify = checkpoint or _ignore_checkpoint
    with committer._lock:
        plan = read_input_registration(committer.delta_root, step_run_id, committer.fs)
        if plan is None:
            msg = "Raw registration has no selected immutable plan"
            raise StoreIntegrityError(msg)
        directory = registration_directory(committer.delta_root, step_run_id)
        with capture_table_effects(
            plan, plan.tables, directory=directory, fs=committer.fs
        ) as tables:
            _verify_content(committer, plan, tables)
            controls = committer._controls()
            control = control_for(plan, controls)
            if control is not None and control["state"] == "complete":
                _verify_effects(committer, plan, controls)
                return plan
            if control is not None and control["state"] != "planned":
                msg = "Raw registration is no longer eligible for recovery"
                raise StoreIntegrityError(msg)
            _preflight(committer, plan, tables, controls)
            if control is None:
                append_table_files(
                    committer._snapshot(TablePath.LOGICAL_COMMITS.value),
                    [planned_control(plan)],
                    step_number=plan.step_number,
                    statistics_columns=["logical_commit_id"],
                )
                controls = committer._controls()
                notify("planned")
            for table in plan.tables:
                apply_table_effect(
                    plan,
                    table,
                    tables[table.table_path],
                    controls,
                    read_snapshot=committer._snapshot,
                )
                notify(f"table:{table.table_path}")
            _verify_effects(committer, plan, controls)
            _verify_content(committer, plan, tables)
            complete_control(
                plan,
                uri_join(committer.delta_root, TablePath.LOGICAL_COMMITS.value),
                storage_options=committer.storage_options,
            )
            _verify_effects(committer, plan, committer._controls())
            notify("complete")
            return plan


def read_registered_inputs(
    snapshot: RetainedReadSnapshot, plan: InputRegistrationPlan
) -> dict[str, FileRefArtifact]:
    """Read completed registered content without recovery or original source access.

    The caller owns the fresh read snapshot. Original acquisition order remains
    in the plan; returned content is unique by artifact identity.
    """
    if snapshot.completed_control_plan(plan.logical_commit_id) != plan:
        msg = "Registration differs from its completed source plan"
        raise StoreIntegrityError(msg)
    committer = snapshot.committer
    _verify_effects(committer, plan, snapshot.controls, read_snapshot=snapshot.snapshot)
    with capture_table_effects(
        plan,
        plan.tables,
        directory=registration_directory(committer.delta_root, plan.step_run_id),
        fs=committer.fs,
    ) as tables:
        _verify_content(committer, plan, tables)
        carriers = {item.digest: item for item in plan.carriers}
        content = {}
        for binding in plan.external_bindings:
            artifact = FileRefArtifact.from_row(
                tables[REGISTRATION_TABLES[0]].get_row((binding.artifact_id,))
            )
            artifact.path = uri_join(
                committer.files_root, carriers[binding.carrier_digest].relative_path
            )
            content[binding.artifact_id] = artifact
        return content


def _prepare(
    committer: RetainedCommitter,
    captured: CapturedRawInputs,
    directory: str,
    scratch: str,
    *,
    step_run_id: str,
    step_number: int,
    operation_name: str,
) -> InputRegistrationPlan:
    controls = committer._controls()
    content = _protected_content(committer, captured, controls)
    bindings = tuple(
        ManagedArtifactBinding(
            artifact_id=key, carrier_digest=cast(str, artifact.content_hash)
        )
        for key, artifact in sorted(captured.artifacts.items())
    )
    sources = _table_sources(content, bindings, captured, committer.files_root, scratch)
    relative = f"tables/{uuid4().hex}"
    tables = tuple(
        publish_table_effect(
            f"input_registration:{step_run_id}",
            path,
            sources[path],
            directory,
            scratch,
            committer.fs,
            relative_directory=relative,
        )
        for path in REGISTRATION_TABLES
    )
    payload = {
        "domain": "artisan.input-registration",
        "schema_version": 1,
        "step_run_id": step_run_id,
        "step_number": step_number,
        "operation_name": operation_name,
        "inputs": [item.model_dump(mode="json") for item in captured.inputs],
        "tables": [item.model_dump(mode="json") for item in tables],
        "carriers": [
            captured.carriers[key].model_dump(mode="json")
            for key in sorted(captured.carriers)
        ],
        "external_bindings": [item.model_dump(mode="json") for item in bindings],
    }
    plan = InputRegistrationPlan.model_validate(
        {**payload, "plan_digest": input_registration_digest(payload)}
    )
    with capture_table_effects(
        plan, tables, directory=directory, fs=committer.fs
    ) as evidence:
        _preflight(committer, plan, evidence, controls)
    for digest, carrier in captured.carriers.items():
        publish_immutable_file(
            committer.fs,
            uri_join(committer.files_root, carrier.relative_path),
            str(captured.paths[digest]),
            source_fs=LocalFileSystem(),
        )
    return plan


def _protected_content(
    committer: RetainedCommitter, captured: CapturedRawInputs, controls: pl.DataFrame
) -> dict[str, dict[str, Any]]:
    ids = set(captured.artifacts)
    existing = {
        path: _existing_rows(committer, path, ids, controls)
        for path in REGISTRATION_TABLES[:2]
    }
    rows = {}
    for artifact_id, artifact in captured.artifacts.items():
        row = artifact.to_row()
        content = existing[REGISTRATION_TABLES[0]].get(artifact_id)
        index = existing[REGISTRATION_TABLES[1]].get(artifact_id)
        if (content is None) != (index is None):
            msg = "Existing raw content has incomplete index membership"
            raise StoreIntegrityError(msg)
        if content is not None and index is not None:
            protected = FileRefArtifact.from_row(content).to_row()
            if {
                key: value for key, value in row.items() if key != "origin_step_number"
            } != {
                key: value
                for key, value in protected.items()
                if key != "origin_step_number"
            } or index != _index_row(protected):
                msg = "Raw input conflicts with existing protected content"
                raise StoreIntegrityError(msg)
            row = protected
        rows[artifact_id] = row
    return rows


def _existing_rows(
    committer: RetainedCommitter, table_path: str, ids: set[str], controls: pl.DataFrame
) -> dict[str, dict[str, Any]]:
    rows = {}
    checked: set[str] = set()
    snapshot = committer._snapshot(table_path)
    for frame in snapshot.iter_frames():
        for row in frame.filter(pl.col("artifact_id").is_in(ids)).iter_rows(named=True):
            owner = row.pop("logical_commit_id")
            control = controls.filter(pl.col("logical_commit_id") == owner)
            if control.height != 1 or control["state"][0] != "complete":
                msg = "Raw input overlaps an incomplete or unknown owner"
                raise StoreIntegrityError(msg)
            if owner not in checked:
                source = committer._control_plan(control.row(0, named=True))
                verify_snapshot_effect(source, table_path, snapshot, controls)
                checked.add(owner)
            if row["artifact_id"] in rows:
                msg = "Raw content has duplicate physical ownership"
                raise StoreIntegrityError(msg)
            rows[row["artifact_id"]] = row
    return rows


def _table_sources(
    content: Mapping[str, dict[str, Any]],
    bindings: tuple[ManagedArtifactBinding, ...],
    captured: CapturedRawInputs,
    files_root: str,
    scratch: str,
) -> dict[str, str]:
    rows = (
        [content[key] for key in sorted(content)],
        [_index_row(content[key]) for key in sorted(content)],
        [
            {
                "artifact_id": item.artifact_id,
                "uri": uri_join(
                    files_root, captured.carriers[item.carrier_digest].relative_path
                ),
            }
            for item in bindings
        ],
    )
    schemas: tuple[dict[str, Any], ...] = (
        FileRefArtifact.POLARS_SCHEMA,
        ARTIFACT_INDEX_SCHEMA,
        ARTIFACT_LOCATIONS_SCHEMA,
    )
    paths = {}
    for index, (table, values, schema) in enumerate(
        zip(REGISTRATION_TABLES, rows, schemas, strict=True)
    ):
        path = str(Path(scratch) / f"raw-{index}.parquet")
        write_parquet_rows(path, values, schema)
        paths[table] = path
    return paths


def _index_row(content: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "artifact_id": content["artifact_id"],
        "artifact_type": "file_ref",
        "origin_step_number": content["origin_step_number"],
        "metadata": content["metadata"],
    }


def _verify_content(
    committer: RetainedCommitter,
    plan: InputRegistrationPlan,
    tables: Mapping[str, DiskTable],
) -> None:
    carriers = {item.digest: item for item in plan.carriers}
    content, index, locations = (tables[path] for path in REGISTRATION_TABLES)
    for item in plan.external_bindings:
        row = content.get_row((item.artifact_id,))
        artifact = FileRefArtifact.from_row(row)
        carrier = carriers[item.carrier_digest]
        if (artifact.content_hash, artifact.size_bytes) != (
            carrier.digest,
            carrier.size_bytes,
        ) or index.get_row((item.artifact_id,)) != _index_row(row):
            msg = "Registration carrier or index differs from finalized content"
            raise StoreIntegrityError(msg)
        uri = uri_join(committer.files_root, carrier.relative_path)
        if (item.artifact_id, uri) not in locations.keys:
            msg = "Registration location differs from its managed carrier"
            raise StoreIntegrityError(msg)
    for carrier in plan.carriers:
        with committer.fs.open(
            uri_join(committer.files_root, carrier.relative_path),
            "rb",
            cache_type="none",
        ) as stream:
            observed = compute_stream_digest(stream)
        if observed != (carrier.digest, carrier.size_bytes):
            msg = "Registered managed carrier bytes changed"
            raise StoreIntegrityError(msg)


def _preflight(
    committer: RetainedCommitter,
    plan: InputRegistrationPlan,
    tables: Mapping[str, DiskTable],
    controls: pl.DataFrame,
) -> None:
    complete = set(controls.filter(pl.col("state") == "complete")["logical_commit_id"])
    for table in plan.tables:
        snapshot = committer._snapshot(table.table_path)
        wanted = set(table.row_keys)
        for frame in snapshot.iter_frames([*table.natural_key, "logical_commit_id"]):
            for row in frame.iter_rows(named=True):
                key = tuple(row[name] for name in table.natural_key)
                owner = row["logical_commit_id"]
                if (
                    key in wanted
                    and owner != plan.logical_commit_id
                    and owner not in complete
                ):
                    msg = "Raw registration overlaps an incomplete foreign owner"
                    raise StoreIntegrityError(msg)
        missing_effect_keys(plan, table, tables[table.table_path], controls, snapshot)


def _verify_effects(
    committer: RetainedCommitter,
    plan: InputRegistrationPlan,
    controls: pl.DataFrame,
    *,
    read_snapshot: Callable[[str], DeltaSnapshot] | None = None,
) -> None:
    read = read_snapshot or committer._snapshot
    paths = set(retained_table_paths()) | {table.value for table in FRAMEWORK_SCHEMAS}
    paths.discard(TablePath.LOGICAL_COMMITS.value)
    for path in sorted(paths):
        exists = committer.fs.exists(uri_join(committer.delta_root, path, "_delta_log"))
        if not exists and plan.table(path) is not None:
            msg = "Registration is missing a declared table effect"
            raise StoreIntegrityError(msg)
        if exists:
            verify_snapshot_effect(plan, path, read(path), controls)


def _verify_retry(
    plan: InputRegistrationPlan,
    uris: tuple[str, ...],
    step_number: int,
    operation_name: str,
) -> None:
    if (
        tuple(item.uri for item in plan.inputs),
        plan.step_number,
        plan.operation_name,
    ) != (uris, step_number, operation_name):
        msg = "Registration retry changed its ordered acquisition or owner"
        raise StoreIntegrityError(msg)


def _continue() -> None:
    pass


def _ignore_checkpoint(_event: str) -> None:
    pass
