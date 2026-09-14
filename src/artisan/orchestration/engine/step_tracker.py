"""Authoritative persistence and reads for pipeline step lifecycle snapshots."""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import pairwise
from typing import Any, Literal, cast

import polars as pl
from deltalake import WriterProperties
from fsspec import AbstractFileSystem
from polars.datatypes import DataType, DataTypeClass

from artisan.errors import PersistenceIntegrityError
from artisan.schemas.enums import CachePolicy, TablePath
from artisan.schemas.orchestration.step_lifecycle import (
    TERMINAL_STEP_STATUSES,
    CancellationAcknowledgement,
    CancellationStatus,
    StepDisposition,
    StepStatus,
    validate_cancellation_transition,
    validate_step_transition,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.schemas.orchestration.step_state import StepState
from artisan.storage.core.run_scope import load_execution_membership
from artisan.storage.core.store_format import assert_store_format
from artisan.storage.core.table_schemas import STEPS_SCHEMA
from artisan.utils.path import uri_join

WRITER_PROPS = WriterProperties(compression="ZSTD")


@dataclass(frozen=True, slots=True)
class _WholeStepCacheHit:
    """Private whole-step hit flattened to actual persisted executions."""

    result: StepResult
    source_step_run_id: str
    execution_run_ids: tuple[str, ...]


class StepTracker:
    """Guard and persist every state snapshot for one Delta store."""

    def __init__(
        self,
        delta_root: str,
        pipeline_run_id: str = "",
        storage_options: dict[str, str] | None = None,
        fs: AbstractFileSystem | None = None,
    ) -> None:
        """Initialize a tracker for one store and optional pipeline run."""
        if fs is None:
            from fsspec.implementations.local import LocalFileSystem

            fs = LocalFileSystem()
        self._fs = fs
        self._delta_root = delta_root
        self._steps_path = uri_join(delta_root, TablePath.STEPS)
        self._pipeline_run_id = pipeline_run_id
        self._storage_options = storage_options
        self._lock = threading.RLock()
        assert_store_format(delta_root, self._fs, self._storage_options)

    def create_attempt(self, record: StepStartRecord) -> StepState:
        """Persist sequence zero as pending, idempotently."""
        with self._lock:
            existing = self._rows_for_attempt(record.step_run_id)
            if not existing.is_empty():
                current = self._validate_attempt_rows(existing)
                if self._record_matches(current, record):
                    return current
                msg = f"Conflicting create_attempt retry for {record.step_run_id}"
                raise PersistenceIntegrityError(msg)
            row = self._base_row(record, StepStatus.PENDING, state_sequence=0)
            self._write_row(row)
            return self._row_to_state(row)

    def transition(
        self,
        step_run_id: str,
        expected: StepStatus,
        target: StepStatus,
        *,
        step_spec_id: str | None = None,
        result: StepResult | None = None,
        error: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> StepState:
        """Validate and append one lifecycle transition.

        Identical terminal retries return the durable state. A stale writer,
        changed retry payload, or transition from a terminal state fails closed.
        """
        with self._lock:
            rows = self._rows_for_attempt(step_run_id)
            if rows.is_empty():
                msg = f"Unknown step attempt {step_run_id}"
                raise PersistenceIntegrityError(msg)
            current = self._validate_attempt_rows(rows)
            if current.status in TERMINAL_STEP_STATUSES:
                if current.status == target and self._terminal_retry_matches(
                    current,
                    result,
                    error,
                    step_spec_id,
                    metadata,
                ):
                    return current
                msg = f"Step attempt {step_run_id} is already {current.status.value}"
                raise PersistenceIntegrityError(msg)
            if current.status != expected:
                msg = (
                    f"Stale step transition for {step_run_id}: expected "
                    f"{expected.value}, found {current.status.value}"
                )
                raise PersistenceIntegrityError(msg)
            try:
                validate_step_transition(current.status, target)
            except ValueError as exc:
                raise PersistenceIntegrityError(str(exc)) from exc

            row = self._state_to_row(current)
            row.update(
                status=target.value,
                state_sequence=self._next_physical_sequence(rows),
                timestamp=datetime.now(UTC),
            )
            if step_spec_id is not None:
                row["step_spec_id"] = step_spec_id
            if target == StepStatus.RUNNING:
                self._validate_nonterminal_row(row)
            else:
                self._apply_terminal_result(row, target, result, error, metadata)
            self._write_row(row)
            return self._row_to_state(row)

    def record_cancellation(
        self,
        step_run_id: str,
        expected: StepStatus,
        outcome: CancellationAcknowledgement,
    ) -> StepState:
        """Append cancellation evidence without changing lifecycle status."""
        with self._lock:
            rows = self._rows_for_attempt(step_run_id)
            if rows.is_empty():
                msg = f"Unknown step attempt {step_run_id}"
                raise PersistenceIntegrityError(msg)
            current = self._validate_attempt_rows(rows)
            if current.status in TERMINAL_STEP_STATUSES:
                return current
            if current.status != expected:
                msg = (
                    f"Stale cancellation update for {step_run_id}: expected "
                    f"{expected.value}, found {current.status.value}"
                )
                raise PersistenceIntegrityError(msg)
            if current.cancellation_status == outcome.status:
                return current
            try:
                validate_cancellation_transition(
                    current.cancellation_status,
                    outcome.status,
                )
            except ValueError as exc:
                raise PersistenceIntegrityError(str(exc)) from exc
            row = self._state_to_row(current)
            row.update(
                state_sequence=self._next_physical_sequence(rows),
                cancellation_status=outcome.status.value,
                timestamp=datetime.now(UTC),
            )
            if outcome.status != CancellationStatus.REQUESTED and outcome.message:
                row["error"] = outcome.message
            self._write_row(row)
            return self._row_to_state(row)

    def current_state(self, step_run_id: str) -> StepState:
        """Return the unique latest authoritative snapshot for an attempt."""
        rows = self._rows_for_attempt(step_run_id)
        if rows.is_empty():
            msg = f"Unknown step attempt {step_run_id}"
            raise PersistenceIntegrityError(msg)
        return self._validate_attempt_rows(rows)

    def load_current_states(
        self, pipeline_run_id: str | None = None
    ) -> list[StepState]:
        """Load the latest authoritative attempt at every logical position."""
        states = self._current_states(self._read_rows())
        if not states:
            return []
        run_id = (
            pipeline_run_id
            or max(states, key=lambda state: state.timestamp).pipeline_run_id
        )
        run_states = [state for state in states if state.pipeline_run_id == run_id]
        return sorted(
            self._latest_by_position(run_states), key=lambda state: state.step_number
        )

    def load_all_current_states(self) -> list[StepState]:
        """Load the authoritative current snapshot for every exact attempt."""
        return self._current_states(self._read_rows())

    def load_resumable_steps(
        self, pipeline_run_id: str | None = None
    ) -> list[StepState]:
        """Load usable terminal states and reject unresolved attempts."""
        states = self.load_current_states(pipeline_run_id)
        unresolved = [
            state
            for state in states
            if state.status in {StepStatus.PENDING, StepStatus.RUNNING}
        ]
        if unresolved:
            ids = [state.step_run_id for state in unresolved]
            msg = f"Pipeline run has unresolved step attempts: {ids!r}"
            raise PersistenceIntegrityError(msg)
        resumable = {
            StepStatus.SUCCEEDED,
            StepStatus.PARTIAL,
            StepStatus.SKIPPED,
        }
        return [state for state in states if state.status in resumable]

    def check_cache(
        self,
        step_spec_id: str,
        cache_policy: CachePolicy = CachePolicy.ALL_SUCCEEDED,
    ) -> _WholeStepCacheHit | None:
        """Return the newest eligible step attempt for a spec."""
        states = [
            state
            for state in self._current_states(self._read_rows())
            if state.step_spec_id == step_spec_id
        ]
        eligible = {StepStatus.SUCCEEDED}
        if cache_policy == CachePolicy.STEP_COMPLETED:
            eligible.add(StepStatus.PARTIAL)
        states = [state for state in states if state.status in eligible]
        if not states:
            return None
        state = max(states, key=lambda candidate: candidate.timestamp)
        membership = load_execution_membership(
            self._delta_root,
            fs=self._fs,
            storage_options=self._storage_options,
            step_run_id=state.step_run_id,
        )
        execution_run_ids = tuple(sorted(set(membership["execution_run_id"].to_list())))
        if not execution_run_ids:
            msg = f"Whole-step cache source {state.step_run_id} has no executions"
            raise PersistenceIntegrityError(msg)
        return _WholeStepCacheHit(
            result=state.to_step_result(),
            source_step_run_id=state.step_run_id,
            execution_run_ids=execution_run_ids,
        )

    def list_runs(self) -> pl.DataFrame:
        """Roll up runs from authoritative lifecycle snapshots."""
        schema: dict[str, DataType | DataTypeClass] = {
            "pipeline_run_id": pl.String,
            "step_count": pl.UInt32,
            "last_status": pl.String,
            "started_at": pl.Datetime("us", "UTC"),
            "ended_at": pl.Datetime("us", "UTC"),
        }
        rows = self._read_rows()
        if rows.is_empty():
            return pl.DataFrame(schema=schema)
        states = self._current_states(rows)
        result: list[dict[str, Any]] = []
        for run_id in {state.pipeline_run_id for state in states}:
            latest_positions = self._latest_by_position(
                [state for state in states if state.pipeline_run_id == run_id]
            )
            latest = max(latest_positions, key=lambda state: state.timestamp)
            started_at = rows.filter(
                (pl.col("pipeline_run_id") == run_id)
                & (pl.col("status") == StepStatus.PENDING.value)
            )["timestamp"].min()
            active = any(
                state.status in {StepStatus.PENDING, StepStatus.RUNNING}
                for state in latest_positions
            )
            ended_at = (
                None if active else max(state.timestamp for state in latest_positions)
            )
            result.append(
                {
                    "pipeline_run_id": run_id,
                    "step_count": len(
                        {state.step_number for state in latest_positions}
                    ),
                    "last_status": latest.status.value,
                    "started_at": started_at,
                    "ended_at": ended_at,
                }
            )
        return pl.DataFrame(result, schema=schema).sort("started_at", descending=True)

    def _rows_for_attempt(self, step_run_id: str) -> pl.DataFrame:
        """Read every physical snapshot for one attempt."""
        rows = self._read_rows()
        return (
            rows
            if rows.is_empty()
            else rows.filter(pl.col("step_run_id") == step_run_id)
        )

    def _read_rows(self) -> pl.DataFrame:
        """Read the steps table or return an empty physical-schema frame."""
        if not self._fs.exists(self._steps_path):
            return pl.DataFrame(schema=STEPS_SCHEMA)
        rows = pl.scan_delta(
            self._steps_path, storage_options=self._storage_options
        ).collect()
        if "state_sequence" not in rows.columns or rows["state_sequence"].null_count():
            msg = "Steps table contains lifecycle rows without state_sequence"
            raise PersistenceIntegrityError(msg)
        if not rows.filter(pl.col("status") == "completed").is_empty():
            msg = "Steps table contains unsupported legacy status 'completed'"
            raise PersistenceIntegrityError(msg)
        try:
            for value in rows["status"].unique().to_list():
                StepStatus(value)
        except (TypeError, ValueError) as exc:
            msg = f"Steps table contains an unknown lifecycle status: {exc}"
            raise PersistenceIntegrityError(msg) from exc
        return rows

    def _current_states(self, rows: pl.DataFrame) -> list[StepState]:
        """Validate all attempts and return one current state per attempt."""
        if rows.is_empty():
            return []
        return [
            self._validate_attempt_rows(group)
            for group in rows.partition_by("step_run_id", maintain_order=True)
        ]

    def _validate_attempt_rows(self, rows: pl.DataFrame) -> StepState:
        """Validate sequence/history invariants and select the current row."""
        # D5 will authorize non-null logical_commit_id rows through its control
        # table. Until that owner lands, only direct D4 snapshots are current.
        physical_sequences = sorted(set(rows["state_sequence"].to_list()))
        if physical_sequences != list(range(physical_sequences[-1] + 1)):
            msg = f"Step attempt {rows['step_run_id'][0]} has a sequence gap"
            raise PersistenceIntegrityError(msg)
        for sequence in physical_sequences:
            candidates = rows.filter(pl.col("state_sequence") == sequence)
            if candidates.unique(maintain_order=True).height != 1:
                msg = (
                    f"Step attempt {rows['step_run_id'][0]} has conflicting "
                    f"snapshots at sequence {sequence}"
                )
                raise PersistenceIntegrityError(msg)

        authoritative = rows.filter(pl.col("logical_commit_id").is_null())
        if authoritative.is_empty():
            msg = f"Step attempt {rows['step_run_id'][0]} has no authoritative state"
            raise PersistenceIntegrityError(msg)
        sequences = sorted(set(authoritative["state_sequence"].to_list()))
        snapshots: list[dict[str, Any]] = []
        for sequence in sequences:
            candidates = authoritative.filter(pl.col("state_sequence") == sequence)
            unique = candidates.unique(maintain_order=True)
            if unique.height != 1:
                msg = (
                    f"Step attempt {rows['step_run_id'][0]} has conflicting "
                    f"snapshots at sequence {sequence}"
                )
                raise PersistenceIntegrityError(msg)
            snapshots.append(unique.row(0, named=True))
        self._validate_history(snapshots)
        states = [self._validated_snapshot(row) for row in snapshots]
        return states[-1]

    @classmethod
    def _validated_snapshot(cls, row: dict[str, Any]) -> StepState:
        """Deserialize one row and enforce its status-specific facts."""
        try:
            state = cls._row_to_state(row)
            if state.status in TERMINAL_STEP_STATUSES:
                state.to_step_result()
            else:
                cls._validate_nonterminal_row(row)
        except PersistenceIntegrityError:
            raise
        except (TypeError, ValueError) as exc:
            msg = (
                f"Step attempt {row.get('step_run_id')} has an invalid "
                f"{row.get('status')} snapshot: {exc}"
            )
            raise PersistenceIntegrityError(msg) from exc
        return state

    @staticmethod
    def _next_physical_sequence(rows: pl.DataFrame) -> int:
        """Return the sequence after every direct or staged physical row."""
        return cast(int, rows["state_sequence"].max()) + 1

    @staticmethod
    def _validate_history(rows: list[dict[str, Any]]) -> None:
        """Validate owner stability and every lifecycle/ack edge."""
        first = rows[0]
        if StepStatus(first["status"]) != StepStatus.PENDING:
            msg = "Step lifecycle must start at pending"
            raise PersistenceIntegrityError(msg)
        owner_fields = (
            "pipeline_run_id",
            "step_number",
            "step_name",
            "operation_class",
            "params_json",
            "input_refs_json",
            "compute_backend",
            "compute_options_json",
        )
        for previous, current in pairwise(rows):
            if any(previous[field] != current[field] for field in owner_fields):
                msg = f"Step attempt {first['step_run_id']} changed owner metadata"
                raise PersistenceIntegrityError(msg)
            try:
                current_status = StepStatus(current["status"])
                if current_status in {
                    StepStatus.PENDING,
                    StepStatus.RUNNING,
                    StepStatus.SUCCEEDED,
                    StepStatus.PARTIAL,
                } and (
                    json.loads(current["output_roles_json"])
                    != json.loads(first["output_roles_json"])
                    or json.loads(current["output_types_json"])
                    != json.loads(first["output_types_json"])
                ):
                    msg = f"Step attempt {first['step_run_id']} changed output contract"
                    raise PersistenceIntegrityError(msg)
            except (TypeError, json.JSONDecodeError) as exc:
                msg = f"Step attempt {first['step_run_id']} has invalid output metadata"
                raise PersistenceIntegrityError(msg) from exc
            try:
                old_status = StepStatus(previous["status"])
                new_status = StepStatus(current["status"])
                old_cancellation = _parse_cancellation(previous["cancellation_status"])
                new_cancellation = _parse_cancellation(current["cancellation_status"])
                status_changed = old_status != new_status
                cancellation_changed = old_cancellation != new_cancellation
                if status_changed and cancellation_changed:
                    msg = (
                        "One lifecycle snapshot cannot change status and "
                        "cancellation evidence together"
                    )
                    raise ValueError(msg)
                if status_changed:
                    validate_step_transition(old_status, new_status)
                elif cancellation_changed:
                    validate_cancellation_transition(
                        old_cancellation,
                        new_cancellation,  # type: ignore[arg-type]
                    )
                else:
                    msg = "Lifecycle snapshots must change status or cancellation evidence"
                    raise ValueError(msg)
            except ValueError as exc:
                raise PersistenceIntegrityError(str(exc)) from exc
            previous_spec = previous["step_spec_id"]
            if previous_spec is not None and current["step_spec_id"] != previous_spec:
                msg = f"Step attempt {first['step_run_id']} changed its step spec ID"
                raise PersistenceIntegrityError(msg)

    def _base_row(
        self,
        record: StepStartRecord,
        status: StepStatus,
        *,
        state_sequence: int,
    ) -> dict[str, Any]:
        """Build the common physical snapshot shape."""
        return {
            "step_run_id": record.step_run_id,
            "step_spec_id": record.step_spec_id,
            "pipeline_run_id": self._pipeline_run_id,
            "step_number": record.step_number,
            "step_name": record.step_name,
            "status": status.value,
            "state_sequence": state_sequence,
            "disposition": None,
            "cancellation_status": None,
            "logical_commit_id": None,
            "operation_class": record.operation_class,
            "params_json": record.params_json,
            "input_refs_json": record.input_refs_json,
            "compute_backend": record.compute_backend,
            "compute_options_json": record.compute_options_json,
            "output_roles_json": record.output_roles_json,
            "output_types_json": record.output_types_json,
            "total_count": None,
            "succeeded_count": None,
            "failed_count": None,
            "timestamp": datetime.now(UTC),
            "duration_seconds": None,
            "error": None,
            "metadata": None,
        }

    @staticmethod
    def _apply_terminal_result(
        row: dict[str, Any],
        target: StepStatus,
        result: StepResult | None,
        error: str | None,
        metadata: dict[str, Any] | None,
    ) -> None:
        """Apply and cross-check the terminal payload."""
        if result is None:
            msg = f"Terminal transition to {target.value} requires StepResult"
            raise PersistenceIntegrityError(msg)
        if result.status != target:
            msg = (
                f"Terminal result status {result.status.value} does not match "
                f"transition target {target.value}"
            )
            raise PersistenceIntegrityError(msg)
        if (
            result.step_run_id != row["step_run_id"]
            or result.step_number != row["step_number"]
            or result.step_name != row["step_name"]
        ):
            msg = "Terminal result does not identify the current step attempt"
            raise PersistenceIntegrityError(msg)
        result_cancellation = (
            result.cancellation_status.value if result.cancellation_status else None
        )
        if result_cancellation != row["cancellation_status"]:
            msg = (
                "Terminal cancellation evidence must match the previously "
                "persisted acknowledgement"
            )
            raise PersistenceIntegrityError(msg)
        row.update(
            disposition=(result.disposition.value if result.disposition else None),
            cancellation_status=result_cancellation,
            output_roles_json=json.dumps(sorted(result.output_roles)),
            output_types_json=json.dumps(result.output_types),
            total_count=result.total_count,
            succeeded_count=result.succeeded_count,
            failed_count=result.failed_count,
            duration_seconds=result.duration_seconds,
            error=error or result.error or row["error"],
            metadata=json.dumps(metadata or result.metadata)
            if (metadata or result.metadata)
            else None,
        )
        if (
            target in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}
            and not row["step_spec_id"]
        ):
            msg = f"{target.value} requires a concrete step_spec_id"
            raise PersistenceIntegrityError(msg)
        if target == StepStatus.FAILED and not row["error"]:
            msg = "failed requires an error"
            raise PersistenceIntegrityError(msg)
        if (
            target == StepStatus.CANCELLED
            and row["cancellation_status"] != CancellationStatus.CONFIRMED.value
        ):
            msg = "cancelled requires confirmed cancellation evidence"
            raise PersistenceIntegrityError(msg)

    @staticmethod
    def _validate_nonterminal_row(row: dict[str, Any]) -> None:
        """Validate that a nonterminal snapshot carries no terminal facts."""
        for field in ("total_count", "succeeded_count", "failed_count", "disposition"):
            if row[field] is not None:
                msg = f"nonterminal snapshot cannot carry {field}"
                raise PersistenceIntegrityError(msg)

    @staticmethod
    def _record_matches(state: StepState, record: StepStartRecord) -> bool:
        """Return whether an existing pending attempt matches its retry."""
        return (
            state.status == StepStatus.PENDING
            and state.state_sequence == 0
            and state.step_run_id == record.step_run_id
            and state.step_spec_id == record.step_spec_id
            and state.step_number == record.step_number
            and state.step_name == record.step_name
            and state.operation_class == record.operation_class
            and state.params_json == record.params_json
            and state.input_refs_json == record.input_refs_json
            and state.compute_backend == record.compute_backend
            and state.compute_options_json == record.compute_options_json
            and state.output_roles == frozenset(json.loads(record.output_roles_json))
            and state.output_types == json.loads(record.output_types_json)
        )

    @staticmethod
    def _terminal_retry_matches(
        current: StepState,
        result: StepResult | None,
        error: str | None,
        step_spec_id: str | None,
        metadata: dict[str, Any] | None,
    ) -> bool:
        """Return whether a repeated terminal request has identical facts."""
        if result is None:
            return False
        return (
            current.status == result.status
            and current.step_run_id == result.step_run_id
            and current.step_number == result.step_number
            and current.step_name == result.step_name
            and current.disposition == result.disposition
            and current.cancellation_status == result.cancellation_status
            and current.total_count == result.total_count
            and current.succeeded_count == result.succeeded_count
            and current.failed_count == result.failed_count
            and current.error == (error or result.error)
            and current.step_spec_id == (step_spec_id or current.step_spec_id)
            and current.output_roles == result.output_roles
            and current.output_types == result.output_types
            and current.duration_seconds == result.duration_seconds
            and current.metadata == (metadata or result.metadata)
        )

    def _write_row(self, row: dict[str, Any]) -> None:
        """Append one physical snapshot."""
        df = pl.DataFrame([row], schema=STEPS_SCHEMA)
        mode: Literal["append", "overwrite"] = (
            "append" if self._fs.exists(self._steps_path) else "overwrite"
        )
        delta_write_options: dict[str, Any] = {"writer_properties": WRITER_PROPS}
        df.write_delta(
            self._steps_path,
            mode=mode,
            storage_options=self._storage_options,
            delta_write_options=delta_write_options,
        )

    @staticmethod
    def _row_to_state(row: dict[str, Any]) -> StepState:
        """Deserialize one physical lifecycle row."""
        metadata = json.loads(row["metadata"]) if row.get("metadata") else {}
        return StepState(
            pipeline_run_id=row["pipeline_run_id"],
            step_run_id=row["step_run_id"],
            step_number=row["step_number"],
            step_name=row["step_name"],
            step_spec_id=row["step_spec_id"],
            status=StepStatus(row["status"]),
            state_sequence=row["state_sequence"],
            disposition=_parse_disposition(row.get("disposition")),
            cancellation_status=_parse_cancellation(row.get("cancellation_status")),
            operation_class=row["operation_class"],
            params_json=row["params_json"],
            input_refs_json=row["input_refs_json"],
            compute_backend=row["compute_backend"],
            compute_options_json=row["compute_options_json"],
            total_count=row["total_count"],
            succeeded_count=row["succeeded_count"],
            failed_count=row["failed_count"],
            timestamp=row["timestamp"],
            duration_seconds=row["duration_seconds"],
            error=row["error"],
            metadata=metadata,
            output_roles=frozenset(json.loads(row["output_roles_json"])),
            output_types=json.loads(row["output_types_json"]),
        )

    @staticmethod
    def _state_to_row(state: StepState) -> dict[str, Any]:
        """Serialize a state for the next immutable snapshot."""
        return {
            "step_run_id": state.step_run_id,
            "step_spec_id": state.step_spec_id,
            "pipeline_run_id": state.pipeline_run_id,
            "step_number": state.step_number,
            "step_name": state.step_name,
            "status": state.status.value,
            "state_sequence": state.state_sequence,
            "disposition": state.disposition.value if state.disposition else None,
            "cancellation_status": (
                state.cancellation_status.value if state.cancellation_status else None
            ),
            "logical_commit_id": None,
            "operation_class": state.operation_class,
            "params_json": state.params_json,
            "input_refs_json": state.input_refs_json,
            "compute_backend": state.compute_backend,
            "compute_options_json": state.compute_options_json,
            "output_roles_json": json.dumps(sorted(state.output_roles)),
            "output_types_json": json.dumps(state.output_types),
            "total_count": state.total_count,
            "succeeded_count": state.succeeded_count,
            "failed_count": state.failed_count,
            "timestamp": state.timestamp,
            "duration_seconds": state.duration_seconds,
            "error": state.error,
            "metadata": json.dumps(state.metadata) if state.metadata else None,
        }

    @staticmethod
    def _latest_by_position(states: list[StepState]) -> list[StepState]:
        """Select the latest attempt at each logical step number."""
        latest: dict[int, StepState] = {}
        for state in states:
            previous = latest.get(state.step_number)
            if previous is None or state.timestamp > previous.timestamp:
                latest[state.step_number] = state
        return list(latest.values())


def _parse_disposition(value: object) -> StepDisposition | None:
    """Parse a nullable persisted disposition."""
    return StepDisposition(value) if isinstance(value, str) else None


def _parse_cancellation(value: object) -> CancellationStatus | None:
    """Parse a nullable persisted cancellation acknowledgement."""
    return CancellationStatus(value) if isinstance(value, str) else None
