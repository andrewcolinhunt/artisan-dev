"""One controller's durable ordering of cancellation and result selections."""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

import polars as pl
from fsspec import AbstractFileSystem
from pydantic import ValidationError

from artisan.errors import StoreIntegrityError
from artisan.schemas.execution.admission import AdmissionReference
from artisan.schemas.execution.assignment import (
    AssignmentAttempt,
    AssignmentIdentity,
    DeliveryIdentity,
    SealedDelivery,
)
from artisan.schemas.execution.assignment_decision import AssignmentDecision
from artisan.storage.core.assignment_schema import (
    ASSIGNMENT_DECISIONS_PATH,
    ASSIGNMENT_DECISIONS_SCHEMA,
)
from artisan.storage.core.store_format import assert_retained_store
from artisan.storage.io.delta_files import DeltaSnapshot, append_table_files
from artisan.storage.io.retained_seal import CapturedRetainedDelivery
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest
from artisan.utils.path import uri_join


@dataclass(frozen=True)
class WinningDelivery:
    """A journal-authorized result; logical commit completion remains separate."""

    receipt: SealedDelivery
    decision_sequence: int


@dataclass(frozen=True)
class ReusedDelivery:
    """A current unit's durable selection of a separately accepted source."""

    receipt: SealedDelivery
    decision_sequence: int


@dataclass(frozen=True)
class AssignmentState:
    """Latest admitted attempt and its irreversible winner or reused source."""

    attempt: AssignmentAttempt
    admission_digest: str
    sequence: int
    cancelled: bool = False
    winner: WinningDelivery | None = None
    reuse: ReusedDelivery | None = None


class AssignmentJournal:
    """Serialize decisions through one controller instance for one store.

    Provider workers never write this journal. All acceptance and cancellation
    paths of the controller must share this instance. Concurrent independent
    controllers are outside the store's writer contract. Construction is read-only;
    candidate store initialization calls ``initialize`` explicitly.
    """

    def __init__(
        self,
        delta_root: str,
        *,
        fs: AbstractFileSystem,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        assert_retained_store(delta_root, fs)
        self._path = uri_join(delta_root, ASSIGNMENT_DECISIONS_PATH)
        self._fs = fs
        self._storage_options = storage_options or {}
        self._lock = threading.RLock()

    def initialize(self) -> None:
        """Create the private journal schema without activating legacy stores."""
        with self._lock:
            if not self._fs.exists(uri_join(self._path, "_delta_log")):
                pl.DataFrame(schema=ASSIGNMENT_DECISIONS_SCHEMA).write_delta(
                    self._path, storage_options=self._storage_options
                )
            self._snapshot()

    def state(self, assignment: AssignmentIdentity) -> AssignmentState | None:
        """Read and validate one unit's complete durable decision history."""
        with self._lock:
            return _read_state(self._snapshot(), assignment)

    def states(
        self, assignments: Sequence[AssignmentIdentity]
    ) -> Mapping[AssignmentIdentity, AssignmentState | None]:
        """Read queried histories once from a fresh locked snapshot, without caching."""
        with self._lock:
            return MappingProxyType(_read_states(self._snapshot(), assignments))

    def all_states(self) -> tuple[AssignmentState, ...]:
        """Validate every recorded history for explicit offline store inspection."""
        with self._lock:
            snapshot = self._snapshot()
            assignments = {
                _row_decision(row).attempt.assignment
                for frame in snapshot.iter_frames()
                for row in frame.iter_rows(named=True)
            }
            selected = _read_states(snapshot, tuple(assignments))
            return tuple(
                state
                for assignment in sorted(
                    assignments, key=lambda item: item.assignment_id
                )
                if (state := selected[assignment]) is not None
            )

    def admit(
        self, attempt: AssignmentAttempt, *, admission_digest: str
    ) -> AssignmentState:
        """Admit frozen work or supersede an unselected generation idempotently."""
        attempt = AssignmentAttempt.model_validate(attempt)
        with self._lock:
            snapshot = self._snapshot()
            state = _read_state(snapshot, attempt.assignment)
            if state is not None and state.attempt == attempt:
                if state.admission_digest != admission_digest:
                    msg = (
                        "Admitted input record changed for the same assignment attempt"
                    )
                    raise StoreIntegrityError(msg)
                return state
            event = AssignmentDecision(
                sequence=0 if state is None else state.sequence + 1,
                action="admit",
                attempt=attempt,
                admission_digest=admission_digest,
            )
            return self._append(snapshot, state, event)

    def cancel(self, attempt: AssignmentAttempt) -> AssignmentState:
        """Fence late results without revoking an existing winner or reused source."""
        attempt = AssignmentAttempt.model_validate(attempt)
        with self._lock:
            snapshot = self._snapshot()
            state = _require_attempt(_read_state(snapshot, attempt.assignment), attempt)
            if state.cancelled:
                return state
            return self._append(
                snapshot,
                state,
                AssignmentDecision(
                    sequence=state.sequence + 1,
                    action="cancel",
                    attempt=attempt,
                    admission_digest=state.admission_digest,
                ),
            )

    def claim(
        self, receipt: SealedDelivery, captured: CapturedRetainedDelivery
    ) -> WinningDelivery:
        """Claim only independently captured, complete evidence of the active unit.

        Capture must already have validated the controller's admitted recipe and
        boundary artifacts. Recovery instead requires the prior durable claim
        through ``require_winner``; discovering an orphan seal cannot create one.
        """
        receipt = SealedDelivery.model_validate(receipt)
        _validate_capture(receipt, captured)
        attempt = receipt.delivery.attempt
        with self._lock:
            snapshot = self._snapshot()
            state = _require_attempt(_read_state(snapshot, attempt.assignment), attempt)
            _require_unique_delivery(snapshot, receipt)
            if state.winner is not None:
                return _require_receipt(state.winner, receipt)
            result = self._append(
                snapshot,
                state,
                AssignmentDecision(
                    sequence=state.sequence + 1,
                    action="claim",
                    attempt=attempt,
                    admission_digest=state.admission_digest,
                    winner=receipt,
                ),
            )
            assert result.winner is not None
            return result.winner

    def require_winner(self, receipt: SealedDelivery) -> WinningDelivery:
        """Require exact prior authorization before commit, recovery or cache use."""
        receipt = SealedDelivery.model_validate(receipt)
        return self.require_winners((receipt,))[receipt]

    def require_winners(
        self, receipts: Sequence[SealedDelivery]
    ) -> Mapping[SealedDelivery, WinningDelivery]:
        """Prove queried winners and global delivery uniqueness in two fresh scans."""
        receipts = tuple(SealedDelivery.model_validate(receipt) for receipt in receipts)
        with self._lock:
            snapshot = self._snapshot()
            states = _read_states(
                snapshot,
                tuple(receipt.delivery.attempt.assignment for receipt in receipts),
            )
            winners = {}
            for receipt in receipts:
                state = states[receipt.delivery.attempt.assignment]
                if state is None or state.winner is None:
                    msg = "Retained delivery has no durable controller winner claim"
                    raise StoreIntegrityError(msg)
                winners[receipt] = _require_receipt(state.winner, receipt)
            _require_unique_deliveries(snapshot, receipts)
            return MappingProxyType(winners)

    def reuse(
        self,
        admission: AdmissionReference,
        receipt: SealedDelivery,
        *,
        verify_source: Callable[[AdmissionReference, SealedDelivery], None],
    ) -> ReusedDelivery:
        """Serialize an exactly verified cache selection against cancellation.

        The verifier must prove the proposed source's complete result and matching
        scientific identity for this admission. The journal then rechecks current
        ownership and the source winner under the same lock used by cancellation.
        """
        admission = AdmissionReference.model_validate(admission)
        receipt = SealedDelivery.model_validate(receipt)
        verify_source(admission, receipt)
        with self._lock:
            snapshot = self._snapshot()
            state = _require_admission(
                _read_state(snapshot, admission.attempt.assignment), admission
            )
            source = _read_state(snapshot, receipt.delivery.attempt.assignment)
            if source is None or source.winner is None:
                msg = "Reuse source has no durable controller winner claim"
                raise StoreIntegrityError(msg)
            _require_unique_delivery(snapshot, receipt)
            _require_receipt(source.winner, receipt)
            if state.reuse is not None:
                return _require_reuse_receipt(state.reuse, receipt)
            result = self._append(
                snapshot,
                state,
                AssignmentDecision(
                    sequence=state.sequence + 1,
                    action="reuse",
                    attempt=admission.attempt,
                    admission_digest=admission.record_digest,
                    cache_source=receipt,
                ),
            )
            assert result.reuse is not None
            return result.reuse

    def require_reuse(
        self, admission: AdmissionReference, receipt: SealedDelivery
    ) -> ReusedDelivery:
        """Require the exact durable current-unit selection, including after cancel.

        Visibility also requires independently rechecking the selected source's
        completed result. This method proves the decision, not its table effects.
        """
        admission = AdmissionReference.model_validate(admission)
        receipt = SealedDelivery.model_validate(receipt)
        with self._lock:
            state = _require_admission(
                _read_state(self._snapshot(), admission.attempt.assignment), admission
            )
            if state.reuse is None:
                msg = "Assignment has no durable cache reuse decision"
                raise StoreIntegrityError(msg)
            self.require_winner(receipt)
            return _require_reuse_receipt(state.reuse, receipt)

    def _snapshot(self) -> DeltaSnapshot:
        return DeltaSnapshot(
            self._path,
            ASSIGNMENT_DECISIONS_SCHEMA,
            fs=self._fs,
            storage_options=self._storage_options,
        )

    def _append(
        self,
        snapshot: DeltaSnapshot,
        state: AssignmentState | None,
        event: AssignmentDecision,
    ) -> AssignmentState:
        result = _apply_decision(state, event)
        append_table_files(
            snapshot,
            [pl.DataFrame([_decision_row(event)], schema=ASSIGNMENT_DECISIONS_SCHEMA)],
            step_number=0,
            statistics_columns=["assignment_id", "sequence"],
        )
        return result


def _read_state(
    snapshot: DeltaSnapshot, assignment: AssignmentIdentity
) -> AssignmentState | None:
    return _read_states(snapshot, (assignment,))[assignment]


def _read_states(
    snapshot: DeltaSnapshot, assignments: Sequence[AssignmentIdentity]
) -> dict[AssignmentIdentity, AssignmentState | None]:
    owners = {assignment.assignment_id: assignment for assignment in assignments}
    if len(owners) != len(set(assignments)):
        msg = "Distinct assignment identities have the same journal key"
        raise StoreIntegrityError(msg)
    if not owners:
        return {}
    events: dict[AssignmentIdentity, list[AssignmentDecision]] = {
        assignment: [] for assignment in assignments
    }
    keys = list(owners)
    for _, _, frame in snapshot.iter_located():
        selected = frame.filter(pl.col("assignment_id").is_in(keys))
        for row in selected.iter_rows(named=True):
            event = _row_decision(row)
            assignment = owners[row["assignment_id"]]
            if event.attempt.assignment != assignment:
                msg = "Assignment journal key disagrees with its logical unit"
                raise StoreIntegrityError(msg)
            events[assignment].append(event)
    states: dict[AssignmentIdentity, AssignmentState | None] = {}
    for assignment, history in events.items():
        state = None
        for event in sorted(history, key=lambda event: event.sequence):
            state = _apply_decision(state, event)
        states[assignment] = state
    return states


def _apply_decision(
    state: AssignmentState | None, event: AssignmentDecision
) -> AssignmentState:
    expected_sequence = 0 if state is None else state.sequence + 1
    if event.sequence != expected_sequence:
        msg = "Assignment journal has duplicate or missing decisions"
        raise StoreIntegrityError(msg)
    if event.action == "admit":
        if state is not None:
            _validate_retry(state, event.attempt)
        return AssignmentState(event.attempt, event.admission_digest, event.sequence)
    state = _require_attempt(state, event.attempt)
    if event.admission_digest != state.admission_digest:
        msg = "Assignment decision changed its admitted input record"
        raise StoreIntegrityError(msg)
    if event.action == "cancel" and not state.cancelled:
        return AssignmentState(
            state.attempt,
            state.admission_digest,
            event.sequence,
            True,
            state.winner,
            state.reuse,
        )
    if (
        event.action in {"claim", "reuse"}
        and not state.cancelled
        and state.winner is None
        and state.reuse is None
    ):
        if event.action == "reuse":
            assert event.cache_source is not None
            return AssignmentState(
                state.attempt,
                state.admission_digest,
                event.sequence,
                reuse=ReusedDelivery(event.cache_source, event.sequence),
            )
        assert event.winner is not None
        return AssignmentState(
            state.attempt,
            state.admission_digest,
            event.sequence,
            winner=WinningDelivery(event.winner, event.sequence),
        )
    msg = "Assignment journal contains an ineligible or repeated decision"
    raise StoreIntegrityError(msg)


def _validate_retry(state: AssignmentState, attempt: AssignmentAttempt) -> None:
    if (
        state.winner is not None
        or state.reuse is not None
        or attempt.generation <= state.attempt.generation
        or attempt.attempt < state.attempt.attempt
    ):
        msg = "A claimed, reused or obsolete assignment cannot acquire this attempt"
        raise StoreIntegrityError(msg)
    counters = {"attempt", "generation"}
    if attempt.model_dump(exclude=counters) != state.attempt.model_dump(
        exclude=counters
    ):
        msg = "Retry changed the frozen assignment identity"
        raise StoreIntegrityError(msg)


def _require_attempt(
    state: AssignmentState | None, attempt: AssignmentAttempt
) -> AssignmentState:
    if state is None or state.attempt != attempt:
        msg = "Assignment is unadmitted, obsolete or has mismatched frozen identity"
        raise StoreIntegrityError(msg)
    return state


def _require_admission(
    state: AssignmentState | None, admission: AdmissionReference
) -> AssignmentState:
    current = _require_attempt(state, admission.attempt)
    if current.admission_digest != admission.record_digest:
        msg = "Cache reuse changed the journal-bound admitted input record"
        raise StoreIntegrityError(msg)
    return current


def _require_reuse_receipt(
    reused: ReusedDelivery, receipt: SealedDelivery
) -> ReusedDelivery:
    if reused.receipt != receipt:
        msg = "Another source already owns this logical unit's reuse decision"
        raise StoreIntegrityError(msg)
    return reused


def _require_receipt(
    winner: WinningDelivery, receipt: SealedDelivery
) -> WinningDelivery:
    if winner.receipt != receipt:
        msg = "Another delivery already owns this logical unit"
        raise StoreIntegrityError(msg)
    return winner


def _require_unique_delivery(snapshot: DeltaSnapshot, receipt: SealedDelivery) -> None:
    _require_unique_deliveries(snapshot, (receipt,))


def _require_unique_deliveries(
    snapshot: DeltaSnapshot, receipts: Sequence[SealedDelivery]
) -> None:
    expected: dict[str, SealedDelivery] = {}
    for receipt in receipts:
        previous = expected.setdefault(receipt.delivery.delivery_id, receipt)
        if previous != receipt:
            msg = "Delivery identity is already claimed by another assignment"
            raise StoreIntegrityError(msg)
    if not expected:
        return
    identifiers = list(expected)
    for _, _, frame in snapshot.iter_located():
        selected = frame.filter(pl.col("delivery_id").is_in(identifiers))
        for row in selected.iter_rows(named=True):
            if _row_decision(row).winner != expected[row["delivery_id"]]:
                msg = "Delivery identity is already claimed by another assignment"
                raise StoreIntegrityError(msg)


def _validate_capture(
    receipt: SealedDelivery, captured: CapturedRetainedDelivery
) -> None:
    observed = compute_content_digest(
        canonical_json_bytes(captured.seal.model_dump(mode="json"))
    )
    if (
        captured.receipt != receipt
        or captured.seal.completion.delivery != receipt.delivery
        or observed != receipt.seal_digest
        or captured.projection.input_context_digest
        != receipt.delivery.attempt.input_context_digest
    ):
        msg = "Captured evidence does not match the claimed delivery and admission"
        raise StoreIntegrityError(msg)


def _decision_row(event: AssignmentDecision) -> dict[str, Any]:
    attempt = event.attempt
    return {
        "assignment_id": attempt.assignment.assignment_id,
        "sequence": event.sequence,
        "action": event.action,
        "admission_digest": event.admission_digest,
        **attempt.assignment.model_dump(),
        **attempt.model_dump(exclude={"assignment"}),
        "delivery_id": None
        if event.winner is None
        else event.winner.delivery.delivery_id,
        "seal_uri": None if event.winner is None else event.winner.seal_uri,
        "seal_digest": None if event.winner is None else event.winner.seal_digest,
        **_source_row(event.cache_source),
    }


def _source_row(receipt: SealedDelivery | None) -> dict[str, Any]:
    if receipt is None:
        return {
            name: None
            for name in ASSIGNMENT_DECISIONS_SCHEMA
            if name.startswith("source_")
        }
    attempt = receipt.delivery.attempt
    values = {
        **attempt.assignment.model_dump(),
        **attempt.model_dump(exclude={"assignment"}),
        "delivery_id": receipt.delivery.delivery_id,
        "seal_uri": receipt.seal_uri,
        "seal_digest": receipt.seal_digest,
    }
    return {f"source_{name}": value for name, value in values.items()}


def _source_receipt(row: dict[str, Any]) -> SealedDelivery | None:
    values = {
        name.removeprefix("source_"): value
        for name, value in row.items()
        if name.startswith("source_")
    }
    if all(value is None for value in values.values()):
        return None
    attempt = AssignmentAttempt.model_validate(
        {
            "assignment": {
                name: values[name] for name in ("step_run_id", "unit_index")
            },
            **{
                name: values[name]
                for name in AssignmentAttempt.model_fields
                if name != "assignment"
            },
        }
    )
    return SealedDelivery(
        delivery=DeliveryIdentity(attempt=attempt, delivery_id=values["delivery_id"]),
        seal_uri=values["seal_uri"],
        seal_digest=values["seal_digest"],
    )


def _row_decision(row: dict[str, Any]) -> AssignmentDecision:
    try:
        attempt = AssignmentAttempt.model_validate(
            {
                "assignment": {
                    name: row[name] for name in ("step_run_id", "unit_index")
                },
                **{
                    name: row[name]
                    for name in AssignmentAttempt.model_fields
                    if name != "assignment"
                },
            }
        )
        receipt = None
        if any(
            row[name] is not None for name in ("delivery_id", "seal_uri", "seal_digest")
        ):
            receipt = SealedDelivery(
                delivery=DeliveryIdentity(
                    attempt=attempt, delivery_id=row["delivery_id"]
                ),
                seal_uri=row["seal_uri"],
                seal_digest=row["seal_digest"],
            )
        if row["assignment_id"] != attempt.assignment.assignment_id:
            msg = "Assignment journal row has a mismatched logical unit key"
            raise StoreIntegrityError(msg)
        return AssignmentDecision(
            sequence=row["sequence"],
            action=row["action"],
            attempt=attempt,
            winner=receipt,
            admission_digest=row["admission_digest"],
            cache_source=_source_receipt(row),
        )
    except ValidationError as exc:
        msg = "Assignment journal contains a malformed controller decision"
        raise StoreIntegrityError(msg) from exc
