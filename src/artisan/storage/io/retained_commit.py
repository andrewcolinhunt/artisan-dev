"""Controller acceptance and recovery of one complete retained assignment."""

from __future__ import annotations

import os
import posixpath
import threading
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from types import MappingProxyType
from typing import Any, cast

import polars as pl
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.external import validate_persistable_uri
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.admission import AdmissionReference, ManagedCarrier
from artisan.schemas.execution.assignment import AssignmentIdentity, SealedDelivery
from artisan.schemas.execution.retained_result import (
    ExternalCarrier,
    RetainedResultSeal,
    RetainedTableFile,
)
from artisan.schemas.orchestration.output_reference import StoreIdentity
from artisan.schemas.provenance.projection import (
    AcceptedCuratorOccurrence,
    AcceptedProducer,
    BoundaryProjection,
    ProjectedOccurrence,
)
from artisan.storage.core.committed_scan import verify_snapshot_effect
from artisan.storage.core.store_format import assert_retained_store
from artisan.storage.core.table_schemas import get_physical_schema_for_path
from artisan.storage.io.admission import capture_admission
from artisan.storage.io.assignment_journal import AssignmentJournal, WinningDelivery
from artisan.storage.io.bounded_evidence import DiskTable
from artisan.storage.io.curator_control import verify_curator_controls
from artisan.storage.io.curator_plan import (
    CuratorCommitPlan,
    CuratorPlan,
    read_curator_plan,
)
from artisan.storage.io.curator_reuse_plan import CuratorReusePlan
from artisan.storage.io.delta_files import DeltaSnapshot, append_table_files
from artisan.storage.io.input_registration_plan import (
    InputRegistrationPlan,
    read_input_registration,
)
from artisan.storage.io.logical_control import (
    complete_control,
    control_for,
    planned_control,
    validate_control_rows,
)
from artisan.storage.io.publication import publish_immutable_file
from artisan.storage.io.retained_effects import (
    capture_retained_plan,
    prepare_retained_plan,
    read_retained_plan,
    retained_plan_directory,
)
from artisan.storage.io.retained_plan import RetainedCommitPlan, retained_table_paths
from artisan.storage.io.retained_read import RetainedControlProof, RetainedReadSnapshot
from artisan.storage.io.retained_seal import capture_retained_delivery
from artisan.storage.io.step_result_plan import (
    StepResultPlan,
    WholeStepReuse,
    read_step_result_plan,
    verify_step_decisions,
)
from artisan.storage.io.table_effect import apply_table_effect
from artisan.utils.hashing import compute_content_digest, compute_stream_digest
from artisan.utils.path import uri_join


def _identity_root(root: str, fs: AbstractFileSystem) -> str:
    """Resolve local aliases without rewriting remote object-key prefixes."""
    if isinstance(fs, LocalFileSystem):
        return os.path.realpath(cast(str, fs._strip_protocol(root)))
    return root.removesuffix("/")


class RetainedCommitter:
    """Apply durable winners through the same exact effects as ordinary commits.

    The controller supplies its shared journal instance. Constructors are read-only;
    ``initialize`` creates only this private domain's tables. Orphan seals never
    acquire acceptance merely because recovery discovers their files.
    """

    def __init__(
        self,
        delta_root: str,
        files_root: str,
        journal: AssignmentJournal,
        *,
        fs: AbstractFileSystem,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        assert_retained_store(delta_root, fs)
        validate_persistable_uri(files_root)
        self.delta_root = delta_root
        self.files_root = files_root
        self.journal = journal
        self.fs = fs
        self.storage_options = storage_options or {}
        self._lock = threading.RLock()
        self._store_identity: StoreIdentity | None = None

    @property
    def store_identity(self) -> StoreIdentity:
        """Return the source binding captured from validated lifecycle metadata."""
        with self._lock:
            if self._store_identity is None:
                self._snapshot(TablePath.STEPS.value)
            assert self._store_identity is not None
            return self._store_identity

    def initialize(self) -> None:
        """Create unpartitioned candidate tables, preserving mixed original origins."""
        for table_path in (*retained_table_paths(), TablePath.LOGICAL_COMMITS.value):
            path = uri_join(self.delta_root, table_path)
            if not self.fs.exists(uri_join(path, "_delta_log")):
                pl.DataFrame(
                    schema=get_physical_schema_for_path(table_path)
                ).write_delta(path, storage_options=self.storage_options)
            self._snapshot(table_path)

    def accept(
        self, receipt: SealedDelivery, *, preserve_staging: bool = False
    ) -> RetainedCommitPlan:
        """Capture the admitted contract, claim one delivery and complete its effects."""
        with self._lock:
            return self._accept(receipt, preserve_staging=preserve_staging)

    def _accept(
        self, receipt: SealedDelivery, *, preserve_staging: bool
    ) -> RetainedCommitPlan:
        receipt = SealedDelivery.model_validate(receipt)
        assignment = receipt.delivery.attempt.assignment
        state = self.journal.state(assignment)
        if state is None or state.attempt != receipt.delivery.attempt:
            msg = "Retained acceptance requires the current admitted assignment"
            raise StoreIntegrityError(msg)
        reference = AdmissionReference(
            attempt=state.attempt, record_digest=state.admission_digest
        )
        plan = read_retained_plan(self.delta_root, assignment, self.fs)
        if plan is not None:
            self._require_plan_owner(plan, receipt, reference)
            controls = self._controls()
            control = control_for(plan, controls)
            if control is not None and control["state"] == "complete":
                self._verify_complete(plan, controls)
                if not preserve_staging:
                    self._cleanup(plan)
                return plan
        with (
            capture_admission(
                reference,
                delta_root=self.delta_root,
                files_root=self.files_root,
                fs=self.fs,
                verify_producer=self._verify_producer,
            ) as admission,
            capture_retained_delivery(
                receipt,
                admission.artifacts,
                expected_recipe=admission.record.recipe,
                fs=self.fs,
            ) as captured,
        ):
            winner = self.journal.claim(receipt, captured)
            self._checkpoint("claimed", receipt, None)
            plan = prepare_retained_plan(
                captured,
                admission,
                winner,
                delta_root=self.delta_root,
                files_root=self.files_root,
                fs=self.fs,
            )
            self._require_plan_owner(plan, receipt, reference)
            self._checkpoint("plan", receipt, None)
            for carrier in captured.seal.carriers:
                managed = ManagedCarrier(
                    digest=carrier.digest, size_bytes=carrier.size_bytes
                )
                publish_immutable_file(
                    self.fs,
                    uri_join(self.files_root, managed.relative_path),
                    captured.carriers[carrier.filename],
                    source_fs=LocalFileSystem(),
                )
                self._checkpoint("carrier", receipt, carrier.filename)
            self._verify_carriers(plan)
            with capture_retained_plan(
                plan, delta_root=self.delta_root, fs=self.fs
            ) as evidence:
                self._apply(plan, evidence.tables)
        if not preserve_staging:
            self._cleanup(plan)
        return plan

    def recover(
        self, assignment: AssignmentIdentity, *, preserve_staging: bool = False
    ) -> RetainedCommitPlan:
        """Resume an already claimed winner, including a claim with no plan yet."""
        state = self.journal.state(assignment)
        if state is None or state.winner is None:
            msg = "Recovery requires a durable winner; orphan seals are diagnostic only"
            raise StoreIntegrityError(msg)
        return self.accept(state.winner.receipt, preserve_staging=preserve_staging)

    def accepted_projection(self, receipt: SealedDelivery) -> BoundaryProjection:
        """Read exact occurrences only after winner and logical completion agree."""
        with self._read_snapshot() as snapshot:
            return snapshot.projection(receipt)

    @contextmanager
    def _read_snapshot(self) -> Iterator[RetainedReadSnapshot]:
        """Keep a fresh proof private to one finite read under the write lock."""
        with self._lock:
            yield RetainedReadSnapshot(self, self._control_proof())

    def _verify_producer(
        self,
        producer: AcceptedProducer,
        boundary: ProjectedOccurrence,
        *,
        _read: RetainedReadSnapshot | None = None,
    ) -> None:
        if _read is None:
            # Remote directory listings can outlive another process's commits.
            with self._read_snapshot() as read:
                self._verify_producer(producer, boundary, _read=read)
            return
        _read.require_committer(self)
        if isinstance(producer, AcceptedCuratorOccurrence):
            from artisan.storage.core.curator_reader import CuratorReader

            reader = CuratorReader(self)
            result = reader._read(producer, _read)
            artifact = result.artifacts.get(producer.address)
            if artifact is None or (artifact.artifact_id, artifact.artifact_type) != (
                boundary.artifact_id,
                boundary.artifact_type,
            ):
                msg = "Curator producer disagrees with admitted content"
                raise StoreIntegrityError(msg)
            return
        plan = _read.plan_for_delivery(producer.delivery_id)
        if plan.receipt.seal_digest != producer.seal_digest:
            msg = "Selected producer has another complete seal"
            raise StoreIntegrityError(msg)
        projection = _read.projection(plan.receipt)
        selected = next(
            (
                item
                for item in projection.occurrences
                if item.address == producer.address
            ),
            None,
        )
        if selected is None or (selected.artifact_id, selected.artifact_type) != (
            boundary.artifact_id,
            boundary.artifact_type,
        ):
            msg = "Selected producer is absent or disagrees with admitted content"
            raise StoreIntegrityError(msg)

    def _require_plan_owner(
        self,
        plan: RetainedCommitPlan,
        receipt: SealedDelivery,
        reference: AdmissionReference,
    ) -> WinningDelivery:
        winner = self.journal.require_winner(receipt)
        self._validate_plan_owner(plan, receipt, reference, winner)
        return winner

    @staticmethod
    def _validate_plan_owner(
        plan: RetainedCommitPlan,
        receipt: SealedDelivery,
        reference: AdmissionReference,
        winner: WinningDelivery,
    ) -> None:
        if (plan.receipt, plan.admission, plan.decision_sequence) != (
            receipt,
            reference,
            winner.decision_sequence,
        ):
            msg = "Retained plan differs from the durable winner or admission"
            raise StoreIntegrityError(msg)

    def _apply(self, plan: RetainedCommitPlan, tables: Mapping[str, DiskTable]) -> None:
        self.journal.require_winner(plan.receipt)
        controls = self._controls()
        control = control_for(plan, controls)
        if control is not None and control["state"] != "planned":
            msg = "Retained plan is not eligible for incomplete application"
            raise StoreIntegrityError(msg)
        if control is None:
            append_table_files(
                self._snapshot(TablePath.LOGICAL_COMMITS.value),
                [planned_control(plan)],
                step_number=plan.step_number,
                statistics_columns=["logical_commit_id"],
            )
            controls = self._controls()
            if control_for(plan, controls) is None:
                msg = "Retained plan control was not durable"
                raise StoreIntegrityError(msg)
            self._checkpoint("planned", plan.receipt, None)
        for table in plan.tables:
            apply_table_effect(
                plan,
                table,
                tables[table.table_path],
                controls,
                read_snapshot=self._snapshot,
            )
            self._checkpoint("table", plan.receipt, table.table_path)
        for table_path in retained_table_paths():
            if plan.table(table_path) is None:
                verify_snapshot_effect(
                    plan, table_path, self._snapshot(table_path), controls
                )
        self._verify_carriers(plan)
        complete_control(
            plan,
            uri_join(self.delta_root, TablePath.LOGICAL_COMMITS.value),
            storage_options=self.storage_options,
        )
        self._verify_complete(plan, self._controls())
        self._checkpoint("complete", plan.receipt, None)

    def _controls(self) -> pl.DataFrame:
        return self._control_proof().controls

    def _control_proof(self) -> RetainedControlProof:
        controls = pl.read_delta(
            uri_join(self.delta_root, TablePath.LOGICAL_COMMITS.value),
            storage_options=self.storage_options,
        )
        if dict(controls.schema) != get_physical_schema_for_path(
            TablePath.LOGICAL_COMMITS.value
        ):
            msg = "Retained logical commit table has an unexpected schema"
            raise StoreIntegrityError(msg)
        validate_control_rows(controls)
        plans = [self._control_plan(row) for row in controls.iter_rows(named=True)]
        states = self.journal.states(
            tuple(
                assignment
                for plan in plans
                for assignment in (
                    (plan.receipt.delivery.attempt.assignment,)
                    if isinstance(plan, RetainedCommitPlan)
                    else tuple(
                        item.attempt.assignment for item in plan.decision_admissions
                    )
                    if isinstance(plan, StepResultPlan)
                    else ()
                )
            )
        )
        winners = self.journal.require_winners(
            tuple(
                receipt
                for plan in plans
                for receipt in (
                    (plan.receipt,)
                    if isinstance(plan, RetainedCommitPlan)
                    else plan.selected_receipts
                    if isinstance(plan, StepResultPlan)
                    else ()
                )
            )
        )
        for plan in plans:
            control_for(plan, controls)
            if isinstance(plan, StepResultPlan):
                verify_step_decisions(plan, states, winners)
                continue
            if isinstance(
                plan, (CuratorCommitPlan, CuratorReusePlan, InputRegistrationPlan)
            ):
                continue
            state = states[plan.receipt.delivery.attempt.assignment]
            if state is None:
                msg = "Retained control has no admitted owner"
                raise StoreIntegrityError(msg)
            self._validate_plan_owner(
                plan,
                plan.receipt,
                AdmissionReference(
                    attempt=state.attempt, record_digest=state.admission_digest
                ),
                winners[plan.receipt],
            )
        from artisan.storage.cache.retained_step_lookup import verify_step_reuse_source

        steps = {
            plan.step_run_id: plan for plan in plans if isinstance(plan, StepResultPlan)
        }
        for step in steps.values():
            if isinstance(step.selection, WholeStepReuse):
                verify_step_reuse_source(step, controls, steps, self)
        verify_curator_controls(self, controls, plans)
        return RetainedControlProof(
            controls,
            MappingProxyType({plan.logical_commit_id: plan for plan in plans}),
        )

    def _control_plan(
        self, row: dict[str, Any]
    ) -> RetainedCommitPlan | StepResultPlan | CuratorPlan | InputRegistrationPlan:
        if row["commit_kind"] == "input_registration":
            registration = read_input_registration(
                self.delta_root, row["step_run_id"], self.fs
            )
            if (
                registration is None
                or row["logical_commit_id"] != registration.logical_commit_id
            ):
                msg = "Input registration control lacks its exact immutable plan"
                raise StoreIntegrityError(msg)
            return registration
        if row["commit_kind"] == "step_result":
            step_plan = read_step_result_plan(
                self.delta_root, row["step_run_id"], self.fs
            )
            curator_plan = read_curator_plan(
                self.delta_root, row["step_run_id"], self.fs
            )
            if curator_plan is not None:
                if (
                    step_plan is not None
                    or row["logical_commit_id"] != curator_plan.logical_commit_id
                ):
                    msg = "Step control has conflicting curator and scientific plans"
                    raise StoreIntegrityError(msg)
                return curator_plan
            if (
                step_plan is None
                or row["logical_commit_id"] != step_plan.logical_commit_id
            ):
                msg = "Step control has no immutable matching result plan"
                raise StoreIntegrityError(msg)
            return step_plan
        if (
            row["commit_kind"] != "retained_result"
            or row["recovery_batch_id"] is not None
            or not row["logical_commit_id"].startswith("retained_result:")
        ):
            msg = "Logical commit control is outside the retained domain"
            raise StoreIntegrityError(msg)
        assignment_id = row["logical_commit_id"].removeprefix("retained_result:")
        if len(assignment_id) != 32 or any(
            char not in "0123456789abcdef" for char in assignment_id
        ):
            msg = "Retained control has an invalid assignment identity"
            raise StoreIntegrityError(msg)
        path = uri_join(
            self.delta_root, "_artisan", "retained-commits", assignment_id, "plan.json"
        )
        with self.fs.open(path, "rb", cache_type="none") as stream:
            candidate = RetainedCommitPlan.model_validate_json(stream.read())
        plan = read_retained_plan(
            self.delta_root, candidate.receipt.delivery.attempt.assignment, self.fs
        )
        if plan != candidate or plan.logical_commit_id != row["logical_commit_id"]:
            msg = "Retained control names another immutable plan"
            raise StoreIntegrityError(msg)
        return plan

    def _verify_complete(
        self, plan: RetainedCommitPlan, controls: pl.DataFrame
    ) -> None:
        self.journal.require_winner(plan.receipt)
        self._verify_complete_effects(plan, controls, read_snapshot=self._snapshot)

    def _verify_complete_effects(
        self,
        plan: RetainedCommitPlan,
        controls: pl.DataFrame,
        *,
        read_snapshot: Callable[[str], DeltaSnapshot],
    ) -> None:
        control = control_for(plan, controls)
        if control is None or control["state"] != "complete":
            msg = "Retained result has no complete logical commit"
            raise StoreIntegrityError(msg)
        for table_path in retained_table_paths():
            verify_snapshot_effect(
                plan, table_path, read_snapshot(table_path), controls
            )
        self._verify_carriers(plan)

    def _verify_carriers(self, plan: RetainedCommitPlan) -> None:
        destinations = {
            uri_join(self.files_root, carrier.relative_path)
            for carrier in plan.carriers
        }
        locations = plan.table(TablePath.ARTIFACT_LOCATIONS.value)
        if (
            locations is not None
            and {key[1] for key in locations.row_keys} != destinations
        ):
            msg = "Retained locations disagree with the configured managed files root"
            raise StoreIntegrityError(msg)
        if destinations and locations is None:
            msg = "Managed retained carriers have no planned locations"
            raise StoreIntegrityError(msg)
        for carrier in plan.carriers:
            path = uri_join(self.files_root, carrier.relative_path)
            with self.fs.open(path, "rb", cache_type="none") as stream:
                if compute_stream_digest(stream) != (
                    carrier.digest,
                    carrier.size_bytes,
                ):
                    msg = "Managed retained carrier changed"
                    raise StoreIntegrityError(msg)

    def _snapshot(self, table_path: str) -> DeltaSnapshot:
        snapshot = DeltaSnapshot(
            uri_join(self.delta_root, table_path),
            get_physical_schema_for_path(table_path),
            fs=self.fs,
            storage_options=self.storage_options,
        )
        if snapshot.partitions:
            msg = "Retained tables require unpartitioned protected origins"
            raise StoreIntegrityError(msg)
        if table_path == TablePath.STEPS.value and self._store_identity is None:
            self._store_identity = StoreIdentity(
                delta_root=_identity_root(self.delta_root, self.fs),
                files_root=_identity_root(self.files_root, self.fs),
                # Delta metadata IDs are UUID strings; the upstream type says int.
                steps_table_id=cast(str, snapshot.delta.metadata().id),
            )
        return snapshot

    def _cleanup(self, plan: RetainedCommitPlan) -> None:
        directory = retained_plan_directory(
            self.delta_root, plan.receipt.delivery.attempt.assignment
        )
        for table in plan.tables:
            for file in table.files:
                self._remove_unchanged(
                    uri_join(directory, file.relative_path),
                    file.digest,
                    file.size_bytes,
                )
        self._cleanup_delivery(plan.receipt)
        self._checkpoint("cleanup", plan.receipt, None)

    def _cleanup_delivery(self, receipt: SealedDelivery) -> None:
        if not self.fs.exists(receipt.seal_uri):
            return
        with self.fs.open(receipt.seal_uri, "rb", cache_type="none") as stream:
            encoded = stream.read()
        if compute_content_digest(encoded) != receipt.seal_digest:
            return
        seal = RetainedResultSeal.model_validate_json(encoded)
        if seal.completion.delivery != receipt.delivery:
            msg = "Cleanup seal belongs to another delivery"
            raise StoreIntegrityError(msg)
        directory = posixpath.dirname(receipt.seal_uri)
        inventory: tuple[RetainedTableFile | ExternalCarrier, ...] = (
            *seal.tables,
            *seal.carriers,
        )
        for item in inventory:
            self._remove_unchanged(
                uri_join(directory, item.filename), item.digest, item.size_bytes
            )
        self._remove_unchanged(receipt.seal_uri, receipt.seal_digest, len(encoded))

    def _remove_unchanged(self, path: str, digest: str, size: int) -> None:
        if self.fs.exists(path):
            with self.fs.open(path, "rb", cache_type="none") as stream:
                unchanged = compute_stream_digest(stream) == (digest, size)
            if unchanged:
                self.fs.rm(path)

    @staticmethod
    def _checkpoint(
        boundary: str, receipt: SealedDelivery, table_path: str | None
    ) -> None:
        """Fault-injection seam for independent controller process-death tests."""
