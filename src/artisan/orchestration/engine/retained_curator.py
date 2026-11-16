"""Commit ordinary curator executions in the exact retained-store domain."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import ExitStack
from typing import Any

import polars as pl

from artisan.errors import StoreIntegrityError
from artisan.orchestration.engine.curator_reuse import (
    recover_curator_reuse,
    reuse_curator_result,
)
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.curator_admission import (
    CuratorAdmission,
    CuratorAdmissionReference,
)
from artisan.schemas.execution.curator_cache import CuratorCacheProof
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_state import StepState
from artisan.schemas.provenance.curator_projection import CuratorProjection
from artisan.storage.cache.curator_dependencies import validate_curator_cache_proof
from artisan.storage.cache.curator_lookup import CuratorCacheHit
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.committed_scan import verify_snapshot_effect
from artisan.storage.core.curator_result import validate_dynamic_result
from artisan.storage.core.curator_view import CuratorView
from artisan.storage.core.table_schemas import get_schema
from artisan.storage.io.bounded_evidence import DiskTable
from artisan.storage.io.curator_admission import (
    publish_curator_admission,
    read_curator_admission,
)
from artisan.storage.io.curator_carrier_effects import (
    CuratorCarrierEvidence,
    capture_curator_carriers,
    carrier_directory,
    publish_curator_carriers,
    verify_curator_carriers,
)
from artisan.storage.io.curator_control import verify_curator_controls
from artisan.storage.io.curator_effects import (
    CapturedCuratorEffects,
    capture_curator_effects,
    prepare_curator_effects,
)
from artisan.storage.io.curator_plan import (
    CuratorCommitPlan,
    CuratorPlan,
    curator_plan_digest,
    curator_table_paths,
    publish_curator_plan,
    read_curator_plan,
    sealed_curator_cache_proof,
)
from artisan.storage.io.curator_projection import build_curator_projection
from artisan.storage.io.curator_reuse_plan import CuratorReusePlan
from artisan.storage.io.curator_terminal import prepare_curator_terminal
from artisan.storage.io.delta_files import append_table_files
from artisan.storage.io.logical_control import (
    complete_control,
    control_for,
    planned_control,
)
from artisan.storage.io.retained_read import RetainedReadSnapshot
from artisan.storage.io.retained_store import initialize_framework_tables
from artisan.storage.io.staging import StagingManager
from artisan.storage.io.table_effect import apply_table_effect
from artisan.utils.path import uri_join


class CuratorResults:
    """Preserve curator computation contracts with distinct completion authority."""

    def __init__(
        self,
        tracker: RetainedStepTracker,
        staging_root: str,
        *,
        checkpoint: Callable[[str], None] | None = None,
        preserve_staging: bool = False,
    ) -> None:
        self.tracker = tracker
        self.committer = tracker.committer
        self.staging = StagingManager(staging_root, self.committer.fs)
        self.checkpoint = checkpoint
        self.preserve_staging = preserve_staging

    def initialize(self) -> None:
        """Explicitly create the shared curator tables without activating a format."""
        initialize_framework_tables(self.committer)

    def admit(self, admission: CuratorAdmission) -> CuratorAdmissionReference:
        """Bind exact source visibility before an ordinary curator may execute."""
        admission = CuratorAdmission.model_validate(admission)
        with self.committer._lock, self.tracker._lock:
            if self.tracker._rows_for_attempt(admission.record.step_run_id).is_empty():
                self.tracker.create_attempt(admission.record)
            state = self.tracker.current_state(admission.record.step_run_id)
            reference = CuratorAdmissionReference(
                step_run_id=admission.record.step_run_id,
                admission_digest=admission.digest,
            )
            if (
                state.status == StepStatus.RUNNING
                and state.step_spec_id == admission.digest
                and read_curator_admission(
                    reference,
                    delta_root=self.committer.delta_root,
                    fs=self.committer.fs,
                )
                == admission
            ):
                return reference
            if state.status != StepStatus.PENDING:
                msg = "Only a pending curator may bind its input view"
                raise StoreIntegrityError(msg)
            self.tracker.create_attempt(admission.record)
            publish_curator_admission(admission, self.committer)
            self.tracker.transition(
                reference.step_run_id,
                StepStatus.PENDING,
                StepStatus.RUNNING,
                step_spec_id=reference.admission_digest,
            )
            return reference

    def record_cancellation(
        self, step_run_id: str, outcome: CancellationAcknowledgement
    ) -> StepState:
        """Recover an already selected curator result before later acknowledgement."""
        with self.committer._lock, self.tracker._lock:
            if self._plan(step_run_id) is not None:
                self.recover(step_run_id)
                return self.tracker.current_state(step_run_id)
            state = self.tracker.current_state(step_run_id)
            return self.tracker.record_cancellation(step_run_id, state.status, outcome)

    def reuse(self, hit: CuratorCacheHit) -> StepResult:
        """Publish current cache membership while preserving original output owners."""
        return reuse_curator_result(self, hit)

    def finish(
        self,
        reference: CuratorAdmissionReference,
        execution_run_id: str,
        result: StepResult,
        *,
        cache_proof: CuratorCacheProof | None = None,
    ) -> StepResult:
        """Select one closed worker execution and the current terminal snapshot."""
        with self.committer._lock, self.tracker._lock:
            existing = self._plan(reference.step_run_id)
            if existing is not None:
                execution = existing.effects.table(TablePath.EXECUTIONS.value)
                if (
                    not isinstance(existing, CuratorCommitPlan)
                    or existing.admission != reference
                    or execution is None
                    or execution.row_keys != ((execution_run_id,),)
                    or (cache_proof is not None and cache_proof != existing.cache_proof)
                ):
                    msg = "Curator completion retry changed its selected execution"
                    raise StoreIntegrityError(msg)
                return self.recover(reference.step_run_id)
            admission = read_curator_admission(
                reference, delta_root=self.committer.delta_root, fs=self.committer.fs
            )
            with self.committer._read_snapshot() as read:
                rows, state = self.tracker._read_state(reference.step_run_id, read)
                if (
                    state.status != StepStatus.RUNNING
                    or state.step_spec_id != reference.admission_digest
                ):
                    msg = "Curator completion lacks its exact running admission"
                    raise StoreIntegrityError(msg)
                if state.cancellation_status in {
                    CancellationStatus.REQUESTED,
                    CancellationStatus.CONFIRMED,
                    CancellationStatus.UNKNOWN,
                }:
                    msg = "Curator success cannot cross a cancellation fence"
                    raise StoreIntegrityError(msg)
                terminal = self.tracker._terminal_candidate(
                    reference.step_run_id,
                    StepStatus.RUNNING,
                    result.status,
                    step_spec_id=reference.admission_digest,
                    result=result,
                    rows=rows,
                    controls=read.controls,
                )
            self.staging.stage_orchestrator_dataframe(
                terminal,
                TablePath.STEPS.value,
                commit_kind="step_result",
                step_run_id=reference.step_run_id,
                step_number=admission.record.step_number,
                operation_name=admission.operation_name,
            )
            with (
                prepare_curator_effects(
                    staging_root=self.staging.staging_dir,
                    fs=self.committer.fs,
                    step_run_id=reference.step_run_id,
                    step_number=admission.record.step_number,
                    operation_name=admission.operation_name,
                    execution_run_ids=(execution_run_id,),
                ) as staged,
                capture_curator_carriers(
                    staged, files_root=self.committer.files_root, fs=self.committer.fs
                ) as carriers,
            ):
                with self.committer._read_snapshot() as read:
                    view = CuratorView(self.committer, admission.view, _read=read)
                    self._validate_evidence(
                        staged,
                        admission,
                        view,
                        read,
                        artifact_tables=carriers.validation_tables,
                    )
                    sealed_proof = _cache_proof(staged)
                    if cache_proof is not None and cache_proof != sealed_proof:
                        msg = "Curator expected cache proof differs from its sealed execution"
                        raise StoreIntegrityError(msg)
                    cache_proof = sealed_proof
                    if cache_proof is not None:
                        validate_curator_cache_proof(
                            cache_proof, view, admission=admission
                        )
                    projection = build_curator_projection(
                        admission, view, carriers.validation_tables
                    )
                    plan = self._select(
                        reference, staged, projection, cache_proof, read, carriers
                    )
                self._publish_selection(plan)
                self._publish_carriers(carriers)
                completed = self._apply(plan, carriers.tables)
            if not self.preserve_staging:
                self._cleanup(plan)
            return completed

    def finish_without_execution(
        self, reference: CuratorAdmissionReference, *, error: str
    ) -> StepResult:
        """Publish cancellation or infrastructure failure without a worker result."""
        with self.committer._lock, self.tracker._lock:
            if self._plan(reference.step_run_id) is not None:
                return self.recover(reference.step_run_id)
            admission = read_curator_admission(
                reference, delta_root=self.committer.delta_root, fs=self.committer.fs
            )
            with self.committer._read_snapshot() as read:
                rows, state = self.tracker._read_state(reference.step_run_id, read)
                if (
                    state.status != StepStatus.RUNNING
                    or state.step_spec_id != reference.admission_digest
                    or state.cancellation_status == CancellationStatus.REQUESTED
                ):
                    msg = "Curator failure requires its exact running admission and final cancellation evidence"
                    raise StoreIntegrityError(msg)
                cancelled = state.cancellation_status == CancellationStatus.CONFIRMED
                result = StepResult(
                    step_run_id=state.step_run_id,
                    step_number=state.step_number,
                    step_name=state.step_name,
                    status=StepStatus.CANCELLED if cancelled else StepStatus.FAILED,
                    cancellation_status=state.cancellation_status,
                    total_count=0 if cancelled else 1,
                    succeeded_count=0,
                    failed_count=0 if cancelled else 1,
                    error=error,
                )
                terminal = self.tracker._terminal_candidate(
                    state.step_run_id,
                    StepStatus.RUNNING,
                    result.status,
                    step_spec_id=admission.digest,
                    result=result,
                    rows=rows,
                    controls=read.controls,
                )
            with prepare_curator_terminal(admission, terminal, self.staging) as staged:
                with self.committer._read_snapshot() as read:
                    view = CuratorView(self.committer, admission.view, _read=read)
                    self._validate_evidence(staged, admission, view, read)
                    plan = self._select(
                        reference,
                        staged,
                        CuratorProjection(occurrences=(), outputs=()),
                        None,
                        read,
                    )
                self._publish_selection(plan)
                completed = self._apply(plan, staged.tables)
            if not self.preserve_staging:
                self._cleanup(plan)
            return completed

    def _select(
        self,
        reference: CuratorAdmissionReference,
        staged: CapturedCuratorEffects,
        projection: CuratorProjection,
        cache_proof: CuratorCacheProof | None,
        read: RetainedReadSnapshot,
        carriers: CuratorCarrierEvidence | None = None,
    ) -> CuratorCommitPlan:
        read.require_committer(self.committer)
        payload = {
            "domain": "artisan.curator-commit",
            "admission": reference.model_dump(mode="json"),
            "effects": staged.plan.model_dump(mode="json"),
            "projection": projection.model_dump(mode="json"),
            "cache_proof": cache_proof.model_dump(mode="json")
            if cache_proof is not None
            else None,
            "carrier_inventory": carriers.inventory.model_dump(mode="json")
            if carriers is not None and carriers.inventory is not None
            else None,
            "managed_locations": carriers.managed_locations.model_dump(mode="json")
            if carriers is not None and carriers.managed_locations is not None
            else None,
        }
        plan = CuratorCommitPlan.model_validate(
            {**payload, "plan_digest": curator_plan_digest(payload)}
        )
        verify_curator_controls(
            self.committer,
            read.controls,
            [*read.control_plans(), plan],
        )
        return plan

    def _publish_selection(self, plan: CuratorCommitPlan) -> None:
        """Publish only after all checks on the preceding finite read have ended."""
        publish_curator_plan(
            plan, delta_root=self.committer.delta_root, fs=self.committer.fs
        )
        self._checkpoint("plan")

    def recover(self, step_run_id: str) -> StepResult:
        """Complete only a selected exact plan; unclaimed worker seals stay invisible."""
        with self.committer._lock, self.tracker._lock:
            plan = self._plan(step_run_id)
            if plan is None:
                msg = "Curator has no selected immutable completion plan"
                raise StoreIntegrityError(msg)
            if isinstance(plan, CuratorReusePlan):
                return recover_curator_reuse(self, plan)
            with ExitStack() as captured:
                with self.committer._read_snapshot() as read:
                    control = control_for(plan, read.controls)
                    if control is not None and control["state"] == "complete":
                        completed = self._completed_result(plan, read)
                    elif control is None or control["state"] == "planned":
                        admission = read_curator_admission(
                            plan.admission,
                            delta_root=self.committer.delta_root,
                            fs=self.committer.fs,
                        )
                        staged = captured.enter_context(
                            capture_curator_effects(
                                plan.effects,
                                self.staging.staging_dir,
                                self.committer.fs,
                            )
                        )
                        view = CuratorView(self.committer, admission.view, _read=read)
                        carriers = captured.enter_context(
                            capture_curator_carriers(
                                staged,
                                files_root=self.committer.files_root,
                                fs=self.committer.fs,
                            )
                        )
                        self._validate_evidence(
                            staged,
                            admission,
                            view,
                            read,
                            artifact_tables=carriers.validation_tables,
                        )
                        if (
                            _cache_proof(staged) != plan.cache_proof
                            or carriers.inventory != plan.carrier_inventory
                            or carriers.managed_locations != plan.managed_locations
                        ):
                            msg = "Curator selected evidence differs from its sealed execution"
                            raise StoreIntegrityError(msg)
                        if plan.cache_proof is not None:
                            validate_curator_cache_proof(
                                plan.cache_proof, view, admission=admission
                            )
                        if (
                            build_curator_projection(
                                admission, view, carriers.validation_tables
                            )
                            != plan.projection
                        ):
                            msg = "Curator selected projection differs from its exact evidence"
                            raise StoreIntegrityError(msg)
                    else:
                        msg = "Curator completion is not eligible for recovery"
                        raise StoreIntegrityError(msg)
                if control is None or control["state"] == "planned":
                    self._publish_carriers(carriers)
                    completed = self._apply(plan, carriers.tables)
            if not self.preserve_staging:
                self._cleanup(plan)
            return completed

    def _cleanup(self, plan: CuratorCommitPlan) -> None:
        cleanup_tables = list(plan.effects.tables)
        if plan.managed_locations is not None:
            cleanup_tables.append(plan.managed_locations)
        self.staging.cleanup_plan(
            [file for table in cleanup_tables for file in table.files]
        )
        if plan.carrier_inventory is not None and plan.carrier_inventory.carriers:
            directory = carrier_directory(plan.effects, self.staging.staging_dir)
            for item in plan.carrier_inventory.carriers:
                self.committer._remove_unchanged(
                    uri_join(directory, item.filename), item.digest, item.size_bytes
                )
        self._checkpoint("cleanup")

    def _validate_evidence(
        self,
        staged: CapturedCuratorEffects,
        admission: CuratorAdmission,
        view: CuratorView,
        read: RetainedReadSnapshot,
        *,
        artifact_tables: dict[str, DiskTable] | None = None,
    ) -> None:
        terminal = staged.tables[TablePath.STEPS.value].get_row(
            staged.tables[TablePath.STEPS.value].keys[0]
        )
        rows, current = self.tracker._read_state(admission.record.step_run_id, read)
        if (
            current.status != StepStatus.RUNNING
            or current.step_spec_id != admission.digest
        ):
            msg = "Curator pending result no longer matches its running admission"
            raise StoreIntegrityError(msg)
        self._validate_terminal_history(terminal, rows=rows)
        if TablePath.EXECUTIONS.value not in staged.tables:
            cancelled = current.cancellation_status == CancellationStatus.CONFIRMED
            if (
                set(staged.tables) != {TablePath.STEPS.value}
                or current.cancellation_status == CancellationStatus.REQUESTED
                or terminal["status"] != ("cancelled" if cancelled else "failed")
                or (
                    terminal["total_count"],
                    terminal["succeeded_count"],
                    terminal["failed_count"],
                )
                != ((0, 0, 0) if cancelled else (1, 0, 1))
            ):
                msg = (
                    "Controller-only curator effects require an unusable exact terminal"
                )
                raise StoreIntegrityError(msg)
            return
        store = ArtifactStore(
            self.committer.delta_root,
            fs=self.committer.fs,
            storage_options=self.committer.storage_options,
            files_root=self.committer.files_root,
            _view=view,
        )
        memberships = _frame(staged, TablePath.EXECUTION_EDGES)
        references = {
            group["execution_run_id"][0]: set(group["artifact_id"])
            for group in memberships.partition_by("execution_run_id")
        }
        executions = _frame(staged, TablePath.EXECUTIONS)
        if executions.height != 1:
            msg = "Curator completion requires one sealed execution"
            raise StoreIntegrityError(msg)
        execution = executions.row(0, named=True)
        success = execution["success"]
        if (
            execution["execution_spec_id"] != admission.digest
            or execution["replay_of_execution_run_id"]
            != admission.record.replay_of_execution_run_id
            or terminal["status"] != ("succeeded" if success else "failed")
            or (
                terminal["total_count"],
                terminal["succeeded_count"],
                terminal["failed_count"],
            )
            != (1, int(success), int(not success))
        ):
            msg = "Curator terminal summary disagrees with its sealed execution"
            raise StoreIntegrityError(msg)
        if not success and (
            set(staged.tables)
            - {
                TablePath.EXECUTIONS.value,
                TablePath.EXECUTION_EDGES.value,
                TablePath.STEPS.value,
            }
            or memberships.filter(pl.col("direction") != "input").height
        ):
            msg = "Failed curator seal must not publish artifact or output effects"
            raise StoreIntegrityError(msg)
        observed_inputs = set(
            memberships.filter(pl.col("direction") == "input")
            .select("role", "artifact_id")
            .iter_rows()
        )
        expected_inputs = {
            (role.role, member.artifact_id)
            for role in admission.inputs
            for member in role.members
        }
        if observed_inputs != expected_inputs:
            msg = "Curator sealed input membership differs from its frozen admission"
            raise StoreIntegrityError(msg)
        for execution_id in executions["execution_run_id"]:
            references.setdefault(execution_id, set())
        types = store.validate_staged_artifacts(
            artifact_tables if artifact_tables is not None else staged.tables,
            references,
            per_execution_artifact_ids=staged.per_execution_artifact_ids,
        )
        frames = {
            table.value: _frame(staged, table)
            for table in (
                TablePath.EXECUTIONS,
                TablePath.EXECUTION_EDGES,
                TablePath.CACHE_REUSE,
            )
        }
        frames[TablePath.ARTIFACT_INDEX.value] = pl.DataFrame(
            {"artifact_id": list(types), "artifact_type": list(types.values())},
            schema={"artifact_id": pl.String, "artifact_type": pl.String},
        )
        validate_dynamic_result(terminal, frames)

    def _validate_terminal_history(
        self, terminal: dict[str, Any], *, rows: pl.DataFrame | None = None
    ) -> None:
        """Validate exact retry when terminal bytes precede completed ownership."""
        history = (
            rows
            if rows is not None
            else self.tracker._rows_for_attempt(terminal["step_run_id"])
        )
        snapshots = history.unique().sort("state_sequence").to_dicts()
        existing = [
            row
            for row in snapshots
            if row["state_sequence"] == terminal["state_sequence"]
        ]
        if existing:
            if len(existing) != 1 or any(
                existing[0][key] != value for key, value in terminal.items()
            ):
                msg = "Curator terminal sequence already contains different evidence"
                raise StoreIntegrityError(msg)
        else:
            snapshots.append(terminal)
        StepTracker._validate_history(snapshots)

    def _publish_carriers(self, carriers: CuratorCarrierEvidence) -> None:
        publish_curator_carriers(
            carriers, files_root=self.committer.files_root, fs=self.committer.fs
        )
        self._checkpoint("carriers")

    def _apply(self, plan: CuratorPlan, tables: Mapping[str, DiskTable]) -> StepResult:
        controls = self.committer._controls()
        control = control_for(plan, controls)
        if control is None:
            append_table_files(
                self.committer._snapshot(TablePath.LOGICAL_COMMITS.value),
                [planned_control(plan)],
                step_number=plan.step_number,
                statistics_columns=["logical_commit_id"],
            )
            controls = self.committer._controls()
            self._checkpoint("planned")
        elif control["state"] != "planned":
            msg = "Curator table effects are not eligible for application"
            raise StoreIntegrityError(msg)
        for table in plan.tables:
            apply_table_effect(
                plan,
                table,
                tables[table.table_path],
                controls,
                read_snapshot=self.committer._snapshot,
            )
            self._checkpoint(f"table:{table.table_path}")
        for path in curator_table_paths():
            verify_snapshot_effect(plan, path, self.committer._snapshot(path), controls)
        complete_control(
            plan,
            uri_join(self.committer.delta_root, TablePath.LOGICAL_COMMITS.value),
            storage_options=self.committer.storage_options,
        )
        with self.committer._read_snapshot() as read:
            completed = self._completed_result(plan, read)
        self._checkpoint("complete")
        return completed

    def _completed_result(
        self, plan: CuratorPlan, read: RetainedReadSnapshot
    ) -> StepResult:
        """Verify payloads and exact lifecycle before leaving the completion read."""
        self._verify_complete(plan, read)
        _, state = self.tracker._read_state(plan.step_run_id, read)
        return state.to_step_result()

    def _verify_complete(self, plan: CuratorPlan, read: RetainedReadSnapshot) -> None:
        read.require_committer(self.committer)
        control = control_for(plan, read.controls)
        if control is None or control["state"] != "complete":
            msg = "Curator selected result has no complete logical effects"
            raise StoreIntegrityError(msg)
        for path in curator_table_paths():
            read.verify_effect(plan, path)
        if isinstance(plan, CuratorCommitPlan):
            verify_curator_carriers(
                plan.carrier_inventory,
                plan.managed_locations,
                files_root=self.committer.files_root,
                fs=self.committer.fs,
            )

    def _plan(self, step_run_id: str) -> CuratorPlan | None:
        return read_curator_plan(
            self.committer.delta_root, step_run_id, self.committer.fs
        )

    def _checkpoint(self, event: str) -> None:
        if self.checkpoint is not None:
            self.checkpoint(event)


def _frame(staged: CapturedCuratorEffects, table: TablePath) -> pl.DataFrame:
    candidate = staged.tables.get(table.value)
    if candidate is None:
        return pl.DataFrame(schema=get_schema(table))
    frames = list(candidate.iter_frames())
    return pl.concat(frames) if frames else pl.DataFrame(schema=candidate.schema)


def _cache_proof(staged: CapturedCuratorEffects) -> CuratorCacheProof | None:
    executions = _frame(staged, TablePath.EXECUTIONS)
    if executions.is_empty():
        return None
    row = executions.row(0, named=True)
    return sealed_curator_cache_proof(row["metadata"], success=row["success"])
