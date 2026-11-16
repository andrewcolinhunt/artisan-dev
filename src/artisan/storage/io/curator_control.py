"""Verify curator source ownership within one fresh logical-control proof."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

import polars as pl

from artisan.errors import StoreIntegrityError
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.curator_admission import CuratorAdmission
from artisan.schemas.provenance.projection import (
    AcceptedCuratorOccurrence,
    AcceptedProducer,
    BoundaryProjection,
    ProjectedOccurrence,
)
from artisan.storage.cache.curator_identity import curator_cache_key
from artisan.storage.core.committed_scan import verify_plan_effect
from artisan.storage.core.projection_schemas import PROJECTION_TABLES
from artisan.storage.io.curator_admission import read_curator_admission
from artisan.storage.io.curator_plan import CuratorCommitPlan, CuratorPlan
from artisan.storage.io.curator_reuse_plan import CuratorReusePlan
from artisan.storage.io.input_registration_plan import InputRegistrationPlan
from artisan.storage.io.logical_control import control_for
from artisan.storage.io.projection_encoding import decode_projection
from artisan.storage.io.retained_plan import RetainedCommitPlan
from artisan.storage.io.step_result_plan import StepResultPlan
from artisan.utils.path import uri_join

if TYPE_CHECKING:
    from artisan.storage.io.retained_commit import RetainedCommitter

type CandidatePlan = (
    RetainedCommitPlan | StepResultPlan | CuratorPlan | InputRegistrationPlan
)


def verify_curator_inputs(
    admission: CuratorAdmission, committer: RetainedCommitter
) -> None:
    """Reject producer references outside the exact admitted view before dispatch."""
    controls = committer._controls()
    owners = {
        row["logical_commit_id"]: committer._control_plan(row)
        for row in controls.iter_rows(named=True)
    }
    _verify_view(admission, controls)
    _verify_inputs(admission, owners, {}, committer, controls)


def _verify_inputs(
    admission: CuratorAdmission,
    owners: Mapping[str, CandidatePlan],
    projections: dict[tuple[str, str], BoundaryProjection],
    committer: RetainedCommitter,
    controls: pl.DataFrame,
) -> None:
    for role in admission.inputs:
        for member in role.members:
            for source in member.producers:
                _verify_source(
                    source,
                    ProjectedOccurrence(
                        address=source.address,
                        artifact_id=member.artifact_id,
                        artifact_type=member.artifact_type,
                    ),
                    admission,
                    owners,
                    projections,
                    committer,
                    controls,
                )


def verify_curator_controls(
    committer: RetainedCommitter, controls: pl.DataFrame, plans: Sequence[CandidatePlan]
) -> None:
    """Use an acyclic prior-owner proof without recursively rereading controls."""
    owners = {plan.logical_commit_id: plan for plan in plans}
    admissions = {
        plan.logical_commit_id: read_curator_admission(
            plan.admission, delta_root=committer.delta_root, fs=committer.fs
        )
        for plan in plans
        if isinstance(plan, (CuratorCommitPlan, CuratorReusePlan))
    }
    _acyclic(admissions)
    projections: dict[tuple[str, str], BoundaryProjection] = {}
    for plan in plans:
        if not isinstance(plan, (CuratorCommitPlan, CuratorReusePlan)):
            continue
        admission = admissions[plan.logical_commit_id]
        if (plan.step_number, plan.effects.operation_name) != (
            admission.record.step_number,
            admission.operation_name,
        ):
            msg = "Curator plan changed its admitted operation or step number"
            raise StoreIntegrityError(msg)
        _verify_view(admission, controls)
        if isinstance(plan, CuratorReusePlan):
            _verify_reuse(plan, admission, owners, controls, committer)
            _verify_inputs(admission, owners, projections, committer, controls)
            continue
        for item in plan.projection.occurrences:
            for source in item.sources:
                _verify_source(
                    source,
                    item.occurrence,
                    admission,
                    owners,
                    projections,
                    committer,
                    controls,
                )
        _verify_inputs(admission, owners, projections, committer, controls)


def _verify_view(admission: CuratorAdmission, controls: pl.DataFrame) -> None:
    for captured in admission.view.controls:
        found = controls.filter(
            pl.col("logical_commit_id") == captured.logical_commit_id
        )
        if found.height != 1:
            msg = "Curator admitted view has a missing completed owner"
            raise StoreIntegrityError(msg)
        row = found.row(0, named=True)
        if row["state"] != "complete" or any(
            row[name] != value for name, value in captured.model_dump().items()
        ):
            msg = "Curator admitted view changed its completed ownership"
            raise StoreIntegrityError(msg)


def _acyclic(admissions: Mapping[str, CuratorAdmission]) -> None:
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(owner: str) -> None:
        if owner in visiting:
            msg = "Curator visibility must refer only to acyclic prior completions"
            raise StoreIntegrityError(msg)
        if owner in visited:
            return
        visiting.add(owner)
        for item in admissions[owner].view.controls:
            if item.logical_commit_id in admissions:
                visit(item.logical_commit_id)
        visiting.remove(owner)
        visited.add(owner)

    for owner in admissions:
        visit(owner)


def _verify_source(
    source: AcceptedProducer,
    expected: ProjectedOccurrence,
    admission: CuratorAdmission,
    owners: Mapping[str, CandidatePlan],
    projections: dict[tuple[str, str], BoundaryProjection],
    committer: RetainedCommitter,
    controls: pl.DataFrame,
) -> None:
    captured = {item.logical_commit_id: item for item in admission.view.controls}
    selected = None
    if isinstance(source, AcceptedCuratorOccurrence):
        plan = owners.get(f"step_result:{source.step_run_id}")
        if (
            isinstance(plan, CuratorCommitPlan)
            and plan.plan_digest == source.plan_digest
            and plan.logical_commit_id in captured
        ):
            selected = next(
                (
                    item.occurrence
                    for item in plan.projection.occurrences
                    if item.occurrence.address == source.address
                ),
                None,
            )
    else:
        plan = next(
            (
                item
                for item in owners.values()
                if isinstance(item, RetainedCommitPlan)
                and item.receipt.delivery.delivery_id == source.delivery_id
            ),
            None,
        )
        if (
            isinstance(plan, RetainedCommitPlan)
            and plan.receipt.seal_digest == source.seal_digest
            and plan.logical_commit_id in captured
        ):
            key = admission.view.digest, source.delivery_id
            if key not in projections:
                projections[key] = _retained_projection(
                    plan, admission, committer, controls
                )
            selected = next(
                (
                    item
                    for item in projections[key].occurrences
                    if item.address == source.address
                ),
                None,
            )
    if selected is None or (selected.artifact_id, selected.artifact_type) != (
        expected.artifact_id,
        expected.artifact_type,
    ):
        msg = "Curator content source is absent from its admitted completed view"
        raise StoreIntegrityError(msg)


def _retained_projection(
    plan: RetainedCommitPlan,
    admission: CuratorAdmission,
    committer: RetainedCommitter,
    controls: pl.DataFrame,
) -> BoundaryProjection:
    tables = {}
    for table in PROJECTION_TABLES:
        version = admission.view.table(table.table_path)
        if version is None:
            msg = "Curator source has no captured projection version"
            raise StoreIntegrityError(msg)
        physical = pl.read_delta(
            uri_join(committer.delta_root, table.table_path),
            version=version.version,
            storage_options=committer.storage_options,
        )
        verify_plan_effect(plan, table.table_path, physical, controls)
        tables[table.name] = physical.filter(
            pl.col("logical_commit_id") == plan.logical_commit_id
        ).drop("logical_commit_id")
    return decode_projection(tables, delivery=plan.receipt.delivery)


def _verify_reuse(
    plan: CuratorReusePlan,
    current: CuratorAdmission,
    owners: Mapping[str, CandidatePlan],
    controls: pl.DataFrame,
    committer: RetainedCommitter,
) -> None:
    source = owners.get(f"step_result:{plan.source.step_run_id}")
    if (
        not isinstance(source, CuratorCommitPlan)
        or source.plan_digest != plan.source.plan_digest
    ):
        msg = "Curator reuse source is not its original executed plan"
        raise StoreIntegrityError(msg)
    control = control_for(source, controls)
    execution = source.table(TablePath.EXECUTIONS.value)
    if (
        control is None
        or control["state"] != "complete"
        or execution is None
        or execution.row_keys != ((plan.source.execution_run_id,),)
    ):
        msg = "Curator reuse requires one exactly completed original execution"
        raise StoreIntegrityError(msg)
    admitted = read_curator_admission(
        source.admission, delta_root=committer.delta_root, fs=committer.fs
    )
    proof = source.cache_proof
    key = curator_cache_key(current)
    if (
        not current.cacheable
        or not current.software_cacheable
        or not admitted.software_cacheable
        or not admitted.cacheable
        or current.record.replay_of_execution_run_id is not None
        or admitted.record.replay_of_execution_run_id is not None
        or key is None
        or key != curator_cache_key(admitted)
        or proof is None
        or proof.ineligible_reasons
        or proof.cache_key != key
        or proof.definition_digest != admitted.definition_digest
        or proof.software_digest != admitted.software_digest
        or proof.snapshot_digest != admitted.view.digest
    ):
        msg = "Curator reuse source differs from its qualified current admission"
        raise StoreIntegrityError(msg)
