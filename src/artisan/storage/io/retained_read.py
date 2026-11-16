"""Fresh, call-owned evidence for finite accepted-result reads."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import polars as pl

from artisan.errors import StoreIntegrityError
from artisan.schemas.execution.assignment import SealedDelivery
from artisan.schemas.provenance.projection import BoundaryProjection
from artisan.storage.core.committed_scan import scan_plan_keys, verify_snapshot_effect
from artisan.storage.core.projection_schemas import PROJECTION_TABLES
from artisan.storage.core.table_schemas import get_physical_schema_for_path
from artisan.storage.io.curator_control import CandidatePlan
from artisan.storage.io.delta_files import DeltaSnapshot
from artisan.storage.io.logical_control import control_for
from artisan.storage.io.projection_encoding import decode_projection
from artisan.storage.io.retained_plan import RetainedCommitPlan
from artisan.utils.path import uri_join

if TYPE_CHECKING:
    from artisan.storage.io.retained_commit import RetainedCommitter


@dataclass(frozen=True)
class RetainedControlProof:
    """Control plans whose admission, winner and global UUID checks have passed."""

    controls: pl.DataFrame
    plans: Mapping[str, CandidatePlan]


class RetainedReadSnapshot:
    """One read's fixed table versions, never retained on the committer or reader.

    The owning committer constructs this only after validating fresh controls and
    holds its write lock until the read returns. Files are still read through the
    bounded snapshot/effect verifier. Only selected single-row metadata is retained,
    never whole table payloads.
    """

    def __init__(
        self, committer: RetainedCommitter, proof: RetainedControlProof
    ) -> None:
        self.committer = committer
        self.controls = proof.controls
        self._plans = proof.plans
        self._snapshots: dict[tuple[str, int | None], DeltaSnapshot] = {}
        self._effects: set[tuple[str, str, int]] = set()
        self._effect_rows: dict[tuple[str, str, int], dict[str, Any]] = {}
        self._verified: set[SealedDelivery] = set()
        self._projections: dict[SealedDelivery, BoundaryProjection] = {}

    def require_committer(self, committer: RetainedCommitter) -> None:
        """Reject reuse of this private proof through another store binding."""
        if committer is not self.committer:
            msg = "Read proof belongs to another store binding"
            raise StoreIntegrityError(msg)

    def control_plans(self) -> tuple[CandidatePlan, ...]:
        """Return this read's already verified planned and completed owners."""
        return tuple(self._plans.values())

    def snapshot(self, table_path: str, *, version: int | None = None) -> DeltaSnapshot:
        """Open each physical table once at one Delta version for this call."""
        key = table_path, version
        if key not in self._snapshots:
            snapshot = (
                self.committer._snapshot(table_path)
                if version is None
                else DeltaSnapshot(
                    uri_join(self.committer.delta_root, table_path),
                    get_physical_schema_for_path(table_path),
                    fs=self.committer.fs,
                    storage_options=self.committer.storage_options,
                    version=version,
                )
            )
            if snapshot.partitions:
                msg = "Retained tables require unpartitioned protected origins"
                raise StoreIntegrityError(msg)
            captured_key = table_path, snapshot.delta.version()
            self._snapshots[key] = self._snapshots.setdefault(captured_key, snapshot)
        return self._snapshots[key]

    def verify_effect(
        self, plan: CandidatePlan, table_path: str, *, version: int | None = None
    ) -> DeltaSnapshot:
        """Verify each exact completed owner/table effect once at its fixed version."""
        if self.completed_control_plan(plan.logical_commit_id) != plan:
            msg = "Effect differs from its validated immutable owner plan"
            raise StoreIntegrityError(msg)
        snapshot = self.snapshot(table_path, version=version)
        key = plan.logical_commit_id, table_path, snapshot.delta.version()
        if key not in self._effects:
            verify_snapshot_effect(plan, table_path, snapshot, self.controls)
            self._effects.add(key)
        return snapshot

    def effect_row(self, plan: CandidatePlan, table_path: str) -> dict[str, Any]:
        """Read one verified execution/terminal row once without caching tables."""
        snapshot = self.verify_effect(plan, table_path)
        key = plan.logical_commit_id, table_path, snapshot.delta.version()
        if key not in self._effect_rows:
            table = plan.table(table_path)
            if table is None or table.row_count != 1:
                msg = "Complete execution/terminal effect requires one exact row"
                raise StoreIntegrityError(msg)
            selected = scan_plan_keys(plan, table, snapshot, self.controls)
            rows = [
                row
                for frame in snapshot.iter_frames(rows_by_file=selected.matched)
                for row in frame.iter_rows(named=True)
            ]
            if len(rows) != 1:
                msg = "Complete execution/terminal effect requires one exact row"
                raise StoreIntegrityError(msg)
            self._effect_rows[key] = rows[0]
        return dict(self._effect_rows[key])

    def plan(self, receipt: SealedDelivery) -> RetainedCommitPlan:
        """Require the exact proven receipt and verify every effect and carrier."""
        receipt = SealedDelivery.model_validate(receipt)
        owner = f"retained_result:{receipt.delivery.attempt.assignment.assignment_id}"
        plan = self.completed_control_plan(owner)
        if not isinstance(plan, RetainedCommitPlan) or plan.receipt != receipt:
            msg = "Accepted delivery has no matching immutable control plan"
            raise StoreIntegrityError(msg)
        if receipt not in self._verified:
            self.committer._verify_complete_effects(
                plan, self.controls, read_snapshot=self.snapshot
            )
            self._verified.add(receipt)
        return plan

    def plan_for_delivery(self, delivery_id: str) -> RetainedCommitPlan:
        """Resolve exact completed ownership without treating metadata as payload proof."""
        plans = [
            plan
            for plan in self._plans.values()
            if isinstance(plan, RetainedCommitPlan)
            and plan.receipt.delivery.delivery_id == delivery_id
        ]
        if len(plans) != 1:
            msg = "Selected producer delivery has no unique completed controller plan"
            raise StoreIntegrityError(msg)
        plan = plans[0]
        self.completed_control_plan(plan.logical_commit_id)
        return plan

    def completed_control_plan(self, owner: str) -> CandidatePlan:
        """Return validated completion metadata, without proving content availability."""
        plan = self._plans.get(owner)
        if plan is None:
            msg = "Accepted metadata has no matching immutable control plan"
            raise StoreIntegrityError(msg)
        control = control_for(plan, self.controls)
        if control is None or control["state"] != "complete":
            msg = "Accepted metadata has no completed control"
            raise StoreIntegrityError(msg)
        return plan

    def projection(self, receipt: SealedDelivery) -> BoundaryProjection:
        """Decode exact occurrences from the same versions whose effects passed."""
        self.plan(receipt)
        return self.projection_metadata(receipt)

    def projection_metadata(self, receipt: SealedDelivery) -> BoundaryProjection:
        """Verify occurrence metadata only; never mark payloads or carriers verified."""
        receipt = SealedDelivery.model_validate(receipt)
        owner = f"retained_result:{receipt.delivery.attempt.assignment.assignment_id}"
        plan = self.completed_control_plan(owner)
        if not isinstance(plan, RetainedCommitPlan) or plan.receipt != receipt:
            msg = "Accepted delivery has no matching immutable control plan"
            raise StoreIntegrityError(msg)
        if receipt not in self._projections:
            tables = {}
            for table in PROJECTION_TABLES:
                if receipt not in self._verified:
                    verify_snapshot_effect(
                        plan,
                        table.table_path,
                        self.snapshot(table.table_path),
                        self.controls,
                    )
                frames = [
                    frame.filter(
                        pl.col("logical_commit_id") == plan.logical_commit_id
                    ).drop("logical_commit_id")
                    for frame in self.snapshot(table.table_path).iter_frames()
                ]
                tables[table.name] = (
                    pl.concat(frames)
                    if frames
                    else pl.DataFrame(schema=dict(table.schema))
                )
            self._projections[receipt] = decode_projection(
                tables, delivery=receipt.delivery
            )
        return self._projections[receipt]
