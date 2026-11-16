"""Lossy content-level curator views of exact retained occurrence evidence."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import polars as pl

from artisan.errors import StoreIntegrityError
from artisan.schemas.execution.assignment import SealedDelivery
from artisan.schemas.provenance.projection import BoundaryProjection
from artisan.storage.core.committed_scan import verify_plan_effect
from artisan.storage.core.projection_schemas import PROJECTION_TABLES
from artisan.storage.core.table_schemas import (
    ARTIFACT_EDGES_SCHEMA,
    EXECUTION_EDGES_SCHEMA,
)
from artisan.storage.io.projection_encoding import decode_projection
from artisan.storage.io.retained_plan import RetainedCommitPlan

if TYPE_CHECKING:
    from artisan.storage.core.curator_view import CuratorView


type RetainedProjections = Mapping[str, tuple[SealedDelivery, BoundaryProjection]]


def capture_retained_projections(view: CuratorView) -> RetainedProjections:
    """Verify only projection metadata against frozen completed logical effects."""
    plans = []
    for row in view.controls.filter(
        pl.col("commit_kind") == "retained_result"
    ).iter_rows(named=True):
        plan = view.committer._control_plan(row)
        if not isinstance(plan, RetainedCommitPlan):
            msg = "Retained curator view has another logical plan domain"
            raise StoreIntegrityError(msg)
        if plan.receipt.delivery.attempt.replay_of_execution_run_id is None:
            plans.append(plan)
    if not plans:
        return {}
    physical = {
        table.name: view.physical(table.table_path).collect()
        for table in PROJECTION_TABLES
    }
    projections = {}
    for plan in plans:
        tables = {}
        for table in PROJECTION_TABLES:
            frame = physical[table.name]
            verify_plan_effect(plan, table.table_path, frame, view.controls)
            tables[table.name] = frame.filter(
                pl.col("logical_commit_id") == plan.logical_commit_id
            ).drop("logical_commit_id")
        projection = decode_projection(tables, delivery=plan.receipt.delivery)
        projections[plan.receipt.delivery.delivery_id] = plan.receipt, projection
    return projections


def artifact_edges(
    projections: Mapping[str, BoundaryProjection], *, direct: bool
) -> pl.DataFrame:
    """Project content IDs only for the curator's existing graph-query contract."""
    rows: list[dict[str, Any]] = []
    for delivery_id, projection in projections.items():
        inventory = {item.address: item for item in projection.occurrences}
        boundary = projection.boundary_addresses()
        for relation in projection.associations if direct else projection.parents:
            source, target = inventory[relation.source], inventory[relation.target]
            if source.artifact_id == target.artifact_id:
                continue
            rows.append(
                {
                    "execution_run_id": delivery_id,
                    "source_artifact_id": source.artifact_id,
                    "target_artifact_id": target.artifact_id,
                    "source_artifact_type": source.artifact_type,
                    "target_artifact_type": target.artifact_type,
                    "source_role": source.address.role,
                    "target_role": target.address.role,
                    "group_id": None,
                    "step_boundary": source.address in boundary,
                }
            )
    return pl.DataFrame(rows, schema=ARTIFACT_EDGES_SCHEMA).unique(maintain_order=True)


def execution_edges(projections: Mapping[str, BoundaryProjection]) -> pl.DataFrame:
    """Expose input/export content membership without inventing execution logs."""
    rows: list[dict[str, str]] = []
    for delivery_id, projection in projections.items():
        ids = {item.address: item.artifact_id for item in projection.occurrences}
        for direction, groups in (
            ("input", projection.boundary_inputs),
            ("output", projection.exports),
        ):
            rows.extend(
                {
                    "execution_run_id": delivery_id,
                    "direction": direction,
                    "role": group.role,
                    "artifact_id": ids[address],
                }
                for group in groups
                for address in group.occurrences
            )
    return pl.DataFrame(rows, schema=EXECUTION_EDGES_SCHEMA).unique(maintain_order=True)
