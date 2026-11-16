"""Shared logical-commit journal shape and conditional completion."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import polars as pl
from deltalake import DeltaTable

from artisan.errors import StoreIntegrityError
from artisan.storage.core.table_schemas import LOGICAL_COMMITS_SCHEMA
from artisan.storage.io.commit_contract import LogicalEffect

_CONTROL_STATES = frozenset({"planned", "complete", "abandoned"})


def validate_control_rows(controls: pl.DataFrame) -> None:
    """Reject malformed journal state; each store domain validates owner identity."""
    required = (
        "logical_commit_id",
        "commit_kind",
        "step_run_id",
        "state",
        "plan_digest",
        "created_at",
    )
    invalid = controls.filter(
        ~pl.col("state").is_in(_CONTROL_STATES)
        | pl.any_horizontal(pl.col(name).is_null() for name in required)
    )
    duplicates = controls.group_by("logical_commit_id").len().filter(pl.col("len") != 1)
    malformed = controls.filter(
        ((pl.col("state") == "complete") & pl.col("completed_at").is_null())
        | ((pl.col("state") != "complete") & pl.col("completed_at").is_not_null())
        | ((pl.col("state") == "abandoned") & pl.col("abandon_reason").is_null())
        | ((pl.col("state") != "abandoned") & pl.col("abandon_reason").is_not_null())
    )
    if not invalid.is_empty() or not duplicates.is_empty() or not malformed.is_empty():
        msg = "Logical commit control rows are inconsistent"
        raise StoreIntegrityError(msg)


def control_for(plan: LogicalEffect, controls: pl.DataFrame) -> dict[str, Any] | None:
    """Require an existing decision to bind this exact immutable plan."""
    matches = controls.filter(pl.col("logical_commit_id") == plan.logical_commit_id)
    if matches.is_empty():
        return None
    row = matches.row(0, named=True)
    if (
        row["commit_kind"] != plan.commit_kind
        or row["step_run_id"] != plan.step_run_id
        or row["plan_digest"] != plan.plan_digest
        or row["recovery_batch_id"] != plan.recovery_batch_id
    ):
        msg = f"Control row disagrees with plan {plan.logical_commit_id}"
        raise StoreIntegrityError(msg)
    return row


def planned_control(plan: LogicalEffect) -> pl.DataFrame:
    """Construct the shared journal's initial invisible plan decision."""
    return pl.DataFrame(
        [
            {
                "logical_commit_id": plan.logical_commit_id,
                "commit_kind": plan.commit_kind,
                "step_run_id": plan.step_run_id,
                "recovery_batch_id": plan.recovery_batch_id,
                "state": "planned",
                "plan_digest": plan.plan_digest,
                "created_at": datetime.now(UTC),
                "completed_at": None,
                "abandon_reason": None,
            }
        ],
        schema=LOGICAL_COMMITS_SCHEMA,
    )


def complete_control(
    plan: LogicalEffect,
    path: str,
    *,
    storage_options: dict[str, str],
) -> None:
    """Conditionally complete exactly one planned effect after table verification."""
    table = DeltaTable(path, storage_options=storage_options)
    ownership = {
        "logical_commit_id": plan.logical_commit_id,
        "commit_kind": plan.commit_kind,
        "step_run_id": plan.step_run_id,
        "recovery_batch_id": plan.recovery_batch_id,
        "plan_digest": plan.plan_digest,
    }
    clauses = ["state = 'planned'"]
    for name, value in ownership.items():
        if value is None:
            clauses.append(f"{name} IS NULL")
        else:
            escaped = value.replace("'", "''")
            clauses.append(f"{name} = '{escaped}'")
    completed_at = datetime.now(UTC).isoformat()
    metrics = table.update(
        predicate=" AND ".join(clauses),
        updates={
            "state": "'complete'",
            "completed_at": f"CAST('{completed_at}' AS TIMESTAMP)",
        },
    )
    if metrics.get("num_updated_rows") != 1:
        msg = f"Control completion was not conditional for {plan.logical_commit_id}"
        raise StoreIntegrityError(msg)
