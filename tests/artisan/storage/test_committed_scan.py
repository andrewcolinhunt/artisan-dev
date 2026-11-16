"""Pure visibility and owner-audit rules used by verified retained reads."""

from __future__ import annotations

import polars as pl
import pytest

from artisan.errors import StoreIntegrityError
from artisan.schemas.enums import TablePath
from artisan.storage.core.committed_scan import (
    audit_table_owners,
    filter_committed_rows,
)
from artisan.storage.core.table_schemas import LOGICAL_COMMITS_SCHEMA


@pytest.mark.parametrize("owner", [None, "input_registration:unknown"])
def test_scan_hides_invalid_owners_while_explicit_audit_rejects(owner):
    physical = pl.DataFrame(
        {"artifact_id": ["a" * 32], "logical_commit_id": [owner]},
        schema={"artifact_id": pl.String, "logical_commit_id": pl.String},
    )
    controls = pl.DataFrame(schema=LOGICAL_COMMITS_SCHEMA)
    assert filter_committed_rows(
        physical, TablePath.ARTIFACT_INDEX.value, controls
    ).is_empty()
    with pytest.raises(StoreIntegrityError, match="unowned|unknown owners"):
        audit_table_owners(physical, TablePath.ARTIFACT_INDEX.value, controls)


def test_step_visibility_requires_valid_terminal_owner():
    complete_id = "step_result:" + "a" * 32
    controls = pl.DataFrame({"logical_commit_id": [complete_id], "state": ["complete"]})
    physical = pl.DataFrame(
        {
            "step_run_id": ["a" * 32] * 6,
            "status": [
                "running",
                "succeeded",
                "partial",
                "succeeded",
                "cancelled",
                "succeeded",
            ],
            "logical_commit_id": [
                None,
                None,
                None,
                "step_result:unknown",
                None,
                complete_id,
            ],
        }
    )
    assert filter_committed_rows(physical, TablePath.STEPS.value, controls)[
        "status"
    ].to_list() == ["running", "cancelled", "succeeded"]
    with pytest.raises(StoreIntegrityError):
        audit_table_owners(physical, TablePath.STEPS.value, controls)
