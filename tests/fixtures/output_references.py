"""Synthetic source-bound references for tests that do not access a store."""

from __future__ import annotations

from artisan.schemas.orchestration.output_reference import (
    OutputReference,
    StepSource,
    StoreIdentity,
)

SYNTHETIC_STORE = StoreIdentity(
    delta_root="/synthetic/delta",
    files_root="/synthetic/files",
    steps_table_id="00000000-0000-0000-0000-000000000001",
)


def output_reference(
    step_run_id: str,
    *,
    role: str = "data",
    artifact_type: str = "any",
    store: StoreIdentity = SYNTHETIC_STORE,
) -> OutputReference:
    """Construct an exact source; real-store tests supply their actual identity."""
    return OutputReference(
        source=StepSource(store=store, step_run_id=step_run_id),
        role=role,
        artifact_type=artifact_type,
    )
