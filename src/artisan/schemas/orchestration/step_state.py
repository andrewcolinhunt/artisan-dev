"""Step state schema for pipeline resume from persisted state."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from artisan.schemas.orchestration.step_lifecycle import (
    CancellationStatus,
    StepDisposition,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult


class StepState(BaseModel):
    """Persisted step state loaded from the steps delta table.

    Used by PipelineManager.resume() to reconstruct StepResult objects.
    Fields are self-contained — no operation class import needed.
    """

    pipeline_run_id: str
    step_run_id: str = ""
    step_number: int
    step_name: str
    step_spec_id: str | None
    status: StepStatus
    state_sequence: int
    disposition: StepDisposition | None = None
    cancellation_status: CancellationStatus | None = None
    operation_class: str
    params_json: str
    input_refs_json: str
    compute_backend: str
    compute_options_json: str
    total_count: int | None
    succeeded_count: int | None
    failed_count: int | None
    timestamp: datetime
    duration_seconds: float | None
    error: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)
    output_roles: frozenset[str]
    output_types: dict[str, str | None]

    def to_step_result(self) -> StepResult:
        """Reconstruct a StepResult from persisted state.

        Preserves the persisted lifecycle state without inference.
        """
        return StepResult(
            step_name=self.step_name,
            step_number=self.step_number,
            status=self.status,
            disposition=self.disposition,
            cancellation_status=self.cancellation_status,
            error=self.error,
            total_count=self.total_count or 0,
            succeeded_count=self.succeeded_count or 0,
            failed_count=self.failed_count or 0,
            output_roles=self.output_roles,
            output_types=self.output_types,
            duration_seconds=self.duration_seconds,
            metadata=self.metadata,
            step_run_id=self.step_run_id or None,
        )

    model_config = {"frozen": True}
