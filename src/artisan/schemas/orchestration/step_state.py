"""Step state schema for pipeline resume from persisted state."""

from __future__ import annotations

from pydantic import BaseModel

from artisan.schemas.orchestration.step_result import StepResult


class StepState(BaseModel):
    """Persisted step state loaded from the steps delta table.

    Used by PipelineManager.resume() to reconstruct StepResult objects.
    Fields are self-contained — no operation class import needed.
    """

    pipeline_run_id: str
    step_number: int
    step_name: str
    step_spec_id: str
    status: str
    operation_class: str
    params_json: str
    input_refs_json: str
    compute_backend: str
    compute_options_json: str
    total_count: int
    succeeded_count: int
    failed_count: int
    duration_seconds: float | None
    output_roles: frozenset[str]
    output_types: dict[str, str | None]

    def to_step_result(self) -> StepResult:
        """Reconstruct a StepResult from persisted state.

        Derives ``success`` from ``failed_count == 0``.
        """
        return StepResult(
            step_name=self.step_name,
            step_number=self.step_number,
            success=self.failed_count == 0,
            total_count=self.total_count,
            succeeded_count=self.succeeded_count,
            failed_count=self.failed_count,
            output_roles=self.output_roles,
            output_types=self.output_types,
            duration_seconds=self.duration_seconds,
        )

    model_config = {"frozen": True}
