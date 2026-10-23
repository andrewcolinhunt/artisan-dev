"""Public outcome of one diagnostic replay and its retained evidence."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

from artisan.schemas.orchestration.step_result import StepResult


class ReplayResult(BaseModel):
    """Committed execution identity is absent when cancellation discarded work."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source_execution_run_id: str
    pipeline_run_id: str
    step_run_id: str
    execution_run_id: str | None
    step_result: StepResult
    diagnostic_roots: dict[str, str | None]
    diagnostic_status: Literal["complete", "incomplete", "unavailable"]
    diagnostic_errors: list[str]
    reproducibility_notes: list[str]
