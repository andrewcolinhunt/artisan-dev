"""Pipeline submission, finite results, and current run inspection."""

from __future__ import annotations

from typing import TYPE_CHECKING

from artisan.orchestration.run_history import list_runs
from artisan.orchestration.step_future import StepFuture
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.orchestration.pipeline_config import PipelineConfig
from artisan.schemas.orchestration.replay_result import ReplayResult
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationStatus,
    StepDisposition,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult

if TYPE_CHECKING:
    from artisan.orchestration.pipeline_manager import PipelineManager
    from artisan.orchestration.replay import replay_execution

__all__ = [
    "CancellationStatus",
    "OutputReference",
    "PipelineConfig",
    "PipelineManager",
    "ReplayResult",
    "StepDisposition",
    "StepFuture",
    "StepResult",
    "StepStatus",
    "list_runs",
    "replay_execution",
]


def __getattr__(name: str) -> object:
    # Worker contracts import orchestration submodules before controller startup.
    if name == "PipelineManager":
        from artisan.orchestration.pipeline_manager import PipelineManager

        return PipelineManager
    if name == "replay_execution":
        from artisan.orchestration.replay import replay_execution

        return replay_execution
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
