"""Orchestration execution engine modules.

This package contains the orchestration engine used by PipelineManager.run():
- inputs: OutputReference -> artifact IDs resolution
- batching: Two-level batching (ExecutionUnit and worker)
- results: Result aggregation and error handling
- dispatch: Prefect task factory for worker execution
- step_executor: Main step execution coordinator
"""

from __future__ import annotations

from artisan.orchestration.engine.batching import (
    generate_execution_unit_batches,
    get_batch_config,
)
from artisan.orchestration.engine.dispatch import (
    execute_unit_task,
)
from artisan.orchestration.engine.inputs import (
    resolve_inputs,
    resolve_output_reference,
)
from artisan.orchestration.engine.results import (
    aggregate_results,
    extract_execution_run_ids,
)
from artisan.orchestration.engine.step_executor import execute_step
from artisan.schemas.orchestration.batch_config import BatchConfig

__all__ = [
    "resolve_output_reference",
    "resolve_inputs",
    "BatchConfig",
    "get_batch_config",
    "generate_execution_unit_batches",
    "aggregate_results",
    "extract_execution_run_ids",
    "execute_unit_task",
    "execute_step",
]
