"""Composite executor — run a composite operation within a single worker.

Uses CollapsedCompositeContext for in-process execution of all internal
operations within a single pipeline step.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from artisan.composites.base.composite_context import CollapsedCompositeContext
from artisan.composites.base.provenance import (
    _collect_composite_artifacts,
    _collect_composite_edges,
    update_ancestor_map,
)
from artisan.execution.models.artifact_source import ArtifactSource
from artisan.execution.models.execution_composite import ExecutionComposite
from artisan.execution.staging.parquet_writer import StagingResult
from artisan.execution.staging.recorder import record_execution_success
from artisan.execution.utils import generate_execution_run_id
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.utils.errors import format_error

logger = logging.getLogger(__name__)


def run_composite(
    composite_transport: ExecutionComposite,
    runtime_env: RuntimeEnvironment,
    worker_id: int = 0,
) -> StagingResult:
    """Execute a composite operation, passing artifacts in-memory.

    Args:
        composite_transport: ExecutionComposite with composite instance,
            resolved inputs, and configuration.
        runtime_env: Paths and backend configuration.
        worker_id: Numeric worker identifier.

    Returns:
        StagingResult from recording the composite's results.
    """
    timestamp_start = datetime.now(UTC)
    composite = composite_transport.composite

    execution_run_id = generate_execution_run_id(
        composite_transport.execution_spec_id, timestamp_start, worker_id
    )

    try:
        # Build artifact sources from resolved input IDs
        sources: dict[str, ArtifactSource] = {
            role: ArtifactSource.from_ids(ids)
            for role, ids in composite_transport.inputs.items()
        }

        # Create artifact store for curator hydration
        from artisan.storage.core.artifact_store import ArtifactStore

        artifact_store = ArtifactStore(
            runtime_env.delta_root_path,
            files_root=runtime_env.files_root_path,
        )

        # Create collapsed context
        ctx = CollapsedCompositeContext(
            sources=sources,
            composite=composite,
            runtime_env=runtime_env,
            step_number=composite_transport.step_number,
            execution_run_id=execution_run_id,
            intermediates=composite_transport.intermediates,
            artifact_store=artifact_store,
        )

        # Execute compose()
        composite.compose(ctx)

        # Collect artifacts and edges based on intermediates mode
        all_artifacts = ctx.get_all_artifacts()
        all_edges = ctx.get_all_edges()
        all_timings = ctx.get_all_timings()

        if not all_artifacts:
            # No internal operations produced artifacts
            return StagingResult(
                success=True,
                execution_run_id=execution_run_id,
                artifact_ids=[],
            )

        # Build ancestor map for shortcut edges
        ancestor_map: dict[str, list[str]] = {}
        for i, op_edges in enumerate(all_edges):
            ancestor_map = update_ancestor_map(
                ancestor_map, op_edges, is_first=(i == 0)
            )

        # Collect final artifacts and edges
        final_artifacts = _collect_composite_artifacts(
            all_artifacts,
            composite.name,
            composite_transport.intermediates,
        )
        final_edges = _collect_composite_edges(
            all_edges,
            ancestor_map,
            all_artifacts[-1],
            execution_run_id,
            composite_transport.intermediates,
        )

        # Build execution context for recording
        from artisan.execution.context.builder import build_creator_execution_context

        working_root = runtime_env.working_root_path
        if working_root is None:
            msg = "RuntimeEnvironment.working_root_path must be set"
            raise ValueError(msg)

        execution_context = build_creator_execution_context(
            execution_run_id=execution_run_id,
            execution_spec_id=composite_transport.execution_spec_id,
            step_number=composite_transport.step_number,
            timestamp_start=timestamp_start,
            worker_id=worker_id,
            delta_root_path=runtime_env.delta_root_path,
            staging_root_path=runtime_env.staging_root_path,
            operation=composite,
            sandbox_path=working_root / "dummy",
            compute_backend_name=runtime_env.compute_backend_name,
            shared_filesystem=runtime_env.shared_filesystem,
            step_run_id=composite_transport.step_run_id,
        )

        from artisan.utils.hashing import serialize_params

        params_dict = serialize_params(composite)

        original_inputs = {
            role: list(ids) for role, ids in composite_transport.inputs.items()
        }

        staging_result = record_execution_success(
            execution_context=execution_context,
            artifacts=final_artifacts,
            lineage_edges=final_edges,
            inputs=original_inputs,
            timestamp_end=datetime.now(UTC),
            params=params_dict,
            result_metadata={"composite_timings": all_timings},
        )

    except Exception as exc:
        error = format_error(exc)
        logger.error("Composite %s failed: %s", composite.name, error)
        staging_result = StagingResult(
            success=False,
            error=error,
            execution_run_id=execution_run_id,
            artifact_ids=[],
        )

    return staging_result
