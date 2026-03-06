"""Chain executor — run multiple creator operations in sequence within a single worker.

Artifacts are passed in-memory between operations, avoiding Delta Lake round-trips
for tightly coupled operations.
"""

from __future__ import annotations

import logging
from typing import Any

from artisan.execution.executors.creator import (
    LifecycleResult,
    _ExecuteFailure,
    _PostprocessFailure,
    run_creator_lifecycle,
)
from artisan.execution.models.artifact_source import ArtifactSource
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.staging.parquet_writer import StagingResult
from artisan.execution.staging.recorder import record_execution_success
from artisan.execution.utils import generate_execution_run_id
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.provenance import ArtifactProvenanceEdge
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.specs.input_spec import InputSpec
from artisan.utils.errors import format_error

logger = logging.getLogger(__name__)


def remap_output_roles(
    prev_outputs: dict[str, ArtifactSource],
    next_input_spec: dict[str, InputSpec],
    mapping: dict[str, str] | None,
) -> dict[str, ArtifactSource]:
    """Map previous operation's outputs to next operation's input roles.

    When mapping is None, role names are matched by identity. When mapping
    is provided, its entries take precedence (keys = prev output roles,
    values = next input roles). Unmapped roles matching by name are still
    forwarded (additive, not exclusive).

    Args:
        prev_outputs: Output artifacts from the previous operation, keyed by role.
        next_input_spec: Input specs of the next operation.
        mapping: Explicit role name remapping, or None for identity.

    Returns:
        Dict of input role name to ArtifactSource for the next operation.
    """
    result: dict[str, ArtifactSource] = {}

    if mapping is not None:
        # Explicit mappings take precedence
        for out_role, in_role in mapping.items():
            if out_role in prev_outputs:
                result[in_role] = prev_outputs[out_role]

    # Identity-match remaining output roles to input roles
    for out_role, source in prev_outputs.items():
        if out_role in next_input_spec and out_role not in result:
            # Only forward if not already mapped by explicit mapping
            already_mapped = mapping is not None and out_role in {
                v for v in mapping.values()
            }
            if not already_mapped:
                result[out_role] = source

    return result


def validate_required_roles(
    sources: dict[str, ArtifactSource],
    input_spec: dict[str, InputSpec],
) -> None:
    """Validate that all required input roles have sources.

    Args:
        sources: Available artifact sources keyed by input role.
        input_spec: Input specifications declaring required/optional roles.

    Raises:
        ValueError: If a required role has no source.
    """
    for role, spec in input_spec.items():
        if spec.required and role not in sources:
            msg = (
                f"Required input role '{role}' has no source. "
                f"Available sources: {list(sources.keys())}"
            )
            raise ValueError(msg)


def run_creator_chain(
    chain: Any,  # ExecutionChain (imported at call time to avoid circular)
    runtime_env: RuntimeEnvironment,
    worker_id: int = 0,
) -> StagingResult:
    """Execute a chain of creator operations, passing artifacts in-memory.

    Args:
        chain: ExecutionChain with operations, role_mappings, and config.
        runtime_env: Paths and backend configuration.
        worker_id: Numeric worker identifier.

    Returns:
        StagingResult from the final operation's recording.
    """
    from datetime import UTC, datetime

    from artisan.execution.context.builder import build_creator_execution_context

    current_sources: dict[str, ArtifactSource] | None = None
    all_artifacts: list[dict[str, list[Artifact]]] = []
    all_edges: list[list[ArtifactProvenanceEdge]] = []
    all_timings: list[dict[str, float]] = []
    initial_input_artifacts: dict[str, list[Artifact]] | None = None

    timestamp_start = datetime.now(UTC)

    # Use first unit's spec ID for the chain-level execution
    first_unit = chain.operations[0]
    execution_run_id = generate_execution_run_id(
        first_unit.execution_spec_id, timestamp_start, worker_id
    )

    try:
        for i, unit in enumerate(chain.operations):
            is_first = i == 0
            op_label = f"[chain {i + 1}/{len(chain.operations)}: {unit.operation.name}]"

            if is_first:
                sources = {
                    role: ArtifactSource.from_ids(ids)
                    for role, ids in unit.inputs.items()
                }
            else:
                input_spec = getattr(type(unit.operation), "inputs", {})
                mapping = chain.role_mappings[i - 1]
                sources = remap_output_roles(current_sources, input_spec, mapping)

                # Merge Delta-backed inputs for roles not satisfied by previous op
                for role, ids in unit.inputs.items():
                    if role not in sources:
                        sources[role] = ArtifactSource.from_ids(ids)

                validate_required_roles(sources, input_spec)

            logger.info("Chain operation %s starting", op_label)

            result = run_creator_lifecycle(
                unit, runtime_env, worker_id,
                execution_run_id=execution_run_id,
                sources=sources if not is_first else None,
            )

            if is_first:
                initial_input_artifacts = result.input_artifacts

            all_artifacts.append(result.artifacts)
            all_edges.append(result.edges)
            all_timings.append(result.timings)

            # Build sources for next operation from this one's outputs
            current_sources = {
                role: ArtifactSource.from_artifacts(arts)
                for role, arts in result.artifacts.items()
            }

            logger.info("Chain operation %s completed", op_label)

        # Record the final operation's results
        final_unit = chain.operations[-1]
        final_artifacts = all_artifacts[-1]
        final_edges = all_edges[-1]

        # Collect original inputs from the first unit
        original_inputs = {
            role: list(ids) for role, ids in first_unit.inputs.items()
        }

        working_root = runtime_env.working_root_path
        if working_root is None:
            msg = "RuntimeEnvironment.working_root_path must be set"
            raise ValueError(msg)

        execution_context = build_creator_execution_context(
            execution_run_id=execution_run_id,
            execution_spec_id=first_unit.execution_spec_id,
            step_number=first_unit.step_number,
            timestamp_start=timestamp_start,
            worker_id=worker_id,
            delta_root_path=runtime_env.delta_root_path,
            staging_root_path=runtime_env.staging_root_path,
            operation=final_unit.operation,
            sandbox_path=working_root / "dummy",
            compute_backend_name=runtime_env.compute_backend_name,
            shared_filesystem=runtime_env.shared_filesystem,
        )

        params_dict = (
            final_unit.operation.params.model_dump(mode="json")
            if hasattr(final_unit.operation, "params")
            else {}
        )

        staging_result = record_execution_success(
            execution_context=execution_context,
            artifacts=final_artifacts,
            lineage_edges=final_edges,
            inputs=original_inputs,
            timestamp_end=datetime.now(UTC),
            params=params_dict,
            result_metadata={"chain_timings": all_timings},
        )

    except (_PostprocessFailure, _ExecuteFailure, Exception) as exc:
        if isinstance(exc, (_PostprocessFailure, _ExecuteFailure)):
            error = str(exc)
        else:
            error = format_error(exc)

        logger.error("Chain failed: %s", error)
        staging_result = StagingResult(
            success=False,
            error=error,
            execution_run_id=execution_run_id,
            artifact_ids=[],
        )

    return staging_result
