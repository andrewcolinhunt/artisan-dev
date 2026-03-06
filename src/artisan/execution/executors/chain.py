"""Chain executor — run multiple creator operations in sequence within a single worker.

Artifacts are passed in-memory between operations, avoiding Delta Lake round-trips
for tightly coupled operations.
"""

from __future__ import annotations

import logging
from typing import Any

from artisan.execution.executors.creator import (
    _ExecuteFailure,
    _PostprocessFailure,
    run_creator_lifecycle,
)
from artisan.execution.models.artifact_source import ArtifactSource
from artisan.execution.models.execution_chain import ChainIntermediates
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
            already_mapped = mapping is not None and out_role in set(mapping.values())
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


def update_ancestor_map(
    ancestor_map: dict[str, list[str]],
    edges: list[ArtifactProvenanceEdge],
    is_first: bool,
) -> dict[str, list[str]]:
    """Compose per-operation edges into transitive ancestor tracking.

    Args:
        ancestor_map: Current ancestor map (artifact_id → initial_input_ids).
        edges: Provenance edges from the current operation.
        is_first: Whether this is the first operation in the chain.

    Returns:
        Updated ancestor map.
    """
    updated = dict(ancestor_map)
    for edge in edges:
        src = edge.source_artifact_id
        tgt = edge.target_artifact_id
        if is_first:
            # First operation: source IS an initial input
            updated.setdefault(tgt, []).append(src)
        elif src in ancestor_map:
            # Subsequent: target inherits source's ancestors
            updated.setdefault(tgt, []).extend(ancestor_map[src])
    return updated


def _build_shortcut_edges(
    ancestor_map: dict[str, list[str]],
    final_artifacts: dict[str, list[Artifact]],
    execution_run_id: str,
) -> list[ArtifactProvenanceEdge]:
    """Create shortcut edges from initial inputs to final outputs.

    Args:
        ancestor_map: Transitive ancestor tracking.
        final_artifacts: Final operation's output artifacts keyed by role.
        execution_run_id: Chain execution run ID.

    Returns:
        List of step_boundary=True shortcut edges.
    """
    shortcut_edges: list[ArtifactProvenanceEdge] = []
    for role, artifacts in final_artifacts.items():
        for artifact in artifacts:
            aid = artifact.artifact_id
            if aid is None or aid not in ancestor_map:
                continue
            for ancestor_id in ancestor_map[aid]:
                shortcut_edges.append(
                    ArtifactProvenanceEdge(
                        execution_run_id=execution_run_id,
                        source_artifact_id=ancestor_id,
                        target_artifact_id=aid,
                        source_artifact_type="UNKNOWN",
                        target_artifact_type=getattr(
                            artifact, "artifact_type", "UNKNOWN"
                        ),
                        source_role="chain_input",
                        target_role=role,
                        step_boundary=True,
                    )
                )
    return shortcut_edges


def _collect_chain_edges(
    all_edges: list[list[ArtifactProvenanceEdge]],
    ancestor_map: dict[str, list[str]],
    final_artifacts: dict[str, list[Artifact]],
    execution_run_id: str,
    intermediates: ChainIntermediates,
) -> list[ArtifactProvenanceEdge]:
    """Assemble final edge set based on intermediates mode.

    Args:
        all_edges: Per-operation edge lists.
        ancestor_map: Transitive ancestor tracking.
        final_artifacts: Final operation's output artifacts.
        execution_run_id: Chain execution run ID.
        intermediates: How to handle intermediate edges.

    Returns:
        Combined edge list for staging.
    """
    shortcut_edges = _build_shortcut_edges(
        ancestor_map, final_artifacts, execution_run_id
    )

    if intermediates == ChainIntermediates.DISCARD:
        # Only shortcut edges — internal edges discarded
        return shortcut_edges

    # PERSIST or EXPOSE: include internal edges with step_boundary=False
    internal_edges: list[ArtifactProvenanceEdge] = []
    for op_edges in all_edges:
        for edge in op_edges:
            internal_edges.append(edge.model_copy(update={"step_boundary": False}))

    return internal_edges + shortcut_edges


def _collect_chain_artifacts(
    all_artifacts: list[dict[str, list[Artifact]]],
    intermediates: ChainIntermediates,
) -> dict[str, list[Artifact]]:
    """Collect artifacts to stage based on intermediates mode.

    Args:
        all_artifacts: Per-operation artifact dicts.
        intermediates: How to handle intermediate artifacts.

    Returns:
        Artifacts dict to stage.
    """
    if intermediates == ChainIntermediates.DISCARD:
        return all_artifacts[-1]

    # PERSIST or EXPOSE: merge all operations' artifacts
    merged: dict[str, list[Artifact]] = {}
    for i, op_artifacts in enumerate(all_artifacts):
        for role, arts in op_artifacts.items():
            # Prefix intermediate roles to avoid collisions
            if i < len(all_artifacts) - 1:
                key = f"_chain_op{i}_{role}"
            else:
                key = role
            merged[key] = arts
    return merged


def run_creator_chain(
    chain: Any,  # ExecutionChain
    runtime_env: RuntimeEnvironment,
    worker_id: int = 0,
) -> StagingResult:
    """Execute a chain of creator operations, passing artifacts in-memory.

    Args:
        chain: ExecutionChain with operations, role_mappings, and config.
        runtime_env: Paths and backend configuration.
        worker_id: Numeric worker identifier.

    Returns:
        StagingResult from recording the chain's results.
    """
    from datetime import UTC, datetime

    from artisan.execution.context.builder import build_creator_execution_context

    current_sources: dict[str, ArtifactSource] | None = None
    all_artifacts: list[dict[str, list[Artifact]]] = []
    all_edges: list[list[ArtifactProvenanceEdge]] = []
    all_timings: list[dict[str, float]] = []
    ancestor_map: dict[str, list[str]] = {}

    timestamp_start = datetime.now(UTC)

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

                for role, ids in unit.inputs.items():
                    if role not in sources:
                        sources[role] = ArtifactSource.from_ids(ids)

                validate_required_roles(sources, input_spec)

            logger.info("Chain operation %s starting", op_label)

            result = run_creator_lifecycle(
                unit,
                runtime_env,
                worker_id,
                execution_run_id=execution_run_id,
                sources=sources if not is_first else None,
            )

            ancestor_map = update_ancestor_map(ancestor_map, result.edges, is_first)

            all_artifacts.append(result.artifacts)
            all_edges.append(result.edges)
            all_timings.append(result.timings)

            current_sources = {
                role: ArtifactSource.from_artifacts(arts)
                for role, arts in result.artifacts.items()
            }

            logger.info("Chain operation %s completed", op_label)

        # Assemble edges and artifacts based on intermediates mode
        final_artifacts = _collect_chain_artifacts(all_artifacts, chain.intermediates)
        final_edges = _collect_chain_edges(
            all_edges,
            ancestor_map,
            all_artifacts[-1],
            execution_run_id,
            chain.intermediates,
        )

        original_inputs = {role: list(ids) for role, ids in first_unit.inputs.items()}

        working_root = runtime_env.working_root_path
        if working_root is None:
            msg = "RuntimeEnvironment.working_root_path must be set"
            raise ValueError(msg)

        final_unit = chain.operations[-1]
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
