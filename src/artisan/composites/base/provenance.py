"""Provenance helpers for composites.

Functions for transitive lineage composition, shortcut edge construction,
and artifact/edge collection based on intermediates mode.
"""

from __future__ import annotations

from artisan.execution.models.execution_composite import CompositeIntermediates
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.provenance import ArtifactProvenanceEdge
from artisan.schemas.specs.input_spec import InputSpec


def validate_required_roles(
    sources: dict[str, object],
    input_spec: dict[str, InputSpec],
) -> None:
    """Validate that all required input roles have sources.

    Args:
        sources: Available sources keyed by input role.
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
        ancestor_map: Current ancestor map (artifact_id -> initial_input_ids).
        edges: Provenance edges from the current operation.
        is_first: Whether this is the first operation in the composite.

    Returns:
        Updated ancestor map (does not mutate the original).
    """
    updated = dict(ancestor_map)
    for edge in edges:
        src = edge.source_artifact_id
        tgt = edge.target_artifact_id
        if is_first:
            updated.setdefault(tgt, []).append(src)
        elif src in ancestor_map:
            updated.setdefault(tgt, []).extend(ancestor_map[src])
    return updated


def _build_shortcut_edges(
    ancestor_map: dict[str, list[str]],
    final_artifacts: dict[str, list[Artifact]],
    execution_run_id: str,
) -> list[ArtifactProvenanceEdge]:
    """Create shortcut edges from composite inputs to composite outputs.

    Args:
        ancestor_map: Transitive ancestor tracking.
        final_artifacts: Final output artifacts keyed by role.
        execution_run_id: Composite execution run ID.

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
                        source_role="composite_input",
                        target_role=role,
                        step_boundary=True,
                    )
                )
    return shortcut_edges


def _collect_composite_edges(
    all_edges: list[list[ArtifactProvenanceEdge]],
    ancestor_map: dict[str, list[str]],
    final_artifacts: dict[str, list[Artifact]],
    execution_run_id: str,
    intermediates: CompositeIntermediates,
) -> list[ArtifactProvenanceEdge]:
    """Assemble final edge set based on intermediates mode.

    Args:
        all_edges: Per-operation edge lists.
        ancestor_map: Transitive ancestor tracking.
        final_artifacts: Final output artifacts keyed by role.
        execution_run_id: Composite execution run ID.
        intermediates: How to handle intermediate edges.

    Returns:
        Combined edge list for staging.
    """
    shortcut_edges = _build_shortcut_edges(
        ancestor_map, final_artifacts, execution_run_id
    )

    if intermediates == CompositeIntermediates.DISCARD:
        return shortcut_edges

    # PERSIST: internal edges with step_boundary=False
    # EXPOSE: internal edges with step_boundary=True
    internal_step_boundary = intermediates == CompositeIntermediates.EXPOSE
    internal_edges: list[ArtifactProvenanceEdge] = []
    for op_edges in all_edges:
        for edge in op_edges:
            internal_edges.append(
                edge.model_copy(update={"step_boundary": internal_step_boundary})
            )

    return internal_edges + shortcut_edges


def _collect_composite_artifacts(
    all_artifacts: list[dict[str, list[Artifact]]],
    composite_name: str,
    intermediates: CompositeIntermediates,
) -> dict[str, list[Artifact]]:
    """Collect artifacts to stage based on intermediates mode.

    Args:
        all_artifacts: Per-operation artifact dicts.
        composite_name: Name of the composite (for role prefixing).
        intermediates: How to handle intermediate artifacts.

    Returns:
        Artifacts dict to stage.
    """
    if intermediates == CompositeIntermediates.DISCARD:
        return all_artifacts[-1]

    # PERSIST or EXPOSE: merge all operations' artifacts
    merged: dict[str, list[Artifact]] = {}
    for i, op_artifacts in enumerate(all_artifacts):
        for role, arts in op_artifacts.items():
            if i < len(all_artifacts) - 1:
                key = f"_composite_{composite_name}_{role}"
            else:
                key = role
            merged[key] = arts
    return merged
