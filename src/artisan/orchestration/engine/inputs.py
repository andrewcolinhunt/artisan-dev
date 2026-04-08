"""Resolve OutputReference pointers to concrete artifact IDs.

Queries the Delta Lake executions and execution_edges tables to
translate lazy step-output references into sorted artifact ID lists.
"""

from __future__ import annotations

import logging

import polars as pl
from fsspec import AbstractFileSystem

logger = logging.getLogger(__name__)

from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.utils.path import uri_join


def resolve_output_reference(
    ref: OutputReference,
    delta_root: str,
    fs: AbstractFileSystem,
    step_run_id: str | None = None,
    storage_options: dict[str, str] | None = None,
) -> list[str]:
    """Resolve an OutputReference to concrete artifact IDs.

    Queries the executions and execution_edges Delta Lake tables
    for successful executions of the source step, extracts artifact IDs for
    outputs matching the requested role, and returns them sorted alphabetically
    for deterministic batching.

    Args:
        ref: OutputReference containing source_step and role.
        delta_root: Root URI for Delta Lake tables.
        fs: Filesystem implementation for path operations.
        step_run_id: If provided, scope results to this step run only.
        storage_options: Delta-rs storage options for cloud backends.

    Returns:
        Sorted list of artifact IDs. Empty list if no outputs match the
        requested role, if the source step has no successful executions,
        or if the executions/edges tables don't exist yet.

    Example:
        >>> ref = OutputReference(source_step=0, role="data")
        >>> ids = resolve_output_reference(ref, "/data/delta", fs)
        >>> # Returns: ["abc123...", "def456...", "ghi789..."] (sorted)
    """
    executions_path = uri_join(delta_root, TablePath.EXECUTIONS)
    execution_edges_path = uri_join(delta_root, TablePath.EXECUTION_EDGES)

    if not fs.exists(executions_path):
        logger.warning(
            "No executions table found for step %d — returning empty inputs.",
            ref.source_step,
        )
        return []

    # Query successful executions for the source step
    query = (
        pl.scan_delta(executions_path, storage_options=storage_options)
        .filter(pl.col("origin_step_number") == ref.source_step)
        .filter(pl.col("success") == True)  # noqa: E712
    )
    if step_run_id:
        query = query.filter(pl.col("step_run_id") == step_run_id)
    records_result = query.select("execution_run_id").collect()

    if records_result.is_empty():
        logger.warning(
            "No successful executions for step %d — returning empty inputs.",
            ref.source_step,
        )
        return []

    # Get list of successful execution_run_ids
    execution_run_ids = records_result["execution_run_id"].to_list()

    # Query execution_edges for outputs matching role
    if not fs.exists(execution_edges_path):
        logger.warning(
            "No execution edges table found for step %d — returning empty inputs.",
            ref.source_step,
        )
        return []

    provenance_result = (
        pl.scan_delta(execution_edges_path, storage_options=storage_options)
        .filter(pl.col("execution_run_id").is_in(execution_run_ids))
        .filter(pl.col("direction") == "output")
        .filter(pl.col("role") == ref.role)
        .select("artifact_id")
        .collect()
    )

    artifact_ids = provenance_result["artifact_id"].to_list()

    if not artifact_ids:
        logger.warning(
            "Step %d produced no outputs for role '%s' "
            "— downstream step will receive empty inputs.",
            ref.source_step,
            ref.role,
        )
        return []

    # Sort alphabetically for deterministic batching
    # Deduplicate in case same artifact appears multiple times
    return sorted(set(artifact_ids))


def resolve_inputs(
    inputs: (dict[str, OutputReference | list[str]] | list[OutputReference] | None),
    delta_root: str,
    fs: AbstractFileSystem,
    step_run_ids: dict[int, str] | None = None,
    storage_options: dict[str, str] | None = None,
) -> dict[str, list[str]]:
    """Resolve all inputs to concrete artifact IDs.

    Handles multiple input formats:
    - dict[str, OutputReference]: Resolve each reference
    - dict[str, list[str]]: Pass through (already artifact IDs)
    - list[OutputReference]: For runtime-defined inputs (MergeOp), auto-generate role names
    - None: Return empty dict (generative operations)

    Note: Raw file paths (list[str] of paths) are NOT handled here.
    File path promotion is done in PipelineManager.submit() before dispatch.

    Args:
        inputs: Input specification in any supported format.
        delta_root: Root URI for Delta Lake tables.
        fs: Filesystem implementation for path operations.
        storage_options: Delta-rs storage options for cloud backends.

    Returns:
        Dict mapping role names to lists of artifact IDs.

    Raises:
        ValueError: If raw file paths are passed (handled elsewhere).

    Example:
        # OutputReference inputs
        resolved = resolve_inputs(
            {"data": OutputReference(source_step=0, role="data")},
            delta_root,
        )
        # Returns: {"data": ["abc123...", "def456...", ...]}

        # List of OutputReferences (for MergeOp) - flattened to single role
        resolved = resolve_inputs(
            [OutputReference(source_step=1, role="out"), OutputReference(source_step=2, role="out")],
            delta_root,
        )
        # Returns: {"_merged_streams": ["abc...", "def...", ...]}  # All IDs flattened
    """
    if inputs is None:
        return {}

    if isinstance(inputs, list):
        if not inputs:
            return {}

        # Distinguish between OutputReference list and file path list
        first_item = inputs[0]
        if isinstance(first_item, OutputReference):
            # List of OutputReferences - convert to dict with auto-generated keys
            return _resolve_list_inputs(
                inputs, delta_root, fs, step_run_ids, storage_options
            )
        # File paths are handled in _execute_curator_step, not here
        raise ValueError(
            "Raw file paths must be handled by _execute_curator_step(). "
            "This function should not receive file paths directly."
        )

    resolved: dict[str, list[str]] = {}

    for role, value in inputs.items():
        if isinstance(value, OutputReference):
            sri = step_run_ids.get(value.source_step) if step_run_ids else None
            resolved[role] = resolve_output_reference(
                value, delta_root, fs, step_run_id=sri, storage_options=storage_options
            )
        elif isinstance(value, list):
            # Already artifact IDs - validate format
            for artifact_id in value:
                if not isinstance(artifact_id, str) or len(artifact_id) != 32:
                    raise ValueError(
                        f"Invalid artifact ID in inputs['{role}']: {artifact_id!r}. "
                        f"Expected 32-character hex string."
                    )
            resolved[role] = sorted(value)  # Sort for determinism
        else:
            raise TypeError(
                f"Invalid input type for role '{role}': {type(value).__name__}. "
                f"Expected OutputReference or list[str]."
            )

    return resolved


def _resolve_list_inputs(
    refs: list[OutputReference],
    delta_root: str,
    fs: AbstractFileSystem,
    step_run_ids: dict[int, str] | None = None,
    storage_options: dict[str, str] | None = None,
) -> dict[str, list[str]]:
    """Flatten a list of OutputReferences into a single ``_merged_streams`` role.

    Used for curator operations (e.g. Merge) that receive all inputs at
    once regardless of source stream.
    """
    all_artifact_ids: list[str] = []

    for i, ref in enumerate(refs):
        if not isinstance(ref, OutputReference):
            raise TypeError(
                f"List inputs must contain OutputReference objects, "
                f"got {type(ref).__name__} at index {i}"
            )
        sri = step_run_ids.get(ref.source_step) if step_run_ids else None
        artifact_ids = resolve_output_reference(
            ref, delta_root, fs, step_run_id=sri, storage_options=storage_options
        )
        all_artifact_ids.extend(artifact_ids)

    # Sort all artifact IDs for determinism
    return {"_merged_streams": sorted(all_artifact_ids)}
