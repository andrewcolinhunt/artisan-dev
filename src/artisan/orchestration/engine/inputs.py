"""Resolve OutputReference pointers to concrete artifact IDs.

Queries the Delta Lake executions and execution_edges tables to
translate lazy step-output references into sorted artifact ID lists.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import polars as pl
from fsspec import AbstractFileSystem

from artisan.errors import ArtifactIntegrityError
from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.utils.hashing import CacheInputIdentity
from artisan.utils.path import uri_join

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PreparedInputs:
    """One resolved, verified, grouped input snapshot used by both caches."""

    inputs: dict[str, list[str]]
    artifact_types: dict[str, str]
    group_ids: list[str] | None
    cache_inputs: dict[str, list[CacheInputIdentity]]


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
    from artisan.storage.core.store_format import assert_store_format

    assert_store_format(delta_root, fs)
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
    - list[OutputReference]: For runtime-defined inputs, auto-generate role names
    - None: Return empty dict (generative operations)

    Note: Raw file paths (list[str] of paths) are NOT handled here.
    File path promotion is done in PipelineManager.submit() before dispatch.

    Args:
        inputs: Input specification in any supported format.
        delta_root: Root URI for Delta Lake tables.
        fs: Filesystem implementation for path operations.
        step_run_ids: Optional mapping of source step number to step run
            ID. When provided, each reference is scoped to the matching
            step run. Defaults to None.
        storage_options: Delta-rs storage options for cloud backends.

    Returns:
        Dict mapping role names to lists of artifact IDs.

    Raises:
        ValueError: If raw file paths are passed as a list, or if an
            artifact ID is not a 32-character hex string.
        TypeError: If a dict value is neither an OutputReference nor a
            list of artifact IDs, or a list element is not an
            OutputReference.

    Example:
        # OutputReference inputs
        resolved = resolve_inputs(
            {"data": OutputReference(source_step=0, role="data")},
            delta_root,
        )
        # Returns: {"data": ["abc123...", "def456...", ...]}

        # List of OutputReferences - flattened to a single role
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
        msg = (  # type: ignore[unreachable]  # runtime defense: list may contain non-OutputReference
            "Raw file paths must be handled by _execute_curator_step(). "
            "This function should not receive file paths directly."
        )
        raise ValueError(msg)

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
                # runtime defense: value items may not be 32-char hex strings
                if not isinstance(artifact_id, str) or len(artifact_id) != 32:  # type: ignore[redundant-expr]
                    msg = (
                        f"Invalid artifact ID in inputs['{role}']: {artifact_id!r}. "
                        f"Expected 32-character hex string."
                    )
                    raise ValueError(msg)
            resolved[role] = list(value)
        else:
            msg = (  # type: ignore[unreachable]  # runtime defense against bad input types
                f"Invalid input type for role '{role}': {type(value).__name__}. "
                f"Expected OutputReference or list[str]."
            )
            raise TypeError(msg)

    return resolved


def prepare_inputs(
    inputs: dict[str, OutputReference | list[str]] | list[OutputReference] | None,
    delta_root: str,
    fs: AbstractFileSystem,
    *,
    group_by: object = None,
    step_run_ids: dict[int, str] | None = None,
    storage_options: dict[str, str] | None = None,
    files_root: str | None = None,
    already_verified: set[str] | None = None,
) -> PreparedInputs:
    """Resolve, type, verify, group, and encode concrete operation inputs."""
    from artisan.execution.inputs.grouping import group_inputs
    from artisan.schemas.enums import GroupByStrategy
    from artisan.storage.core.artifact_store import ArtifactStore

    resolved = resolve_inputs(
        inputs,
        delta_root,
        fs,
        step_run_ids=step_run_ids,
        storage_options=storage_options,
    )
    store = ArtifactStore(
        delta_root,
        fs=fs,
        storage_options=storage_options,
        files_root=files_root,
    )
    ordered_ids = [artifact_id for ids in resolved.values() for artifact_id in ids]
    type_map = store.load_type_map(ordered_ids)
    missing = [
        artifact_id for artifact_id in ordered_ids if artifact_id not in type_map
    ]
    if missing:
        msg = f"Input artifact IDs are missing from the index: {missing!r}"
        raise ArtifactIntegrityError(msg)

    verified = already_verified or set()
    ids_by_type: dict[str, list[str]] = {}
    for artifact_id in ordered_ids:
        if artifact_id not in verified:
            ids_by_type.setdefault(type_map[artifact_id], []).append(artifact_id)
    for artifact_type, artifact_ids in ids_by_type.items():
        model = store.get_artifacts_by_type(artifact_ids, artifact_type)
        if len(model) != len(set(artifact_ids)):
            missing_content = sorted(set(artifact_ids) - set(model))
            msg = f"Input artifacts are missing content rows: {missing_content!r}"
            raise ArtifactIntegrityError(msg)

    if group_by is not None:
        if not isinstance(group_by, GroupByStrategy):
            msg = f"Invalid group_by value: {group_by!r}"
            raise TypeError(msg)
        aligned, group_ids = group_inputs(
            resolved,
            group_by,
            store,
            artifact_types=type_map,
        )
    else:
        aligned, group_ids = resolved, None

    cache_inputs = _build_cache_inputs(aligned, type_map, group_ids)
    return PreparedInputs(aligned, type_map, group_ids, cache_inputs)


def _build_cache_inputs(
    inputs: dict[str, list[str]],
    artifact_types: dict[str, str],
    group_ids: list[str] | None,
) -> dict[str, list[CacheInputIdentity]]:
    """Build ordered role-local cache occurrences for prepared inputs."""
    return {
        role: [
            CacheInputIdentity(
                role=role,
                group_id=group_ids[position] if group_ids is not None else None,
                position=position,
                artifact_type=artifact_types[artifact_id],
                artifact_id=artifact_id,
            )
            for position, artifact_id in enumerate(artifact_ids)
        ]
        for role, artifact_ids in inputs.items()
    }


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
            msg = (  # type: ignore[unreachable]  # runtime defense against bad input types
                f"List inputs must contain OutputReference objects, "
                f"got {type(ref).__name__} at index {i}"
            )
            raise TypeError(msg)
        sri = step_run_ids.get(ref.source_step) if step_run_ids else None
        artifact_ids = resolve_output_reference(
            ref, delta_root, fs, step_run_id=sri, storage_options=storage_options
        )
        all_artifact_ids.extend(artifact_ids)

    # Sort all artifact IDs for determinism
    return {"_merged_streams": sorted(all_artifact_ids)}
