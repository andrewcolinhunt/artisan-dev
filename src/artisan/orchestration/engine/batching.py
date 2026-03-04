"""Two-level batch generation for orchestration.

Level 1 (artifacts_per_unit): Artifacts per ExecutionUnit
Level 2 (units_per_worker): ExecutionUnits per worker
"""

from __future__ import annotations

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.orchestration.batch_config import BatchConfig


def get_batch_config(
    operation: OperationDefinition,
) -> BatchConfig:
    """Extract batch configuration from a fully configured operation instance.

    Reads directly from the operation's execution config.

    Args:
        operation: Fully configured OperationDefinition instance.

    Returns:
        BatchConfig with artifacts_per_unit and units_per_worker.
    """
    artifacts_per_unit = operation.execution.artifacts_per_unit
    units_per_worker = operation.execution.units_per_worker

    # Optional cap from execution config
    max_artifacts = operation.execution.max_artifacts_per_unit
    if max_artifacts is not None and artifacts_per_unit > max_artifacts:
        artifacts_per_unit = max_artifacts

    return BatchConfig(
        artifacts_per_unit=max(1, artifacts_per_unit),
        units_per_worker=max(1, units_per_worker),
    )


def generate_execution_unit_batches(
    inputs: dict[str, list[str]],
    batch_config: BatchConfig,
    group_ids: list[str] | None = None,
) -> list[tuple[dict[str, list[str]], list[str] | None]]:
    """Generate ExecutionUnit input batches (Level 1 batching).

    Splits artifact IDs into batches based on artifacts_per_unit.
    Each returned tuple contains inputs for one ExecutionUnit and the
    corresponding sliced group_ids (or None if no pairing was applied).

    Args:
        inputs: Dict mapping role to sorted artifact IDs.
        batch_config: Batching configuration.
        group_ids: Optional per-index group_id list from framework pairing.
            Sliced in sync with input lists when present.

    Returns:
        List of (input_dict, group_ids_slice) tuples, one per ExecutionUnit.

    Example:
        >>> inputs = {"data": ["a", "b", "c", "d", "e"]}
        >>> config = BatchConfig(artifacts_per_unit=2)
        >>> batches = generate_execution_unit_batches(inputs, config)
        >>> # Returns: [
        >>> #   ({"data": ["a", "b"]}, None),
        >>> #   ({"data": ["c", "d"]}, None),
        >>> #   ({"data": ["e"]}, None),  # Remainder
        >>> # ]
    """
    if not inputs:
        # Generative operation - single batch with empty inputs
        return [({}, None)]

    # Get total item count from first role
    first_role = next(iter(inputs.keys()))
    total_items = len(inputs[first_role])

    if total_items == 0:
        return [({}, None)]

    artifacts_per_unit = batch_config.artifacts_per_unit
    batches: list[tuple[dict[str, list[str]], list[str] | None]] = []

    for start in range(0, total_items, artifacts_per_unit):
        end = min(start + artifacts_per_unit, total_items)
        batch = {role: ids[start:end] for role, ids in inputs.items()}
        batch_group_ids = group_ids[start:end] if group_ids is not None else None
        batches.append((batch, batch_group_ids))

    return batches
