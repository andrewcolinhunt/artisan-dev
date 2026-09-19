"""Two-level batch generation for orchestration.

Level 1 (artifacts_per_unit): Artifacts per ExecutionUnit
Level 2 (units_per_worker): ExecutionUnits per worker
"""

from __future__ import annotations

from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.orchestration.batch_config import BatchConfig
from artisan.utils.hashing import CacheInputIdentity


def get_batch_config(
    operation: OperationDefinition,
) -> BatchConfig:
    """Extract batch configuration from a fully configured operation instance.

    Args:
        operation: Fully configured OperationDefinition instance.

    Returns:
        BatchConfig with artifacts_per_unit and units_per_worker.
    """
    artifacts_per_unit = operation.batch_strategy.artifacts_per_unit
    units_per_worker = operation.batch_strategy.units_per_worker

    max_artifacts = operation.batch_strategy.max_artifacts_per_unit
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
    *,
    cache_inputs: dict[str, list[CacheInputIdentity]],
) -> list[
    tuple[
        dict[str, list[str]],
        list[str] | None,
        dict[str, list[CacheInputIdentity]],
    ]
]:
    """Generate ExecutionUnit input batches (Level 1 batching).

    Slice input IDs, optional group IDs, and typed cache occurrences together.
    Cache occurrence positions restart at zero within each unit.

    Args:
        inputs: Ordered artifact IDs for each input role.
        batch_config: Batching configuration.
        group_ids: Optional per-index group_id list from framework pairing.
            Sliced in sync with input lists when present.
        cache_inputs: Ordered typed cache occurrences to slice in lockstep.

    Returns:
        Tuples of (input IDs, group IDs or None, cache occurrences), one
        per execution unit. Generative inputs produce one empty unit.
    """
    if not inputs:
        # Generative operation - single batch with empty inputs
        return [({}, None, {})]

    first_role = next(iter(inputs.keys()))
    total_items = len(inputs[first_role])

    if total_items == 0:
        return [({}, None, {})]

    artifacts_per_unit = batch_config.artifacts_per_unit
    batches: list[
        tuple[
            dict[str, list[str]],
            list[str] | None,
            dict[str, list[CacheInputIdentity]],
        ]
    ] = []

    for start in range(0, total_items, artifacts_per_unit):
        end = min(start + artifacts_per_unit, total_items)
        batch = {role: ids[start:end] for role, ids in inputs.items()}
        batch_group_ids = group_ids[start:end] if group_ids is not None else None
        batch_cache_inputs = {
            role: [
                CacheInputIdentity(
                    role=entry.role,
                    group_id=entry.group_id,
                    position=position,
                    artifact_type=entry.artifact_type,
                    artifact_id=entry.artifact_id,
                )
                for position, entry in enumerate(entries[start:end])
            ]
            for role, entries in cache_inputs.items()
        }
        batches.append((batch, batch_group_ids, batch_cache_inputs))

    return batches


def pack_units(
    units: list[ExecutionUnit],
    units_per_worker: int,
) -> list[list[ExecutionUnit]]:
    """Pack execution units into ordered worker batches.

    Args:
        units: Execution units in submission order.
        units_per_worker: Maximum units assigned to one worker invocation.

    Returns:
        Ordered batches containing at most ``units_per_worker`` units.

    Raises:
        ValueError: If ``units_per_worker`` is not positive.
    """
    if units_per_worker < 1:
        msg = "units_per_worker must be at least 1"
        raise ValueError(msg)
    return [
        units[start : start + units_per_worker]
        for start in range(0, len(units), units_per_worker)
    ]
