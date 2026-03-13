"""Transport model for composite operations dispatched to workers.

ExecutionComposite bundles a CompositeDefinition instance with resolved
inputs and configuration. Uses @dataclass (not Pydantic) for pickle
serialization compatibility with Prefect/SLURM dispatch.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.operation_config.resource_config import ResourceConfig


class CompositeIntermediates(StrEnum):
    """Controls intermediate artifact handling in a composite.

    Attributes:
        DISCARD: Default. Intermediates discarded after composite completes.
        PERSIST: Intermediates committed to Delta with step_boundary=False.
        EXPOSE: Intermediates committed to Delta with step_boundary=True.
    """

    DISCARD = "discard"
    PERSIST = "persist"
    EXPOSE = "expose"


@dataclass
class ExecutionComposite:
    """A composite operation to execute on a single worker.

    Attributes:
        composite: Instantiated CompositeDefinition with params.
        inputs: Resolved artifact IDs per input role.
        step_number: Pipeline step number.
        execution_spec_id: Cache key for this execution.
        resources: Worker resource allocation.
        execution: Batching/scheduling config.
        intermediates: How to handle intermediate artifacts.
    """

    composite: CompositeDefinition
    inputs: dict[str, list[str]]
    step_number: int
    execution_spec_id: str
    resources: ResourceConfig = field(default_factory=ResourceConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)
    intermediates: CompositeIntermediates = CompositeIntermediates.DISCARD
