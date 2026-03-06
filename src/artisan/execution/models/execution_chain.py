"""Transport model for chained creator operations.

ExecutionChain bundles multiple ExecutionUnits with role mappings and
chain-level configuration. Uses @dataclass (not Pydantic) for pickle
serialization compatibility with Prefect/SLURM dispatch.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.operation_config.resource_config import ResourceConfig


class ChainIntermediates(str, Enum):
    """Controls intermediate artifact handling in a chain.

    Attributes:
        DISCARD: Default. Intermediates discarded after chain completes.
        PERSIST: Intermediates committed to Delta + internal edges stored.
        EXPOSE: Like PERSIST, but execution edges include intermediate outputs.
    """

    DISCARD = "discard"
    PERSIST = "persist"
    EXPOSE = "expose"


@dataclass
class ExecutionChain:
    """A chain of creator operations to execute in a single worker.

    Attributes:
        operations: Ordered list of ExecutionUnits to execute.
        role_mappings: Role remappings between adjacent operations.
            Length must be len(operations) - 1. None means identity mapping.
        resources: Chain-level resource allocation (configures the worker).
        execution: Chain-level batching/scheduling config.
        intermediates: How to handle intermediate operation outputs.
    """

    operations: list[ExecutionUnit]
    role_mappings: list[dict[str, str] | None]
    resources: ResourceConfig = field(default_factory=ResourceConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)
    intermediates: ChainIntermediates = ChainIntermediates.DISCARD
