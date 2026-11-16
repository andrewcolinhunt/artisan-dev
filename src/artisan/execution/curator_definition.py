"""Capture and verify trusted live curator configuration before authored entry."""

from __future__ import annotations

from artisan.composites.base.snapshots import (
    input_contracts,
    invocation_policy,
    output_contracts,
    parameter_class,
    parameter_snapshot,
)
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.execution.curator_definition import CuratorDefinitionContract
from artisan.utils.hashing import compute_content_digest


def capture_curator_definition(
    operation: OperationDefinition,
) -> CuratorDefinitionContract:
    """Freeze effective configuration while persisting only a parameter digest.

    Code bytes belong to separately measured software. The live operation comes
    from trusted controller code, never an import selected by stored names.
    """
    definition = type(operation)
    if definition._kind() != "curator":
        msg = "Curator admission requires a curator definition"
        raise TypeError(msg)
    if operation.environment is not None:
        msg = "A separate operation environment applies only to command execution"
        raise ValueError(msg)
    params_model = parameter_class(definition)
    if (
        params_model is not None
        and type(getattr(operation, "params", None)) is not params_model
    ):
        msg = "Curator parameters must use their exact declared Params model"
        raise TypeError(msg)
    params = parameter_snapshot(operation, allow_private=True)
    return CuratorDefinitionContract(
        name=definition.name,
        version=definition.version,
        module=definition.__module__,
        qualname=definition.__qualname__,
        params_digest=compute_content_digest(
            b"artisan.curator-params-v1\0" + params.encode()
        ),
        inputs=input_contracts(definition),
        outputs=output_contracts(definition),
        policy=invocation_policy(operation),
        runtime_defined_inputs=definition.runtime_defined_inputs,
        hydrate_inputs=definition.hydrate_inputs,
        cacheable=definition.cacheable,
    )


def verify_curator_definition(
    operation: OperationDefinition, expected: CuratorDefinitionContract
) -> None:
    """Check once before authored execution, without constraining iterator state."""
    expected = CuratorDefinitionContract.model_validate(expected)
    if capture_curator_definition(operation) != expected:
        msg = "Live curator definition differs from its exact admitted configuration"
        raise ValueError(msg)
