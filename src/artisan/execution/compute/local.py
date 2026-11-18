"""Execute native function phases in the current interpreter."""

from __future__ import annotations

from typing import Any

from artisan.execution.compute.base import ExecuteRouter
from artisan.execution.recording.commands import invocation_scope, reserve_invocations
from artisan.execution.scientific_failure import scientific_phase
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.specs.input_models import ExecuteInput


class LocalExecuteRouter(ExecuteRouter):
    """Run Python phases directly; commands require an explicitly selected adapter."""

    def route_execute(
        self,
        operation: OperationDefinition,
        execute_inputs: list[ExecuteInput],
        sandbox_root: str | None,
    ) -> list[Any]:
        if operation.is_command_op():
            msg = "Command execution requires a selected prepared software adapter"
            raise ValueError(msg)
        slots = reserve_invocations(len(execute_inputs))
        results = []
        for slot, inputs in zip(slots, execute_inputs, strict=True):
            with invocation_scope(operation, slot), scientific_phase("execute"):
                results.append(operation.execute_function(inputs))
        return results
