"""Local execute router — direct passthrough."""

from __future__ import annotations

from typing import Any

from artisan.execution.compute.base import ExecuteRouter
from artisan.execution.compute.invoke import invoke_op_work
from artisan.schemas.specs.input_models import ExecuteInput


class LocalExecuteRouter(ExecuteRouter):
    """Run the execute phase here: in-process Python or a local subprocess."""

    def route_execute(
        self,
        operation: Any,
        execute_inputs: list[ExecuteInput],
        sandbox_root: str,
    ) -> list[Any]:
        return [invoke_op_work(operation, ei) for ei in execute_inputs]
