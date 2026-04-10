"""Local compute router — direct passthrough (today's behavior)."""

from __future__ import annotations

from typing import Any

from artisan.execution.compute.base import ComputeRouter
from artisan.schemas.specs.input_models import ExecuteInput


class LocalComputeRouter(ComputeRouter):
    """Direct call — today's behavior."""

    def route_execute(
        self,
        operation: Any,
        execute_input: ExecuteInput,
        sandbox_root: str,
    ) -> Any:
        return operation.execute(execute_input)
