"""ComputeRouter abstract base class."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from artisan.schemas.specs.input_models import ExecuteInput


class ComputeRouter(ABC):
    """Route execute() to a compute target."""

    @abstractmethod
    def route_execute(
        self,
        operation: Any,
        execute_input: ExecuteInput,
        sandbox_root: str,
    ) -> Any:
        """Run operation.execute() on the configured target.

        Args:
            operation: The operation instance.
            execute_input: Frozen input container for execute().
            sandbox_root: Path to the sandbox directory tree.

        Returns:
            The raw result from execute().
        """
        ...
