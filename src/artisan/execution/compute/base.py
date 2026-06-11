"""ExecuteRouter abstract base class."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from artisan.schemas.specs.input_models import ExecuteInput


class ExecuteRouter(ABC):
    """Route the execute phase to a compute_provider target."""

    @abstractmethod
    def route_execute(
        self,
        operation: Any,
        execute_input: ExecuteInput,
        sandbox_root: str,
    ) -> Any:
        """Run operation.execute_function() on the configured target.

        Args:
            operation: The operation instance.
            execute_input: Frozen input container for the execute phase.
            sandbox_root: Path to the sandbox directory tree.

        Returns:
            The raw result from the execute phase.
        """
        ...
