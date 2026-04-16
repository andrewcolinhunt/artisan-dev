"""ComputeRouter abstract base class."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
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

    def route_execute_batch(
        self,
        operation: Any,
        execute_inputs: list[ExecuteInput],
        sandbox_root: str,
    ) -> Iterable[Any]:
        """Batch-execute across multiple artifacts.

        Default implementation loops ``route_execute()``. Override
        for parallel dispatch (e.g. Modal ``experimental_spawn_map``).

        Args:
            operation: Single operation instance (shared across all
                artifacts in the batch).
            execute_inputs: One ExecuteInput per artifact.
            sandbox_root: Unit-level sandbox root (shared by all
                artifacts in the batch).

        Returns:
            Iterable of raw results, one per artifact. Failures are
            the exception instance at that index.
        """
        results: list[Any] = []
        for ei in execute_inputs:
            try:
                results.append(self.route_execute(operation, ei, sandbox_root))
            except Exception as exc:
                results.append(exc)
        return results
