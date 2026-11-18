"""ExecuteRouter abstract base class."""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import AbstractContextManager, nullcontext
from typing import Any

from artisan.schemas.specs.input_models import ExecuteInput


class ExecuteRouter(ABC):
    """Route the execute phase of one unit to a compute target."""

    def context(self) -> AbstractContextManager[None]:
        """Scope execution settings shared by preprocessing and postprocessing."""
        return nullcontext()

    @abstractmethod
    def route_execute(
        self,
        operation: Any,
        execute_inputs: list[ExecuteInput],
        sandbox_root: str | None,
    ) -> list[Any]:
        """Run the execute phase for every artifact of one unit.

        Args:
            operation: The operation instance.
            execute_inputs: One ExecuteInput per artifact (a single
                monolithic entry when ``per_artifact_dispatch=False``).
            sandbox_root: Path to the sandbox directory tree.

        Returns:
            Raw results positionally aligned with ``execute_inputs``.
            Entries may be Exception instances (per-artifact failures);
            the lifecycle surfaces them.
        """
        ...
