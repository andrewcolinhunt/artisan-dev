"""Non-blocking handle for asynchronous step execution.

Returned by ``PipelineManager.submit()``. Wraps a
``concurrent.futures.Future``, adding ``output()`` for step wiring
and pipeline-aware status reporting.
"""

from __future__ import annotations

from concurrent.futures import Future

from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.orchestration.step_result import StepResult


class StepFuture:
    """Non-blocking handle to a pipeline step in progress.

    Attributes:
        step_number: The step's position in the pipeline.
        step_name: Human-readable operation name.
    """

    def __init__(
        self,
        step_number: int,
        step_name: str,
        output_roles: frozenset[str],
        output_types: dict[str, str | None],
        future: Future[StepResult],
    ) -> None:
        """Initialize with step metadata and an underlying future.

        Args:
            step_number: Position of this step in the pipeline.
            step_name: Human-readable operation name.
            output_roles: Set of valid output role names.
            output_types: Mapping of role name to artifact type (or None).
            future: Underlying concurrent future driving execution.
        """
        self.step_number = step_number
        self.step_name = step_name
        self._output_roles = output_roles
        self._output_types = output_types
        self._future = future

    @property
    def output_roles(self) -> frozenset[str]:
        """Available output role names for this step."""
        return self._output_roles

    def output(self, role: str) -> OutputReference:
        """Create a lazy reference to a step output without blocking.

        Args:
            role: Output role name to reference.

        Returns:
            OutputReference suitable for wiring to downstream steps.

        Raises:
            ValueError: If role is not among the operation's declared outputs.
        """
        if role not in self._output_roles:
            available = ", ".join(sorted(self._output_roles)) or "(none)"
            msg = f"Output role '{role}' not available. Available roles: {available}"
            raise ValueError(msg)
        return OutputReference(
            source_step=self.step_number,
            role=role,
            artifact_type=self._output_types.get(role),
        )

    @property
    def done(self) -> bool:
        """True if the step has completed (success or failure)."""
        return self._future.done()

    @property
    def status(self) -> str:
        """Current status: 'running', 'completed', or 'failed'."""
        if not self._future.done():
            return "running"
        exc = self._future.exception()
        return "failed" if exc is not None else "completed"

    def result(self, timeout: float | None = None) -> StepResult:
        """Block until done and return the StepResult.

        Args:
            timeout: Maximum seconds to wait. None means wait forever.

        Returns:
            The StepResult from the completed step.

        Raises:
            TimeoutError: If timeout expires before completion.
            Exception: The original exception if the step failed.
        """
        try:
            return self._future.result(timeout=timeout)
        except TimeoutError:
            msg = (
                f"Step {self.step_number} ({self.step_name}) "
                f"did not complete within {timeout}s"
            )
            raise TimeoutError(msg) from None
