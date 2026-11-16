"""Non-blocking handle for asynchronous step execution.

Returned by ``PipelineManager.submit()``. Wraps a
``concurrent.futures.Future``, adding ``output()`` for step wiring
and pipeline-aware status reporting.
"""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import Future

from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.orchestration.output_reference import OutputReference, StepSource
from artisan.schemas.orchestration.step_lifecycle import StepStatus
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
        status_reader: Callable[[], StepStatus],
        *,
        source: StepSource,
    ) -> None:
        """Initialize with step metadata and an underlying future.

        Args:
            step_number: Position of this step in the pipeline.
            step_name: Human-readable operation name.
            output_roles: Set of valid output role names.
            output_types: Mapping of role name to artifact type (or None).
            future: Underlying concurrent future driving execution.
            status_reader: Manager-owned reader updated after durable snapshots.
            source: Exact pending attempt and its authoritative store.
        """
        self.step_number = step_number
        self.step_name = step_name
        self._output_roles = output_roles
        self._output_types = output_types
        self._future = future
        self._status_reader = status_reader
        self._source = source

    @property
    def output_roles(self) -> frozenset[str]:
        """Available output role names for this step."""
        return self._output_roles

    @property
    def output_types(self) -> dict[str, str | None]:
        """Return a copy of the declared output-role type mapping."""
        return dict(self._output_types)

    def output(self, role: str) -> OutputReference:
        """Reference this attempt's declared output without blocking or store I/O.

        Dynamic curator roles are available on ``result().output(role)`` after
        completion. Waiting for pending work is managed by its owning pipeline.

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
            source=self._source,
            role=role,
            artifact_type=self._output_types.get(role) or ArtifactTypes.ANY,
        )

    @property
    def done(self) -> bool:
        """Return whether the underlying future is done."""
        return self._future.done()

    @property
    def cancelled_before_start(self) -> bool:
        """Whether the internal closure was cancelled before it ran."""
        return self._future.cancelled()

    @property
    def status(self) -> StepStatus:
        """Return the latest durably persisted lifecycle status."""
        return self._status_reader()

    def result(self, timeout: float | None = None) -> StepResult:
        """Block until done and return the StepResult.

        Args:
            timeout: Maximum seconds to wait. None means wait forever.

        Returns:
            The terminal StepResult.

        Raises:
            TimeoutError: If timeout expires before completion.
            Exception: The original exception if the step failed.
        """
        try:
            result = self._future.result(timeout=timeout)
        except TimeoutError:
            msg = (
                f"Step {self.step_number} ({self.step_name}) "
                f"did not complete within {timeout}s"
            )
            raise TimeoutError(msg) from None
        return result._bind_source(self._source)
