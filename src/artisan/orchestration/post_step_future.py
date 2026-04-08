"""Compound future for main + post-step pipeline pairs."""

from __future__ import annotations

from artisan.orchestration.step_future import StepFuture
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.orchestration.step_result import StepResult


class PostStepFuture:
    """Compound future spanning a main step and its post-step.

    Routes .output(role) to the post-step for roles the post-step
    declares, and to the main step for remaining roles.

    Attributes:
        main_future: StepFuture for the main operation (step N).
        post_future: StepFuture for the post-step (step N+1).
    """

    def __init__(
        self,
        main_future: StepFuture,
        post_future: StepFuture,
        output_map: dict[str, OutputReference],
        output_types: dict[str, str | None],
    ) -> None:
        self.main_future = main_future
        self.post_future = post_future
        self._output_map = output_map
        self._output_types = output_types

    @property
    def output_roles(self) -> frozenset[str]:
        """Union of main-step and post-step output roles."""
        return frozenset(self._output_map)

    @property
    def step_name(self) -> str:
        """Name of the post-step (the terminal step)."""
        return self.post_future.step_name

    def output(self, role: str) -> OutputReference:
        """Get an OutputReference, routed to the correct source step.

        Args:
            role: Output role name.

        Returns:
            OutputReference from the post-step (if it declares this role)
            or from the main step (for remaining roles).

        Raises:
            ValueError: If role is not available from either step.
        """
        if role not in self._output_map:
            available = ", ".join(sorted(self._output_map)) or "(none)"
            msg = f"Output role '{role}' not available. Available roles: {available}"
            raise ValueError(msg)
        return self._output_map[role]

    def result(self, timeout: float | None = None) -> StepResult:
        """Block until the post-step completes and return its StepResult."""
        return self.post_future.result(timeout=timeout)

    @property
    def done(self) -> bool:
        """True when the post-step has completed."""
        return self.post_future.done

    @property
    def status(self) -> str:
        """Status of the post-step: 'running', 'completed', or 'failed'."""
        return self.post_future.status
