"""Runtime result types for composites.

``CompositeStepHandle`` is returned by ``ctx.run()`` for wiring downstream
operations; ``CompositeResult`` is returned by ``submit_composite`` /
``run_composite``. Both carry runtime behavior (they reach into the parent
pipeline's futures), so they live in ``composites/base`` rather than the pure
data layer — only the ``CompositeRef`` dataclass stays in ``schemas``.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from artisan.schemas.composites.composite_ref import CompositeRef

if TYPE_CHECKING:
    from artisan.orchestration.step_future import StepFuture
    from artisan.schemas.orchestration.output_reference import OutputReference
    from artisan.schemas.specs.output_spec import OutputSpec


class CompositeStepHandle:
    """Handle returned by ``ctx.run()`` for wiring downstream operations.

    Wraps the parent pipeline's ``StepFuture`` so a later ``ctx.run()`` can
    reference this operation's outputs.

    Attributes:
        _step_future: Parent pipeline step future.
        _operation_outputs: Output specs for role validation.
    """

    def __init__(
        self,
        *,
        step_future: StepFuture | None = None,
        operation_outputs: dict[str, OutputSpec] | None = None,
    ) -> None:
        self._step_future = step_future
        self._operation_outputs = operation_outputs

    def output(self, role: str) -> CompositeRef:
        """Reference an output role of this internal operation.

        Args:
            role: Output role name.

        Returns:
            CompositeRef for wiring to downstream operations.

        Raises:
            ValueError: If role is not a valid output of this operation, or
                if this handle has no step future.
        """
        if self._operation_outputs and role not in self._operation_outputs:
            available = sorted(self._operation_outputs.keys())
            msg = f"Unknown output role '{role}'. Available: {available}"
            raise ValueError(msg)
        if self._step_future is None:
            msg = "CompositeStepHandle has no step_future set"
            raise ValueError(msg)
        return CompositeRef(
            source=None,
            output_reference=self._step_future.output(role),
            role=role,
        )


class CompositeResult:
    """Returned by ``pipeline.submit_composite``.

    Maps composite outputs to internal steps and exposes the child
    StepFutures so that ``run_composite`` can block on completion via
    ``.wait()``. Duck-types with StepResult and StepFuture for
    ``.output(role) -> OutputReference``.

    Attributes:
        _output_map: Composite output role to OutputReference.
        _output_types: Composite output role to artifact type.
        _child_futures: StepFutures of the composite's child steps. For
            nested composites this is empty — the children register on the
            parent pipeline's _active_futures, so the top-level wait() drains
            them transitively.
    """

    def __init__(
        self,
        output_map: dict[str, OutputReference],
        output_types: dict[str, str | None],
        child_futures: list[StepFuture] | None = None,
    ) -> None:
        self._output_map = output_map
        self._output_types = output_types
        self._child_futures = child_futures if child_futures is not None else []

    @property
    def output_roles(self) -> frozenset[str]:
        """Available output role names from this composite."""
        return frozenset(self._output_map)

    def output(self, role: str) -> OutputReference:
        """Get the OutputReference for a composite output role.

        Args:
            role: Composite output role name.

        Returns:
            OutputReference pointing at the internal step that produces it.

        Raises:
            ValueError: If role is not a declared output.
        """
        if role not in self._output_map:
            available = sorted(self._output_map.keys())
            msg = f"Unknown output role '{role}'. Available: {available}"
            raise ValueError(msg)
        return self._output_map[role]

    def wait(self, *, timeout: float | None = None) -> CompositeResult:
        """Block until every child step completes.

        Args:
            timeout: Optional total deadline in seconds. If None, waits
                indefinitely. The deadline is enforced across the aggregate
                set of futures, not per-future.

        Returns:
            Self, with all child steps now resolved. Use ``.output(role)`` to
            access individual outputs after the wait.

        Raises:
            TimeoutError: If timeout expires before all children resolve.
        """
        deadline: float | None = None
        if timeout is not None:
            deadline = time.monotonic() + timeout

        for future in self._child_futures:
            remaining: float | None = None
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    msg = (
                        f"CompositeResult.wait timed out after {timeout}s "
                        f"with {sum(1 for f in self._child_futures if not f.done)} "
                        "children still pending"
                    )
                    raise TimeoutError(msg)
            try:
                future.result(timeout=remaining)
            except TimeoutError as e:
                msg = f"CompositeResult.wait timed out after {timeout}s"
                raise TimeoutError(msg) from e
        return self
