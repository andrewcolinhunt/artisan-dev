"""Composite reference types for wiring operations within compose().

CompositeRef is a lightweight reference used as input wiring between
internal operations. CompositeStepHandle wraps the result of ctx.run().
ExpandedCompositeResult maps composite outputs to parent pipeline steps.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from artisan.execution.models.artifact_source import ArtifactSource
    from artisan.orchestration.step_future import StepFuture
    from artisan.schemas.artifact.base import Artifact
    from artisan.schemas.orchestration.output_reference import OutputReference
    from artisan.schemas.specs.output_spec import OutputSpec


@dataclass(frozen=True)
class CompositeRef:
    """A reference to artifacts within a composite.

    Either ``source`` (collapsed mode) or ``output_reference`` (expanded mode)
    is set, never both.

    Attributes:
        source: In-memory artifact source (collapsed mode).
        output_reference: Lazy pipeline reference (expanded mode).
        role: Output role name this ref points to.
    """

    source: ArtifactSource | None
    output_reference: OutputReference | None
    role: str


class CompositeStepHandle:
    """Handle returned by ctx.run() for wiring downstream operations.

    A single class with mode-dependent internals. In collapsed mode, wraps
    in-memory artifacts. In expanded mode, wraps a StepFuture from the
    parent pipeline.

    Attributes:
        _artifacts: Per-role artifact lists (collapsed mode).
        _step_future: Parent pipeline step future (expanded mode).
        _operation_outputs: Output specs for validation.
    """

    def __init__(
        self,
        *,
        artifacts: dict[str, list[Artifact]] | None = None,
        step_future: StepFuture | None = None,
        operation_outputs: dict[str, OutputSpec] | None = None,
    ) -> None:
        self._artifacts = artifacts
        self._step_future = step_future
        self._operation_outputs = operation_outputs

    def output(self, role: str) -> CompositeRef:
        """Reference an output role of this internal operation.

        Args:
            role: Output role name.

        Returns:
            CompositeRef for wiring to downstream operations.

        Raises:
            ValueError: If role is not a valid output of this operation.
            KeyError: If role has no artifacts in collapsed mode.
        """
        if self._operation_outputs and role not in self._operation_outputs:
            available = sorted(self._operation_outputs.keys())
            msg = f"Unknown output role '{role}'. Available: {available}"
            raise ValueError(msg)

        if self._artifacts is not None:
            from artisan.execution.models.artifact_source import ArtifactSource

            return CompositeRef(
                source=ArtifactSource.from_artifacts(self._artifacts[role]),
                output_reference=None,
                role=role,
            )
        if self._step_future is None:
            msg = "CompositeStepHandle has neither artifacts nor step_future set"
            raise ValueError(msg)
        return CompositeRef(
            source=None,
            output_reference=self._step_future.output(role),
            role=role,
        )


class ExpandedCompositeResult:
    """Returned by pipeline.submit_composite(expand=True).

    Maps composite outputs to internal steps and exposes the child
    StepFutures so that ``run_composite(expand=True)`` can block on
    completion via ``.wait()``. Duck-types with StepResult and
    StepFuture for ``.output(role) -> OutputReference``.

    Attributes:
        _output_map: Composite output role to OutputReference.
        _output_types: Composite output role to artifact type.
        _child_futures: StepFutures of the composite's expanded children.
            For nested composites this is empty — the children register
            on the parent pipeline's _active_futures, so the top-level
            wait() drains them transitively.
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
        """Available output role names from this composite expansion."""
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

    def wait(self, *, timeout: float | None = None) -> ExpandedCompositeResult:
        """Block until every child step completes.

        Args:
            timeout: Optional total deadline in seconds. If None, waits
                indefinitely. The deadline is enforced across the
                aggregate set of futures, not per-future.

        Returns:
            Self, with all child steps now resolved. Use ``.output(role)``
            to access individual outputs after the wait.

        Raises:
            TimeoutError: If timeout expires before all children resolve.
        """
        import time

        deadline: float | None = None
        if timeout is not None:
            deadline = time.monotonic() + timeout

        for future in self._child_futures:
            remaining: float | None = None
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    msg = (
                        f"ExpandedCompositeResult.wait timed out after {timeout}s "
                        f"with {sum(1 for f in self._child_futures if not f.done)} "
                        "children still pending"
                    )
                    raise TimeoutError(msg)
            try:
                future.result(timeout=remaining)
            except TimeoutError as e:
                msg = f"ExpandedCompositeResult.wait timed out after {timeout}s"
                raise TimeoutError(msg) from e
        return self
