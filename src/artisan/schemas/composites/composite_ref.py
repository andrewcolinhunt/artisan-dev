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
        artifacts: dict[str, list] | None = None,
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
            raise ValueError(f"Unknown output role '{role}'. Available: {available}")

        if self._artifacts is not None:
            from artisan.execution.models.artifact_source import ArtifactSource

            return CompositeRef(
                source=ArtifactSource.from_artifacts(self._artifacts[role]),
                output_reference=None,
                role=role,
            )
        return CompositeRef(
            source=None,
            output_reference=self._step_future.output(role),
            role=role,
        )


class ExpandedCompositeResult:
    """Returned by pipeline.expand(). Maps composite outputs to internal steps.

    Duck-types with StepResult and StepFuture (.output(role) -> OutputReference).

    Attributes:
        _output_map: Composite output role to OutputReference.
        _output_types: Composite output role to artifact type.
    """

    def __init__(
        self,
        output_map: dict[str, OutputReference],
        output_types: dict[str, str | None],
    ) -> None:
        self._output_map = output_map
        self._output_types = output_types

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
            raise ValueError(f"Unknown output role '{role}'. Available: {available}")
        return self._output_map[role]
