"""Composite reference type for wiring operations within compose().

CompositeRef is a lightweight reference used as input wiring between internal
operations of a composite.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from artisan.execution.models.artifact_source import ArtifactSource
    from artisan.schemas.orchestration.output_reference import OutputReference


@dataclass(frozen=True)
class CompositeRef:
    """A reference to artifacts within a composite.

    Attributes:
        source: In-memory artifact source (unused in the current single-mode
            expansion path; retained for the frozen dataclass shape).
        output_reference: Lazy pipeline reference to the producing step.
        role: Output role name this ref points to.
    """

    source: ArtifactSource | None
    output_reference: OutputReference | None
    role: str
