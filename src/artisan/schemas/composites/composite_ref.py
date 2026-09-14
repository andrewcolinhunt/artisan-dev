"""Composite reference type for wiring operations within compose().

CompositeRef is a lightweight reference used as input wiring between internal
operations of a composite.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from artisan.schemas.orchestration.output_reference import OutputReference


@dataclass(frozen=True)
class CompositeRef:
    """A reference to artifacts within a composite.

    Attributes:
        output_reference: Lazy pipeline reference to the producing step.
        role: Output role name this ref points to.
    """

    output_reference: OutputReference | None
    role: str
