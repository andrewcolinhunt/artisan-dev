"""Protocol for types that provide OutputReferences for downstream wiring."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from artisan.schemas.orchestration.output_reference import OutputReference


@runtime_checkable
class OutputSource(Protocol):
    """Anything that can provide OutputReferences for downstream wiring.

    Implemented by StepFuture, StepResult, PostStepFuture, and
    ExpandedCompositeResult.
    """

    @property
    def output_roles(self) -> frozenset[str]: ...

    def output(self, role: str) -> OutputReference: ...
