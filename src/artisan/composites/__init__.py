"""Composites: reusable compositions of operations with declared I/O."""

from __future__ import annotations

from artisan.composites.base.composite_context import (
    CollapsedCompositeContext,
    CompositeContext,
    ExpandedCompositeContext,
)
from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.schemas.composites.composite_ref import (
    CompositeRef,
    CompositeStepHandle,
    ExpandedCompositeResult,
)

__all__ = [
    "CollapsedCompositeContext",
    "CompositeContext",
    "CompositeDefinition",
    "CompositeRef",
    "CompositeStepHandle",
    "ExpandedCompositeContext",
    "ExpandedCompositeResult",
]
