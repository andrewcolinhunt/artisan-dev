"""Composites: reusable compositions of operations with declared I/O."""

from __future__ import annotations

from artisan.composites.base.composite_context import CompositeContext
from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.composites.base.results import CompositeResult, CompositeStepHandle

__all__ = [
    "CompositeContext",
    "CompositeDefinition",
    "CompositeResult",
    "CompositeStepHandle",
]
