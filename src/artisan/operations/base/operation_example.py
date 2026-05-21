"""Declarative usage examples for operations.

``OperationExample`` instances are declared on each ``OperationDefinition``
subclass as a ClassVar; the registry surfaces them via
``artisan.registry.examples(name)`` and folds them into
``OperationMetadata`` for the MCP server.

Lives in ``operations/base/`` (not ``registry/``) so the
``OperationDefinition.examples`` ClassVar annotation does not force an
``operations -> registry`` import. The registry module re-exports the
symbol for end-user discoverability.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class OperationExample(BaseModel):
    """Canonical usage example for an operation.

    Attributes:
        description: One-sentence summary of what the example demonstrates.
        params: Parameter values to set on the operation for this example.
        inputs: Mapping of role name to an upstream step name that produces
            the artifact for that role. Resolved at pipeline construction
            time; for standalone examples a sentinel step name is used.
        notes: Optional additional commentary (caveats, prerequisites).
    """

    description: str
    params: dict[str, Any] = {}
    inputs: dict[str, str] = {}
    notes: str | None = None
