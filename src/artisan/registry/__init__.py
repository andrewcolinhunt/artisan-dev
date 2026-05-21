"""Agent-facing operation registry: discovery, schemas, summaries.

Public surface for the MCP server (``artisan_list_operations``,
``artisan_describe_operation``, ``artisan_examples``) and for Python-side
callers introspecting available operations.

See ``_dev/design/0_active/agents-v2/01_registry-and-schemas.md`` for the
design.
"""

from __future__ import annotations

from artisan.registry.api import describe, examples, list_operations
from artisan.registry.discovery import discover
from artisan.registry.models import (
    DiscoveryError,
    DiscoveryReport,
    DiscoverySource,
    InputSpecMetadata,
    NameCollision,
    OperationExample,
    OperationMetadata,
    OperationSummary,
    OutputSpecMetadata,
)
from artisan.registry.schemas import params_schema_for

__all__ = [
    "DiscoveryError",
    "DiscoveryReport",
    "DiscoverySource",
    "InputSpecMetadata",
    "NameCollision",
    "OperationExample",
    "OperationMetadata",
    "OperationSummary",
    "OutputSpecMetadata",
    "describe",
    "discover",
    "examples",
    "list_operations",
    "params_schema_for",
]
