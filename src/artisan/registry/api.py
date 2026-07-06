"""Public registry surface: ``list_operations``, ``describe``, ``examples``.

These three calls back the MCP server's
``artisan_list_operations`` / ``artisan_describe_operation`` /
``artisan_examples`` tools. The registry self-populates as a side effect
of importing op modules; callers may invoke
``artisan.registry.discover()`` for explicit module loading and attribution.
"""

from __future__ import annotations

from typing import Literal

from artisan.errors import ArtisanError, ErrorCode, suggest
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.base.operation_example import OperationExample
from artisan.registry.models import OperationMetadata, OperationSummary


def list_operations(
    kind: Literal["creator", "curator"] | None = None,
    query: str | None = None,
    tag: str | None = None,
) -> list[OperationSummary]:
    """Return summaries of registered operations, sorted by name.

    Filters AND together: an operation must match every provided filter.

    Args:
        kind: When set, restrict to creator or curator operations.
        query: When set, restrict to operations whose name or description
            contains the (case-insensitive) substring.
        tag: When set, restrict to operations declaring this exact tag in
            their ``tags``.

    Returns:
        Sorted list of ``OperationSummary``.
    """
    summaries = [cls.to_summary() for cls in OperationDefinition._registry.values()]
    if kind is not None:
        summaries = [s for s in summaries if s.kind == kind]
    if query is not None:
        needle = query.lower()
        summaries = [
            s
            for s in summaries
            if needle in s.name.lower() or needle in s.description.lower()
        ]
    if tag is not None:
        summaries = [s for s in summaries if tag in s.tags]
    summaries.sort(key=lambda s: s.name)
    return summaries


def describe(name: str) -> OperationMetadata:
    """Return full metadata for one operation.

    Args:
        name: The operation's registry name.

    Returns:
        ``OperationMetadata`` with inputs, outputs, params_schema, examples.

    Raises:
        ArtisanError: With ``code=UNKNOWN_OPERATION`` and ``suggestions``
            populated from the registry's name set when ``name`` is not
            registered.
    """
    cls = OperationDefinition._registry.get(name)
    if cls is None:
        registered = set(OperationDefinition._registry.keys())
        raise ArtisanError(
            code=ErrorCode.UNKNOWN_OPERATION,
            error_type="validation",
            message=f"Unknown operation: {name!r}",
            field="name",
            suggestions=suggest(name, registered),
            recovery_hint="CHECK_INPUT",
        )
    return cls.to_metadata()


def examples(name: str) -> list[OperationExample]:
    """Return declared examples for an operation.

    Args:
        name: The operation's registry name.

    Returns:
        The op's ``examples`` ClassVar as a new list.

    Raises:
        ArtisanError: With ``code=UNKNOWN_OPERATION`` when ``name`` is not
            registered.
    """
    cls = OperationDefinition._registry.get(name)
    if cls is None:
        registered = set(OperationDefinition._registry.keys())
        raise ArtisanError(
            code=ErrorCode.UNKNOWN_OPERATION,
            error_type="validation",
            message=f"Unknown operation: {name!r}",
            field="name",
            suggestions=suggest(name, registered),
            recovery_hint="CHECK_INPUT",
        )
    return list(cls.examples)
