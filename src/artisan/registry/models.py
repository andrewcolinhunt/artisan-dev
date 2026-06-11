"""Pydantic shapes for the registry's agent-facing payload.

Two-tier disclosure: ``OperationSummary`` for list views, ``OperationMetadata``
(which inherits the summary fields) for full describe payloads. All shapes
carry ``schema_version`` so the MCP layer can branch on schema upgrades.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel

from artisan.operations.base.operation_example import OperationExample


class OperationSummary(BaseModel):
    """Lightweight summary surfaced by ``list_operations()``.

    Attributes:
        schema_version: Pinned to ``"1"`` for v1 payloads. Adding new
            ``kind`` literals or optional fields does not bump the version;
            removing or renaming does.
        name: Operation identifier used for registry lookup.
        kind: Whether the op fans out via the execute phase (creator) or runs
            in-process via ``execute_curator()`` (curator).
        description: Human-readable summary from ``OperationDefinition.description``.
        input_roles: Role names declared in ``inputs``.
        output_roles: Role names declared in ``outputs``.
        tags: Author-declared tags for agent-side filtering.
    """

    schema_version: Literal["1"] = "1"
    name: str
    kind: Literal["creator", "curator"]
    description: str
    input_roles: list[str]
    output_roles: list[str]
    tags: list[str] = []


class InputSpecMetadata(BaseModel):
    """Wire shape for a single ``InputSpec``.

    Trimmed vs the source spec to the fields an agent uses today;
    ``materialize_as``, ``hydrate``, and ``with_associated`` return when an
    MCP tool needs them.

    Attributes:
        artifact_type: Expected artifact type string.
        required: Whether the input is required.
        description: Human-readable description from the spec.
        materialize: True when the op receives a file path; False when it
            receives the artifact content directly.
    """

    artifact_type: str
    required: bool
    description: str
    materialize: bool


class OutputSpecMetadata(BaseModel):
    """Wire shape for a single ``OutputSpec``.

    ``infer_lineage_from`` is omitted until ``artisan_get_provenance_graph``
    needs it.

    Attributes:
        artifact_type: Artifact type this output produces.
        required: Whether output is expected (warns if missing, doesn't fail).
        description: Human-readable description from the spec.
    """

    artifact_type: str
    required: bool
    description: str


class OperationMetadata(OperationSummary):
    """Full describe payload — inherits summary fields, adds shape detail.

    Attributes:
        inputs: Role to ``InputSpecMetadata`` mapping.
        outputs: Role to ``OutputSpecMetadata`` mapping.
        params_schema: JSON Schema for the op's ``Params`` class, with
            descriptions merged from docstring or ``Field``.
        examples: Author-declared usage examples.
        source_module: Importable module path the op was registered from.
    """

    inputs: dict[str, InputSpecMetadata]
    outputs: dict[str, OutputSpecMetadata]
    params_schema: dict[str, Any]
    examples: list[OperationExample] = []
    source_module: str


class DiscoverySource(BaseModel):
    """One module that ``discover()`` imported.

    Attributes:
        kind: Origin category — ``"builtin"`` for Artisan's own ops,
            ``"manual"`` for kwarg/env-var modules.
        module: Dotted Python module path.
        classes_registered: Names of ``OperationDefinition`` subclasses
            that were registered by importing this module.
    """

    kind: Literal["builtin", "manual"]
    module: str
    classes_registered: list[str] = []


class DiscoveryError(BaseModel):
    """One module that failed to import during ``discover()``.

    ``discover()`` does not re-raise these; they're collected in the report
    so agents can self-diagnose missing extras or broken packages.

    Attributes:
        module: Dotted Python module path that failed.
        error_type: Exception class name (e.g. ``"ModuleNotFoundError"``).
        error_message: ``str(exc)`` from the caught exception.
    """

    module: str
    error_type: str
    error_message: str


class NameCollision(BaseModel):
    """Two modules registered ops under the same ``name``.

    First registration wins; the second is dropped and recorded here.

    Attributes:
        name: The colliding operation name.
        first_module: Source module of the winning registration.
        second_module: Source module of the dropped registration.
    """

    name: str
    first_module: str
    second_module: str


class DiscoveryReport(BaseModel):
    """Outcome of one ``discover()`` invocation.

    Attributes:
        operations_count: Number of ops in the registry after discovery.
        sources: One entry per imported module.
        errors: One entry per failed import (not raised).
        name_collisions: One entry per duplicate-name registration attempt.
    """

    operations_count: int
    sources: list[DiscoverySource]
    errors: list[DiscoveryError] = []
    name_collisions: list[NameCollision] = []


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
]
