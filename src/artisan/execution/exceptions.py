"""Execution-layer exceptions for artifact, lineage, and passthrough validation.

These are the framework validating an operation's declared output/lineage
contract *after* it ran — a validation category, distinct from a tool crash
(``compute``) or an unstructured Python exception in ``execute()``. Each
subclasses ``ArtisanError`` so its structured code reaches the persisted
``executions.error_envelope`` and ``inspect_failures``; ``str(err)`` stays
the plain message and by-type ``except``/``pytest.raises`` keep working.
"""

from __future__ import annotations

from typing import Any

from artisan.errors import ArtisanError, ErrorCode


class ArtifactValidationError(ArtisanError):
    """Raised when artifacts don't match output specs.

    This includes:
    - Missing required output roles
    - Empty artifact lists for required roles
    - Artifact type mismatches (e.g., DataArtifact for METRIC spec)
    - Unexpected output roles not declared in specs
    """

    def __init__(self, message: str, **fields: Any) -> None:
        super().__init__(
            code=ErrorCode.ARTIFACT_VALIDATION_FAILED,
            error_type="validation",
            message=message,
            recovery_hint="REPORT_TO_USER",
            **fields,
        )


class LineageCompletenessError(ArtisanError):
    """Raised when artifacts are missing lineage mappings.

    Non-orphan outputs (those without infer_lineage_from={"inputs": []})
    must have lineage mappings declaring their source artifacts.
    """

    def __init__(self, message: str, **fields: Any) -> None:
        super().__init__(
            code=ErrorCode.LINEAGE_INCOMPLETE,
            error_type="validation",
            message=message,
            recovery_hint="REPORT_TO_USER",
            **fields,
        )


class LineageIntegrityError(ArtisanError):
    """Raised when lineage references are invalid.

    This includes:
    - Source artifact_id references a non-existent input or output artifact
    - Draft original_name references a non-existent output artifact
    - Multiple lineage mappings for the same draft within a single
      source_role (whether identical sources, or distinct sources both
      in that role — split into separate source_roles instead)
    """

    def __init__(self, message: str, **fields: Any) -> None:
        super().__init__(
            code=ErrorCode.LINEAGE_INTEGRITY_FAILED,
            error_type="validation",
            message=message,
            recovery_hint="REPORT_TO_USER",
            **fields,
        )


class PassthroughValidationError(ArtisanError):
    """Raised when passthrough result validation fails.

    This includes:
    - Missing output role in passthrough dict
    - Empty artifact ID list for a required output role
    - Invalid artifact ID references
    """

    def __init__(self, message: str, **fields: Any) -> None:
        super().__init__(
            code=ErrorCode.PASSTHROUGH_VALIDATION_FAILED,
            error_type="validation",
            message=message,
            recovery_hint="REPORT_TO_USER",
            **fields,
        )
