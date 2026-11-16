"""Execution-layer exceptions for artifact, lineage, and passthrough validation.

These are the framework validating an operation's declared output/lineage
contract *after* it ran — a validation category, distinct from a tool crash
(``compute``) or an unstructured Python exception in ``execute()``. Each
subclasses ``ArtisanError`` so its structured code reaches curator records or
optional scientific delivery diagnostics and ``inspect_failures``; ``str(err)`` stays
the plain message and by-type ``except``/``pytest.raises`` keep working.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from artisan.errors import ArtisanError, ErrorCode

if TYPE_CHECKING:
    from artisan.schemas.execution.assignment import DeliveryIdentity


class AssignmentExecutionError(RuntimeError):
    """A confirmed operation failure, distinct from assignment infrastructure.

    Only direct scientific hooks and their output contracts create this marker.
    The assignment boundary attaches its fresh delivery only after session cleanup.
    Providers preserve that identity, sanitized diagnostic, and call location.
    """

    def __init__(
        self,
        message: str,
        *,
        phase: str | None = None,
        call_path: tuple[str, ...] = (),
        delivery: DeliveryIdentity | None = None,
        error_envelope: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.phase = phase
        self.call_path = call_path
        self.delivery = delivery
        self.error_envelope = error_envelope


class ArtifactValidationError(ArtisanError):
    """Raised when artifacts don't match output specs.

    This includes:
    - Missing required output roles
    - Empty artifact lists for required roles
    - Artifact type mismatches (e.g., CSVArtifact for METRIC spec)
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

    Derived outputs must explicitly declare every parent role required by
    their ``derives_from`` contract for each output occurrence.
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
    - An input ID is absent from the declared input role
    - A target or sibling index references a non-existent output occurrence
    - A source role or reference kind violates the output's contract
    - The same source reference is declared twice for an output occurrence
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
