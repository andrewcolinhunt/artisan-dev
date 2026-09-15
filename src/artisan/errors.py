"""Structured error identity for agent-recoverable failures.

``ArtisanError`` + ``ErrorCode`` is the single source of truth for error
*identity*; ``ArtisanErrorEnvelope`` is its *serialization*, consumed at
machine-readable boundaries including CLI/MCP responses, the tool-endpoint
wire, and persisted failure records. The envelope is not a field bolted onto
every string-carrying result: a new ``ErrorCode`` or envelope field ships only
together with a raise site **and** a reader that consumes it.
"""

from __future__ import annotations

from difflib import get_close_matches
from typing import Any, Literal

from pydantic import BaseModel

ErrorType = Literal["validation", "runtime", "io", "compute", "config"]
RecoveryHint = Literal[
    "RETRY_LATER",
    "CHECK_INPUT",
    "TRY_ALTERNATIVE",
    "REPORT_TO_USER",
]


class ArtisanErrorEnvelope(BaseModel):
    """JSON-serializable payload for any Artisan-raised error.

    The same shape ships over the MCP wire and is returned by
    ``ArtisanError.to_dict()`` on the Python side. Fields are optional
    where not every raise site can populate them honestly.

    Attributes:
        error_type: Coarse category. Required at every raise site.
        code: Stable agent-recognizable identifier (e.g. ``"op_execute_failed"``).
        message: Human-readable failure summary.
        operation_name: The operation whose validation/execution raised.
        step_name: The pipeline step name, when known.
        field: Dotted path locating the offending field
            (e.g. ``"params.multiplyer"``).
        hint: Free-form note about what to do next; pairs with
            ``recovery_hint``.
        suggestions: ``difflib`` close matches for typo'd identifiers.
        recovery_hint: Next-action signal for an agent. ``None`` allowed
            but discouraged.
        doc_uri: Stable URL keyed off ``code``; the URI may be repointed
            without breaking the envelope contract.
    """

    error_type: ErrorType
    code: str
    message: str

    operation_name: str | None = None
    step_name: str | None = None
    field: str | None = None

    hint: str | None = None
    suggestions: list[str] = []
    recovery_hint: RecoveryHint | None = None
    doc_uri: str | None = None


class ArtisanError(Exception):
    """Structured exception carrying an ``ArtisanErrorEnvelope``.

    Extends ``Exception`` (not ``ValueError``) to avoid MRO surprises when
    callers catch ``ValueError`` for unrelated reasons. Use
    ``raise ArtisanError(...) from caught_exc`` to chain causes; ``to_dict``
    walks ``__cause__`` one level and serializes it under the ``cause`` key.

    Domain-specific subclasses (e.g. ``ArtifactValidationError``) inherit
    from this and supply their own ``code`` / ``recovery_hint`` in
    ``__init__``.
    """

    def __init__(
        self,
        code: str,
        message: str,
        *,
        error_type: ErrorType,
        operation_name: str | None = None,
        step_name: str | None = None,
        field: str | None = None,
        hint: str | None = None,
        suggestions: list[str] | None = None,
        recovery_hint: RecoveryHint | None = None,
        doc_uri: str | None = None,
    ) -> None:
        """Build the envelope and forward ``message`` to ``Exception``.

        Args:
            code: Stable identifier from ``ErrorCode``.
            message: Human-readable summary.
            error_type: Coarse category (required, no default).
            operation_name: The operation whose validation/execution raised.
            step_name: The pipeline step name, when known.
            field: Dotted path locating the offending field.
            hint: Free-form note about what to do next.
            suggestions: ``difflib`` close matches for typo'd identifiers.
            recovery_hint: Next-action signal for an agent.
            doc_uri: Stable URL keyed off ``code``; defaults to
                ``https://artisan.dev/docs/errors/<code>``.
        """
        self.envelope = ArtisanErrorEnvelope(
            error_type=error_type,
            code=code,
            message=message,
            operation_name=operation_name,
            step_name=step_name,
            field=field,
            hint=hint,
            suggestions=suggestions or [],
            recovery_hint=recovery_hint,
            doc_uri=doc_uri or _default_doc_uri(code),
        )
        super().__init__(message)

    @property
    def code(self) -> str:
        """The stable error code identifier."""
        return self.envelope.code

    @property
    def error_type(self) -> ErrorType:
        """The coarse error category."""
        return self.envelope.error_type

    def to_dict(self, include_cause: bool = True) -> dict[str, Any]:
        """Serialize the envelope (and optional cause) to a plain dict.

        Args:
            include_cause: When True and ``__cause__`` is set, include the
                cause type, message, and traceback under the ``cause`` key.

        Returns:
            A dict shaped like the envelope, with an optional ``cause`` key.
        """
        # Imported lazily: ``artisan.utils.traceback`` pulls the ``artisan.utils``
        # package init, which re-exports ``external_tools`` (a module that imports
        # this one) — a module-level import here would form a cycle.
        from artisan.utils.traceback import format_error

        data = self.envelope.model_dump(exclude_none=False)
        if include_cause and self.__cause__ is not None:
            data["cause"] = {
                "type": type(self.__cause__).__name__,
                "message": str(self.__cause__),
                "traceback": format_error(self.__cause__),
            }
        return data


class ErrorCode:
    """Stable agent-recognizable identifiers used at raise sites.

    Stringly-typed namespace (not an ``Enum``) so a typo surfaces at the
    raise site. Codes are added on demand — a new one ships only with a
    raise site and a reader that consumes it.
    """

    UNKNOWN_OPERATION = "unknown_operation"
    OP_PARAMS_UNDOCUMENTED = "op_params_undocumented"
    OP_EXECUTE_FAILED = "op_execute_failed"
    TOOL_ENDPOINT_MISCONFIGURED = "tool_endpoint_misconfigured"
    PARAM_TYPE_MISMATCH = "param_type_mismatch"

    # config/io — machine-readable store APIs
    DELTA_ROOT_UNSET = "delta_root_unset"
    STORE_NOT_FOUND = "store_not_found"
    ARTIFACT_INTEGRITY_FAILED = "artifact_integrity_failed"
    PERSISTENCE_INTEGRITY_FAILED = "persistence_integrity_failed"
    STORE_INTEGRITY_FAILED = "store_integrity_failed"
    COMMIT_FAILED = "commit_failed"
    INCOMPATIBLE_STORE = "incompatible_store"

    # io — worker-side input resolution / output delivery (tool-endpoint wire)
    INPUT_RESOLUTION_FAILED = "input_resolution_failed"
    OUTPUT_DELIVERY_FAILED = "output_delivery_failed"

    # validation — execution-time contract checks (domain-exception re-parenting)
    ARTIFACT_VALIDATION_FAILED = "artifact_validation_failed"
    LINEAGE_INCOMPLETE = "lineage_incomplete"
    LINEAGE_INTEGRITY_FAILED = "lineage_integrity_failed"
    PASSTHROUGH_VALIDATION_FAILED = "passthrough_validation_failed"


class CommitError(ArtisanError):
    """Raised at the first failed table in one immutable commit plan."""

    def __init__(
        self,
        logical_commit_id: str,
        table: str,
        plan_key: str,
        verified_tables: list[str],
        staging_objects: list[str],
        message: str,
    ) -> None:
        """Create a structured, repair-oriented commit failure."""
        self.logical_commit_id = logical_commit_id
        self.table = table
        self.plan_key = plan_key
        self.verified_tables = tuple(verified_tables)
        self.staging_objects = tuple(staging_objects)
        super().__init__(
            code=ErrorCode.COMMIT_FAILED,
            message=message,
            error_type="io",
            hint=f"Run `artisan store repair` for {logical_commit_id}",
            recovery_hint="REPORT_TO_USER",
        )

    def to_dict(self, include_cause: bool = True) -> dict[str, Any]:
        """Serialize the standard envelope with exact repair context."""
        data = super().to_dict(include_cause=include_cause)
        data.update(
            logical_commit_id=self.logical_commit_id,
            table=self.table,
            plan_key=self.plan_key,
            verified_tables=list(self.verified_tables),
            staging_objects=list(self.staging_objects),
        )
        return data


class StoreIntegrityError(ArtisanError):
    """Raised when plan, control, staged, or persisted evidence conflicts."""

    def __init__(self, message: str) -> None:
        """Create a fail-closed store integrity error."""
        super().__init__(
            code=ErrorCode.STORE_INTEGRITY_FAILED,
            message=message,
            error_type="io",
            recovery_hint="REPORT_TO_USER",
        )


class ArtifactIntegrityError(ArtisanError):
    """Raised when persisted or externally backed artifact bytes drift."""

    def __init__(self, message: str) -> None:
        """Create a fail-closed artifact integrity error."""
        super().__init__(
            code=ErrorCode.ARTIFACT_INTEGRITY_FAILED,
            message=message,
            error_type="io",
            recovery_hint="CHECK_INPUT",
        )


class IncompatibleStoreError(ArtisanError):
    """Raised when a Delta root is not the required Artisan store format."""

    def __init__(self, message: str) -> None:
        """Create a store-format compatibility error."""
        super().__init__(
            code=ErrorCode.INCOMPATIBLE_STORE,
            message=message,
            error_type="config",
            recovery_hint="CHECK_INPUT",
        )


class PersistenceIntegrityError(ArtisanError):
    """Raised when persisted framework relations are inconsistent."""

    def __init__(self, message: str) -> None:
        """Create a fail-closed persistence integrity error."""
        super().__init__(
            code=ErrorCode.PERSISTENCE_INTEGRITY_FAILED,
            message=message,
            error_type="io",
            recovery_hint="REPORT_TO_USER",
        )


def _default_doc_uri(code: str) -> str:
    """Return the canonical doc URL for an error code."""
    return f"https://artisan.dev/docs/errors/{code}"


def suggest(
    value: str,
    candidates: list[str] | set[str],
    n: int = 3,
    cutoff: float = 0.6,
) -> list[str]:
    """Top-N did-you-mean candidates by string similarity.

    Args:
        value: The (possibly typo'd) user-supplied identifier.
        candidates: The set of valid identifiers to match against.
        n: Maximum number of suggestions to return.
        cutoff: Minimum similarity ratio (0.0-1.0).

    Returns:
        Up to ``n`` close matches sorted by descending similarity.
    """
    return get_close_matches(value, list(candidates), n=n, cutoff=cutoff)
