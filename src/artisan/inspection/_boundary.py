"""Validate shared evidence and sanitize known query failures."""

from __future__ import annotations

from typing import Any, Literal, Never, cast

from pydantic import BaseModel, ConfigDict, TypeAdapter

from artisan.errors import ArtisanError, ArtisanErrorEnvelope, CommitError, ErrorCode
from artisan.inspection._evidence import (
    MAX_PAYLOAD_BYTES,
    encode_json,
    ensure_fits,
    json_ready,
    response_too_large,
)


class ErrorPayload(ArtisanErrorEnvelope):
    """Base handled error without cause or undeclared extension fields."""

    model_config = ConfigDict(extra="forbid")


class CommitErrorPayload(ErrorPayload):
    """Commit repair identities retained by the inspection boundary."""

    code: Literal["commit_failed"] = "commit_failed"
    logical_commit_id: str
    table: str
    plan_key: str
    verified_tables: list[str]
    staging_objects: list[str]


def error_payload(exc: ArtisanError) -> dict[str, Any]:
    """Validate a known error subtype, suppress its cause and bound its envelope."""
    error_type = CommitErrorPayload if isinstance(exc, CommitError) else ErrorPayload
    payload = json_ready(error_type.model_validate(exc.to_dict(include_cause=False)))
    if len(encode_json(payload)) > MAX_PAYLOAD_BYTES:
        return ErrorPayload.model_validate(response_too_large()).model_dump(mode="json")
    return cast("dict[str, Any]", payload)


def validate_response(value: Any, success_type: Any) -> BaseModel:
    """Validate only the selected success type and its complete canonical size."""
    model = TypeAdapter(success_type).validate_json(encode_json(value))
    ensure_fits(json_ready(model))
    return cast("BaseModel", model)


def invalid_input(field: str, message: str = "Invalid inspection request") -> Never:
    """Raise fixed guidance for a request field without echoing its value."""
    raise ArtisanError(
        ErrorCode.PARAM_TYPE_MISMATCH,
        message,
        error_type="validation",
        field=field,
        hint="Provide fields and values matching the inspection request schema.",
        recovery_hint="CHECK_INPUT",
    )


def validate_int_range(value: int, *, field: str, minimum: int, maximum: int) -> None:
    """Require a strict integer within an inclusive range."""
    if isinstance(value, bool) or not isinstance(value, int):
        invalid_input(field, f"{field} must be an integer")
    if not minimum <= value <= maximum:
        invalid_input(field, f"{field} must be between {minimum} and {maximum}")


def require_delta_root(root: str | None) -> str:
    """Require an explicitly supplied nonempty store root."""
    if not root:
        raise ArtisanError(
            ErrorCode.DELTA_ROOT_UNSET,
            "No Delta root configured",
            error_type="config",
            hint="Supply the intended store root through the calling interface.",
            recovery_hint="CHECK_INPUT",
        )
    if not isinstance(root, str):
        invalid_input("delta_root")
    return root


def store_not_found() -> ArtisanError:
    """Return neutral guidance without disclosing the reader's filesystem error."""
    return ArtisanError(
        ErrorCode.STORE_NOT_FOUND,
        "Artisan store not found",
        error_type="io",
        hint="Check the configured root for the intended store.",
        recovery_hint="CHECK_INPUT",
    )
