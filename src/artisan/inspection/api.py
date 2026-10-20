"""Validate and dispatch bounded inspection requests to existing Artisan readers."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ValidationError

from artisan.errors import (
    ArtisanError,
    AssessmentSpecError,
    ComparisonSpecError,
    OutcomeSelectionError,
)
from artisan.inspection._boundary import (
    invalid_input,
    require_delta_root,
    store_not_found,
    validate_response,
)
from artisan.inspection.catalog import definition
from artisan.registry.models import DiscoveryReport
from artisan.visualization.outcomes import OutcomeSelection


def validate_request(capability: str, arguments: Mapping[str, Any]) -> BaseModel:
    """Validate the exact advertised request before application setup or store reads.

    Caller errors have fixed guidance and field paths; raw validation text is
    retained only as the native exception cause for local diagnostics.
    """
    item = definition(capability)
    if not isinstance(arguments, Mapping):
        invalid_input("arguments")
    values = dict(arguments)
    try:
        json_valid = _json_values(values)
    except RecursionError as exc:
        try:
            invalid_input(
                "arguments", "Inspection arguments must contain finite JSON values"
            )
        except ArtisanError as error:
            raise error from exc
    if not json_valid:
        invalid_input(
            "arguments", "Inspection arguments must contain only finite JSON values"
        )
    try:
        return item.request_type.model_validate(values)
    except ValidationError as exc:
        issue = exc.errors(include_input=False)[0]
        location = issue["loc"]
        if issue["type"] == "extra_forbidden":
            location = location[:-1]
        first = location[0] if location else None
        field = (
            ".".join(str(part) for part in location)
            if first in item.request_type.model_fields
            else "arguments"
        )
        if capability in {"assess_outcomes", "compare_runs"}:
            if first == "spec":
                error_type = (
                    AssessmentSpecError
                    if capability == "assess_outcomes"
                    else ComparisonSpecError
                )
                message = "Invalid inspection specification"
                raise error_type(message, field=field) from exc
            if first in {*OutcomeSelection.model_fields, "baseline", "candidate"}:
                message = "Invalid outcome selection"
                raise OutcomeSelectionError(message, field=field) from exc
        try:
            invalid_input(field)
        except ArtisanError as error:
            raise error from exc


def _json_values(value: Any) -> bool:
    if isinstance(value, dict):
        return all(
            isinstance(key, str) and _json_values(item) for key, item in value.items()
        )
    if isinstance(value, list):
        return all(_json_values(item) for item in value)
    if type(value) is float:
        return math.isfinite(value)
    return value is None or type(value) in {str, int, bool}


def inspect_evidence(
    capability: str,
    arguments: Mapping[str, Any],
    *,
    delta_root: str | None = None,
    discovery: DiscoveryReport | None = None,
) -> BaseModel:
    """Return the validated bounded response for one installed capability.

    Args:
        capability: Exact installed name returned by ``list_inspections``.
        arguments: JSON-valued fields matching its advertised request schema.
        delta_root: Explicit store root for capabilities requiring a store.
        discovery: Caller-supplied startup report for ``capabilities``.

    Raises:
        ArtisanError: Request, store, selection or evidence-limit failure.
        Exception: Unexpected reader or projection defects retain native causes.
    """
    request = validate_request(capability, arguments)
    item = definition(capability)
    kwargs = {name: getattr(request, name) for name in type(request).model_fields}
    if item.requires_discovery:
        if not isinstance(discovery, DiscoveryReport):
            invalid_input("discovery", "Supply an application discovery report")
        kwargs["discovery"] = discovery
    if item.requires_store:
        kwargs["root"] = require_delta_root(delta_root)
        try:
            payload = item.handler(**kwargs)
        except FileNotFoundError as exc:
            raise store_not_found() from exc
    else:
        payload = item.handler(**kwargs)
    return validate_response(payload, item.response_type)
