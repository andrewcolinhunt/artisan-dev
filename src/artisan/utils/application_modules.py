"""Validate application selection without importing application code."""

from __future__ import annotations

import os
from collections.abc import Sequence

from artisan.errors import ApplicationRegistrationError


def validate_application_modules(value: object) -> tuple[str, ...]:
    """Return an ordered unique module tuple, rejecting unsafe input shapes."""
    if isinstance(value, str) or not isinstance(value, Sequence):
        msg = "Application modules must be a sequence of names"
        raise ApplicationRegistrationError(msg)
    if any(
        not isinstance(name, str)
        or not name
        or not all(part.isidentifier() for part in name.split("."))
        for name in value
    ):
        msg = "Application modules must be dotted Python names"
        raise ApplicationRegistrationError(msg)
    return tuple(dict.fromkeys(value))


def resolve_application_modules(
    explicit: Sequence[str] | None = None,
) -> tuple[str, ...]:
    """Resolve trusted entry-point environment first, then explicit modules."""
    environment = [
        name.strip()
        for name in os.environ.get("ARTISAN_LOAD_MODULES", "").split(",")
        if name.strip()
    ]
    supplied = () if explicit is None else validate_application_modules(explicit)
    return validate_application_modules([*environment, *supplied])


def require_application_modules(required: object, authorized: tuple[str, ...]) -> None:
    """Check recorded requirements without granting them import authority."""
    modules = validate_application_modules(required)
    missing = [name for name in modules if name not in authorized]
    if missing:
        msg = f"Authorize the recorded application modules before continuing: {', '.join(missing)}"
        raise ApplicationRegistrationError(msg)


def module_is_within(module: str, parent: str) -> bool:
    """Compare module ancestry at dotted component boundaries."""
    return module == parent or module.startswith(parent + ".")
