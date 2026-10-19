"""Shared classification for explicit environment configuration and evidence."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import urlsplit

COMMAND_ATTACHMENT_ENV = (
    "_ARTISAN_COMMAND_SOCKET",
    "_ARTISAN_COMMAND_SECRET",
    "_ARTISAN_COMMAND_SCOPE",
    "_ARTISAN_PARENT_COMMAND",
)


def strip_command_attachment(value: Any) -> Any:
    """Exclude transient command metadata from an authored environment mapping."""
    if isinstance(value, Mapping):
        return {
            key: item
            for key, item in value.items()
            if key not in COMMAND_ATTACHMENT_ENV
        }
    return value


_CREDENTIAL = re.compile(r"TOKEN|SECRET|PASSWORD|PASSWD|CREDENTIAL|API_KEY", re.I)
_AUTHENTICATION = re.compile(
    r"(?:^|[^a-z0-9])(?:auth|authentication|authori[sz]ation|oauth|basicauth)\d*"
    r"(?:codes?|keys?|headers?)?\d*(?:$|[^a-z0-9])",
    re.I,
)


def credential_name(name: str) -> bool:
    """Recognize the documented credential variable/key names."""
    # Authentication terms are words, including camelCase/acronym boundaries.
    words = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    words = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", words)
    return (
        bool(_CREDENTIAL.search(name.replace("-", "_")))
        or bool(_AUTHENTICATION.search(words))
        or name.upper() in {"AWS_ACCESS_KEY_ID", "XAUTHORITY"}
    )


def credential_url(value: object) -> bool:
    """Recognize URI credentials and capabilities without exposing their values."""
    if not isinstance(value, str) or "://" not in value:
        return False
    try:
        parsed = urlsplit(value)
    except ValueError:
        return True
    return bool(parsed.username or parsed.password or parsed.query or parsed.fragment)


def validate_public_env(env: dict[str, str], public_env: object) -> None:
    """Validate a local declaration, including unvalidated model-copy updates.

    Raises:
        ValueError: If names are invalid, missing, duplicated, or sensitive.
    """
    if isinstance(public_env, str) or not isinstance(public_env, Sequence):
        msg = "public_env must be a sequence of nonempty environment names"
        raise ValueError(msg)
    seen: set[str] = set()
    for name in public_env:
        if not isinstance(name, str) or not name.strip() or name in seen:
            msg = "public_env names must be nonempty strings without duplicates"
            raise ValueError(msg)
        if name not in env:
            msg = "public_env names must exist in the same env mapping"
            raise ValueError(msg)
        if credential_name(name) or credential_url(env[name]):
            msg = "public_env cannot declare a recognized credential public"
            raise ValueError(msg)
        seen.add(name)


def sensitive_environment(
    env: dict[str, str], public_env: tuple[str, ...] = ()
) -> dict[str, str]:
    """Select conservative env sensitivity; known redactor values remain intact."""
    validate_public_env(env, public_env)
    return {name: value for name, value in env.items() if name not in public_env}
