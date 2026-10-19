"""Shared classification for explicit environment configuration and evidence."""

from __future__ import annotations

import re
from collections.abc import Sequence
from urllib.parse import urlsplit

_CREDENTIAL = re.compile(r"TOKEN|SECRET|PASSWORD|PASSWD|CREDENTIAL|AUTH|API_KEY", re.I)


def credential_name(name: str) -> bool:
    """Recognize the documented credential variable/key names."""
    return (
        bool(_CREDENTIAL.search(name.replace("-", "_")))
        or name.upper() == "AWS_ACCESS_KEY_ID"
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
