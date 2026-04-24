"""Shared pydantic validators for input and output specs."""

from __future__ import annotations

from artisan.schemas.artifact.types import ArtifactTypes


def validate_artifact_type_str(value: str) -> str:
    """Accept ``ArtifactTypes.ANY`` or any registered artifact type key.

    ``ANY`` is a spec-only wildcard sentinel and is not present in the
    registry, so it is checked explicitly before the registry lookup.

    Args:
        value: The ``artifact_type`` string from an ``InputSpec`` or
            ``OutputSpec``.

    Returns:
        The input value, unchanged, when valid.

    Raises:
        ValueError: If ``value`` is neither ``ANY`` nor a registered key.
            The message lists the bad string, the registered keys, and
            how to register a new type via ``ArtifactTypeDef``.
    """
    if value == ArtifactTypes.ANY or ArtifactTypes.is_registered(value):
        return value
    registered = sorted(ArtifactTypes.all())
    msg = (
        f"artifact_type={value!r} is not registered. "
        f"Valid options: ArtifactTypes.ANY or one of {registered}. "
        f"Define an ArtifactTypeDef subclass with key={value!r} to register it."
    )
    raise ValueError(msg)
