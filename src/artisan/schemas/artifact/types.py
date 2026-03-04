"""Extensible artifact type namespace.

Provides ``ArtifactTypes``, a facade that supports both built-in types
(with IDE autocomplete) and dynamically registered types.  All values
are plain ``str``: ``ArtifactTypes.DATA == "data"`` is True.
"""

from __future__ import annotations

from enum import StrEnum
from typing import ClassVar


class _BuiltinArtifactType(StrEnum):
    """Private enum for built-in types — gives IDE autocomplete."""

    METRIC = "metric"
    FILE_REF = "file_ref"
    CONFIG = "config"
    DATA = "data"


class _ArtifactTypesMeta(type):
    """Metaclass enabling ``in`` checks and iteration on ArtifactTypes."""

    def __contains__(cls, item: object) -> bool:
        """Check if a type key is registered."""
        if not isinstance(item, str):
            return False
        return item in cls._registry  # type: ignore[attr-defined]

    def __iter__(cls):  # type: ignore[override]
        """Iterate over all registered type keys."""
        return iter(cls._registry)  # type: ignore[attr-defined]


class ArtifactTypes(metaclass=_ArtifactTypesMeta):
    """Public API for all artifact types — built-in and dynamic.

    Built-in types have IDE autocomplete via ClassVar assignments.
    Dynamic types are added at registration time via ``register()``.

    Examples:
        >>> ArtifactTypes.DATA
        'data'
        >>> "data" in ArtifactTypes
        True
        >>> ArtifactTypes.get("metric")
        'metric'
    """

    METRIC: ClassVar[str] = _BuiltinArtifactType.METRIC
    FILE_REF: ClassVar[str] = _BuiltinArtifactType.FILE_REF
    CONFIG: ClassVar[str] = _BuiltinArtifactType.CONFIG
    DATA: ClassVar[str] = _BuiltinArtifactType.DATA

    ANY: ClassVar[str] = "any"
    """Wildcard sentinel for specs: "accept/produce any concrete type."

    NOT a registered type — cannot appear on concrete ``Artifact`` instances.
    Used only in ``InputSpec`` and ``OutputSpec`` declarations.
    """

    _registry: ClassVar[dict[str, str]] = {
        m.value: m.value for m in _BuiltinArtifactType
    }

    @classmethod
    def get(cls, key: str) -> str:
        """Lookup by string key.

        Args:
            key: Artifact type key (e.g. "data").

        Returns:
            The registered type string.

        Raises:
            KeyError: If key is not registered.
        """
        if key not in cls._registry:
            raise KeyError(
                f"Unknown artifact type: {key!r}. "
                f"Registered: {list(cls._registry.keys())}"
            )
        return cls._registry[key]

    @classmethod
    def register(cls, key: str) -> str:
        """Register a new artifact type.

        Called automatically by ``ArtifactTypeDef.__init_subclass__``.
        Also sets ``ArtifactTypes.<KEY>`` as a class attribute for
        runtime dot-access (no IDE autocomplete for dynamic types).

        Args:
            key: Artifact type key (e.g. "data_record").

        Returns:
            The registered type string.

        Raises:
            ValueError: If key is already registered.
        """
        if key in cls._registry:
            # Allow re-registration of built-in types (idempotent)
            return cls._registry[key]
        cls._registry[key] = key
        setattr(cls, key.upper(), key)
        return key

    @classmethod
    def all(cls) -> list[str]:
        """Return all registered type keys."""
        return list(cls._registry.keys())

    @classmethod
    def is_registered(cls, key: str) -> bool:
        """Return True if the given type key is in the registry."""
        return key in cls._registry

    @classmethod
    def is_concrete(cls, key: str) -> bool:
        """Check if a type key refers to a concrete (registered) type.

        Args:
            key: Artifact type key.

        Returns:
            True if key is in the registry, False for ANY or unknown keys.
        """
        return key in cls._registry

    @classmethod
    def matches(cls, spec_type: str, concrete_type: str) -> bool:
        """Check if a concrete type satisfies a spec type constraint.

        Args:
            spec_type: The type declared on a spec (may be ANY).
            concrete_type: The type on a concrete artifact.

        Returns:
            True if spec_type is ANY or spec_type equals concrete_type.
        """
        if spec_type == cls.ANY:
            return True
        return spec_type == concrete_type
