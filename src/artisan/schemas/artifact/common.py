"""Shared helpers for artifact schema models."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, PrivateAttr


def get_compound_extension(filename: str) -> str:
    """Get the compound extension from a filename.

    Handles compound extensions like '.dat.gz' by joining all suffixes.

    Args:
        filename: The filename to extract extension from.

    Returns:
        The compound extension (e.g., '.dat.gz') or single extension (e.g., '.dat').
        Returns empty string if no extension.
    """
    suffixes = Path(filename).suffixes
    return "".join(suffixes) if suffixes else ""


class JsonContentMixin(BaseModel):
    """Mixin providing a cached ``values`` property for JSON content artifacts.

    Expects the concrete class to have a ``content: bytes | None`` field
    containing JSON-encoded data.
    """

    _cached_values: dict[str, Any] | None = PrivateAttr(default=None)

    @property
    def values(self) -> dict[str, Any]:
        """Return parsed JSON values, caching after first access.

        Raises:
            ValueError: If the artifact is not hydrated.
        """
        if self._cached_values is not None:
            return self._cached_values
        content: bytes | None = getattr(self, "content", None)
        if content is None:
            raise ValueError(
                "Cannot access values: artifact was not hydrated. "
                "Use artifact_store.get_artifact(id, hydrate=True)."
            )
        self._cached_values = json.loads(content.decode("utf-8"))
        assert self._cached_values is not None
        return self._cached_values
