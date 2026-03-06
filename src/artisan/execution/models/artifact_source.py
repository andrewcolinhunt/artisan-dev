"""Unified artifact source for Delta-backed and in-memory artifacts.

ArtifactSource wraps artifact inputs so the chain executor and lifecycle
can handle both Delta-backed (by ID) and in-memory (from previous operation)
artifacts through a single interface.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.specs.input_spec import InputSpec

if TYPE_CHECKING:
    from artisan.storage.core.artifact_store import ArtifactStore


@dataclass
class ArtifactSource:
    """Wraps artifact inputs from different backing stores.

    Either ``_ids`` or ``_artifacts`` is set, never both.
    ``hydrate()`` resolves to concrete Artifact objects regardless of backing.

    Attributes:
        _ids: Artifact IDs to resolve from Delta (ID-backed).
        _artifacts: Pre-resolved artifacts (in-memory).
    """

    _ids: list[str] | None = field(default=None, repr=False)
    _artifacts: list[Artifact] | None = field(default=None, repr=False)

    @staticmethod
    def from_ids(ids: list[str]) -> ArtifactSource:
        """Create a source backed by artifact IDs (resolved from Delta).

        Args:
            ids: List of 32-char hex artifact IDs.

        Returns:
            ArtifactSource that will hydrate from the artifact store.
        """
        return ArtifactSource(_ids=list(ids))

    @staticmethod
    def from_artifacts(artifacts: list[Artifact]) -> ArtifactSource:
        """Create a source backed by in-memory artifacts.

        Args:
            artifacts: Pre-resolved Artifact objects.

        Returns:
            ArtifactSource that returns artifacts directly on hydrate().
        """
        return ArtifactSource(_artifacts=list(artifacts))

    def hydrate(
        self,
        store: ArtifactStore,
        spec: InputSpec,
        default_hydrate: bool = True,
    ) -> list[Artifact]:
        """Resolve to concrete Artifact objects.

        For in-memory sources, returns artifacts directly.
        For ID-backed sources, loads from the artifact store.

        Args:
            store: Artifact store for loading ID-backed artifacts.
            spec: Input spec controlling hydration behavior.
            default_hydrate: Fallback hydration flag.

        Returns:
            List of hydrated Artifact objects.
        """
        if self._artifacts is not None:
            return list(self._artifacts)

        if self._ids is None or not self._ids:
            return []

        from artisan.execution.inputs.instantiation import instantiate_inputs

        role_inputs = {"_hydrate_role": self._ids}
        role_specs = {"_hydrate_role": spec}
        result, _ = instantiate_inputs(role_inputs, store, role_specs, default_hydrate)
        return result.get("_hydrate_role", [])

    @property
    def is_materialized(self) -> bool:
        """Whether this source has pre-resolved artifacts (no Delta lookup needed)."""
        return self._artifacts is not None
