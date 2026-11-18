"""Runtime references retained by an operation result, without executor ownership."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.provenance.occurrence import ArtifactOccurrence, OccurrenceAddress
from artisan.schemas.provenance.projection import BoundaryProjection


@dataclass(frozen=True)
class ArtifactResultSupport:
    """Hold an immutable occurrence inventory and its validated projection.

    Membership is copied while artifact payloads remain shared. Keeping this
    value retains neither an executor nor its command scopes or file cleanup.
    """

    projection: BoundaryProjection
    artifacts: Mapping[OccurrenceAddress, Artifact]

    def __post_init__(self) -> None:
        projection = BoundaryProjection.model_validate(self.projection)
        artifacts = dict(self.artifacts)
        if set(artifacts) != {item.address for item in projection.occurrences}:
            msg = "Result support must contain exactly the projected occurrences"
            raise ValueError(msg)
        for item in projection.occurrences:
            artifact = artifacts[item.address]
            ArtifactOccurrence(item.address, artifact)
            if (artifact.artifact_id, artifact.artifact_type) != (
                item.artifact_id,
                item.artifact_type,
            ):
                msg = "Result support artifact differs from its projected identity"
                raise ValueError(msg)
        object.__setattr__(self, "projection", projection)
        object.__setattr__(self, "artifacts", MappingProxyType(artifacts))

    def referenced_artifacts(
        self,
        role: str,
        *,
        index: int,
        artifact: ExecutionConfigArtifact,
    ) -> Mapping[str, Artifact]:
        """Return direct referents selected by one exact exported occurrence."""
        occurrences = self.projection.export_roles().get(role, ())
        if type(index) is not int or not 0 <= index < len(occurrences):
            msg = "Result support has no matching output position"
            raise ValueError(msg)
        source = occurrences[index]
        if self.artifacts[source] is not artifact:
            msg = "Result support no longer matches the output occurrence"
            raise ValueError(msg)
        artifact.finalize()
        if artifact.content is None:
            msg = "Result reference access requires hydrated configuration content"
            raise ValueError(msg)
        targets = [
            edge.target for edge in self.projection.references if edge.source == source
        ]
        referents: dict[str, Artifact] = {}
        for target in targets:
            referent = self.artifacts[target]
            referent.finalize()
            assert referent.artifact_id is not None
            if referent.artifact_id in referents:
                msg = "Result configuration references ambiguous occurrences"
                raise ValueError(msg)
            referents[referent.artifact_id] = referent
        if set(referents) != set(artifact.get_artifact_references()):
            msg = "Result support does not match configuration references"
            raise ValueError(msg)
        return MappingProxyType(referents)
