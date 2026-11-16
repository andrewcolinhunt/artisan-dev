"""An application-owned schema carried inside the inert transport payload."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from artisan.schemas.artifact.metric import MetricArtifact


class ProbeInput(BaseModel):
    """Carry a registered artifact through an application-owned Pydantic model."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    metric: MetricArtifact
