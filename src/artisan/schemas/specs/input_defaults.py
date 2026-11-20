"""Capture declared artifacts for omitted roles at fresh public call boundaries."""

from __future__ import annotations

from collections.abc import Collection, Mapping

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.specs.input_spec import InputSpec


def capture_input_defaults(
    specs: Mapping[str, InputSpec], present: Collection[str]
) -> dict[str, tuple[Artifact, ...]]:
    """Detach only defaults that the caller has not explicitly overridden."""
    result = {}
    for role, spec in specs.items():
        if role not in present and (artifacts := spec.snapshot_default()) is not None:
            result[role] = artifacts
    return result
