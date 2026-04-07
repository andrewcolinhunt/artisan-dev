"""Artifact materialization for creator execution."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.specs.input_spec import InputSpec

if TYPE_CHECKING:
    from artisan.storage.core.artifact_store import ArtifactStore


def materialize_inputs(
    artifacts: dict[str, list[Artifact]],
    input_specs: dict[str, InputSpec],
    directory: Path,
    artifact_store: ArtifactStore,
) -> tuple[dict[str, list[Artifact]], set[str]]:
    """Materialize input artifacts to disk with dependency-aware ordering.

    Non-config artifacts are materialized first so that config artifacts
    can resolve file-path references to already-written files.

    Args:
        artifacts: Role-keyed hydrated input artifacts.
        input_specs: Role-keyed input specs controlling materialization.
        directory: Target directory for materialized files.
        artifact_store: Store for hydrating config-referenced artifacts.

    Returns:
        Tuple of (artifacts dict, set of artifact_ids that were materialized).
        The artifacts dict is the same input dict (files written as side effect).
    """
    non_configs: list[tuple[Artifact, str | None]] = []
    configs: list[ExecutionConfigArtifact] = []
    seen_ids: set[str] = set()

    for role, artifact_list in artifacts.items():
        spec = input_specs.get(role, InputSpec())
        for artifact in artifact_list:
            if not artifact.is_hydrated:
                continue

            if not spec.materialize:
                continue
            if artifact.artifact_id is None:
                continue
            if artifact.artifact_id in seen_ids:
                continue
            seen_ids.add(artifact.artifact_id)

            if isinstance(artifact, ExecutionConfigArtifact):
                configs.append(artifact)
            else:
                non_configs.append((artifact, spec.materialize_as))

    for config in configs:
        for ref_id in config.get_artifact_references():
            if ref_id in seen_ids:
                continue
            seen_ids.add(ref_id)
            ref_artifact = artifact_store.get_artifact(ref_id, hydrate=True)
            if ref_artifact is not None:
                non_configs.append((ref_artifact, None))

    materialized_ids: set[str] = set()
    resolved_paths: dict[str, Path] = {}
    for artifact, fmt in non_configs:
        if artifact.artifact_id is None:
            continue
        materialized = artifact.materialize_to(directory, format=fmt)
        if isinstance(materialized, Path):
            resolved_paths[artifact.artifact_id] = materialized
            materialized_ids.add(artifact.artifact_id)

    for config in configs:
        config.materialize_to(directory, resolved_paths=resolved_paths)
        if config.artifact_id is not None:
            materialized_ids.add(config.artifact_id)

    return artifacts, materialized_ids
