"""Artifact materialization for creator execution."""

from __future__ import annotations

from typing import TYPE_CHECKING

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.specs.input_spec import InputSpec

if TYPE_CHECKING:
    from artisan.storage.core.artifact_store import ArtifactStore


def _is_remote(path: str | None) -> bool:
    """True when ``path`` is a cloud URI (``scheme://`` other than file)."""
    return bool(path) and "://" in path and not path.startswith("file://")


def materialize_inputs(
    artifacts: dict[str, list[Artifact]],
    input_specs: dict[str, InputSpec],
    directory: str,
    artifact_store: ArtifactStore,
    *,
    endpoint_routed: bool = False,
) -> tuple[dict[str, list[Artifact]], set[str]]:
    """Materialize input artifacts to disk with dependency-aware ordering.

    Non-config artifacts are materialized first so that config artifacts
    can resolve file-path references to already-written files.

    Args:
        artifacts: Role-keyed hydrated input artifacts.
        input_specs: Role-keyed input specs controlling materialization.
        directory: Target directory for materialized files.
        artifact_store: Store for hydrating config-referenced artifacts.
        endpoint_routed: When True, a cloud-hosted (non-config, file-backed)
            input is shipped to the worker by reference: its download is
            skipped and ``materialized_path`` is set to its cloud
            ``external_path`` so the URI flows through the endpoint client's
            existing ``pack_inputs`` passthrough. Local execution keeps the
            default (False), materializing every input to disk as before.

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

    # Get fs from artifact_store for cloud-capable source reads
    fs = artifact_store._fs if hasattr(artifact_store, "_fs") else None

    materialized_ids: set[str] = set()
    resolved_paths: dict[str, str] = {}
    for artifact, fmt in non_configs:
        if artifact.artifact_id is None:
            continue
        # Endpoint-routed steps ship cloud-hosted inputs by reference: the
        # worker fetches the URI with ambient creds, so the client must not
        # download it (and must not inline it past the 100 MB cap). The op's
        # preprocess reads materialized_path — pointing it at the URI makes
        # the reference flow through pack_inputs' existing passthrough. No
        # local file is written, so the artifact stays out of
        # materialized_ids (the filesystem-passthrough match map).
        if endpoint_routed and _is_remote(artifact.external_path):
            artifact.materialized_path = artifact.external_path
            continue
        materialized = artifact.materialize_to(directory, format=fmt, fs=fs)
        if isinstance(materialized, str):
            resolved_paths[artifact.artifact_id] = materialized
            materialized_ids.add(artifact.artifact_id)

    for config in configs:
        config.materialize_to(directory, resolved_paths=resolved_paths)
        if config.artifact_id is not None:
            materialized_ids.add(config.artifact_id)

    return artifacts, materialized_ids
