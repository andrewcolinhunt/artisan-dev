"""Artifact materialization for creator execution."""

from __future__ import annotations

from typing import TYPE_CHECKING

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.large_file import LargeFileArtifact
from artisan.schemas.specs.input_spec import InputSpec

if TYPE_CHECKING:
    from artisan.storage.core.artifact_store import ArtifactStore


def _is_remote(path: str | None) -> bool:
    """True when ``path`` is a cloud URI (``scheme://`` other than file)."""
    return path is not None and "://" in path and not path.startswith("file://")


def materialize_inputs(
    artifacts: dict[str, list[Artifact]],
    input_specs: dict[str, InputSpec],
    directory: str,
    artifact_store: ArtifactStore,
    *,
    endpoint_routed: bool = False,
) -> dict[str, list[Artifact]]:
    """Materialize input artifacts to disk with dependency-aware ordering.

    Non-config artifacts are materialized first so that config artifacts
    can resolve file-path references to already-written files.

    Args:
        artifacts: Role-keyed hydrated input artifacts.
        input_specs: Role-keyed input specs controlling materialization.
        directory: Target directory for materialized files.
        artifact_store: Store for hydrating config-referenced artifacts.
        endpoint_routed: When True, a remote ``FileRefArtifact`` or
            ``LargeFileArtifact`` is shipped to the worker by reference.
            Other artifact families still materialize locally. Local execution
            keeps the default (False), materializing every input as before.

    Returns:
        The same artifacts dict, with materialized paths set on its artifacts.

    Raises:
        ValueError: If a materialized endpoint config contains artifact references,
            or a locally referenced artifact cannot be loaded.
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
                if endpoint_routed and artifact.get_artifact_references():
                    msg = (
                        "Endpoint materialization does not support config artifact "
                        "references. Build worker-local configuration in "
                        "execute_command from explicit input roles."
                    )
                    raise ValueError(msg)
                configs.append(artifact)
            else:
                non_configs.append((artifact, spec.materialize_as))

    for config in configs:
        for ref_id in config.get_artifact_references():
            if ref_id in seen_ids:
                continue
            seen_ids.add(ref_id)
            ref_artifact = artifact_store.get_artifact(ref_id, hydrate=True)
            if ref_artifact is None:
                msg = f"Referenced artifact {ref_id!r} could not be loaded"
                raise ValueError(msg)
            non_configs.append((ref_artifact, None))

    # Get fs from artifact_store for cloud-capable source reads
    fs = artifact_store._fs

    resolved_paths: dict[str, str] = {}
    for artifact, fmt in non_configs:
        if artifact.artifact_id is None:
            continue
        # Only complete-file artifact families can cross by URI. Appendable
        # records and embedded artifacts must materialize their selected bytes
        # locally. The client and worker separately enforce policy and carry
        # the already-verified complete-file integrity contract.
        locator = (
            getattr(artifact, next(iter(artifact.LOCATOR_FIELDS)))
            if artifact.EXTERNALLY_BACKED
            else None
        )
        if (
            endpoint_routed
            and isinstance(artifact, (FileRefArtifact, LargeFileArtifact))
            and _is_remote(locator)
        ):
            artifact.verify_external_content(fs=fs)
            artifact.materialized_path = locator
            continue
        materialized = artifact.materialize_to(directory, format=fmt, fs=fs)
        if isinstance(materialized, str):
            resolved_paths[artifact.artifact_id] = materialized

    for config in configs:
        config.materialize_to(directory, resolved_paths=resolved_paths)
    return artifacts
