"""Artifact materialization for creator execution."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import TYPE_CHECKING

from fsspec import AbstractFileSystem

from artisan.execution.inputs._validation import is_hex_id
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.specs.input_spec import InputSpec

if TYPE_CHECKING:
    from artisan.storage.core.artifact_store import ArtifactStore


def materialize_inputs(
    artifacts: dict[str, list[Artifact]],
    input_specs: dict[str, InputSpec],
    directory: str,
    artifact_store: ArtifactStore,
) -> dict[str, list[Artifact]]:
    """Capture store dependencies, then materialize the supplied input artifacts."""
    selected_specs = {
        role: spec for role, spec in input_specs.items() if spec.materialize
    }
    references = capture_input_references(
        {
            role: values
            for role, values in artifacts.items()
            if input_specs.get(role, InputSpec()).materialize
        },
        selected_specs,
        artifact_store,
    )
    return materialize_captured_inputs(
        artifacts,
        input_specs,
        directory,
        references,
        fs=artifact_store._fs,
    )


def materialize_captured_inputs(
    artifacts: dict[str, list[Artifact]],
    input_specs: dict[str, InputSpec],
    directory: str,
    references: Mapping[str, Mapping[str, Artifact]],
    *,
    fs: AbstractFileSystem | None = None,
) -> dict[str, list[Artifact]]:
    """Materialize finalized inputs using only explicitly captured dependencies.

    Reference membership is checked before writing any files. Missing captures,
    including absent captures for empty configs, fail without a store fallback.
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
        for ref_id, ref_artifact in _captured_references(config, references).items():
            if ref_id in seen_ids:
                continue
            seen_ids.add(ref_id)
            non_configs.append((ref_artifact, None))

    resolved_paths: dict[str, str] = {}
    materialized_paths: dict[str, str] = {}
    for artifact, fmt in non_configs:
        if artifact.artifact_id is None:
            continue
        materialized = _materialize_artifact(artifact, directory, fs, fmt)
        if isinstance(materialized, str):
            resolved_paths[artifact.artifact_id] = materialized
            materialized_paths[artifact.artifact_id] = materialized

    for config in configs:
        materialized = config.materialize_to(directory, resolved_paths=resolved_paths)
        assert config.artifact_id is not None
        materialized_paths[config.artifact_id] = materialized
    for role, values in artifacts.items():
        if input_specs.get(role, InputSpec()).materialize:
            for artifact in values:
                if artifact.artifact_id in materialized_paths:
                    artifact.materialized_path = materialized_paths[
                        artifact.artifact_id
                    ]
    return artifacts


def resolve_config_references(
    config: ExecutionConfigArtifact,
    artifact_store: ArtifactStore,
    loaded: dict[str, Artifact],
    *,
    typed: bool = False,
) -> dict[str, Artifact]:
    """Load direct references once through normal store integrity verification.

    Typed delivery validates reference IDs and excludes config-to-config graphs.
    The caller owns the per-preparation cache; nothing is persisted here.
    """
    references = config.get_artifact_references()
    if typed and any(not is_hex_id(ref_id) for ref_id in references):
        msg = "Config references must be 32-character hexadecimal artifact IDs"
        raise ValueError(msg)
    result: dict[str, Artifact] = {}
    for ref_id in references:
        if ref_id not in loaded:
            artifact = artifact_store.get_artifact(ref_id, hydrate=True)
            if artifact is None:
                msg = f"Referenced artifact {ref_id!r} could not be loaded"
                raise ValueError(msg)
            loaded[ref_id] = artifact
        artifact = loaded[ref_id]
        if typed and isinstance(artifact, ExecutionConfigArtifact):
            msg = "Typed config references cannot target another config"
            raise ValueError(msg)
        result[ref_id] = artifact
    return result


def capture_input_references(
    artifacts: dict[str, list[Artifact]],
    input_specs: dict[str, InputSpec],
    artifact_store: ArtifactStore,
) -> dict[str, dict[str, Artifact]]:
    """Hydrate config dependencies before entering a store-free invocation."""
    loaded = {
        artifact.artifact_id: artifact
        for values in artifacts.values()
        for artifact in values
        if artifact.artifact_id is not None and artifact.is_hydrated
    }
    result: dict[str, dict[str, Artifact]] = {}
    for role, values in artifacts.items():
        spec = input_specs.get(role, InputSpec())
        for config in values:
            if not spec.resolve_references and not (
                spec.materialize and isinstance(config, ExecutionConfigArtifact)
            ):
                continue
            if (
                not isinstance(config, ExecutionConfigArtifact)
                or not config.is_hydrated
                or config.artifact_id is None
            ):
                if not spec.resolve_references:
                    continue
                msg = "Reference resolution requires a hydrated, finalized config"
                raise ValueError(msg)
            result[config.artifact_id] = resolve_config_references(
                config,
                artifact_store,
                loaded,
                typed=spec.resolve_references,
            )
    return result


def resolve_input_references(
    artifacts: dict[str, list[Artifact]],
    input_specs: dict[str, InputSpec],
    directory: str,
    artifact_store: ArtifactStore,
) -> dict[str, dict[str, Artifact]]:
    """Capture and materialize the legacy typed configuration dependencies."""
    selected = {
        role: spec for role, spec in input_specs.items() if spec.resolve_references
    }
    references = capture_input_references(
        {role: values for role, values in artifacts.items() if role in selected},
        selected,
        artifact_store,
    )
    return materialize_captured_references(
        artifacts,
        input_specs,
        directory,
        references,
        fs=artifact_store._fs,
    )


def _captured_references(
    config: ExecutionConfigArtifact,
    references: Mapping[str, Mapping[str, Artifact]],
) -> dict[str, Artifact]:
    """Require exact config membership, preserving captured-empty context."""
    if config.artifact_id is None or config.artifact_id not in references:
        msg = "Config references were not captured for this input"
        raise ValueError(msg)
    captured = references[config.artifact_id]
    if set(captured) != set(config.get_artifact_references()):
        msg = "Captured config references do not match its declared dependencies"
        raise ValueError(msg)
    for artifact_id, artifact in captured.items():
        if artifact.artifact_id != artifact_id or not artifact.is_hydrated:
            msg = "Captured config dependency is not hydrated with the expected ID"
            raise ValueError(msg)
    return dict(captured)


def materialize_captured_references(
    artifacts: dict[str, list[Artifact]],
    input_specs: dict[str, InputSpec],
    directory: str,
    references: Mapping[str, Mapping[str, Artifact]],
    *,
    fs: AbstractFileSystem | None = None,
) -> dict[str, dict[str, Artifact]]:
    """Prepare explicitly captured typed dependencies without store access."""
    result: dict[str, dict[str, Artifact]] = {}
    for role, values in artifacts.items():
        if not input_specs.get(role, InputSpec()).resolve_references:
            continue
        for config in values:
            if not isinstance(config, ExecutionConfigArtifact):
                msg = "Reference resolution requires a hydrated, finalized config"
                raise ValueError(msg)
            captured = _captured_references(config, references)
            if any(not is_hex_id(key) for key in captured):
                msg = "Config references must be 32-character hexadecimal artifact IDs"
                raise ValueError(msg)
            if any(
                isinstance(value, ExecutionConfigArtifact)
                for value in captured.values()
            ):
                msg = "Typed config references cannot target another config"
                raise ValueError(msg)
            assert config.artifact_id is not None
            result[config.artifact_id] = captured
    loaded = {key: value for values in result.values() for key, value in values.items()}
    for artifact_id, artifact in loaded.items():
        target = os.path.join(directory, "references", artifact_id)
        os.makedirs(target, exist_ok=True)
        _materialize_artifact(artifact, target, fs)
    for captured in result.values():
        for artifact_id, artifact in captured.items():
            artifact.materialized_path = loaded[artifact_id].materialized_path
    return result


def _materialize_artifact(
    artifact: Artifact,
    directory: str,
    fs: AbstractFileSystem | None,
    fmt: str | None = None,
) -> str | None:
    """Materialize one verified input using the explicitly supplied filesystem."""
    return artifact.materialize_to(directory, format=fmt, fs=fs)
