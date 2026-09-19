"""Artifact instantiation for execution inputs."""

from __future__ import annotations

from typing import TYPE_CHECKING

from artisan.execution.inputs._validation import is_hex_id
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.execution.replay import ReplayAssociation
from artisan.schemas.specs.input_spec import InputSpec

if TYPE_CHECKING:
    from artisan.storage.core.artifact_store import ArtifactStore


def instantiate_inputs(
    inputs: dict[str, list[str]],
    artifact_store: ArtifactStore,
    input_specs: dict[str, InputSpec],
    default_hydrate: bool = True,
    *,
    recorded_associated: list[ReplayAssociation] | None = None,
) -> tuple[dict[str, list[Artifact]], dict[tuple[str, str], list[Artifact]]]:
    """Instantiate all input artifacts from storage using bulk queries.

    Avoids N+1 delta scans by: (1) resolving all artifact types in one
    scan, (2) batch-loading per type for hydrated roles, and (3) resolving
    ``with_associated`` specs via provenance descendant lookup.

    Args:
        inputs: Role-keyed artifact ID lists from the execution unit.
        artifact_store: Store for loading and hydrating artifacts.
        input_specs: Role-keyed input specs controlling hydration behavior.
        default_hydrate: Fallback hydration flag when a role has no spec.

    Returns:
        Tuple of (artifacts_by_role, associated_map) where associated_map
        keys are ``(primary_artifact_id, associated_type)`` tuples.
    """
    # Bulk-resolve artifact types (1 delta scan instead of N)
    all_ids = [aid for ids in inputs.values() for aid in ids]
    for role, artifact_ids in inputs.items():
        for artifact_id in artifact_ids:
            if not is_hex_id(artifact_id):
                msg = (
                    f"Invalid artifact ID for role '{role}': {artifact_id!r}. "
                    "Expected a 32-character hexadecimal string."
                )
                raise ValueError(msg)

    type_map = artifact_store.provenance.load_type_map(all_ids) if all_ids else {}
    missing_ids = sorted(set(all_ids) - set(type_map))
    if missing_ids:
        msg = f"Input artifact IDs not found in the artifact store: {missing_ids}"
        raise ValueError(msg)

    # Pre-load hydrated artifacts in bulk by type
    hydrated_ids_by_type: dict[str, list[str]] = {}
    for role, artifact_ids in inputs.items():
        spec = input_specs.get(role)
        hydrate = spec.hydrate if spec is not None else default_hydrate
        if hydrate:
            for aid in artifact_ids:
                atype = type_map.get(aid)
                if atype is not None:
                    hydrated_ids_by_type.setdefault(atype, []).append(aid)

    hydrated_cache: dict[str, Artifact] = {}
    for atype, ids in hydrated_ids_by_type.items():
        hydrated_cache.update(artifact_store.get_artifacts_by_type(ids, atype))

    # Pre-build non-hydrated artifacts in bulk (no store calls needed)
    non_hydrated_cache: dict[str, Artifact] = {}
    non_hydrated_ids_by_type: dict[str, list[str]] = {}
    for role, artifact_ids in inputs.items():
        spec = input_specs.get(role)
        hydrate = spec.hydrate if spec is not None else default_hydrate
        if not hydrate:
            for aid in artifact_ids:
                atype = type_map.get(aid)
                if atype is not None:
                    non_hydrated_ids_by_type.setdefault(atype, []).append(aid)

    for atype, ids in non_hydrated_ids_by_type.items():
        model_cls = ArtifactTypeDef.get_model(atype)
        for aid in ids:
            non_hydrated_cache[aid] = model_cls(artifact_id=aid, artifact_type=atype)

    # Iterate — all lookups are dict hits
    result: dict[str, list[Artifact]] = {}
    for role, artifact_ids in inputs.items():
        spec = input_specs.get(role)
        hydrate = spec.hydrate if spec is not None else default_hydrate

        artifacts: list[Artifact] = []
        for artifact_id in artifact_ids:
            if hydrate and artifact_id in hydrated_cache:
                artifacts.append(hydrated_cache[artifact_id])
            elif not hydrate and artifact_id in non_hydrated_cache:
                artifacts.append(non_hydrated_cache[artifact_id])
            else:
                atype = type_map[artifact_id]
                artifact = artifact_store.get_artifact(
                    artifact_id, artifact_type=atype, hydrate=hydrate
                )
                if artifact is None:
                    msg = f"Artifact {artifact_id!r} for role '{role}' could not be loaded"
                    raise ValueError(msg)
                artifacts.append(artifact)

        result[role] = artifacts

    if recorded_associated is not None:
        associated = {}
        for capture in recorded_associated:
            artifacts = []
            for artifact_id in capture.artifact_ids:
                artifact = artifact_store.get_artifact(
                    artifact_id, artifact_type=capture.artifact_type
                )
                if artifact is None:
                    msg = f"Recorded associated artifact {artifact_id} is missing"
                    raise ValueError(msg)
                artifacts.append(artifact)
            associated[(capture.primary_id, capture.artifact_type)] = artifacts
        return result, associated

    # Resolve associated artifacts via provenance
    associated = {}
    for role, artifacts in result.items():
        spec = input_specs.get(role)
        if spec is None or not spec.with_associated:
            continue
        primary_ids = {a.artifact_id for a in artifacts if a.artifact_id}
        if not primary_ids:
            continue
        for assoc_type in spec.with_associated:
            assoc_map = artifact_store.get_associated(primary_ids, assoc_type)
            for primary_id in primary_ids:
                associated[(primary_id, assoc_type)] = assoc_map.get(primary_id, [])

    return result, associated
