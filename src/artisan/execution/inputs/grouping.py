"""Pair multi-role input artifact IDs for batched execution.

Two pairing patterns:

1. ``group_inputs`` -- all roles paired equally (creator multi-input ops).
2. ``match_inputs_to_primary`` -- one anchor role paired independently
   against N other roles (e.g., Filter matching passthrough vs. metrics).

Strategies: ZIP (positional), LINEAGE (provenance ancestry), CROSS_PRODUCT
(cartesian), NAME (shared ``original_name`` stem).
"""

from __future__ import annotations

import json
import logging
from itertools import product
from typing import TYPE_CHECKING

import polars as pl
import xxhash

from artisan.execution.inputs.lineage_matching import match_by_ancestry
from artisan.schemas.enums import GroupByStrategy
from artisan.utils.filename import strip_extensions

if TYPE_CHECKING:
    from artisan.storage.core.artifact_store import ArtifactStore

logger = logging.getLogger(__name__)


def compute_group_id(artifact_ids: list[str]) -> str:
    """Compute a deterministic group_id from sorted source artifact IDs.

    The group_id is a content-addressed hash that identifies a particular
    pairing of source artifacts, independent of role names or target outputs.

    Args:
        artifact_ids: All source artifact IDs in the pairing (one per role).

    Returns:
        32-character xxh3_128 hex digest.
    """
    content = json.dumps(sorted(artifact_ids), sort_keys=True).encode("utf-8")
    return xxhash.xxh3_128(content).hexdigest()


def group_inputs(
    inputs: dict[str, list[str]],
    strategy: GroupByStrategy,
    artifact_store: ArtifactStore | None = None,
) -> tuple[dict[str, list[str]], list[str]]:
    """Pair multi-role input artifact IDs using the given strategy.

    All roles are paired together -- every role participates equally.
    Use for creator operations where inputs are jointly necessary
    (e.g., multi_input_tool: input_a + input_b).

    Args:
        inputs: Role-name to artifact-ID-list mapping (unaligned, possibly
            different lengths).
        strategy: ZIP, LINEAGE, CROSS_PRODUCT, or NAME.
        artifact_store: Required for LINEAGE and NAME strategies (provenance
            and ``original_name`` access respectively).

    Returns:
        Tuple of:
        - Aligned inputs: same roles, same length, positionally paired.
        - group_ids: list of group_id strings, one per paired index.
          group_id = deterministic hash of sorted source artifact IDs in the pair.

    Raises:
        ValueError: If inputs are incompatible with the chosen strategy.
        RuntimeError: If LINEAGE or NAME strategy is used without artifact_store.
    """
    if strategy == GroupByStrategy.ZIP:
        matched_sets = _match_zip(inputs)
    elif strategy == GroupByStrategy.CROSS_PRODUCT:
        matched_sets = _match_cross_product(inputs)
    elif strategy == GroupByStrategy.LINEAGE:
        matched_sets = _match_by_ancestry(inputs, artifact_store)
    elif strategy == GroupByStrategy.NAME:
        matched_sets = _match_by_name(inputs, artifact_store)
    else:
        # Defensive branch for non-enum values; mypy sees enum as exhausted.
        msg = f"Unknown group_by strategy: {strategy}"  # type: ignore[unreachable]
        raise ValueError(msg)

    return _matched_sets_to_aligned(inputs, matched_sets)


def match_inputs_to_primary(
    primary_role: str,
    inputs: dict[str, list[str]],
    strategy: GroupByStrategy,
    artifact_store: ArtifactStore | None = None,
) -> dict[str, list[str]]:
    """Pair each non-primary role independently against a primary role.

    For each primary artifact, finds the best match in EVERY other role.
    Only complete matches (matched in all roles) are returned. This is
    the pairing pattern used by FilterOp: primary_input paired independently
    with each metric role.

    Args:
        primary_role: The anchor role name (e.g., "passthrough").
        inputs: Role-name to artifact-ID-list mapping. Must contain
            primary_role and at least one other role.
        strategy: LINEAGE (provenance ancestry) or NAME (shared
            ``original_name`` stem).
        artifact_store: Required for both LINEAGE and NAME strategies.

    Returns:
        Aligned inputs: all roles have same length, positionally aligned
        to the primary role. Only includes primary artifacts that matched
        in every other role. Unmatched primary artifacts are excluded.

    Raises:
        ValueError: If primary_role is not in inputs, no other roles exist,
            or strategy is not LINEAGE or NAME.
        RuntimeError: If LINEAGE or NAME is used without artifact_store.
    """
    if primary_role not in inputs:
        msg = f"Primary role '{primary_role}' not found in inputs. Got: {sorted(inputs.keys())}"
        raise ValueError(msg)

    other_roles = [r for r in inputs if r != primary_role]
    if not other_roles:
        msg = "match_inputs_to_primary requires at least one non-primary role"
        raise ValueError(msg)

    if strategy == GroupByStrategy.LINEAGE:
        return _match_primary_by_ancestry(
            primary_role, inputs, other_roles, artifact_store
        )

    if strategy == GroupByStrategy.NAME:
        return _match_primary_by_name(primary_role, inputs, other_roles, artifact_store)

    msg = (
        "match_inputs_to_primary supports LINEAGE and NAME strategies only, "
        f"got: {strategy}"
    )
    raise ValueError(msg)


def validate_stem_match_uniqueness(
    role_name: str,
    artifact_names: list[str],
) -> None:
    """Validate that artifacts in the stem-matched role have unique names.

    The stem-matching algorithm in ``capture_lineage_metadata()`` requires
    unique ``original_name`` values within the stem-matched role. Duplicate
    names cause ambiguous matches and silently dropped lineage edges.

    Args:
        role_name: Name of the role being validated (for error messages).
        artifact_names: List of ``original_name`` values from the role's artifacts.

    Raises:
        ValueError: If any names are duplicated.
    """
    seen: dict[str, int] = {}
    duplicates: list[str] = []
    for name in artifact_names:
        count = seen.get(name, 0)
        if count == 1:
            duplicates.append(name)
        seen[name] = count + 1

    if duplicates:
        msg = (
            f"Stem-matched role '{role_name}' has duplicate original_name values: "
            f"{sorted(duplicates)}. Each artifact in the stem-matched role must "
            f"have a unique name for lineage inference."
        )
        raise ValueError(msg)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _match_zip(inputs: dict[str, list[str]]) -> list[dict[str, str]]:
    """Positional matching: 1st with 1st, 2nd with 2nd, etc.

    All input lists must have the same length.
    """
    roles = list(inputs.keys())
    if not roles:
        return []

    first_role = roles[0]
    expected_len = len(inputs[first_role])
    for role, artifact_ids in inputs.items():
        if len(artifact_ids) != expected_len:
            msg = (
                f"ZIP matching requires all inputs to have same length. "
                f"'{first_role}' has {expected_len}, "
                f"'{role}' has {len(artifact_ids)}"
            )
            raise ValueError(msg)

    matched_sets: list[dict[str, str]] = []
    for i in range(expected_len):
        matched_sets.append({role: inputs[role][i] for role in roles})

    return matched_sets


def _match_cross_product(inputs: dict[str, list[str]]) -> list[dict[str, str]]:
    """All combinations: N * M * ... groups."""
    roles = list(inputs.keys())
    if not roles:
        return []

    lists = [inputs[role] for role in roles]

    matched_sets: list[dict[str, str]] = []
    for combo in product(*lists):
        matched_sets.append(dict(zip(roles, combo, strict=True)))

    return matched_sets


def _match_by_ancestry(
    inputs: dict[str, list[str]],
    artifact_store: ArtifactStore | None,
) -> list[dict[str, str]]:
    """Match artifacts sharing common ancestry in provenance graph.

    Requires exactly 2 roles. Uses backward-walk ancestry matching via
    ``match_by_ancestry``. Step numbers determine which role is the
    target (index) role.
    """
    if artifact_store is None:
        msg = "LINEAGE matching requires artifact_store for provenance access."
        raise RuntimeError(msg)

    roles = list(inputs.keys())
    if len(roles) != 2:
        msg = f"LINEAGE matching requires exactly 2 input roles, got {len(roles)}: {roles}"
        raise ValueError(msg)

    role_a, role_b = roles

    if not inputs[role_a] or not inputs[role_b]:
        return []

    # Determine target role via step numbers
    sample_ids = {inputs[role_a][0], inputs[role_b][0]}
    step_map = artifact_store.provenance.load_step_map(sample_ids)
    step_a = step_map.get(inputs[role_a][0])
    step_b = step_map.get(inputs[role_b][0])

    if step_a is not None and step_b is not None:
        if step_a <= step_b:
            target_role, candidate_role = role_a, role_b
        else:
            target_role, candidate_role = role_b, role_a
    else:
        # Fallback: smaller set as targets for efficiency
        logger.warning(
            "LINEAGE matching: step numbers unavailable, using smaller role as targets"
        )
        if len(inputs[role_a]) <= len(inputs[role_b]):
            target_role, candidate_role = role_a, role_b
        else:
            target_role, candidate_role = role_b, role_a

    target_ids = set(inputs[target_role])
    candidate_ids_by_role = {candidate_role: inputs[candidate_role]}

    # Load edges scoped to the step range of the involved artifacts
    all_ids = pl.Series(list(inputs[role_a]) + list(inputs[role_b]))
    step_range = artifact_store.provenance.get_step_range(all_ids)
    if step_range is None:
        return []
    edges = artifact_store.provenance.load_edges_df(*step_range)

    result = match_by_ancestry(target_ids, candidate_ids_by_role, edges)

    matched_sets = []
    for target_id, role_matches in result.items():
        for candidate_id in role_matches[candidate_role]:
            matched_sets.append({target_role: target_id, candidate_role: candidate_id})
    return matched_sets


def _match_by_name(
    inputs: dict[str, list[str]],
    artifact_store: ArtifactStore | None,
) -> list[dict[str, str]]:
    """Match artifacts whose ``original_name`` stem appears in every role.

    Stems are computed via :func:`strip_extensions` (greedy ``strip_all=True``).
    Each role must have unique stems among artifacts with a non-null
    ``original_name``; duplicates raise ``ValueError``. For every stem present
    in all roles, emits a matched set.

    Artifacts without an ``original_name`` are silently skipped.
    """
    if artifact_store is None:
        msg = "NAME matching requires artifact_store for original_name access."
        raise RuntimeError(msg)

    if any(not ids for ids in inputs.values()):
        return []

    stems_by_role = _build_stems_by_role(inputs, artifact_store)
    role_stem_sets = [set(stems) for stems in stems_by_role.values()]
    common_stems = set.intersection(*role_stem_sets)

    missing = set.union(*role_stem_sets) - common_stems
    if missing:
        logger.warning(
            "NAME matching: %d stem(s) present in some roles but not all; "
            "skipped (sample: %s)",
            len(missing),
            sorted(missing)[:5],
        )

    return [
        {role: stems_by_role[role][stem] for role in inputs}
        for stem in sorted(common_stems)
    ]


def _match_primary_by_ancestry(
    primary_role: str,
    inputs: dict[str, list[str]],
    other_roles: list[str],
    artifact_store: ArtifactStore | None,
) -> dict[str, list[str]]:
    """Anchor-based LINEAGE matching: primary against each other role.

    Primary role IS the target role. No step number lookup needed.
    Uses ``match_by_ancestry`` with primary artifacts as targets.
    """
    if artifact_store is None:
        msg = "LINEAGE matching requires artifact_store for provenance access."
        raise RuntimeError(msg)

    target_ids = set(inputs[primary_role])
    candidate_ids_by_role = {role: inputs[role] for role in other_roles}

    # Load edges scoped to the step range of all involved artifacts
    all_id_list = list(inputs[primary_role])
    for role in other_roles:
        all_id_list.extend(inputs[role])
    step_range = artifact_store.provenance.get_step_range(pl.Series(all_id_list))
    if step_range is None:
        return {role: [] for role in inputs}
    edges = artifact_store.provenance.load_edges_df(*step_range)

    result = match_by_ancestry(target_ids, candidate_ids_by_role, edges)

    # Reshape to aligned dict, preserving primary artifact order.
    # Each primary may have multiple candidates per role; expand with
    # cross-product so every combination gets its own execution unit.
    aligned: dict[str, list[str]] = {role: [] for role in inputs}

    for primary_id in inputs[primary_role]:
        if primary_id in result:
            role_lists = [result[primary_id][role] for role in other_roles]
            for combo in product(*role_lists):
                aligned[primary_role].append(primary_id)
                for role, cid in zip(other_roles, combo, strict=True):
                    aligned[role].append(cid)

    return aligned


def _match_primary_by_name(
    primary_role: str,
    inputs: dict[str, list[str]],
    other_roles: list[str],
    artifact_store: ArtifactStore | None,
) -> dict[str, list[str]]:
    """Anchor-based NAME matching: primary stems against each other role.

    For each primary artifact with an ``original_name``, find the artifact
    in each other role that shares the same stem. With unique stems per role,
    each primary matches at most one artifact per other role. Primary
    artifacts unmatched in any role are dropped; output preserves the input
    order of the primary role.
    """
    if artifact_store is None:
        msg = "NAME matching requires artifact_store for original_name access."
        raise RuntimeError(msg)

    aligned: dict[str, list[str]] = {role: [] for role in inputs}

    if any(not ids for ids in inputs.values()):
        return aligned

    stems_by_role = _build_stems_by_role(inputs, artifact_store)

    # Forward lookup: primary_id -> stem. Reverse the role's stem dict.
    primary_id_to_stem = {
        aid: stem for stem, aid in stems_by_role[primary_role].items()
    }

    for primary_id in inputs[primary_role]:
        stem = primary_id_to_stem.get(primary_id)
        if stem is None:
            continue
        if not all(stem in stems_by_role[role] for role in other_roles):
            continue
        aligned[primary_role].append(primary_id)
        for role in other_roles:
            aligned[role].append(stems_by_role[role][stem])

    return aligned


def _build_stems_by_role(
    inputs: dict[str, list[str]],
    artifact_store: ArtifactStore,
) -> dict[str, dict[str, str]]:
    """Build per-role ``{stem: artifact_id}`` index for NAME matching.

    Loads ``original_name`` for all input IDs in one pass, strips
    extensions, and validates uniqueness within each role. Artifacts
    without an ``original_name`` are silently skipped.

    Raises:
        ValueError: If any role has duplicate stems among named artifacts.
    """
    all_ids = [aid for ids in inputs.values() for aid in ids]
    name_map = artifact_store.load_original_names(all_ids)

    skipped = len(all_ids) - len(name_map)
    if skipped:
        logger.debug(
            "NAME matching: %d artifact(s) skipped — no original_name", skipped
        )

    stems_by_role: dict[str, dict[str, str]] = {}
    for role, ids in inputs.items():
        role_stems = [
            (strip_extensions(name_map[aid]), aid) for aid in ids if aid in name_map
        ]
        validate_stem_match_uniqueness(role, [stem for stem, _ in role_stems])
        stems_by_role[role] = dict(role_stems)
    return stems_by_role


def _matched_sets_to_aligned(
    inputs: dict[str, list[str]],
    matched_sets: list[dict[str, str]],
) -> tuple[dict[str, list[str]], list[str]]:
    """Convert a list of matched dicts to positionally-aligned lists + group_ids.

    Args:
        inputs: Original inputs (used for role ordering).
        matched_sets: Each element is {role: artifact_id} for one pairing.

    Returns:
        Tuple of (aligned dict, group_ids list).
    """
    roles = list(inputs.keys())
    aligned: dict[str, list[str]] = {role: [] for role in roles}
    group_ids: list[str] = []

    for matched_set in matched_sets:
        ids_at_index: list[str] = []
        for role in roles:
            artifact_id = matched_set[role]
            aligned[role].append(artifact_id)
            ids_at_index.append(artifact_id)
        group_ids.append(compute_group_id(ids_at_index))

    return aligned, group_ids
