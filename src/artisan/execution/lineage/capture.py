"""Capture lineage metadata by matching output filenames to input stems.

Stem matching strips file extensions and uses longest-prefix lookup to
pair each output artifact with its source input artifact.
"""

from __future__ import annotations

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.provenance.lineage_mapping import LineageMapping
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.utils.filename import strip_extensions


def capture_lineage_metadata(
    output_artifacts: dict[str, list[Artifact]],
    input_artifacts: dict[str, list[Artifact]],
    output_specs: dict[str, OutputSpec],
    group_by: GroupByStrategy | None = None,
    group_ids: list[str] | None = None,
    filesystem_match_map: dict[str, str] | None = None,
) -> dict[str, list[LineageMapping]]:
    """Capture lineage metadata from infer_lineage_from configuration.

    When ``group_by`` is set and an output stem-matches a primary input at
    index *i*, co-input edges are created from ALL other input roles at
    index *i* to that output.  All co-input edges for the same output share
    the ``group_id`` from ``group_ids[i]``.

    Args:
        output_artifacts: Role-keyed dict of output artifacts.
        input_artifacts: Role-keyed dict of input artifacts.
        output_specs: Role-keyed dict of OutputSpec declarations.
        group_by: When set, signals multi-input pairing is active.
            Co-input edges will be created for non-primary input roles.
        group_ids: Per-index group ID list from framework pairing.
            Must be provided when group_by is set. Length must match
            the primary input role length.
        filesystem_match_map: Optional dict mapping output filename stems
            to input artifact_ids. When an output stem has an entry,
            the mapped input_id is used directly instead of stem matching.

    Returns:
        Dict mapping output role to list of LineageMapping entries.
        Each LineageMapping carries an optional group_id for co-input edges.
    """
    result: dict[str, list[LineageMapping]] = {}

    for role, artifacts in output_artifacts.items():
        spec = output_specs.get(role)
        if spec is None:
            continue

        lineage_config = spec.infer_lineage_from
        if lineage_config is not None and lineage_config.get("inputs") == []:
            result[role] = []
            continue

        has_inputs = (
            lineage_config is not None
            and "inputs" in lineage_config
            and lineage_config["inputs"]
        )
        has_outputs = (
            lineage_config is not None
            and "outputs" in lineage_config
            and lineage_config["outputs"]
        )

        if lineage_config is None:
            # Curator ops with None config — skip lineage capture
            continue

        if has_outputs:
            candidates = _build_candidates_from_outputs(
                output_artifacts,
                lineage_config["outputs"],
            )
            # Output->output edges are unrelated to grouping
            role_mappings = _match_outputs_to_candidates(artifacts, candidates)
            result[role] = role_mappings
            continue

        if not has_inputs:
            continue

        primary_role = lineage_config["inputs"][0]
        candidates = _build_candidates_from_inputs(
            input_artifacts,
            lineage_config["inputs"],
        )
        stem_index = _build_stem_index(candidates)

        # Build index lookup for primary role: artifact_id -> list index
        primary_id_to_idx: dict[str, int] = {}
        if group_by is not None:
            for idx, art in enumerate(input_artifacts.get(primary_role, [])):
                if art.artifact_id is not None:
                    primary_id_to_idx[art.artifact_id] = idx

        # Build artifact_id -> role lookup for filesystem match map resolution
        id_to_role: dict[str, str] = {}
        if filesystem_match_map:
            for cand_name, cand_id, cand_role in candidates:
                id_to_role[cand_id] = cand_role

        role_mappings: list[LineageMapping] = []
        for artifact in artifacts:
            original_name = getattr(artifact, "original_name", None)
            if original_name is None:
                continue

            # Try filesystem match map first, fall back to stem matching
            matched: tuple[str, str] | None = None
            if filesystem_match_map and original_name in filesystem_match_map:
                fs_input_id = filesystem_match_map[original_name]
                fs_role = id_to_role.get(fs_input_id)
                if fs_role is not None:
                    matched = (fs_input_id, fs_role)
            if matched is None:
                matched = _match_by_stem_indexed(original_name, stem_index)
            if not matched:
                continue

            matched_id, matched_src_role = matched

            # Determine group_id for this match
            matched_idx = primary_id_to_idx.get(matched_id)
            edge_group_id: str | None = None
            if (
                group_by is not None
                and matched_idx is not None
                and group_ids is not None
                and matched_idx < len(group_ids)
            ):
                edge_group_id = group_ids[matched_idx]

            # Primary edge (stem-matched role)
            role_mappings.append(
                LineageMapping(
                    draft_original_name=original_name,
                    source_artifact_id=matched_id,
                    source_role=matched_src_role,
                    group_id=edge_group_id,
                )
            )

            # Co-input edges from all other roles at the same index
            if group_by is not None and matched_idx is not None:
                for co_role, co_artifacts in input_artifacts.items():
                    if co_role == primary_role:
                        continue
                    if matched_idx < len(co_artifacts):
                        co_artifact = co_artifacts[matched_idx]
                        if co_artifact.artifact_id is not None:
                            role_mappings.append(
                                LineageMapping(
                                    draft_original_name=original_name,
                                    source_artifact_id=co_artifact.artifact_id,
                                    source_role=co_role,
                                    group_id=edge_group_id,
                                )
                            )

        result[role] = role_mappings

    return result


def _match_outputs_to_candidates(
    artifacts: list[Artifact],
    candidates: list[tuple[str, str, str]],
) -> list[LineageMapping]:
    """Match output artifacts to candidates (output->output or ungrouped)."""
    stem_index = _build_stem_index(candidates)
    mappings: list[LineageMapping] = []
    for artifact in artifacts:
        original_name = getattr(artifact, "original_name", None)
        if original_name is None:
            continue
        matched = _match_by_stem_indexed(original_name, stem_index)
        if matched:
            matched_id, matched_role = matched
            mappings.append(
                LineageMapping(
                    draft_original_name=original_name,
                    source_artifact_id=matched_id,
                    source_role=matched_role,
                )
            )
    return mappings


def _build_candidates_from_inputs(
    input_artifacts: dict[str, list[Artifact]],
    roles: list[str],
) -> list[tuple[str, str, str]]:
    """Collect (original_name, artifact_id, role) tuples from input roles."""
    candidates: list[tuple[str, str, str]] = []
    for role in roles:
        for artifact in input_artifacts.get(role, []):
            original_name = getattr(artifact, "original_name", None)
            if original_name is not None and artifact.artifact_id is not None:
                candidates.append((original_name, artifact.artifact_id, role))
    return candidates


def _build_candidates_from_outputs(
    output_artifacts: dict[str, list[Artifact]],
    roles: list[str],
) -> list[tuple[str, str, str]]:
    """Collect (original_name, artifact_id, role) tuples from output roles."""
    candidates: list[tuple[str, str, str]] = []
    for role in roles:
        for artifact in output_artifacts.get(role, []):
            original_name = getattr(artifact, "original_name", None)
            if original_name is None:
                continue
            artifact_id = artifact.artifact_id or f"__draft__{original_name}"
            candidates.append((original_name, artifact_id, role))
    return candidates


def _build_stem_index(
    candidates: list[tuple[str, str, str]],
) -> dict[str, list[tuple[str, str]]]:
    """Pre-compute stripped stems and build a lookup index.

    Args:
        candidates: List of (original_name, artifact_id, role) tuples.

    Returns:
        Dict mapping stripped stem to list of (artifact_id, role) pairs.
    """
    index: dict[str, list[tuple[str, str]]] = {}
    for candidate_name, candidate_id, role in candidates:
        stem = strip_extensions(candidate_name)
        index.setdefault(stem, []).append((candidate_id, role))
    return index


def _match_by_stem_indexed(
    output_name: str,
    stem_index: dict[str, list[tuple[str, str]]],
) -> tuple[str, str] | None:
    """Match an output name against a pre-built stem index.

    Checks exact match first (O(1)), then probes valid prefixes
    longest-first where the next character is not a digit
    (digit-boundary protection). Returns the first prefix level that has
    exactly one candidate, so longer stems always win over shorter ones.

    Args:
        output_name: The output filename to match.
        stem_index: Pre-built index from ``_build_stem_index``.

    Returns:
        (artifact_id, role) if exactly one match at any level, else None.
    """
    if not stem_index:
        return None

    stripped = strip_extensions(output_name)

    # Exact match (most common case)
    if stripped in stem_index:
        entries = stem_index[stripped]
        if len(entries) == 1:
            return entries[0]

    # Prefix matches: longest-first, return first unique match.
    for i in range(len(stripped) - 1, 0, -1):
        if not stripped[i].isdigit():
            prefix = stripped[:i]
            if prefix in stem_index:
                entries = stem_index[prefix]
                if len(entries) == 1:
                    return entries[0]

    return None
