"""Provenance-based artifact lineage matching.

Per-candidate BFS through directed provenance edges. For each candidate,
walks backward from the candidate node until hitting a target; the first
target reached at the shortest hop depth is the match. Multiple targets
at the same hop depth on different branches raise.

Re-exports ``walk_backward`` and ``walk_forward`` under their legacy
names for backwards compatibility during migration.
"""

from __future__ import annotations

import logging

import polars as pl

from artisan.provenance.traversal import walk_backward as walk_provenance_to_targets
from artisan.provenance.traversal import walk_forward as walk_forward_to_targets

logger = logging.getLogger(__name__)

# Re-export so existing callers don't break
__all__ = [
    "match_by_ancestry",
    "walk_forward_to_targets",
    "walk_provenance_to_targets",
]


def _walk_to_target(
    candidate_id: str,
    target_ids: set[str],
    edges: pl.DataFrame,
) -> tuple[str, int] | None:
    """Walk backward from a candidate until a target is reached.

    A candidate that is itself a target self-matches at depth 0.

    Args:
        candidate_id: Artifact ID to walk back from.
        target_ids: Set of artifact IDs to test against at each hop.
        edges: DataFrame with ``source_artifact_id`` (parent) and
            ``target_artifact_id`` (child) columns.

    Returns:
        ``(target_id, depth)`` for the closest target reached, or ``None``
        if no directed path to any target exists.

    Raises:
        RuntimeError: If the candidate has multiple distinct targets at
            the same hop depth on different branches.
    """
    frontier: set[str] = {candidate_id}
    visited: set[str] = {candidate_id}
    depth = 0

    while frontier:
        hits = frontier & target_ids
        if len(hits) > 1:
            msg = (
                f"LINEAGE matching: candidate {candidate_id} has multiple "
                f"targets at hop depth {depth}: {sorted(hits)}. The pipeline "
                "graph is ambiguous at this candidate; either restructure "
                "inputs or use a more specific GroupByStrategy."
            )
            raise RuntimeError(msg)
        if hits:
            return next(iter(hits)), depth

        if edges.is_empty():
            return None

        parents_series = (
            edges.filter(pl.col("target_artifact_id").is_in(list(frontier)))
            .get_column("source_artifact_id")
            .unique()
        )
        parents = set(parents_series.to_list()) - visited
        if not parents:
            return None

        visited |= parents
        frontier = parents
        depth += 1

    return None


def match_by_ancestry(
    target_ids: set[str],
    candidate_ids_by_role: dict[str, list[str]],
    edges: pl.DataFrame,
) -> dict[str, dict[str, list[str]]]:
    """Match candidates to targets via directed ancestor walk.

    For each candidate, walks backward through directed provenance edges
    until a target is encountered. The closest target by hop count wins.
    Multiple targets at the same hop depth on different branches raise.
    Candidates with no directed path to any target are dropped with a
    WARNING log.

    Supports 1:N matching — multiple candidates in the same role can map
    to the same target (e.g., one anchor with N items each having the
    anchor in their directed ancestry).

    Args:
        target_ids: Artifact IDs of the target role (lower step number,
            or the anchor/primary role).
        candidate_ids_by_role: ``{role_name: [artifact_ids]}`` for other roles.
        edges: DataFrame with ``source_artifact_id`` (parent),
            ``target_artifact_id`` (child) columns.

    Returns:
        ``{target_id: {role: [candidate_ids]}}`` for targets with matches
        in every role. Each role maps to a list of matched candidates.

    Raises:
        RuntimeError: If any candidate has multiple targets at the same
            hop depth on different branches.
    """
    if not target_ids or not candidate_ids_by_role:
        return {}

    matches: dict[str, dict[str, list[str]]] = {}

    for role, candidate_ids in candidate_ids_by_role.items():
        for candidate_id in candidate_ids:
            result = _walk_to_target(candidate_id, target_ids, edges)
            if result is None:
                logger.warning(
                    "LINEAGE matching: candidate %s... from role '%s' has "
                    "no directed path to any target",
                    candidate_id[:8],
                    role,
                )
                continue
            target_id, _depth = result
            matches.setdefault(target_id, {}).setdefault(role, []).append(
                candidate_id
            )

    n_roles = len(candidate_ids_by_role)
    return {t: rm for t, rm in matches.items() if len(rm) == n_roles}
