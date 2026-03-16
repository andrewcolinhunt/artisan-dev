"""Provenance-based artifact lineage matching.

DataFrame-based backward matching for multi-input grouping, using
common-ancestor resolution through provenance edges.

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


def _collect_ancestors(
    nodes: pl.DataFrame,
    edges: pl.DataFrame,
    id_col: str = "node_id",
) -> pl.DataFrame:
    """Walk backward from nodes collecting all visited ancestors.

    Returns DataFrame with [node_id, ancestor_id] — each node maps to
    itself and all transitive ancestors reachable via backward edges.

    Args:
        nodes: DataFrame with ``artifact_id`` column.
        edges: DataFrame with ``source_artifact_id``, ``target_artifact_id``.
        id_col: Name for the tracking column in the output.

    Returns:
        DataFrame with columns [<id_col>, ancestor_id].
    """
    empty = pl.DataFrame(schema={id_col: pl.String, "ancestor_id": pl.String})

    if nodes.is_empty() or edges.is_empty():
        # Still include self-mapping for non-empty nodes
        if not nodes.is_empty():
            return pl.DataFrame(
                {
                    id_col: nodes["artifact_id"],
                    "ancestor_id": nodes["artifact_id"],
                }
            )
        return empty

    # frontier: (id_col, current_node)
    frontier = pl.DataFrame(
        {
            id_col: nodes["artifact_id"],
            "current_node": nodes["artifact_id"],
        }
    )

    # Start: each node is its own ancestor
    all_ancestors = frontier.select(
        pl.col(id_col), pl.col("current_node").alias("ancestor_id")
    )
    visited = frontier.select(id_col, pl.col("current_node").alias("node"))

    while not frontier.is_empty():
        stepped = frontier.join(
            edges,
            left_on="current_node",
            right_on="target_artifact_id",
            how="inner",
        ).select(
            pl.col(id_col),
            pl.col("source_artifact_id").alias("current_node"),
        )

        if stepped.is_empty():
            break

        stepped = stepped.join(
            visited,
            left_on=[id_col, "current_node"],
            right_on=[id_col, "node"],
            how="anti",
        )

        if stepped.is_empty():
            break

        visited = pl.concat(
            [visited, stepped.select(id_col, pl.col("current_node").alias("node"))]
        )
        all_ancestors = pl.concat(
            [
                all_ancestors,
                stepped.select(id_col, pl.col("current_node").alias("ancestor_id")),
            ]
        )
        frontier = stepped

    return all_ancestors


def match_by_ancestry(
    target_ids: set[str],
    candidate_ids_by_role: dict[str, list[str]],
    edges: pl.DataFrame,
) -> dict[str, dict[str, list[str]]]:
    """Match candidates to targets via common-ancestor resolution.

    For each role, collects the full ancestry set of both targets and
    candidates, then joins on shared ancestors to find which target
    each candidate is related to. First-claim-wins for candidates
    reachable from multiple targets via different ancestors.

    Supports 1:N matching — multiple candidates in the same role can
    map to the same target (e.g., parameter sweep producing N configs
    from 1 dataset).

    Args:
        target_ids: Artifact IDs of the target role (lower step number,
            or the anchor/primary role).
        candidate_ids_by_role: {role_name: [artifact_ids]} for other roles.
        edges: DataFrame with ``source_artifact_id``, ``target_artifact_id``
            columns (from ``load_provenance_edges_df``).

    Returns:
        {target_id: {role: [candidate_ids]}} for targets with matches
        in all roles. Each role maps to a list of matched candidates.
    """
    if not target_ids or not candidate_ids_by_role:
        return {}

    # Collect ancestors of all targets once
    targets_df = pl.DataFrame({"artifact_id": sorted(target_ids)})
    target_ancestors = _collect_ancestors(targets_df, edges, id_col="target_id")

    matches: dict[str, dict[str, list[str]]] = {}

    for role, candidate_ids in candidate_ids_by_role.items():
        candidates_df = pl.DataFrame({"artifact_id": candidate_ids})
        candidate_ancestors = _collect_ancestors(
            candidates_df, edges, id_col="candidate_id"
        )

        # Join on shared ancestors
        joined = candidate_ancestors.join(
            target_ancestors,
            on="ancestor_id",
            how="inner",
        )

        if joined.is_empty():
            for candidate_id in candidate_ids:
                logger.warning(
                    "LINEAGE matching: candidate %s... from role '%s' has "
                    "no common ancestor with any target",
                    candidate_id[:8],
                    role,
                )
            continue

        # First-claim-wins per candidate: keep first match
        resolved = joined.unique(subset=["candidate_id"], keep="first").select(
            "candidate_id", "target_id"
        )

        matched_candidates = set()
        for row in resolved.iter_rows(named=True):
            candidate_id = row["candidate_id"]
            target_id = row["target_id"]
            matched_candidates.add(candidate_id)
            matches.setdefault(target_id, {}).setdefault(role, []).append(candidate_id)

        unmatched = set(candidate_ids) - matched_candidates
        for candidate_id in unmatched:
            logger.warning(
                "LINEAGE matching: candidate %s... from role '%s' has "
                "no common ancestor with any target",
                candidate_id[:8],
                role,
            )

    n_roles = len(candidate_ids_by_role)
    return {t: rm for t, rm in matches.items() if len(rm) == n_roles}
