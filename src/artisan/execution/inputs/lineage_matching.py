"""Provenance graph walking for artifact lineage matching.

Forward and backward walks through provenance edges for metric
discovery and multi-input pairing.

Complexity: O((T + M) x A) where T = targets, M = candidates,
A = average ancestry depth.
"""

from __future__ import annotations

import logging
from collections import deque

import polars as pl

logger = logging.getLogger(__name__)


def walk_provenance_to_targets(
    candidates: pl.DataFrame,
    targets: pl.DataFrame,
    edges: pl.DataFrame,
) -> pl.DataFrame:
    """Walk backward from candidates through provenance edges to find targets.

    Iterative backward walk: frontier starts as candidates, each iteration
    joins on edges to step backward one hop, checks for target matches,
    continues with unmatched candidates. First-match semantics.

    Args:
        candidates: DataFrame with at least an ``artifact_id`` column.
        targets: DataFrame with at least an ``artifact_id`` column.
        edges: DataFrame with ``source_artifact_id``, ``target_artifact_id``.

    Returns:
        DataFrame with columns [candidate_id, target_id] for matched pairs.
        Candidates without a matching target are omitted.
    """
    empty = pl.DataFrame(schema={"candidate_id": pl.String, "target_id": pl.String})

    if candidates.is_empty() or targets.is_empty() or edges.is_empty():
        return empty

    target_ids = targets.select(pl.col("artifact_id").alias("_target_id"))

    # frontier: (candidate_id, current_node) — tracks the walk state
    frontier = pl.DataFrame(
        {
            "candidate_id": candidates["artifact_id"],
            "current_node": candidates["artifact_id"],
        }
    )

    # visited: (candidate_id, node) — all nodes visited per candidate
    visited = frontier.select("candidate_id", pl.col("current_node").alias("node"))

    # Check if any candidates are themselves targets
    initial_matches = frontier.join(
        target_ids, left_on="current_node", right_on="_target_id", how="semi"
    ).select(
        pl.col("candidate_id"),
        pl.col("current_node").alias("target_id"),
    )

    all_matched: list[pl.DataFrame] = []
    if not initial_matches.is_empty():
        all_matched.append(initial_matches)

    # Remove matched candidates from frontier
    frontier = frontier.join(
        initial_matches.select("candidate_id"),
        on="candidate_id",
        how="anti",
    )

    while not frontier.is_empty():
        # Walk backward one hop: current_node is a target_artifact_id in edges
        stepped = frontier.join(
            edges,
            left_on="current_node",
            right_on="target_artifact_id",
            how="inner",
        ).select(
            pl.col("candidate_id"),
            pl.col("source_artifact_id").alias("current_node"),
        )

        if stepped.is_empty():
            break

        # Anti-join with visited to skip already-seen nodes
        stepped = stepped.join(
            visited,
            left_on=["candidate_id", "current_node"],
            right_on=["candidate_id", "node"],
            how="anti",
        )

        if stepped.is_empty():
            break

        # Add to visited
        visited = pl.concat(
            [
                visited,
                stepped.select("candidate_id", pl.col("current_node").alias("node")),
            ]
        )

        # Check for target matches — semi-join with targets
        new_matches = stepped.join(
            target_ids, left_on="current_node", right_on="_target_id", how="semi"
        )

        if not new_matches.is_empty():
            # First-match per candidate: keep first match only
            new_matches = new_matches.unique(
                subset=["candidate_id"], keep="first"
            ).select(
                pl.col("candidate_id"),
                pl.col("current_node").alias("target_id"),
            )
            all_matched.append(new_matches)

            # Remove matched candidates from frontier
            stepped = stepped.join(
                new_matches.select("candidate_id"),
                on="candidate_id",
                how="anti",
            )

        if stepped.is_empty():
            break
        frontier = stepped

    if not all_matched:
        return empty

    return pl.concat(all_matched)


def walk_forward_to_targets(
    sources: pl.DataFrame,
    edges: pl.DataFrame,
    target_type: str | None = None,
) -> pl.DataFrame:
    """Walk forward from sources through provenance edges to find targets.

    Iterative forward walk: frontier starts as sources, each iteration
    joins on edges to step forward one hop, checks for target matches,
    continues with all sources (all-match semantics — one source can
    match multiple targets).

    Args:
        sources: DataFrame with at least an ``artifact_id`` column.
        edges: DataFrame with ``source_artifact_id``, ``target_artifact_id``,
            and optionally ``target_artifact_type`` (required when
            ``target_type`` is specified).
        target_type: When set, only nodes whose ``target_artifact_type``
            matches count as targets.

    Returns:
        DataFrame with columns [source_id, target_id] for matched pairs.
        Sources without a matching target are omitted.
    """
    empty = pl.DataFrame(schema={"source_id": pl.String, "target_id": pl.String})

    if sources.is_empty() or edges.is_empty():
        return empty

    # frontier: (source_id, current_node) — tracks the walk state
    frontier = pl.DataFrame(
        {
            "source_id": sources["artifact_id"],
            "current_node": sources["artifact_id"],
        }
    )

    # visited: (source_id, node) — all nodes visited per source
    visited = frontier.select("source_id", pl.col("current_node").alias("node"))

    all_matched: list[pl.DataFrame] = []

    while not frontier.is_empty():
        # Walk forward one hop: current_node is a source_artifact_id in edges
        stepped = frontier.join(
            edges,
            left_on="current_node",
            right_on="source_artifact_id",
            how="inner",
        ).select(
            pl.col("source_id"),
            pl.col("target_artifact_id").alias("current_node"),
            *(
                [pl.col("target_artifact_type")]
                if "target_artifact_type" in edges.columns
                else []
            ),
        )

        if stepped.is_empty():
            break

        # Anti-join with visited to skip already-seen nodes
        stepped = stepped.join(
            visited,
            left_on=["source_id", "current_node"],
            right_on=["source_id", "node"],
            how="anti",
        )

        if stepped.is_empty():
            break

        # Add to visited
        visited = pl.concat(
            [
                visited,
                stepped.select("source_id", pl.col("current_node").alias("node")),
            ]
        )

        # Check for target matches
        if target_type is not None and "target_artifact_type" in stepped.columns:
            new_matches = stepped.filter(
                pl.col("target_artifact_type") == target_type
            ).select(
                pl.col("source_id"),
                pl.col("current_node").alias("target_id"),
            )
        else:
            new_matches = stepped.select(
                pl.col("source_id"),
                pl.col("current_node").alias("target_id"),
            )

        if not new_matches.is_empty():
            all_matched.append(new_matches)

        # All-match semantics: keep all sources in frontier (not just unmatched)
        frontier = stepped.select("source_id", "current_node")

    if not all_matched:
        return empty

    return pl.concat(all_matched).unique()


def match_by_ancestry(
    target_ids: set[str],
    candidate_ids_by_role: dict[str, list[str]],
    provenance_map: dict[str, list[str]],
) -> dict[str, dict[str, list[str]]]:
    """Match candidates to targets via backward provenance walk.

    Two-phase algorithm:
    1. Build target ancestry index: BFS backward from each target,
       recording {ancestor_id: target_id}. Targets map to themselves.
    2. For each candidate, BFS backward through provenance. At each
       node, check if it's in the index -- resolve to its target.

    Supports 1:N matching — multiple candidates in the same role can
    map to the same target (e.g., parameter sweep producing N configs
    from 1 dataset).

    Args:
        target_ids: Artifact IDs of the target role (lower step number,
            or the anchor/primary role).
        candidate_ids_by_role: {role_name: [artifact_ids]} for other roles.
        provenance_map: {target_id: [source_ids]} from
            ArtifactStore.load_provenance_map().

    Returns:
        {target_id: {role: [candidate_ids]}} for targets with matches
        in all roles. Each role maps to a list of matched candidates.
    """
    if not target_ids or not candidate_ids_by_role:
        return {}

    target_ancestry_index = _build_target_ancestry_index(target_ids, provenance_map)

    matches: dict[str, dict[str, list[str]]] = {}

    for role, candidate_ids in candidate_ids_by_role.items():
        for candidate_id in candidate_ids:
            target = _find_target(candidate_id, target_ancestry_index, provenance_map)

            if target is None:
                logger.warning(
                    "LINEAGE matching: candidate %s... from role '%s' has "
                    "no common ancestor with any target",
                    candidate_id[:8],
                    role,
                )
                continue

            matches.setdefault(target, {}).setdefault(role, []).append(candidate_id)

    n_roles = len(candidate_ids_by_role)
    return {t: rm for t, rm in matches.items() if len(rm) == n_roles}


def _build_target_ancestry_index(
    target_ids: set[str],
    provenance_map: dict[str, list[str]],
) -> dict[str, str]:
    """BFS backward from each target to build an ancestry lookup index.

    Records {ancestor_id: target_id} so any ancestor resolves to its
    target in O(1). Targets are included in the index (map to themselves).
    First-claim-wins for shared ancestors.

    Args:
        target_ids: Set of target artifact IDs.
        provenance_map: {target_id: [source_ids]}.

    Returns:
        Dict mapping ancestor IDs to their owning target ID.
    """
    index: dict[str, str] = {}
    for target_id in target_ids:
        queue = deque([target_id])
        visited = {target_id}
        while queue:
            current = queue.popleft()
            if current not in index:
                index[current] = target_id
            for source in provenance_map.get(current, []):
                if source not in visited:
                    visited.add(source)
                    queue.append(source)
    return index


def _find_target(
    artifact_id: str,
    target_ancestry_index: dict[str, str],
    provenance_map: dict[str, list[str]],
) -> str | None:
    """BFS backward from a candidate to find its matching target.

    At each node, checks the target ancestry index. Returns on first hit.

    Args:
        artifact_id: Candidate artifact ID.
        target_ancestry_index: Index built by _build_target_ancestry_index.
        provenance_map: {target_id: [source_ids]}.

    Returns:
        The matching target ID, or None if no connection found.
    """
    if artifact_id in target_ancestry_index:
        return target_ancestry_index[artifact_id]

    queue = deque([artifact_id])
    visited = {artifact_id}

    while queue:
        current = queue.popleft()
        for source in provenance_map.get(current, []):
            if source in target_ancestry_index:
                return target_ancestry_index[source]
            if source not in visited:
                visited.add(source)
                queue.append(source)

    return None
