"""Traverse provenance edges with DataFrame joins for artifact discovery.

Backward walks find the first nearest target per candidate; forward walks
collect all reachable targets. Strict input matching has separate rules.
"""

from __future__ import annotations

import polars as pl


def walk_backward(
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

    if candidates.is_empty() or targets.is_empty():
        return empty

    target_ids = targets.select(pl.col("artifact_id").alias("_target_id"))

    frontier = pl.DataFrame(
        {
            "candidate_id": candidates["artifact_id"],
            "current_node": candidates["artifact_id"],
        }
    )

    initial_matches = frontier.join(
        target_ids, left_on="current_node", right_on="_target_id", how="semi"
    ).select(
        pl.col("candidate_id"),
        pl.col("current_node").alias("target_id"),
    )
    if edges.is_empty():
        return initial_matches

    all_matched: list[pl.DataFrame] = []
    if not initial_matches.is_empty():
        all_matched.append(initial_matches)

    frontier = frontier.unique(maintain_order=True)
    visited = frontier.select("candidate_id", pl.col("current_node").alias("node"))
    frontier = frontier.join(
        initial_matches.select("candidate_id"),
        on="candidate_id",
        how="anti",
    )

    while not frontier.is_empty():
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

        stepped = stepped.join(
            visited,
            left_on=["candidate_id", "current_node"],
            right_on=["candidate_id", "node"],
            how="anti",
        ).unique(maintain_order=True)

        if stepped.is_empty():
            break

        visited = pl.concat(
            [
                visited,
                stepped.select("candidate_id", pl.col("current_node").alias("node")),
            ]
        )

        new_matches = stepped.join(
            target_ids, left_on="current_node", right_on="_target_id", how="semi"
        )

        if not new_matches.is_empty():
            new_matches = new_matches.unique(
                subset=["candidate_id"], keep="first"
            ).select(
                pl.col("candidate_id"),
                pl.col("current_node").alias("target_id"),
            )
            all_matched.append(new_matches)

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


def walk_forward(
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

    Raises:
        ValueError: A target type is requested but edges lack the
            ``target_artifact_type`` column, including empty frames.
    """
    empty = pl.DataFrame(schema={"source_id": pl.String, "target_id": pl.String})

    if target_type is not None and "target_artifact_type" not in edges.columns:
        msg = "target_type requires a target_artifact_type column in edges"
        raise ValueError(msg)

    if sources.is_empty() or edges.is_empty():
        return empty

    frontier = pl.DataFrame(
        {
            "source_id": sources["artifact_id"],
            "current_node": sources["artifact_id"],
        }
    ).unique()

    visited = frontier.select("source_id", pl.col("current_node").alias("node"))

    all_matched: list[pl.DataFrame] = []

    while not frontier.is_empty():
        stepped = frontier.join(
            edges,
            left_on="current_node",
            right_on="source_artifact_id",
            how="inner",
        ).select(
            pl.col("source_id"),
            pl.col("target_artifact_id").alias("current_node"),
            *([pl.col("target_artifact_type")] if target_type is not None else []),
        )

        if stepped.is_empty():
            break

        stepped = stepped.join(
            visited,
            left_on=["source_id", "current_node"],
            right_on=["source_id", "node"],
            how="anti",
        )

        if stepped.is_empty():
            break

        frontier = stepped.select("source_id", "current_node").unique()
        visited = pl.concat([visited, frontier.rename({"current_node": "node"})])

        matches = frontier
        if target_type is not None:
            matches = (
                stepped.filter(pl.col("target_artifact_type") == target_type)
                .select("source_id", "current_node")
                .unique()
            )
        if not matches.is_empty():
            all_matched.append(matches.rename({"current_node": "target_id"}))

    if not all_matched:
        return empty

    return pl.concat(all_matched).unique()
