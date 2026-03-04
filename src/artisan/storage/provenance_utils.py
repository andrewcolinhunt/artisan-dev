"""Provenance graph traversal utilities.

Pure functions for BFS traversal of artifact provenance graphs.
"""

from __future__ import annotations

from collections import deque


def trace_derived_artifacts(
    source_id: str,
    forward_map: dict[str, list[str]],
    target_ids: set[str],
) -> list[str]:
    """Collect reachable target IDs via BFS from a source artifact.

    Args:
        source_id: Starting artifact ID for the traversal.
        forward_map: Adjacency list mapping each artifact ID to its
            direct descendants.
        target_ids: Candidate IDs to collect when encountered.

    Returns:
        Target artifact IDs reachable from source_id, in BFS visit order.
    """
    found: list[str] = []
    visited: set[str] = {source_id}
    queue: deque[str] = deque([source_id])

    while queue:
        current = queue.popleft()
        for target_id in forward_map.get(current, []):
            if target_id in visited:
                continue
            visited.add(target_id)
            if target_id in target_ids:
                found.append(target_id)
            queue.append(target_id)

    return found
