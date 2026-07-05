"""Store-aware provenance reading: bounded edge-list neighborhoods.

Sits between ``traversal.py`` (pure DataFrame joins over pre-loaded
edges) and ``visualization/graph/`` (rendering): it loads edges from the
store and walks them, but emits plain edge data for the caller — the CLI
``provenance`` command and the future MCP provenance tool — to shape.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel

if TYPE_CHECKING:
    from artisan.schemas.execution.storage_config import StorageConfig


class ProvenanceEdges(BaseModel):
    """Bounded edge-list neighborhood around one artifact.

    Attributes:
        artifact_id: The artifact the walk started from.
        direction: Walk direction the edges were collected in.
        depth: Maximum number of hops walked.
        edges: One ``{"source_artifact_id": ..., "target_artifact_id": ...}``
            per traversed edge, deduplicated.
        truncated: True when unvisited neighbors remained past ``depth``.
    """

    artifact_id: str
    direction: Literal["backward", "forward"]
    depth: int
    edges: list[dict[str, str]]
    truncated: bool


def provenance_edges(
    delta_root: str,
    artifact_id: str,
    *,
    direction: Literal["backward", "forward"] = "backward",
    depth: int = 3,
    storage: StorageConfig | None = None,
) -> ProvenanceEdges:
    """Walk the provenance graph around one artifact, bounded by depth.

    Loads the full adjacency for ``direction`` in one Delta scan
    (``ProvenanceStore.load_backward_map`` / ``load_forward_map``) and
    BFS-walks up to ``depth`` hops from ``artifact_id``. A missing
    ``artifact_edges`` table or unknown artifact yields empty edges, not
    an error.

    Args:
        delta_root: Root path for Delta Lake tables.
        artifact_id: Artifact to walk from.
        direction: ``"backward"`` toward ancestors, ``"forward"`` toward
            descendants. Defaults to ``"backward"``.
        depth: Maximum hops from ``artifact_id``. Defaults to 3.
        storage: Storage configuration for cloud backends. Defaults to
            local filesystem.

    Returns:
        ``ProvenanceEdges`` with the traversed edges and a ``truncated``
        flag set when the walk stopped at ``depth`` with frontier left.
    """
    from artisan.schemas.execution.storage_config import StorageConfig
    from artisan.storage.core.provenance_store import ProvenanceStore

    storage = storage or StorageConfig()
    store = ProvenanceStore(
        delta_root,
        fs=storage.filesystem(),
        storage_options=storage.delta_storage_options(),
    )
    adjacency = (
        store.load_backward_map()
        if direction == "backward"
        else store.load_forward_map()
    )

    edges: list[dict[str, str]] = []
    seen_edges: set[tuple[str, str]] = set()
    visited = {artifact_id}
    frontier = [artifact_id]
    for _ in range(depth):
        next_frontier: list[str] = []
        for node in frontier:
            for neighbor in adjacency.get(node, []):
                # Backward maps target -> sources; forward maps source -> targets.
                pair = (neighbor, node) if direction == "backward" else (node, neighbor)
                if pair not in seen_edges:
                    seen_edges.add(pair)
                    edges.append(
                        {"source_artifact_id": pair[0], "target_artifact_id": pair[1]}
                    )
                if neighbor not in visited:
                    visited.add(neighbor)
                    next_frontier.append(neighbor)
        frontier = next_frontier
        if not frontier:
            break

    truncated = any(
        neighbor not in visited
        for node in frontier
        for neighbor in adjacency.get(node, [])
    )
    return ProvenanceEdges(
        artifact_id=artifact_id,
        direction=direction,
        depth=depth,
        edges=edges,
        truncated=truncated,
    )
