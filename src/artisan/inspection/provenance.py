"""Paged provenance edges with independent traversal-coverage metadata."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import StrictInt

from artisan.inspection._boundary import validate_int_range
from artisan.inspection._evidence import MAX_PAYLOAD_BYTES, json_ready
from artisan.inspection._pagination import paginate

MAX_PROVENANCE_DEPTH = 10


def provenance_payload(
    root: str,
    artifact_id: str,
    direction: Literal["backward", "forward"] = "backward",
    depth: StrictInt = 3,
    limit: StrictInt = 50,
    cursor: str | None = None,
    *,
    budget: int = MAX_PAYLOAD_BYTES,
) -> dict[str, Any]:
    """Page the exact selected edge set, keeping its depth boundary visible."""
    from artisan.provenance import provenance_edges

    validate_int_range(limit, field="limit", minimum=1, maximum=100)
    validate_int_range(depth, field="depth", minimum=1, maximum=MAX_PROVENANCE_DEPTH)
    result = json_ready(
        provenance_edges(root, artifact_id, direction=direction, depth=depth)
    )
    edges = sorted(
        result.pop("edges"),
        key=lambda row: (row["source_artifact_id"], row["target_artifact_id"]),
    )
    result["traversal_truncated"] = result.pop("truncated")
    return paginate(
        edges,
        limit,
        cursor,
        capability="get_provenance_graph",
        arguments={"artifact_id": artifact_id, "direction": direction, "depth": depth},
        scope=root,
        header=result,
        page_key="edges",
        budget=budget,
    )
