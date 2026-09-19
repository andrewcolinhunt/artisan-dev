"""Artifact tools: query_artifacts, get_step_result.

Return references, never payloads. Run-scoped queries use accepted current-step
output membership, including cached and passthrough outputs; origin metadata
identifies where each artifact was first created.
"""

from __future__ import annotations

from typing import Any

from fastmcp import Context, FastMCP

from artisan_mcp._boundary import boundary, require_delta_root
from artisan_mcp._common import READ_ONLY
from artisan_mcp._pagination import paginate


def register(mcp: FastMCP) -> None:
    """Attach the artifact tools to ``mcp``."""

    @mcp.tool(annotations=READ_ONLY)
    async def artisan_query_artifacts(
        ctx: Context,
        artifact_type: str | None = None,
        pipeline_run_id: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """Find artifacts by type or run to get ids for provenance walks.

        Returns a page of artifact references — artifact_id, artifact_type,
        origin_step_number, current_step_number, and index metadata — filtered
        by artifact_type and/or pipeline_run_id (AND-ed), with has_more and
        next_cursor. Pages are capped at 100 items. Run-scoped results reflect
        accepted outputs, including cached and passthrough artifacts; their
        current step can differ from their origin. References only, never
        content. Use an artifact_id with artisan_get_provenance_graph.
        """
        config = ctx.lifespan_context["config"]

        def payload() -> dict[str, Any]:
            from artisan.storage.core.artifact_query import query_artifacts

            root = require_delta_root(config)
            refs = query_artifacts(
                root,
                artifact_type=artifact_type,
                pipeline_run_id=pipeline_run_id,
            )
            return paginate([ref.model_dump() for ref in refs], limit, cursor)

        return boundary(payload)

    @mcp.tool(annotations=READ_ONLY)
    async def artisan_get_step_result(
        ctx: Context, pipeline_run_id: str, step_name: str
    ) -> dict[str, Any]:
        """Get one step's accepted output references, grouped by artifact type.

        Resolves the step name within the run and includes accepted direct,
        cached, and passthrough outputs. Each reference carries artifact_id,
        origin_step_number, current_step_number, and metadata: the current
        step can differ from the artifact's origin. References only, never
        payloads. Feed an artifact_id to artisan_get_provenance_graph to trace
        lineage. Results are grouped by artifact_type, not role. An unknown
        step name yields an empty result.
        """
        config = ctx.lifespan_context["config"]

        def payload() -> dict[str, Any]:
            from artisan.orchestration.run_status import resolve_step_number
            from artisan.schemas.execution.storage_config import StorageConfig
            from artisan.storage.core.artifact_query import query_artifacts

            root = require_delta_root(config)
            storage = StorageConfig()
            number = resolve_step_number(
                root,
                pipeline_run_id,
                step_name,
                storage_options=storage.delta_storage_options(),
                fs=storage.filesystem(),
            )
            if number is None:
                return {}

            refs = query_artifacts(
                root, pipeline_run_id=pipeline_run_id, storage=storage
            )
            grouped: dict[str, list[dict[str, Any]]] = {}
            for ref in refs:
                if ref.current_step_number == number:
                    grouped.setdefault(ref.artifact_type, []).append(ref.model_dump())
            return grouped

        return boundary(payload)
