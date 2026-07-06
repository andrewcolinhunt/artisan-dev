"""Artifact tools: query_artifacts, get_step_result.

Both return artifact references (ids, types, index metadata) — never
payloads. ``get_step_result`` resolves the step name to its number via the
steps table (``inspect_step`` and the index are keyed by number) and groups
the step's refs.
"""

from __future__ import annotations

from fastmcp import Context, FastMCP

from artisan_mcp._boundary import boundary, require_delta_root
from artisan_mcp._pagination import paginate

_READ_ONLY = {"readOnlyHint": True, "idempotentHint": True}


def register(mcp: FastMCP) -> None:
    """Attach the artifact tools to ``mcp``."""

    @mcp.tool(annotations=_READ_ONLY)
    async def artisan_query_artifacts(
        ctx: Context,
        artifact_type: str | None = None,
        pipeline_run_id: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> dict:
        """Find artifacts by type or run to get ids for provenance walks.

        Returns a page of artifact references — artifact_id, artifact_type,
        origin_step_number, and index metadata — filtered by artifact_type
        and/or pipeline_run_id (AND-ed), with has_more and next_cursor.
        References only: never artifact content. Use the returned
        artifact_id with artisan_get_provenance_graph to walk lineage. The
        index is not run-scoped, so a run filter resolves the run's step
        numbers and can over-return in multi-run stores.
        """
        config = ctx.lifespan_context["config"]

        def payload() -> dict:
            from artisan.storage.core.artifact_query import query_artifacts

            root = require_delta_root(config)
            refs = query_artifacts(
                root,
                artifact_type=artifact_type,
                pipeline_run_id=pipeline_run_id,
            )
            return paginate([ref.model_dump() for ref in refs], limit, cursor)

        return boundary(payload)

    @mcp.tool(annotations=_READ_ONLY)
    async def artisan_get_step_result(
        ctx: Context, pipeline_run_id: str, step_name: str
    ) -> dict:
        """Get the artifacts one step produced, grouped by artifact type.

        Resolves the step name to its number within the run, then returns
        that step's artifact references grouped by artifact_type
        (artifact_id, origin_step_number, metadata). References only, never
        payloads — feed an artifact_id to artisan_get_provenance_graph to
        trace lineage. Grouping is by type rather than input role: the
        artifact index carries type, not role. An unknown step name yields
        an empty result.
        """
        config = ctx.lifespan_context["config"]

        def payload() -> dict:
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
            grouped: dict[str, list[dict]] = {}
            for ref in refs:
                if ref.origin_step_number == number:
                    grouped.setdefault(ref.artifact_type, []).append(ref.model_dump())
            return grouped

        return boundary(payload)
