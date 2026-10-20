"""Bounded artifact references preserving accepted occurrence membership."""

from __future__ import annotations

from typing import Any

from fastmcp import Context, FastMCP
from fastmcp.tools import ToolResult
from pydantic import StrictInt

from artisan_mcp._boundary import render_tool, require_delta_root, validate_int_range
from artisan_mcp._common import READ_ONLY
from artisan_mcp._pagination import paginate
from artisan_mcp.models import (
    ArtifactIdentity,
    ArtifactSelected,
    PageResponse,
    output_schema,
)

ArtifactPage = PageResponse[ArtifactIdentity | ArtifactSelected]


def artifact_payload(
    root: str,
    artifact_type: str | None,
    pipeline_run_id: str | None,
    metadata_keys: list[str] | None,
    limit: StrictInt,
    cursor: str | None,
    *,
    step_name: str | None = None,
) -> dict[str, Any]:
    """Project accepted references with selected top-level metadata keys."""
    from artisan.orchestration.run_status import resolve_step_number
    from artisan.storage.core.artifact_query import query_artifacts

    validate_int_range(limit, field="limit", minimum=1, maximum=100)
    keys = sorted(set(metadata_keys or []))
    args = {
        "artifact_type": artifact_type,
        "pipeline_run_id": pipeline_run_id,
        "metadata_keys": keys,
    }
    tool = "artisan_query_artifacts"
    number = None
    if step_name is not None:
        assert pipeline_run_id is not None
        args["step_name"] = step_name
        tool = "artisan_get_step_result"
        number = resolve_step_number(root, pipeline_run_id, step_name)
    refs = (
        []
        if step_name is not None and number is None
        else query_artifacts(
            root, artifact_type=artifact_type, pipeline_run_id=pipeline_run_id
        )
    )
    rows = []
    for ref in refs:
        if step_name is not None and ref.current_step_number != number:
            continue
        row = ref.model_dump(exclude={"metadata"})
        if keys:
            row["metadata"] = {
                key: ref.metadata[key] for key in keys if key in ref.metadata
            }
        rows.append(row)
    return paginate(
        rows,
        limit,
        cursor,
        tool=tool,
        arguments=args,
        scope=root,
        omitted_fields=["items.metadata"]
        if not keys
        else ["items.metadata.unselected_keys"],
        hint="Request fewer metadata_keys, or omit metadata; inspect the source/Python reference if identity cannot fit.",
    )


def register(mcp: FastMCP) -> None:
    """Register flat artifact-reference pages."""

    @mcp.tool(annotations=READ_ONLY, output_schema=output_schema(ArtifactPage))
    def artisan_query_artifacts(
        ctx: Context,
        artifact_type: str | None = None,
        pipeline_run_id: str | None = None,
        metadata_keys: list[str] | None = None,
        limit: StrictInt = 50,
        cursor: str | None = None,
    ) -> ToolResult:
        """Find artifact references by type and/or accepted run membership.

        References preserve origin and current step numbers; cached and passthrough
        outputs can have different origins. Metadata is omitted unless exact
        top-level metadata_keys are selected. No artifact content is returned.
        Follow the cursor for all selected occurrences and use IDs for provenance.
        """
        config = ctx.lifespan_context["config"]
        return render_tool(
            lambda: artifact_payload(
                require_delta_root(config),
                artifact_type,
                pipeline_run_id,
                metadata_keys,
                limit,
                cursor,
            ),
            ArtifactPage,
        )

    @mcp.tool(annotations=READ_ONLY, output_schema=output_schema(ArtifactPage))
    def artisan_get_step_result(
        ctx: Context,
        pipeline_run_id: str,
        step_name: str,
        artifact_type: str | None = None,
        metadata_keys: list[str] | None = None,
        limit: StrictInt = 50,
        cursor: str | None = None,
    ) -> ToolResult:
        """Get a flat page of one step's accepted output references, optionally by type.

        Direct, cached and passthrough outputs retain current/origin identity.
        Metadata requires explicit metadata_keys; payloads and role grouping are
        not returned. Unknown steps give empty pages. Repeated step names select
        the existing first match, so resolve uniqueness through run status first.
        """
        config = ctx.lifespan_context["config"]
        return render_tool(
            lambda: artifact_payload(
                require_delta_root(config),
                artifact_type,
                pipeline_run_id,
                metadata_keys,
                limit,
                cursor,
                step_name=step_name,
            ),
            ArtifactPage,
        )
