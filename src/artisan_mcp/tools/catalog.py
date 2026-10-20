"""Bounded catalogue projections shared by tools and resources."""

from __future__ import annotations

from typing import Any, Literal

from fastmcp import Context, FastMCP
from fastmcp.tools import ToolResult
from pydantic import StrictInt

from artisan_mcp._boundary import render_tool, validate_int_range
from artisan_mcp._common import READ_ONLY
from artisan_mcp._evidence import MAX_PAYLOAD_BYTES, ensure_fits, json_ready
from artisan_mcp._pagination import paginate, reject_cursor
from artisan_mcp.models import (
    CapabilityResult,
    OperationDescription,
    OperationList,
    output_schema,
)

CapabilityView = Literal["summary", "sources", "errors", "name_collisions"]
OperationSection = Literal[
    "all", "summary", "inputs", "outputs", "params", "examples", "tags"
]


def capability_payload(
    state: dict[str, Any],
    view: CapabilityView = "summary",
    limit: StrictInt = 50,
    cursor: str | None = None,
) -> dict[str, Any]:
    """Project startup discovery without reading the configured store."""
    from artisan.registry import capabilities
    from artisan_mcp import __version__

    validate_int_range(limit, field="limit", minimum=1, maximum=100)
    payload = capabilities(
        server_version=__version__,
        discovery=state["discovery"],
        delta_root=None,
        read_only=True,
    ).model_dump()
    discovery = payload.pop("discovery")
    header = {
        **payload,
        "view": view,
        "operations_count": discovery["operations_count"],
        "source_count": len(discovery["sources"]),
        "error_count": len(discovery["errors"]),
        "name_collision_count": len(discovery["name_collisions"]),
    }
    if view == "summary":
        reject_cursor(cursor)
        return ensure_fits({"projection_version": "1", "follow_up": None, **header})
    keys = {
        "sources": ("kind", "module"),
        "errors": ("module", "error_type", "error_message"),
        "name_collisions": ("name", "first_module", "second_module"),
    }[view]
    rows = sorted(discovery[view], key=lambda row: tuple(row[key] for key in keys))
    return paginate(
        rows,
        limit,
        cursor,
        tool="artisan_capabilities",
        arguments={"view": view},
        header=header,
        hint="Inspect the selected module or Python discovery report for an oversized discovery row.",
    )


def operation_list_payload(
    query: str | None,
    kind: Literal["creator", "curator"] | None,
    tag: str | None,
    detail: str,
    limit: StrictInt,
    cursor: str | None,
) -> dict[str, Any]:
    """Select catalogue identities or full summaries in deterministic order."""
    from artisan.registry import list_operations

    query = query.lower() if query is not None else None
    rows = [
        item.model_dump() for item in list_operations(kind=kind, query=query, tag=tag)
    ]
    rows.sort(key=lambda row: row["name"])
    if detail == "identity":
        rows = [{"name": row["name"], "kind": row["kind"]} for row in rows]
    return paginate(
        rows,
        limit,
        cursor,
        tool="artisan_list_operations",
        arguments={"query": query, "kind": kind, "tag": tag, "detail": detail},
        header={"detail": detail},
        omitted_fields=[
            "items.description",
            "items.input_roles",
            "items.output_roles",
            "items.tags",
        ]
        if detail == "identity"
        else [],
        hint="Use detail='identity'; inspect the operation source if its identity cannot fit.",
    )


def operation_payload(
    name: str,
    section: OperationSection = "all",
    limit: StrictInt = 50,
    cursor: str | None = None,
    *,
    budget: int = MAX_PAYLOAD_BYTES,
) -> dict[str, Any]:
    """Return a complete metadata section or one whole-row section page."""
    from artisan.registry import describe

    validate_int_range(limit, field="limit", minimum=1, maximum=100)
    metadata = json_ready(describe(name))
    header = {
        "name": metadata["name"],
        "kind": metadata["kind"],
        "source_module": metadata["source_module"],
        "section": section,
    }
    if section in {"all", "summary", "params"}:
        reject_cursor(cursor)
        if section == "all":
            result = {**metadata, "section": section}
        elif section == "params":
            result = {**header, "params_schema": metadata["params_schema"]}
        else:
            result = {
                **header,
                "description": metadata["description"],
                "input_count": len(metadata["inputs"]),
                "output_count": len(metadata["outputs"]),
                "example_count": len(metadata["examples"]),
                "tag_count": len(metadata["tags"]),
            }
        return ensure_fits(
            {"projection_version": "1", "follow_up": None, **result},
            budget=budget,
            hint="Select section='summary', 'inputs', 'outputs', 'params', 'examples' or 'tags'; an oversized parameter schema requires source/Python inspection.",
        )
    if section in {"inputs", "outputs"}:
        rows = [
            {"role": role, "spec": spec}
            for role, spec in sorted(metadata[section].items())
        ]
    else:
        rows = sorted(metadata[section]) if section == "tags" else metadata[section]
    return paginate(
        rows,
        limit,
        cursor,
        tool="artisan_describe_operation",
        arguments={"name": name, "section": section},
        header=header,
        budget=budget,
        hint="Inspect the operation source/Python metadata when one complete role, example or tag cannot fit.",
    )


def register(mcp: FastMCP) -> None:
    """Register the read-only catalogue tools with explicit output contracts."""

    @mcp.tool(annotations=READ_ONLY, output_schema=output_schema(CapabilityResult))
    async def artisan_capabilities(
        ctx: Context,
        view: CapabilityView = "summary",
        limit: StrictInt = 50,
        cursor: str | None = None,
    ) -> ToolResult:
        """Report versions, read-only behavior and startup discovery coverage.

        The default summary includes exact discovery counts and withholds the
        configured root. Select sources, errors or name_collisions to page that
        collection. No store read or store health claim is made. Echo a returned
        cursor with the same view; restart if the selected evidence changed.
        """
        return render_tool(
            lambda: capability_payload(ctx.lifespan_context, view, limit, cursor),
            CapabilityResult,
        )

    @mcp.tool(annotations=READ_ONLY, output_schema=output_schema(OperationList))
    def artisan_list_operations(
        query: str | None = None,
        kind: Literal["creator", "curator"] | None = None,
        tag: str | None = None,
        detail: Literal["summary", "identity"] = "summary",
        limit: StrictInt = 50,
        cursor: str | None = None,
    ) -> ToolResult:
        """List registered operations using AND-combined query, kind and tag filters.

        Summary rows contain descriptions and roles, never parameter schemas.
        Use detail='identity' if one summary is too large. Each page reports its
        selected total, omissions and continuation. Use artisan_describe_operation
        for a candidate's complete contract or selected metadata section.
        """
        return render_tool(
            lambda: operation_list_payload(query, kind, tag, detail, limit, cursor),
            OperationList,
        )

    @mcp.tool(annotations=READ_ONLY, output_schema=output_schema(OperationDescription))
    def artisan_describe_operation(
        name: str,
        section: OperationSection = "all",
        limit: StrictInt = 50,
        cursor: str | None = None,
    ) -> ToolResult:
        """Describe an operation's contracts, parameters and declared usage examples.

        The all section returns complete metadata when it fits. Select summary,
        inputs, outputs, params, examples or tags to narrow evidence. Role/example
        lists page; parameter schemas remain whole with their references. An
        oversized indivisible section requires source or Python inspection.
        """
        return render_tool(
            lambda: operation_payload(name, section, limit, cursor),
            OperationDescription,
        )
