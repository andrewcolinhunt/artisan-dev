"""Bounded catalogue projections shared by tools and resources."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import StrictInt

from artisan.inspection._boundary import validate_int_range
from artisan.inspection._evidence import MAX_PAYLOAD_BYTES, ensure_fits, json_ready
from artisan.inspection._pagination import paginate, reject_cursor
from artisan.inspection.requests import CapabilityView, OperationSection
from artisan.registry.models import DiscoveryReport


def capability_payload(
    discovery: DiscoveryReport,
    view: CapabilityView = "summary",
    limit: StrictInt = 50,
    cursor: str | None = None,
) -> dict[str, Any]:
    """Project startup discovery without reading the configured store."""
    from artisan import __version__

    validate_int_range(limit, field="limit", minimum=1, maximum=100)
    report = json_ready(discovery)
    header = {
        "artisan_version": __version__,
        "read_only": True,
        "delta_root": None,
        "view": view,
        "operations_count": report["operations_count"],
        "source_count": len(report["sources"]),
        "error_count": len(report["errors"]),
        "name_collision_count": len(report["name_collisions"]),
    }
    if view == "summary":
        reject_cursor(cursor)
        return ensure_fits({"projection_version": "2", "follow_up": None, **header})
    keys = {
        "sources": ("kind", "module"),
        "errors": ("module", "error_type", "error_message"),
        "name_collisions": ("name", "first_module", "second_module"),
    }[view]
    rows = sorted(report[view], key=lambda row: tuple(row[key] for key in keys))
    return paginate(
        rows,
        limit,
        cursor,
        capability="capabilities",
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
        capability="list_operations",
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
            {"projection_version": "2", "follow_up": None, **result},
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
        capability="describe_operation",
        arguments={"name": name, "section": section},
        header=header,
        budget=budget,
        hint="Inspect the operation source/Python metadata when one complete role, example or tag cannot fit.",
    )
