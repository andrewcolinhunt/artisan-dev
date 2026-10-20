"""Bounded artifact references preserving accepted occurrence membership."""

from __future__ import annotations

from typing import Any

from pydantic import StrictInt

from artisan.inspection._boundary import validate_int_range
from artisan.inspection._pagination import paginate


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
    capability = "query_artifacts"
    number = None
    if step_name is not None:
        assert pipeline_run_id is not None
        args["step_name"] = step_name
        capability = "get_step_result"
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
        capability=capability,
        arguments=args,
        scope=root,
        omitted_fields=["items.metadata"]
        if not keys
        else ["items.metadata.unselected_keys"],
        hint="Request fewer metadata_keys, or omit metadata; inspect the source/Python reference if identity cannot fit.",
    )
