"""Read dynamic curator summaries through candidate completion visibility."""

from __future__ import annotations

import polars as pl

from artisan.schemas.enums import TablePath
from artisan.storage.core.committed_scan import _visibility_expression
from artisan.storage.core.curator_view import CuratorView, capture_curator_view
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_read import RetainedReadSnapshot


def read_curator_result_evidence(
    committer: RetainedCommitter,
    step_run_id: str,
    *,
    _read: RetainedReadSnapshot | None = None,
) -> dict[str, pl.DataFrame]:
    """Select one step's complete records without reading unrelated payloads."""
    view = CuratorView(
        committer, capture_curator_view(committer, _read=_read), _read=_read
    )
    reuse = (
        view.scan(TablePath.CACHE_REUSE)
        .filter(pl.col("current_step_run_id") == step_run_id)
        .select("current_step_run_id", "cached_execution_run_id")
        .collect()
    )
    executions = (
        view.scan(TablePath.EXECUTIONS)
        .filter(
            (pl.col("step_run_id") == step_run_id)
            | pl.col("execution_run_id").is_in(reuse["cached_execution_run_id"])
        )
        .select("execution_run_id", "step_run_id", "success", "metadata")
        .collect()
    )
    edges = (
        view.physical(TablePath.EXECUTION_EDGES.value)
        .filter(_visibility_expression(TablePath.EXECUTION_EDGES.value, view.controls))
        .filter(pl.col("execution_run_id").is_in(executions["execution_run_id"]))
        .select("execution_run_id", "direction", "role", "artifact_id")
        .collect()
    )
    index = (
        view.scan(TablePath.ARTIFACT_INDEX)
        .filter(pl.col("artifact_id").is_in(edges["artifact_id"].unique()))
        .select("artifact_id", "artifact_type")
        .collect()
    )
    return {
        TablePath.EXECUTIONS.value: executions,
        TablePath.EXECUTION_EDGES.value: edges,
        TablePath.ARTIFACT_INDEX.value: index,
        TablePath.CACHE_REUSE.value: reuse,
    }
