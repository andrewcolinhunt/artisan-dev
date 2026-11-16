"""Real direct/reused outer selections for public reader isolation checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from pathlib import Path

from artisan.execution.recording.delivery_diagnostics import (
    publish_delivery_diagnostics,
)
from artisan.schemas import CSVArtifact, MetricArtifact
from artisan.schemas.execution.command_record import CommandRecording
from artisan.schemas.execution.delivery_diagnostics import (
    DeliveryDiagnostics,
    InvocationDiagnostic,
)
from artisan.storage.core.retained_reader import RetainedReader
from fixtures.retained_inspection import commit_inspection_attempt, inspection_store


@dataclass(frozen=True)
class CacheIsolationStore:
    root: Path
    source_run: str
    current_run: str
    other_run: str
    source_metric_execution: str
    current_data_execution: str
    other_data_execution: str
    data_id: str
    metric_id: str
    other_data_id: str
    other_metric_id: str
    current_cache_step_id: str


def observe_execution(root: Path, execution_id: str, duration: float = 2.0) -> None:
    """Publish explicit optional measurements bound to the actual accepted seal."""
    committer = inspection_store(root)
    plan = next(
        item
        for item in RetainedReader(committer).completed_plans()
        if item.receipt.delivery.delivery_id == execution_id
    )
    publish_delivery_diagnostics(
        DeliveryDiagnostics(
            timestamp_start=datetime(2024, 1, 1, tzinfo=UTC),
            delivery=plan.receipt.delivery,
            seal_digest=plan.receipt.seal_digest,
            outcome="success",
            cleanup="closed",
            duration_seconds=duration,
            invocations=(
                InvocationDiagnostic(
                    invocation_id="root",
                    call_path=(),
                    operation_name="fixture",
                    operation_version="1",
                    succeeded=True,
                    cleanup="closed",
                    duration_seconds=duration,
                    timings={"total": duration},
                    command_recording=CommandRecording.empty(),
                    tool_output=None,
                    phase=None,
                    error=None,
                ),
            ),
            omitted_invocations=0,
            phase=None,
            call_path=(),
            error=None,
        ),
        delta_root=committer.delta_root,
        fs=committer.fs,
    )


def build_cache_isolation_store(
    tmp_path: Path,
    *,
    reuse_source_metric_at_step_zero: bool = False,
    recorded_pipeline_defaults: bool = False,
    with_lineage: bool = False,
) -> CacheIsolationStore:
    """Keep original metric origin separate from current repeated membership."""
    commit = partial(
        commit_inspection_attempt, recorded_pipeline_defaults=recorded_pipeline_defaults
    )
    root = tmp_path / "cache-isolation-delta"
    data = CSVArtifact.draft(b"value\n1\n", "shared.csv", 0).finalize()
    metric = MetricArtifact.draft({"score": 0.9}, "source_metric.json", 1).finalize()
    other_data = CSVArtifact.draft(b"value\n2\n", "other.csv", 0).finalize()
    other_metric = MetricArtifact.draft(
        {"score": 0.1}, "other_metric.json", 5
    ).finalize()
    commit(root, run_id="source-run", name="source_data", outputs={"csv": [data]})
    _, source = commit(
        root,
        run_id="source-run",
        number=1,
        name="source_metric",
        outputs={"metric": [metric]},
        inputs={"data": [data]} if with_lineage else None,
    )
    assert source is not None
    _, current = commit(
        root,
        run_id="current-run",
        name="current_data",
        outputs={"csv": [data]},
        reused_execution_id=source if reuse_source_metric_at_step_zero else None,
    )
    assert current is not None
    cached, _ = commit(
        root,
        run_id="current-run",
        number=5,
        name="current_metric_cached",
        reused_execution_id=source,
    )
    _, other = commit(
        root, run_id="other-run", name="other_data", outputs={"csv": [other_data]}
    )
    commit(
        root,
        run_id="other-run",
        number=5,
        name="other_metric",
        outputs={"metric": [other_metric]},
        inputs={"data": [other_data]} if with_lineage else None,
    )
    assert other is not None
    if not reuse_source_metric_at_step_zero:
        observe_execution(root, current)
    return CacheIsolationStore(
        root,
        "source-run",
        "current-run",
        "other-run",
        source,
        current,
        other,
        data.artifact_id,
        metric.artifact_id,
        other_data.artifact_id,
        other_metric.artifact_id,
        cached,
    )
