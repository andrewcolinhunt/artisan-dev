"""Cancellation evidence remains authoritative through manager publication."""

from __future__ import annotations

import polars as pl
import pytest

from artisan.execution.recording.parquet_writer import StagingResult
from artisan.operations.curator.ingest_data import IngestData
from artisan.operations.examples.data_generator import DataGenerator
from artisan.orchestration import PipelineManager
from artisan.orchestration.engine.lifecycle_router import LifecycleRouter
from artisan.orchestration.run_status import run_status
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepStatus,
)
from artisan.storage.core.committed_scan import read_committed
from artisan.storage.io.repair import repair_store
from artisan.storage.io.worker_seal import verify_worker_seal


@pytest.mark.parametrize("request_recorded", [False, True])
@pytest.mark.parametrize("operation", [DataGenerator, IngestData])
def test_unknown_cancellation_persists_custom_named_attempt_and_allows_resume(
    tmp_path, monkeypatch, request_recorded, operation
):
    roots = {
        "delta_root": str(tmp_path / "delta"),
        "staging_root": str(tmp_path / "staging"),
    }
    pipeline = PipelineManager.create("uncertain", **roots, skip_cache=True)
    acknowledgement = CancellationAcknowledgement(
        CancellationStatus.UNKNOWN, "Worker exit could not be proved"
    )

    def record_request(unit):
        pipeline.cancel()
        if request_recorded:
            pipeline._step_tracker.record_cancellation(
                unit.step_run_id,
                StepStatus.RUNNING,
                CancellationAcknowledgement(CancellationStatus.REQUESTED),
            )

    class UnknownRouter(LifecycleRouter):
        def _dispatch(self, units, runtime_env):
            record_request(units[0])
            self._results = [
                UnitResult(
                    success=False,
                    error=acknowledgement.message,
                    item_count=1,
                    execution_run_ids=[],
                    cancellation_acknowledgement=acknowledgement,
                )
                for _ in units
            ]
            self._done.set()

        def cancel(self):
            return acknowledgement

    monkeypatch.setattr(
        pipeline._default_step_runner,
        "create_lifecycle_router",
        lambda *_args, **_kwargs: UnknownRouter(),
    )

    def uncertain_curator(unit, runtime, event):
        record_request(unit)
        return StagingResult(
            success=False,
            error=acknowledgement.message,
            cancellation_acknowledgement=acknowledgement,
        )

    monkeypatch.setattr(
        "artisan.orchestration.engine.step_executor._run_curator_in_subprocess",
        uncertain_curator,
    )
    try:
        future = pipeline.submit(operation, name="custom diagnostic", compact=False)
        result = future.result(timeout=10)
        state = pipeline._step_tracker.current_state(result.step_run_id)
        assert state.status is result.status is future.status is StepStatus.FAILED
        assert state.cancellation_status is CancellationStatus.UNKNOWN
        assert state.step_name == result.step_name == "custom diagnostic"
        assert result.duration_seconds is not None
        assert result.duration_seconds > 0
        rows = pl.read_delta(tmp_path / "delta" / TablePath.STEPS).sort(
            "state_sequence"
        )
        assert rows["status"].to_list() == [
            "pending",
            "running",
            "running",
            "running",
            "failed",
        ]
        assert rows["cancellation_status"].to_list() == [
            None,
            None,
            "requested",
            "unknown",
            "unknown",
        ]
        assert rows["logical_commit_id"].null_count() == rows.height
        assert read_committed(
            roots["delta_root"],
            TablePath.EXECUTIONS,
            fs=pipeline.config.storage.filesystem(),
        ).is_empty()
        retained = {
            path: path.read_bytes()
            for path in (tmp_path / "staging").rglob("*.parquet")
        }
        if operation is DataGenerator:
            seals = [path for path in retained if path.name == "executions.parquet"]
            assert len(seals) == 1
            execution = verify_worker_seal(
                str(seals[0].parent), pipeline.config.storage.filesystem()
            )
            assert execution.item(0, "success") is False
            assert execution.item(0, "step_run_id") == result.step_run_id
            report = repair_store(
                **roots,
                fs=pipeline.config.storage.filesystem(),
                recover_staging=True,
            )
            assert not report.blocking
            assert any(item.classification == "ineligible" for item in report.items)
        else:
            # This curator reports no execution ID and publishes no worker seal.
            assert not retained
        status = run_status(roots["delta_root"], pipeline.config.pipeline_run_id)
        assert status.last_status is StepStatus.FAILED
        assert status.steps[0].status is StepStatus.FAILED
    finally:
        summary = pipeline.finalize()
    assert pipeline.finalize() is summary
    repeated = pipeline._persist_unknown_cancellation(
        result,
        state.step_spec_id,
        operation=operation,
        step_name=result.step_name,
        duration_seconds=result.duration_seconds,
    )
    assert repeated == result
    late = pipeline._persist_unknown_cancellation(
        result.model_copy(update={"error": "late conflicting result"}),
        state.step_spec_id,
        operation=operation,
        step_name=result.step_name,
        duration_seconds=99.0,
    )
    assert late == result
    assert len(pipeline) == 1
    assert pl.read_delta(tmp_path / "delta" / TablePath.STEPS).height == rows.height
    with PipelineManager.resume(
        **roots, pipeline_run_id=pipeline.config.pipeline_run_id
    ) as resumed:
        assert resumed.current_step == 1
        assert len(resumed) == 0
        assert read_committed(
            roots["delta_root"],
            TablePath.EXECUTIONS,
            fs=resumed.config.storage.filesystem(),
        ).is_empty()
    assert {path: path.read_bytes() for path in retained} == retained
