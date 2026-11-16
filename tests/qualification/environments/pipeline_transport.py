"""Qualify private pipeline coordination through real observed local child workers.

Run with PYTHONDONTWRITEBYTECODE=1 and TMPDIR pointing to an owned local scratch
root. The report measures editable source bytes; it is not installed-wheel or
public factory qualification. No environment preparation or builds are invoked.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import shutil
from pathlib import Path
from tempfile import mkdtemp

from artisan.execution.assignment_transport import AssignmentDispatch
from artisan.execution.curator_worker import LocalCuratorHandle
from artisan.execution.environments.observed_worker import ObservedWorker
from artisan.execution.local_worker import LocalAssignmentHandle
from artisan.execution.worker_requirements import observed_definition_sources
from artisan.operations.curator.ingest_csv import IngestCSV
from artisan.operations.curator.merge import Merge
from artisan.operations.examples.data_transformer import DataTransformer
from artisan.orchestration.engine.current_outputs import read_current_step_outputs
from artisan.orchestration.engine.retained_curator import CuratorResults
from artisan.orchestration.engine.retained_curator_executor import (
    RetainedCuratorExecution,
)
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.orchestration.pipeline_manager import Execution, PipelineManager
from artisan.orchestration.runners.local import LocalRunner
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.schemas.orchestration.step_result import StepResult
from artisan.storage.core.retained_reader import AcceptedArtifact
from artisan.storage.core.store_format import initialize_retained_store
from artisan.storage.io.admission import read_admission_record
from artisan.storage.io.assignment_journal import AssignmentJournal
from artisan.storage.io.curator_admission import read_curator_admission
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.utils.hashing import canonical_json_bytes


def _source_hashes() -> dict[str, str]:
    _, sources = observed_definition_sources(
        (IngestCSV, DataTransformer, Merge), (LocalRunner,)
    )
    return {
        f"{name}/{logical}": hashlib.sha256(path.read_bytes()).hexdigest()
        for name, files in sources.items()
        for logical, path in files.items()
    }


def _open_pipeline(root: Path) -> PipelineManager:
    storage = StorageConfig()
    delta_root, files_root = str(root / "delta"), str(root / "files")
    fs = storage.filesystem()
    initialize_retained_store(delta_root, fs)
    journal = AssignmentJournal(delta_root, fs=fs)
    journal.initialize()
    committer = RetainedCommitter(delta_root, files_root, journal, fs=fs)
    committer.initialize()
    dispatch = AssignmentDispatch(
        delta_root,
        files_root,
        str(root / "staging"),
        storage,
        root / "working",
        {"SYNTHETIC_LITERAL": "literal $HOME with spaces"},
    )
    CuratorResults(
        RetainedStepTracker(committer, "qualification-pipeline"), dispatch.staging_root
    ).initialize()
    return PipelineManager(
        committer,
        dispatch,
        pipeline_run_id="qualification-pipeline",
        default_step_runner=LocalRunner(cache_root=root / "no-install-cache"),
    )


def _output(
    pipeline: PipelineManager, result: StepResult, role: str
) -> tuple[AcceptedArtifact, ...]:
    assert result.status == StepStatus.SUCCEEDED, result.error
    assert result.disposition == StepDisposition.EXECUTED
    assert result.step_run_id is not None
    groups = read_current_step_outputs(pipeline.committer, result.step_run_id)
    return next(group.occurrences for group in groups if group.role == role)


def _execution_report(execution: Execution) -> dict[str, object]:
    assert execution.ownership_state() == OwnershipState.CLOSED
    assert len(execution.handles) == 1
    handle = execution.handles[0]
    assert isinstance(handle, (LocalCuratorHandle, LocalAssignmentHandle))
    assert handle.done()
    assert handle.ownership_state() == OwnershipState.CLOSED
    worker = handle._profile.worker
    assert isinstance(worker, ObservedWorker)
    if isinstance(execution, RetainedCuratorExecution):
        assert isinstance(handle, LocalCuratorHandle)
        admission = read_curator_admission(
            handle.admission,
            delta_root=execution.dispatch.delta_root,
            fs=execution.tracker.committer.fs,
        )
        cacheable, digest = admission.software_cacheable, admission.software_digest
        admission_digest = admission.digest
    else:
        assert isinstance(handle, LocalAssignmentHandle)
        cacheable, digest = (
            handle.attempt.software_cacheable,
            handle.attempt.software_digest,
        )
        admission_digest = execution.tracker.read_admission(
            execution.record.step_run_id
        ).digest
    assert not cacheable
    assert all(
        owner.ownership_state() == OwnershipState.CLOSED
        for owner in execution.worker_owners
    )
    state = execution.tracker.current_state(execution.record.step_run_id)
    return {
        "step_run_id": state.step_run_id,
        "operation": state.step_name,
        "status": state.status.value,
        "disposition": state.disposition.value
        if state.disposition is not None
        else None,
        "software_digest": digest,
        "software_cacheable": cacheable,
        "admission_digest": admission_digest,
        "worker_interpreter": str(worker.executable),
        "worker_executable_sha256": worker.executable_sha256,
        "worker_packages": [
            {"name": package.name, "version": package.version, "digest": package.digest}
            for package in worker.compatibility.packages
        ],
        "handle_type": type(handle).__name__,
        "handles": 1,
        "owner_states": [
            owner.ownership_state().value for owner in execution.worker_owners
        ],
        "accepted_outputs": [
            {
                "role": group.role,
                "occurrences": [
                    {
                        "artifact_id": item.artifact.artifact_id,
                        "artifact_type": item.artifact.artifact_type,
                        "producer": item.reference.model_dump(mode="json"),
                    }
                    for item in group.occurrences
                ],
            }
            for group in read_current_step_outputs(
                execution.tracker.committer, state.step_run_id
            )
        ],
        "closed": True,
    }


def run() -> dict[str, object]:
    before = _source_hashes()
    root = Path(mkdtemp(prefix="artisan-pipeline-transport-"))
    pipeline: PipelineManager | None = None
    try:
        pipeline = _open_pipeline(root)
        source = root / "input.csv"
        original = b"id,x,y,z,score\nsample,1,2,3,4\n"
        source.write_bytes(original)
        params = {"scale_factor": 2.0, "noise_amplitude": 0.0, "seed": 1}
        with pipeline:
            ingested = pipeline.run(IngestCSV, inputs=[str(source)])
            imported = _output(pipeline, ingested, "csv")
            assert len(imported) == 1
            assert imported[0].artifact.content == original
            source.unlink()
            transformed_future = pipeline.submit(
                DataTransformer,
                inputs={"dataset": ingested.output("csv")},
                params=params,
            )
            merged_future = pipeline.submit(
                Merge,
                inputs={
                    "original": ingested.output("csv"),
                    "transformed": transformed_future.output("dataset"),
                },
            )
            transformed = transformed_future.result(timeout=120)
            merged = merged_future.result(timeout=120)
            outputs = _output(pipeline, transformed, "dataset")
            assert len(outputs) == 1
            content = outputs[0].artifact.content
            assert content is not None
            row = next(csv.DictReader(io.StringIO(content.decode())))
            assert row["id"] == "sample"
            assert [float(row[key]) for key in ("x", "y", "z", "score")] == [2, 4, 6, 8]
            combined = _output(pipeline, merged, "merged")
            assert {item.artifact.artifact_id for item in combined} == {
                imported[0].artifact.artifact_id,
                outputs[0].artifact.artifact_id,
            }
            _verify_inputs(pipeline, imported[0], outputs[0])
            repeated = pipeline.run(
                DataTransformer,
                inputs={"dataset": ingested.output("csv")},
                params=params,
            )
            assert _output(pipeline, repeated, "dataset")[0].artifact.content == content
            reports = [
                _execution_report(execution) for execution in pipeline.executions
            ]
            assert len(reports) == 4
            assert not source.exists()
            assert not list(pipeline.dispatch.working_root.iterdir())
            assert not (root / "no-install-cache").exists()
        assert pipeline.ownership_state() == OwnershipState.CLOSED
        after = _source_hashes()
        assert before == after
        report = {
            "runtime_kind": "observed-current-interpreter",
            "installed_build": False,
            "public_pipeline_factory": False,
            "steps": reports,
            "source_inventory_before": before,
            "source_inventory_after": after,
            "source_digest": hashlib.sha256(canonical_json_bytes(before)).hexdigest(),
            "source_unchanged": True,
            "original_removed_after_ingestion": True,
            "exact_upstream_references": True,
            "transformed_values": [2, 4, 6, 8],
            "sync_async_verified": True,
            "observed_repeat_executed": True,
            "no_environment_preparation": True,
            "workers_closed": True,
        }
    finally:
        if pipeline is not None and pipeline.ownership_state() == OwnershipState.CLOSED:
            shutil.rmtree(root)
    assert not root.exists()
    return {**report, "scratch_closed": True}


def _verify_inputs(
    pipeline: PipelineManager,
    imported: AcceptedArtifact,
    transformed: AcceptedArtifact,
) -> None:
    scientific = pipeline.executions[1]
    admission = pipeline.tracker.read_admission(scientific.record.step_run_id)
    assert len(admission.assignments) == 1
    record = read_admission_record(
        admission.assignments[0],
        delta_root=pipeline.committer.delta_root,
        fs=pipeline.committer.fs,
    )
    assert [item.producer for item in record.boundary.input_attribution] == [
        imported.reference
    ]
    curator = pipeline.executions[2]
    handle = curator.handles[0]
    assert isinstance(handle, LocalCuratorHandle)
    selected = read_curator_admission(
        handle.admission,
        delta_root=pipeline.committer.delta_root,
        fs=pipeline.committer.fs,
    )
    assert {
        role.role: tuple(member.producers for member in role.members)
        for role in selected.inputs
    } == {
        "original": ((imported.reference,),),
        "transformed": ((transformed.reference,),),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    args.report.write_text(json.dumps(run(), indent=2) + "\n")


if __name__ == "__main__":
    main()
