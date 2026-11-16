"""Qualify observed curator workers through real admission, streams and acceptance."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory

from artisan.execution.assignment_transport import AssignmentDispatch
from artisan.execution.worker_requirements import observed_definition_sources
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.curator.ingest_csv import IngestCSV
from artisan.operations.curator.merge import Merge
from artisan.orchestration.engine.retained_curator_executor import (
    RetainedCuratorExecution,
)
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.orchestration.runners.local import LocalRunner
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.core.curator_reader import AcceptedCuratorResult, CuratorReader
from artisan.storage.core.retained_reader import AcceptedArtifact
from artisan.storage.core.store_format import initialize_retained_store
from artisan.storage.io.assignment_journal import AssignmentJournal
from artisan.storage.io.curator_admission import read_curator_admission
from artisan.storage.io.curator_plan import read_curator_plan
from artisan.storage.io.input_registration import register_raw_inputs
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.utils.hashing import canonical_json_bytes, serialize_params


def _source_hashes() -> dict[str, str]:
    _, sources = observed_definition_sources((IngestCSV, Merge), (LocalRunner,))
    return {
        f"{name}/{logical}": hashlib.sha256(path.read_bytes()).hexdigest()
        for name, files in sources.items()
        for logical, path in files.items()
    }


def _record(operation: OperationDefinition, number: int) -> StepStartRecord:
    return StepStartRecord(
        step_run_id=f"{number + 1:032x}",
        step_number=number,
        step_name=operation.name,
        operation_class=f"{type(operation).__module__}.{type(operation).__qualname__}",
        params_json=json.dumps(serialize_params(operation)),
        input_refs_json="{}",
        compute_backend="local",
        compute_options_json=json.dumps(
            {"dynamic_output_roles": not operation.outputs}
        ),
        output_roles_json=json.dumps(list(operation.outputs)),
        output_types_json=json.dumps(
            {role: spec.artifact_type for role, spec in operation.outputs.items()}
        ),
    )


def _run_step(
    operation: OperationDefinition,
    number: int,
    resolve_inputs: Callable[[], Mapping[str, Sequence[AcceptedArtifact | str]]],
    *,
    committer: RetainedCommitter,
    runner: LocalRunner,
    dispatch: AssignmentDispatch,
    pool: ThreadPoolExecutor,
    expected: StepStatus = StepStatus.SUCCEEDED,
) -> tuple[AcceptedCuratorResult, dict[str, object]]:
    execution = RetainedCuratorExecution(
        _record(operation, number),
        operation=operation,
        resolve_inputs=resolve_inputs,
        tracker=RetainedStepTracker(committer, "qualification-pipeline"),
        runner=runner,
        dispatch=dispatch,
        runner_resources=RunnerResources(gpus=0),
    )
    execution.results.initialize()
    result = (
        execution.run(pool)
        if number % 2 == 0
        else execution.submit(pool).result(timeout=90)
    )
    assert result.status == expected, result.error
    assert result.disposition == (
        StepDisposition.EXECUTED if expected == StepStatus.SUCCEEDED else None
    )
    assert execution.ownership_state() == OwnershipState.CLOSED
    assert len(execution.handles) == 1
    handle = execution.handles[0]
    assert handle.done()
    assert handle.ownership_state() == OwnershipState.CLOSED
    admission = read_curator_admission(
        handle.admission, delta_root=committer.delta_root, fs=committer.fs
    )
    assert not admission.software_cacheable
    plan = read_curator_plan(committer.delta_root, result.step_run_id, committer.fs)
    assert plan is not None
    accepted = CuratorReader(committer).read_result(plan.step_run_id, plan.plan_digest)
    assert not list(dispatch.working_root.iterdir())
    return accepted, {
        "step_run_id": result.step_run_id,
        "status": result.status.value,
        "disposition": result.disposition.value
        if result.disposition is not None
        else None,
        "software_digest": admission.software_digest,
        "software_cacheable": admission.software_cacheable,
        "admission_digest": admission.digest,
        "plan_digest": plan.plan_digest,
        "closed": True,
        "handles": 1,
    }


def run() -> dict[str, object]:
    before = _source_hashes()
    with (
        TemporaryDirectory(prefix="artisan-curator-transport-") as temporary,
        ThreadPoolExecutor(max_workers=2) as pool,
    ):
        root = Path(temporary)
        storage = StorageConfig()
        delta_root, files_root = str(root / "delta"), str(root / "files")
        fs = storage.filesystem()
        initialize_retained_store(delta_root, fs)
        journal = AssignmentJournal(delta_root, fs=fs)
        journal.initialize()
        committer = RetainedCommitter(delta_root, files_root, journal, fs=fs)
        committer.initialize()
        runner = LocalRunner(cache_root=root / "no-install-cache")
        dispatch = AssignmentDispatch(
            delta_root,
            files_root,
            str(root / "staging"),
            storage,
            root / "working",
            {"SYNTHETIC_LITERAL": "literal $HOME with spaces"},
        )
        source = root / "input.csv"
        source.write_bytes(b"value\n7\n")

        def register() -> dict[str, list[str]]:
            acquired = register_raw_inputs(
                committer,
                [str(source)],
                step_run_id=f"{1:032x}",
                step_number=0,
                operation_name="ingest_csv",
                storage=storage,
            )
            source.unlink()
            return {"file": [item.artifact_id for item in acquired.inputs]}

        ingested, first = _run_step(
            IngestCSV(),
            0,
            register,
            committer=committer,
            runner=runner,
            dispatch=dispatch,
            pool=pool,
        )
        csv = ingested.output("csv")
        assert len(csv) == 1
        assert csv[0].artifact.content == b"value\n7\n"
        merged, second = _run_step(
            Merge(),
            1,
            lambda: {"left": csv, "right": csv},
            committer=committer,
            runner=runner,
            dispatch=dispatch,
            pool=pool,
        )
        assert len(merged.output("merged")) == 1
        assert (
            merged.output("merged")[0].artifact.artifact_id
            == csv[0].artifact.artifact_id
        )
        empty, third = _run_step(
            Merge(),
            2,
            lambda: {"empty": ()},
            expected=StepStatus.FAILED,
            committer=committer,
            runner=runner,
            dispatch=dispatch,
            pool=pool,
        )
        assert empty.outputs == ()
        assert "required output role" in (empty.result.error or "")
        again, fourth = _run_step(
            Merge(),
            3,
            lambda: {"left": csv, "right": csv},
            committer=committer,
            runner=runner,
            dispatch=dispatch,
            pool=pool,
        )
        assert (
            again.output("merged")[0].artifact.artifact_id
            == csv[0].artifact.artifact_id
        )
        assert not source.exists()
        assert not runner.cache_root.exists()
        assert _source_hashes() == before
        report = {
            "steps": [first, second, third, fourth],
            "source_digest": hashlib.sha256(canonical_json_bytes(before)).hexdigest(),
            "source_inventory": before,
            "source_unchanged": True,
            "source_removed_before_ingestion": True,
            "streamed_csv_verified": True,
            "required_empty_output_failure_verified": True,
            "sync_async_verified": True,
            "observed_repeat_executed": True,
            "prepared_execution": False,
        }
    assert not root.exists()
    return {**report, "scratch_closed": True}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    args.report.write_text(json.dumps(run(), indent=2) + "\n")


if __name__ == "__main__":
    main()
