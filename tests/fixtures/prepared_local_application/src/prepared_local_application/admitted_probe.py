"""Exercise an installed local controller, persisted admissions and worker packs."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from unittest.mock import patch

from artisan.composites.base.compiler import Definition, compile_definition
from artisan.execution.assignment_transport import AssignmentDispatch
from artisan.execution.environments.container_runtime import DockerRuntime
from artisan.orchestration.engine.retained_executor import RetainedStepExecution
from artisan.orchestration.engine.retained_inputs import prepare_step_inputs
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.orchestration.runners.local import LocalRunner
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.operation_config.environment import Environment
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.core.retained_reader import AcceptedArtifact, RetainedReader
from artisan.storage.core.store_format import initialize_retained_store
from artisan.storage.io.assignment_journal import AssignmentJournal
from artisan.storage.io.retained_commit import RetainedCommitter
from prepared_local_application import (
    ApplicationMetric,
    ReadFile,
    SelectiveFailure,
    TwoToolSequence,
)


def _step(
    root: Path,
    runner: LocalRunner,
    committer: RetainedCommitter,
    number: int,
    definition: Definition,
    inputs: Mapping[str, Sequence[Artifact | AcceptedArtifact]],
    params: dict[str, Any] | None = None,
) -> RetainedStepExecution:
    plan = compile_definition(definition, params=params)
    identity = f"{number + 1:032x}"
    record = StepStartRecord(
        step_run_id=identity,
        step_number=number,
        step_name=plan.recipe.definition.name,
        operation_class=f"{plan.recipe.definition.module}.{plan.recipe.definition.qualname}",
        params_json=plan.recipe.params_json,
        input_refs_json="{}",
        compute_backend=runner.name,
        compute_options_json="{}",
        output_roles_json=json.dumps([item.role for item in plan.recipe.outputs]),
        output_types_json=json.dumps(
            {item.role: item.artifact_type for item in plan.recipe.outputs}
        ),
    )
    return RetainedStepExecution(
        record,
        resolve_plan=lambda: plan,
        resolve_inputs=lambda compiled: prepare_step_inputs(
            compiled.recipe,
            inputs,
            step_run_id=identity,
            reader=RetainedReader(committer),
        ),
        tracker=RetainedStepTracker(committer, "installed-controller-proof"),
        runner=runner,
        dispatch=AssignmentDispatch(
            committer.delta_root,
            committer.files_root,
            str(root / "staging"),
            StorageConfig(),
            root / "workers",
            {
                "PROBE_LITERAL": "a literal $value with spaces",
                "PROBE_CREDENTIAL": "synthetic-selected",
            },
        ),
        runner_resources=RunnerResources(cpus=1, memory_gb=4, gpus=0),
        batch_strategy=BatchStrategy(
            artifacts_per_unit=1, units_per_worker=2, max_workers=1
        ),
    )


def _outputs(step: RetainedStepExecution, role: str) -> tuple[AcceptedArtifact, ...]:
    return next(
        item.occurrences
        for item in step.results.outputs(step.record.step_run_id)
        if item.role == role
    )


def run(args: argparse.Namespace) -> dict[str, object]:
    """Prepare explicitly, then prove all controller execution paths use lookup only."""
    args.work.mkdir(parents=True, exist_ok=False)
    runner = LocalRunner(
        Environment.from_pixi(args.manifest, name="worker"),
        args.cache,
        pixi=args.pixi,
        tool_backend=args.backend,
        docker=DockerRuntime(args.docker) if args.backend == "docker" else None,
        base_image=args.base_image,
    )
    params = {"manifest": str(args.manifest)}
    runner.prepare(TwoToolSequence, params=params)
    runner.prepare(SelectiveFailure)
    storage = StorageConfig()
    fs = storage.filesystem()
    delta_root = str(args.work / "store")
    initialize_retained_store(delta_root, fs)
    journal = AssignmentJournal(delta_root, fs=fs)
    journal.initialize()
    committer = RetainedCommitter(delta_root, str(args.work / "files"), journal, fs=fs)
    committer.initialize()
    source = [
        ApplicationMetric.draft({"value": value}, "input.json").finalize()
        for value in (5, 6)
    ]
    assert all(
        type(ApplicationMetric.from_row(item.to_row())) is ApplicationMetric
        for item in source
    )
    expected = runner.resolve(
        compile_definition(TwoToolSequence, params=params),
        RunnerResources(cpus=1, memory_gb=4, gpus=0),
    )
    with (
        patch(
            "artisan.orchestration.runners.local.prepare_native_environment",
            side_effect=AssertionError("Run installed software"),
        ),
        patch(
            "artisan.orchestration.runners.local.prepare_oci_environment",
            side_effect=AssertionError("Run built software"),
        ),
        patch(
            "artisan.orchestration.runners.local.prepare_worker_qualification",
            side_effect=AssertionError("Run qualified software"),
        ),
        patch(
            "artisan.execution.environments.worker_qualification.qualify_local_worker",
            side_effect=AssertionError("Lookup launched qualification"),
        ),
        ThreadPoolExecutor(max_workers=1) as pool,
    ):
        first = _step(
            args.work, runner, committer, 0, TwoToolSequence, {"source": source}, params
        )
        result = first.run(pool)
        assert result.status == StepStatus.SUCCEEDED, result.error
        metrics = _outputs(first, "metrics")
        assert [item.artifact.values["value"] for item in metrics] == [16, 18]
        assert len({item.artifact.values["worker_pid"] for item in metrics}) == 1
        files = _outputs(first, "files")
        assert [
            json.loads(item.artifact.read_content())["value"] for item in files
        ] == [16, 18]
        for handle in first.handles:
            with first.reader.capture_inputs(handle.result()) as captured:
                assert all(
                    isinstance(item, ApplicationMetric)
                    for item in captured.artifacts.values()
                )
        cached = _step(
            args.work, runner, committer, 1, TwoToolSequence, {"source": source}, params
        )
        cached_result = cached.run(pool)
        assert cached_result.disposition == StepDisposition.CACHE_HIT, (
            cached_result.error
        )
        assert not cached.handles
        changed_params = {**params, "increment": 2}
        changed_binding = runner.resolve(
            compile_definition(TwoToolSequence, params=changed_params),
            RunnerResources(cpus=1, memory_gb=4, gpus=0),
        )
        assert changed_binding.software_digest == expected.software_digest
        changed = _step(
            args.work,
            runner,
            committer,
            2,
            TwoToolSequence,
            {"source": source[:1]},
            changed_params,
        )
        changed_result = changed.run(pool)
        assert changed_result.status == StepStatus.SUCCEEDED, changed_result.error
        assert _outputs(changed, "metrics")[0].artifact.values["value"] == 18
        downstream = _step(args.work, runner, committer, 3, ReadFile, {"source": files})
        downstream_result = downstream.run(pool)
        assert downstream_result.status == StepStatus.SUCCEEDED, downstream_result.error
        assert [
            item.artifact.values["value"] for item in _outputs(downstream, "metrics")
        ] == [16, 18]
        partial = _step(
            args.work,
            runner,
            committer,
            4,
            SelectiveFailure,
            {
                "source": [
                    source[0],
                    ApplicationMetric.draft({"value": -1}, "negative.json").finalize(),
                ]
            },
        )
        partial_result = partial.run(pool)
        assert partial_result.status == StepStatus.PARTIAL, partial_result.error
        assert partial_result.succeeded_count == partial_result.failed_count == 1
        assert "synthetic-selected" not in (partial_result.error or "")
    assert not list((args.work / "workers").iterdir())
    return {
        "status": "passed",
        "backend": args.backend,
        "software_digest": expected.software_digest,
        "compatibility": expected.compatibility.model_dump(mode="json"),
        "worker_target": expected.worker.digest,
        "tool_targets": [item.target.digest for item in expected.tools],
        "ordered_values": [16, 18],
        "same_process_pack": True,
        "registered_schema_hydrated": True,
        "downstream_full_carrier_localized": True,
        "cache": cached_result.disposition.value,
        "numeric_change_reuses_software": True,
        "scientific_partial": partial_result.model_dump(mode="json"),
        "workers_closed": True,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("backend", choices=("pixi", "docker"))
    for name in ("manifest", "cache", "pixi", "work", "report"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--docker", type=Path, default=Path("/usr/bin/docker"))
    parser.add_argument("--base-image")
    args = parser.parse_args()
    report = run(args)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"status": report["status"], "backend": report["backend"]}))


if __name__ == "__main__":
    main()
