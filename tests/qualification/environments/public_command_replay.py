"""Qualify a prepared public command and its exact frozen-input diagnostic replay.

The manifest names the matched locked core environment. Preparation is explicit;
submission and replay only resolve prepared targets. Run the controller outside
any managed command attachment, as for the ordinary public local runner.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from artisan.execution.recording.delivery_diagnostics import read_delivery_diagnostics
from artisan.operations.curator import IngestCSV
from artisan.operations.examples import (
    DataGenerator,
    DataTransformerConfig,
    DataTransformerScript,
)
from artisan.orchestration import PipelineManager, replay_execution
from artisan.orchestration.engine.current_outputs import read_current_step_outputs
from artisan.orchestration.engine.retained_execution_query import (
    read_retained_execution_membership,
)
from artisan.orchestration.runners import LocalRunner
from artisan.schemas.execution.admission import AdmissionRecord
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.schemas.operation_config.environment import Environment
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.schemas.provenance.projection import BoundaryProjection
from artisan.storage.core.execution_source import (
    FailedScientificExecutionSource,
    ScientificExecutionSource,
    read_execution_source,
)
from artisan.storage.core.retained_reader import RetainedReader
from artisan.visualization.inspect import inspect_failures


def _assert_replay_inputs(source: AdmissionRecord, replayed: AdmissionRecord) -> None:
    """Require the full frozen input proof with only fresh consuming addresses."""
    assert replayed.recipe == source.recipe
    original_id = source.attempt.assignment.assignment_id
    replay_id = replayed.attempt.assignment.assignment_id
    assert original_id != replay_id
    expected = source.boundary.model_dump(mode="json")
    addresses = [
        address
        for group in expected["boundary_inputs"]
        for address in group["occurrences"]
    ]
    addresses.extend(item["address"] for item in expected["occurrences"])
    addresses.extend(item["boundary"] for item in expected["input_attribution"])
    for capture in expected["frozen_context"]["associations"]:
        addresses.extend((capture["primary"], *capture["members"]))
    for capture in expected["frozen_context"]["references"]:
        addresses.extend(
            (capture["source"], *capture["members"], *capture["direct_parents"])
        )
    replacements = {
        original_id: replay_id,
        f"{original_id}/context": f"{replay_id}/context",
    }
    for address in addresses:
        address["invocation_id"] = replacements[address["invocation_id"]]
    assert replayed.boundary == BoundaryProjection.model_validate(expected)


def qualify(root: Path, runner: LocalRunner) -> dict[str, object]:
    """Require a real command observation, fresh replay and unchanged source selection."""
    for operation in (DataGenerator, DataTransformerConfig, DataTransformerScript):
        runner.prepare(operation)
    with PipelineManager.create(
        "prepared-command",
        str(root / "delta"),
        str(root / "staging"),
        files_root=str(root / "files"),
        working_root=str(root / "working"),
        default_step_runner=runner,
    ) as pipeline:
        generated = pipeline.run(DataGenerator, params={"count": 1, "rows_per_file": 4})
        assert generated.status == StepStatus.SUCCEEDED, generated.error
        configs = pipeline.run(
            DataTransformerConfig,
            inputs={"dataset": generated.output("datasets")},
            params={"scale_factors": [2.0], "noise_amplitudes": [0.0]},
        )
        assert configs.status == StepStatus.SUCCEEDED, configs.error
        transformed = pipeline.run(
            DataTransformerScript,
            inputs={
                "dataset": generated.output("datasets"),
                "config": configs.output("config"),
            },
        )
        assert transformed.status == StepStatus.SUCCEEDED, transformed.error
        selected = [
            item.source
            for item in read_retained_execution_membership(
                pipeline.committer, pipeline_run_id=pipeline.config.pipeline_run_id
            )
            if item.state.step_run_id == transformed.step_run_id
        ]
        assert len(selected) == 1
        source = selected[0]
        assert isinstance(source, ScientificExecutionSource)
        source_outputs = read_current_step_outputs(
            pipeline.committer, transformed.step_run_id
        )
        original_state = pipeline.tracker.current_state(transformed.step_run_id)
        assert source.admission.attempt.software_cacheable
    assert pipeline.ownership_state() == OwnershipState.CLOSED
    original = read_delivery_diagnostics(
        source.plan.receipt.delivery,
        delta_root=pipeline.committer.delta_root,
        fs=pipeline.committer.fs,
        expected_seal_digest=source.plan.receipt.seal_digest,
    )
    assert original is not None
    assert original.cleanup == "closed"
    commands = [
        command
        for invocation in original.invocations
        for command in invocation.command_recording.commands
    ]
    assert len(commands) == 1
    assert commands[0].outcome == "succeeded"
    replay = replay_execution(
        source.plan.receipt.delivery.delivery_id,
        operation=DataTransformerScript,
        delta_root=pipeline.committer.delta_root,
        files_root=pipeline.committer.files_root,
        step_runner=runner,
        working_root=root / "replay-working",
        staging_root=str(root / "replay-staging"),
    )
    assert replay.step_result.status == StepStatus.SUCCEEDED, replay.step_result.error
    assert replay.diagnostic_status == "complete", replay.diagnostic_errors
    assert replay.execution_run_id
    assert replay.execution_run_id != source.plan.receipt.delivery.delivery_id
    replayed = read_execution_source(pipeline.committer, replay.execution_run_id)
    assert isinstance(replayed, ScientificExecutionSource)
    _assert_replay_inputs(source.admission, replayed.admission)
    reader = RetainedReader(pipeline.committer)
    assert [
        item.artifact.artifact_id
        for item in reader.read(replayed.plan.receipt).output("dataset")
    ] == [
        item.artifact.artifact_id
        for item in reader.read(source.plan.receipt).output("dataset")
    ]
    assert pipeline.tracker.current_state(transformed.step_run_id) == original_state
    assert (
        read_current_step_outputs(pipeline.committer, transformed.step_run_id)
        == source_outputs
    )
    observed = read_delivery_diagnostics(
        replayed.plan.receipt.delivery,
        delta_root=pipeline.committer.delta_root,
        fs=pipeline.committer.fs,
        expected_seal_digest=replayed.plan.receipt.seal_digest,
    )
    assert observed is not None
    assert observed.cleanup == "closed"
    replay_commands = [
        command
        for invocation in observed.invocations
        for command in invocation.command_recording.commands
    ]
    assert len(replay_commands) == 1
    assert replay_commands[0].outcome == "succeeded"
    assert replay_commands[0].cwd != commands[0].cwd
    return {
        "source": source.plan.receipt.model_dump(mode="json"),
        "replay": replay.model_dump(mode="json"),
        "source_diagnostics": original.model_dump(mode="json"),
        "replay_diagnostics": observed.model_dump(mode="json"),
        "source_unchanged": True,
    }


def qualify_failed_command(root: Path, runner: LocalRunner) -> dict[str, object]:
    """Require a witnessed nonzero exit, typed failure, owned log and fresh replay."""
    root.mkdir()
    source_file = root / "invalid-number.csv"
    source_file.write_text("id,x,y,z,score\n0,not-numeric,1,2,3\n")
    runner.prepare_curator(IngestCSV())
    with PipelineManager.create(
        "prepared-command-failure",
        str(root / "delta"),
        str(root / "staging"),
        files_root=str(root / "files"),
        working_root=str(root / "working"),
        default_step_runner=runner,
    ) as pipeline:
        ingested = pipeline.run(IngestCSV, inputs=[str(source_file)])
        assert ingested.status == StepStatus.SUCCEEDED, ingested.error
        configs = pipeline.run(
            DataTransformerConfig,
            inputs={"dataset": ingested.output("csv")},
            params={"scale_factors": [2.0], "noise_amplitudes": [0.0]},
        )
        assert configs.status == StepStatus.SUCCEEDED, configs.error
        failed = pipeline.run(
            DataTransformerScript,
            inputs={
                "dataset": ingested.output("csv"),
                "config": configs.output("config"),
            },
        )
        assert failed.status == StepStatus.FAILED, failed.error
        selected = [
            item.source
            for item in read_retained_execution_membership(
                pipeline.committer, pipeline_run_id=pipeline.config.pipeline_run_id
            )
            if item.state.step_run_id == failed.step_run_id
        ]
        assert len(selected) == 1
        source = selected[0]
        assert isinstance(source, FailedScientificExecutionSource)
        delivery = source.unit.failed_delivery
        assert delivery is not None
        original_state = pipeline.tracker.current_state(failed.step_run_id)
    assert pipeline.ownership_state() == OwnershipState.CLOSED
    diagnostic = read_delivery_diagnostics(
        delivery, delta_root=pipeline.committer.delta_root, fs=pipeline.committer.fs
    )
    assert diagnostic is not None
    assert diagnostic.cleanup == "closed"
    assert diagnostic.outcome == "science"
    assert diagnostic.seal_digest is None
    commands = [
        command
        for invocation in diagnostic.invocations
        for command in invocation.command_recording.commands
    ]
    assert len(commands) == 1
    assert commands[0].outcome == "failed"
    assert commands[0].returncode not in (None, 0)
    failures = inspect_failures(
        pipeline.committer.delta_root,
        files_root=pipeline.committer.files_root,
        pipeline_run_id=pipeline.config.pipeline_run_id,
    ).to_dicts()
    assert len(failures) == 1
    row = failures[0]
    assert row["execution_run_id"] == delivery.delivery_id
    assert row["code"] == "op_execute_failed"
    assert row["recovery_hint"] == "REPORT_TO_USER"
    assert row["log"]
    log = root / "logs" / "failures" / row["log"]
    assert log.is_file()
    assert "not-numeric" in log.read_text()
    replay = replay_execution(
        delivery.delivery_id,
        operation=DataTransformerScript,
        delta_root=pipeline.committer.delta_root,
        files_root=pipeline.committer.files_root,
        step_runner=runner,
        working_root=root / "replay-working",
        staging_root=str(root / "replay-staging"),
    )
    assert replay.step_result.status == StepStatus.FAILED, replay.step_result.error
    assert replay.diagnostic_status == "complete", replay.diagnostic_errors
    assert replay.execution_run_id
    assert replay.execution_run_id != delivery.delivery_id
    replayed = read_execution_source(pipeline.committer, replay.execution_run_id)
    assert isinstance(replayed, FailedScientificExecutionSource)
    _assert_replay_inputs(source.admission, replayed.admission)
    assert pipeline.tracker.current_state(failed.step_run_id) == original_state
    assert read_current_step_outputs(pipeline.committer, failed.step_run_id) == ()
    return {
        "source_delivery": delivery.model_dump(mode="json"),
        "source_diagnostics": diagnostic.model_dump(mode="json"),
        "source_failure": {key: str(value) for key, value in row.items()},
        "failure_log_bytes": log.stat().st_size,
        "replay": replay.model_dump(mode="json"),
        "source_unchanged": True,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--environment", default="dev")
    parser.add_argument("--cache-root", required=True, type=Path)
    arguments = parser.parse_args()
    arguments.root.mkdir(parents=True, exist_ok=False)
    runner = LocalRunner(
        environment=Environment.from_pixi(
            arguments.manifest.resolve(), name=arguments.environment
        ),
        cache_root=arguments.cache_root.resolve(),
    )
    report = qualify(arguments.root.resolve(), runner)
    report["failed_command"] = qualify_failed_command(
        arguments.root.resolve() / "failure", runner
    )
    (arguments.root / "report.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
