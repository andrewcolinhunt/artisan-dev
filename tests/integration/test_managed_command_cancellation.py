"""Exercise command ownership through native creator dispatch and diagnostic replay."""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from pydantic import Field

from artisan.operations.examples import DataGenerator
from artisan.orchestration import PipelineManager, replay_execution
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.command_record import CommandRecording
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
from artisan.schemas.orchestration.step_lifecycle import CancellationStatus, StepStatus
from artisan.schemas.specs.input_models import ExecuteInput
from artisan.storage.core.committed_scan import read_committed
from artisan.utils.configuration_secrets import COMMAND_ATTACHMENT_ENV
from artisan.utils.external_tools import run_command

pytestmark = pytest.mark.integration

_COMMAND = """
import json, os, pathlib, signal, sys, time
root = pathlib.Path(sys.argv[1])
root.mkdir(exist_ok=True)
waiting = sys.argv[2] == 'wait'
child = None
if waiting:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.alarm(30)
    child = os.fork()
    if child == 0:
        time.sleep(25)
        os._exit(0)
identity = {'owner': os.getppid(), 'command': os.getpid(),
            'child': child, 'worker': int(sys.argv[3])}
with (root / 'invocations.jsonl').open('a') as handle:
    handle.write(json.dumps(identity) + '\\n')
marker = root / 'ready.tmp'
marker.write_text(json.dumps(identity))
marker.replace(root / 'ready.json')
if waiting:
    time.sleep(25)
print('managed command finished')
"""


class ManagedCreator(DataGenerator):
    """Run a real owned command before producing ordinary source artifacts."""

    name = "managed_cancellation_creator"

    class Params(DataGenerator.Params):
        marker_root: str = Field(description="Test-owned process observation files.")
        wait_for_cancel: bool = Field(
            default=False, description="Wait for cancellation."
        )
        streaming: bool = Field(default=False, description="Stream command output.")

    params: Params

    def execute_function(self, inputs: ExecuteInput) -> Any:
        run_command(
            LocalEnvironmentSpec(),
            [
                sys.executable,
                "-c",
                _COMMAND,
                self.params.marker_root,
                "wait" if self.params.wait_for_cancel else "complete",
                str(os.getpid()),
            ],
            stream_output=self.params.streaming,
        )
        return super().execute_function(inputs)


def _wait_ready(root: Path) -> dict[str, int | None]:
    marker = root / "ready.json"
    deadline = time.monotonic() + 15
    while not marker.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert marker.exists(), "managed command never reported startup"
    return json.loads(marker.read_text())


def _assert_processes_stopped(identities: dict[str, int | None]) -> None:
    for pid in identities.values():
        if pid is not None:
            with pytest.raises(ProcessLookupError):
                os.kill(pid, 0)


@pytest.mark.parametrize("streaming", [False, True])
def test_creator_pipeline_cancellation_stops_worker_command_and_grandchild(
    tmp_path: Path, streaming: bool
) -> None:
    marker_root = tmp_path / "markers"
    runtime = RuntimeEnvironment(
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
    )
    with PipelineManager.create(
        "owned-creator-cancel",
        delta_root=runtime.delta_root,
        staging_root=runtime.staging_root,
        working_root=runtime.working_root,
    ) as pipeline:
        future = pipeline.submit(
            ManagedCreator,
            params={
                "marker_root": str(marker_root),
                "wait_for_cancel": True,
                "streaming": streaming,
                "count": 1,
                "rows_per_file": 1,
            },
            compact=False,
        )
        try:
            identities = _wait_ready(marker_root)
        finally:
            started = time.monotonic()
            pipeline.cancel()
        result = future.result(timeout=15)
        assert time.monotonic() - started < 12

    assert result.status is StepStatus.CANCELLED
    assert result.cancellation_status is CancellationStatus.CONFIRMED
    _assert_processes_stopped(identities)
    assert list((tmp_path / "work").rglob("execute"))
    assert read_committed(
        runtime.delta_root, TablePath.EXECUTIONS, fs=runtime.storage.filesystem()
    ).is_empty()


def test_creator_replay_uses_fresh_owner_and_keeps_original_evidence(
    tmp_path: Path,
) -> None:
    marker_root = tmp_path / "markers"
    runtime = RuntimeEnvironment(
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "debug" / "staging"),
        working_root=str(tmp_path / "debug" / "work"),
        files_root=str(tmp_path / "debug" / "files"),
        failure_logs_root=str(tmp_path / "debug" / "logs"),
    )
    with PipelineManager.create(
        "owned-creator-source",
        delta_root=runtime.delta_root,
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
    ) as pipeline:
        source_step = pipeline.run(
            ManagedCreator,
            params={"marker_root": str(marker_root), "count": 1, "rows_per_file": 1},
            compact=False,
        )
    assert source_step.status is StepStatus.SUCCEEDED
    before = read_committed(
        runtime.delta_root, TablePath.EXECUTIONS, fs=runtime.storage.filesystem()
    )
    assert before.height == 1
    original = before.row(0, named=True)
    first = _wait_ready(marker_root)
    _assert_processes_stopped(first)

    replay = replay_execution(
        original["execution_run_id"], runtime=runtime, operation_class=ManagedCreator
    )

    assert replay.step_result.status is StepStatus.SUCCEEDED
    assert replay.diagnostic_status == "complete"
    assert replay.execution_run_id != original["execution_run_id"]
    invocations = [
        json.loads(line)
        for line in (marker_root / "invocations.jsonl").read_text().splitlines()
    ]
    assert len(invocations) == 2
    assert invocations[0]["owner"] != invocations[1]["owner"]
    assert invocations[0]["worker"] != invocations[1]["worker"]
    _assert_processes_stopped(invocations[1])
    after = read_committed(
        runtime.delta_root, TablePath.EXECUTIONS, fs=runtime.storage.filesystem()
    )
    assert after.filter(
        pl.col("execution_run_id") == original["execution_run_id"]
    ).equals(before)
    replayed = after.filter(pl.col("execution_run_id") == replay.execution_run_id).row(
        0, named=True
    )
    for row in (original, replayed):
        recording = CommandRecording.model_validate_json(row["command_recording"])
        assert len(recording.commands) == 1
        assert recording.commands[0].outcome == "succeeded"
        assert recording.commands[0].launch_seconds is not None
        assert all(key not in row["replay_snapshot"] for key in COMMAND_ATTACHMENT_ENV)
    assert list(Path(replay.diagnostic_roots["working_root"]).rglob("execute"))
