"""Replay through real workers, logical commits, and both cache selectors."""

from __future__ import annotations

import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from pydantic import Field

from artisan.errors import ArtifactIntegrityError, CommitError
from artisan.operations.curator.ingest_data import IngestData
from artisan.operations.curator.merge import Merge
from artisan.operations.examples import CsvHead
from artisan.operations.examples.data_generator import DataGenerator
from artisan.operations.examples.data_transformer import DataTransformer
from artisan.orchestration import PipelineManager, StepStatus, replay_execution
from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.schemas.enums import CachePolicy, TablePath
from artisan.schemas.execution.cache_result import CacheMiss
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.storage.cache.cache_lookup import cache_lookup
from artisan.storage.core.committed_scan import read_committed
from artisan.utils.external_tools import run_command


class MultiCommandGenerator(DataGenerator):
    """Fail on the second command until an external debugging marker exists."""

    name = "replay_multi_command_generator"

    class Params(DataGenerator.Params):
        marker: str = Field(description="External file enabling successful retry.")

    params: Params

    def execute_function(self, inputs: Any) -> Any:
        run_command(
            self.environments.current(),
            [sys.executable, "-c", "print('first command')"],
            log_path=inputs.log_path,
        )
        run_command(
            self.environments.current(),
            [
                sys.executable,
                "-c",
                "import os,sys; sys.exit(0 if os.path.exists(sys.argv[1]) else 9)",
                self.params.marker,
            ],
            log_path=inputs.log_path,
            log_mode="a",
        )
        return super().execute_function(inputs)


class SecretGenerator(DataGenerator):
    """A required diagnostic replacement participates in concrete Params."""

    name = "replay_secret_generator_integration"

    class Params(DataGenerator.Params):
        token: str = Field(description="Credential used by the declared operation.")

    params: Params


pytestmark = pytest.mark.integration


def _runtime(root):
    debug = root / "debug"
    return RuntimeEnvironment(
        delta_root=str(root / "delta"),
        staging_root=str(debug / "staging"),
        working_root=str(debug / "work"),
        files_root=str(debug / "files"),
        failure_logs_root=str(debug / "logs"),
    )


def _read(runtime, table):
    return read_committed(runtime.delta_root, table, fs=runtime.storage.filesystem())


def test_replay_python_unit_duplicates_fresh_ids_and_cache_exclusion(tmp_path):
    manager = PipelineManager.create(
        "source",
        str(tmp_path / "delta"),
        str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
    )
    generated = manager.run(
        DataGenerator, params={"count": 2, "seed": 7}, compact=False
    )
    assert generated.status == StepStatus.SUCCEEDED
    runtime = _runtime(tmp_path)
    edges = _read(runtime, TablePath.EXECUTION_EDGES)
    ids = edges.filter(pl.col("direction") == "output")["artifact_id"].to_list()
    transformed = manager.run(
        DataTransformer,
        inputs={"dataset": [ids[1], ids[0], ids[1]]},
        params={"seed": 7},
        batch_strategy={"artifacts_per_unit": 3},
        compact=False,
    )
    manager.finalize()
    assert transformed.status == StepStatus.SUCCEEDED
    before = _read(runtime, TablePath.EXECUTIONS)
    source = before.filter(pl.col("step_run_id") == transformed.step_run_id).row(
        0, named=True
    )
    level = logging.getLogger("artisan").level
    result = replay_execution(source["execution_run_id"], runtime=runtime)
    assert logging.getLogger("artisan").level == level
    assert result.step_result.status == StepStatus.SUCCEEDED
    assert result.diagnostic_status == "complete"
    assert result.execution_run_id != source["execution_run_id"]
    assert result.step_run_id != transformed.step_run_id
    assert result.step_result.step_number == transformed.step_number
    after = _read(runtime, TablePath.EXECUTIONS)
    assert after.filter(
        pl.col("execution_run_id").is_in(before["execution_run_id"].to_list())
    ).equals(before)
    replayed = after.filter(pl.col("execution_run_id") == result.execution_run_id).row(
        0, named=True
    )
    snapshot = json.loads(replayed["replay_snapshot"])
    assert [entry["artifact_id"] for entry in snapshot["inputs"]["dataset"]] == [
        ids[1],
        ids[0],
        ids[1],
    ]
    assert replayed["replay_of_execution_run_id"] == source["execution_run_id"]
    artifact_edges = _read(runtime, TablePath.ARTIFACT_EDGES)
    columns = [
        "source_artifact_id",
        "target_artifact_id",
        "source_role",
        "target_role",
        "source_artifact_type",
        "target_artifact_type",
        "group_id",
    ]
    source_edges = artifact_edges.filter(
        pl.col("execution_run_id") == source["execution_run_id"]
    )
    replay_edges = artifact_edges.filter(
        pl.col("execution_run_id") == result.execution_run_id
    )
    assert source_edges.height > 0
    assert set(source_edges.select(columns).iter_rows()) == set(
        replay_edges.select(columns).iter_rows()
    )
    assert (
        cache_lookup(
            runtime.delta_root,
            source["execution_spec_id"],
            runtime.storage.filesystem(),
        ).execution_run_id
        == source["execution_run_id"]
    )
    steps = _read(runtime, TablePath.STEPS)
    diagnostic = steps.filter(pl.col("step_run_id") == result.step_run_id)
    assert set(diagnostic["replay_of_execution_run_id"].to_list()) == {
        source["execution_run_id"]
    }
    tracker = StepTracker(runtime.delta_root)
    for policy in CachePolicy:
        cached = tracker.check_cache(diagnostic["step_spec_id"][0], policy)
        assert cached is None or cached.source_step_run_id != result.step_run_id
    assert result.diagnostic_roots["failure_logs_root"].startswith(
        str(tmp_path / "debug" / "logs")
    )


def test_replay_generative_unit(tmp_path):
    manager = PipelineManager.create(
        "source",
        str(tmp_path / "delta"),
        str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
    )
    manager.run(DataGenerator, params={"count": 2, "seed": 19}, compact=False)
    manager.finalize()
    runtime = _runtime(tmp_path)
    source = _read(runtime, TablePath.EXECUTIONS)["execution_run_id"][0]
    result = replay_execution(source, runtime=runtime)
    assert result.step_result.status == StepStatus.SUCCEEDED
    assert result.execution_run_id is not None
    rows = _read(runtime, TablePath.EXECUTIONS)
    assert all(not json.loads(value)["commands"] for value in rows["command_recording"])


def test_replay_composite_child_only_appends_one_execution(tmp_path):
    from .test_composite import GenTransformMetrics

    manager = PipelineManager.create(
        "source",
        str(tmp_path / "delta"),
        str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
    )
    manager.submit_composite(GenTransformMetrics)
    manager.finalize()
    runtime = _runtime(tmp_path)
    before = _read(runtime, TablePath.EXECUTIONS)
    source = before.filter(pl.col("operation_name") == DataTransformer.name).row(
        0, named=True
    )
    result = replay_execution(source["execution_run_id"], runtime=runtime)
    assert result.step_result.status == StepStatus.SUCCEEDED
    assert result.step_result.step_number == source["origin_step_number"]
    after = _read(runtime, TablePath.EXECUTIONS)
    assert after.height == before.height + 1
    assert after.filter(
        pl.col("execution_run_id").is_in(before["execution_run_id"].to_list())
    ).equals(before)


def test_replay_multicommand_failed_attempt_then_success(tmp_path):
    manager = PipelineManager.create(
        "source",
        str(tmp_path / "delta"),
        str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
    )
    failed = manager.run(
        MultiCommandGenerator,
        params={"marker": str(tmp_path / "allow"), "seed": 4},
        compact=False,
    )
    manager.finalize()
    assert failed.status == StepStatus.FAILED
    runtime = _runtime(tmp_path)
    source = _read(runtime, TablePath.EXECUTIONS).row(0, named=True)
    assert len(json.loads(source["command_recording"])["commands"]) == 2
    (tmp_path / "allow").touch()
    result = replay_execution(source["execution_run_id"], runtime=runtime)
    assert result.step_result.status == StepStatus.SUCCEEDED
    row = (
        _read(runtime, TablePath.EXECUTIONS)
        .filter(pl.col("execution_run_id") == result.execution_run_id)
        .row(0, named=True)
    )
    assert [
        command["outcome"]
        for command in json.loads(row["command_recording"])["commands"]
    ] == ["succeeded", "succeeded"]
    assert list(Path(result.diagnostic_roots["working_root"]).rglob("tool_output.log"))


@pytest.mark.parametrize("operation", [CsvHead, IngestData, Merge])
def test_replay_command_and_both_curator_result_types(tmp_path, operation):
    manager = PipelineManager.create(
        "source",
        str(tmp_path / "delta"),
        str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
    )
    if operation is IngestData:
        source_file = tmp_path / "input.csv"
        source_file.write_text("x,y\n1,2\n")
        step = manager.run(operation, inputs=[str(source_file)], compact=False)
    else:
        generated = manager.run(DataGenerator, params={"seed": 19}, compact=False)
        role = "dataset" if operation is CsvHead else "one"
        step = manager.run(
            operation, inputs={role: generated.output("datasets")}, compact=False
        )
    manager.finalize()
    assert step.status == StepStatus.SUCCEEDED
    runtime = _runtime(tmp_path)
    source = _read(runtime, TablePath.EXECUTIONS).filter(
        pl.col("step_run_id") == step.step_run_id
    )["execution_run_id"][0]
    result = replay_execution(source, runtime=runtime)
    assert result.step_result.status == StepStatus.SUCCEEDED
    assert "current committed store" in result.reproducibility_notes[0]
    if operation is IngestData:
        before = _read(runtime, TablePath.STEPS)
        source_file.write_text("x,y\nchanged,content\n")
        with pytest.raises(ArtifactIntegrityError):
            replay_execution(source, runtime=runtime)
        assert _read(runtime, TablePath.STEPS).equals(before)


def test_replay_secret_replacement_absent_from_diagnostic_framework_rows(
    tmp_path, monkeypatch
):
    manager = PipelineManager.create(
        "source",
        str(tmp_path / "delta"),
        str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
    )
    manager.run(
        SecretGenerator, params={"token": "historical-secret", "seed": 4}, compact=False
    )
    manager.finalize()
    runtime = _runtime(tmp_path)
    source = _read(runtime, TablePath.EXECUTIONS)["execution_run_id"][0]
    monkeypatch.setenv("REPLAY_FRESH_TOKEN", "unique-new-replay-secret")
    result = replay_execution(
        source, runtime=runtime, replacement_env={"/params/token": "REPLAY_FRESH_TOKEN"}
    )
    assert result.step_result.status == StepStatus.SUCCEEDED
    execution = _read(runtime, TablePath.EXECUTIONS).filter(
        pl.col("execution_run_id") == result.execution_run_id
    )
    steps = _read(runtime, TablePath.STEPS).filter(
        pl.col("step_run_id") == result.step_run_id
    )
    assert "unique-new-replay-secret" not in str(execution.to_dicts())
    assert "unique-new-replay-secret" not in str(steps.to_dicts())
    assert "unique-new-replay-secret" not in result.model_dump_json()
    assert isinstance(
        cache_lookup(
            runtime.delta_root,
            execution["execution_spec_id"][0],
            runtime.storage.filesystem(),
        ),
        CacheMiss,
    )
    tracker = StepTracker(runtime.delta_root)
    for policy in CachePolicy:
        assert tracker.check_cache(steps["step_spec_id"][0], policy) is None


def test_replay_cli_json_preserves_stdout_for_machine_result(tmp_path):
    manager = PipelineManager.create(
        "source",
        str(tmp_path / "delta"),
        str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
    )
    manager.run(DataGenerator, params={"seed": 4}, compact=False)
    manager.finalize()
    runtime = _runtime(tmp_path)
    source = _read(runtime, TablePath.EXECUTIONS)["execution_run_id"][0]
    process = subprocess.run(
        [
            sys.executable,
            "-m",
            "artisan.cli",
            "execution",
            "replay",
            source,
            "--delta-root",
            runtime.delta_root,
            "--debug-root",
            str(tmp_path / "cli-debug"),
            "--json",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert process.returncode == 0, process.stderr
    payload = json.loads(process.stdout)
    assert payload["source_execution_run_id"] == source
    assert payload["execution_run_id"] != source
    assert payload["step_result"]["status"] == "succeeded"


def test_replay_cancelled_before_dispatch_has_no_committed_execution(
    tmp_path, monkeypatch
):
    manager = PipelineManager.create(
        "source",
        str(tmp_path / "delta"),
        str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
    )
    manager.run(DataGenerator, params={"seed": 4}, compact=False)
    manager.finalize()
    runtime = _runtime(tmp_path)
    before = _read(runtime, TablePath.EXECUTIONS)
    source = before["execution_run_id"][0]
    original = PipelineManager._run_prepared_unit

    def cancel_before_dispatch(self, *args, **kwargs):
        self._cancel_event.set()
        return original(self, *args, **kwargs)

    monkeypatch.setattr(PipelineManager, "_run_prepared_unit", cancel_before_dispatch)
    result = replay_execution(source, runtime=runtime)
    assert result.step_result.status == StepStatus.CANCELLED
    assert result.execution_run_id is None
    assert result.diagnostic_status == "unavailable"
    assert _read(runtime, TablePath.EXECUTIONS).equals(before)
    steps = _read(runtime, TablePath.STEPS).filter(
        pl.col("step_run_id") == result.step_run_id
    )
    assert set(steps["replay_of_execution_run_id"].to_list()) == {source}


def test_replay_commit_failure_retains_structured_attempt_context(
    tmp_path, monkeypatch
):
    from artisan.storage.io.commit import DeltaCommitter

    manager = PipelineManager.create(
        "source",
        str(tmp_path / "delta"),
        str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
    )
    manager.run(DataGenerator, params={"seed": 4}, compact=False)
    manager.finalize()
    runtime = _runtime(tmp_path)
    before = _read(runtime, TablePath.EXECUTIONS)

    def fail_commit(self, plan, **kwargs):
        raise CommitError(
            plan.logical_commit_id,
            "executions",
            "plan",
            [],
            [],
            "persistence test failure",
        )

    monkeypatch.setattr(DeltaCommitter, "commit_logical", fail_commit)
    with pytest.raises(CommitError) as caught:
        replay_execution(before["execution_run_id"][0], runtime=runtime)
    assert caught.value.code == "commit_failed"
    assert "Diagnostic run replay-" in caught.value.envelope.hint
    assert caught.value.logical_commit_id.startswith("step_result:")
    assert _read(runtime, TablePath.EXECUTIONS).equals(before)


def test_replay_worker_code_must_match_selected_driver_even_with_override(
    tmp_path, monkeypatch
):
    import importlib

    replay_module = importlib.import_module("artisan.orchestration.replay")
    manager = PipelineManager.create(
        "source",
        str(tmp_path / "delta"),
        str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
    )
    manager.run(DataGenerator, params={"seed": 4}, compact=False)
    manager.finalize()
    runtime = _runtime(tmp_path)
    source = _read(runtime, TablePath.EXECUTIONS)["execution_run_id"][0]
    identity = replay_module.operation_identity(DataGenerator).model_copy(
        update={"module_digest": "selected-driver-code"}
    )
    monkeypatch.setattr(replay_module, "operation_identity", lambda operation: identity)
    result = replay_execution(source, runtime=runtime, allow_code_change=True)
    assert result.step_result.status == StepStatus.FAILED
    assert result.execution_run_id is not None
    row = (
        _read(runtime, TablePath.EXECUTIONS)
        .filter(pl.col("execution_run_id") == result.execution_run_id)
        .row(0, named=True)
    )
    assert json.loads(row["error_envelope"])["code"] == "replay_code_changed"
