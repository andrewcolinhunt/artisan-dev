"""Tests for compute_provider routing through the creator lifecycle."""

from __future__ import annotations

import json
import os
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from fixtures.store_format import commit_test_tables
from fsspec.implementations.local import LocalFileSystem

from artisan.execution.compute.local import LocalExecuteRouter
from artisan.execution.executors.creator import (
    LifecycleResult,
    run_creator_flow,
    run_creator_lifecycle,
)
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.base.per_artifact import PerArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.table_schemas import ARTIFACT_INDEX_SCHEMA


def _setup_delta(base_path: Path, artifacts: list[MetricArtifact]) -> None:
    """Write Delta Lake tables for test input artifacts."""
    commit_test_tables(
        str(base_path),
        str(base_path.parent / "seed-staging"),
        LocalFileSystem(),
        {
            "artifacts/metrics": pl.DataFrame(
                [artifact.to_row() for artifact in artifacts],
                schema=MetricArtifact.POLARS_SCHEMA,
            ),
            TablePath.ARTIFACT_INDEX.value: pl.DataFrame(
                [
                    {
                        "artifact_id": artifact.artifact_id,
                        "artifact_type": artifact.artifact_type,
                        "origin_step_number": artifact.origin_step_number,
                        "metadata": json.dumps(artifact.metadata),
                    }
                    for artifact in artifacts
                ],
                schema=ARTIFACT_INDEX_SCHEMA,
            ),
        },
        step_run_id="f" * 32,
        operation_name="seed_compute_routing",
    )


class _SimpleOp(OperationDefinition):
    """Minimal test operation for compute_provider routing validation."""

    class InputRole(StrEnum):
        source = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "compute_routing_test"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.source: InputSpec(artifact_type="metric", required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type="metric",
            infer_lineage_from={"inputs": ["source"]},
        ),
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            role: PerArtifact([a.materialized_path for a in artifacts])
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute_function(self, inputs: ExecuteInput) -> dict:
        for path in inputs.inputs["source"]:
            with open(path) as fh:
                content = json.loads(fh.read())
            content["routed"] = True
            stem = os.path.splitext(os.path.basename(path))[0]
            out = os.path.join(inputs.execute_dir, f"{stem}_out.json")
            with open(out, "w") as fh:
                fh.write(json.dumps(content))
        return {}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        drafts = []
        for fp in inputs.file_outputs:
            if fp.endswith(".json"):
                with open(fp) as fh:
                    content = json.loads(fh.read())
                drafts.append(
                    MetricArtifact.draft(
                        content=content,
                        original_name=os.path.basename(fp),
                        step_number=inputs.step_number,
                    )
                )
        return ArtifactResult(success=True, artifacts={"output": drafts})


@pytest.fixture
def delta_env(tmp_path: Path):
    """Create Delta root, working dir, and staging dir with one input."""
    base = tmp_path / "delta"
    artifact = MetricArtifact.draft(
        {"value": 1}, "test_metric.json", step_number=0
    ).finalize()
    aid = artifact.artifact_id

    _setup_delta(
        base,
        [artifact],
    )

    working = tmp_path / "working"
    working.mkdir()
    staging = tmp_path / "staging"
    staging.mkdir()

    runtime_env = RuntimeEnvironment(
        delta_root=str(base),
        working_root=str(working),
        staging_root=str(staging),
    )
    return runtime_env, aid


class TestCreatorComputeRouting:
    def test_explicit_local_router_matches_baseline(self, delta_env):
        """Passing an explicit LocalExecuteRouter produces identical results."""
        runtime_env, input_id = delta_env

        unit = ExecutionUnit(
            operation=_SimpleOp(),
            inputs={"source": [input_id]},
            execution_spec_id="spec_cr" + "0" * 26,
            step_number=1,
        )

        result = run_creator_lifecycle(
            unit,
            runtime_env,
            execute_router=LocalExecuteRouter(),
        )

        assert isinstance(result, LifecycleResult)
        assert "output" in result.artifacts
        assert len(result.artifacts["output"]) == 1
        assert result.artifacts["output"][0].artifact_id is not None
        assert len(result.edges) >= 1

    def test_default_router_from_operation_config(self, delta_env):
        """When execute_router is None, router is created from operation config."""
        runtime_env, input_id = delta_env

        unit = ExecutionUnit(
            operation=_SimpleOp(),
            inputs={"source": [input_id]},
            execution_spec_id="spec_df" + "0" * 26,
            step_number=1,
        )

        # No execute_router — should auto-create from operation.compute_provider
        result = run_creator_lifecycle(unit, runtime_env)

        assert isinstance(result, LifecycleResult)
        assert "output" in result.artifacts
        assert len(result.artifacts["output"]) == 1


class TestRunCreatorFlowRouterForwarding:
    """Verify run_creator_flow forwards the execute_router parameter."""

    @patch("artisan.execution.executors.creator.run_creator_lifecycle")
    def test_forwards_explicit_router(self, mock_lifecycle):
        """An explicit execute_router is forwarded to run_creator_lifecycle."""
        mock_lifecycle.return_value = LifecycleResult(
            input_artifacts={},
            artifacts={},
            edges=[],
            timings={},
        )

        unit = MagicMock(
            replay_snapshot=ReplaySnapshot.unavailable("mock_unit"),
            replay_of_execution_run_id=None,
            replay_sensitive_values=(),
        )
        unit.operation = _SimpleOp()
        unit.user_overrides = None
        runtime_env = MagicMock()
        router = MagicMock()

        run_creator_flow(unit, runtime_env, execute_router=router)

        _, kwargs = mock_lifecycle.call_args
        assert kwargs["execute_router"] is router

    @patch("artisan.execution.executors.creator.run_creator_lifecycle")
    def test_default_forwards_none(self, mock_lifecycle):
        """Without execute_router, None is forwarded (lifecycle auto-creates)."""
        mock_lifecycle.return_value = LifecycleResult(
            input_artifacts={},
            artifacts={},
            edges=[],
            timings={},
        )

        unit = MagicMock(
            replay_snapshot=ReplaySnapshot.unavailable("mock_unit"),
            replay_of_execution_run_id=None,
            replay_sensitive_values=(),
        )
        unit.operation = _SimpleOp()
        unit.user_overrides = None
        runtime_env = MagicMock()

        run_creator_flow(unit, runtime_env)

        _, kwargs = mock_lifecycle.call_args
        assert kwargs["execute_router"] is None


def test_endpoint_dispatch_contexts_preserve_input_order_and_isolate_units(
    monkeypatch, tmp_path
):
    import sys
    import threading
    from concurrent.futures import ThreadPoolExecutor

    from artisan.execution.compute.endpoint import EndpointExecuteRouter
    from artisan.execution.recording.commands import (
        capture_commands,
        current_recorder,
        invocation_scope,
    )
    from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
    from artisan.utils.external_tools import run_command

    barriers = {unit: threading.Barrier(3) for unit in ("a", "b")}
    release = {(unit, i): threading.Event() for unit in ("a", "b") for i in range(3)}

    def endpoint(operation, execute_input):
        unit = execute_input.inputs["unit"]
        index = execute_input.inputs["index"]
        parent = current_recorder()
        with invocation_scope() as slot:
            with capture_commands(location="endpoint") as worker, invocation_scope():
                for _ in range(2):
                    run_command(
                        LocalEnvironmentSpec(), [sys.executable, "-c", "pass", unit]
                    )
            barriers[unit].wait(timeout=10)
            if index < 2:
                assert release[(unit, index + 1)].wait(timeout=10)
            parent.merge(worker.snapshot(), slot)
            release[(unit, index)].set()

    monkeypatch.setattr("artisan.execution.compute.endpoint.call_endpoint", endpoint)

    def unit_run(unit):
        with capture_commands() as recorder:
            results = EndpointExecuteRouter().route_execute(
                _SimpleOp(),
                [
                    ExecuteInput(
                        execute_dir=str(tmp_path), inputs={"unit": unit, "index": i}
                    )
                    for i in range(3)
                ],
                str(tmp_path),
            )
        assert results == [None, None, None]
        return recorder.snapshot()

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {unit: pool.submit(unit_run, unit) for unit in ("a", "b")}
        results = {unit: future.result() for unit, future in futures.items()}
    for unit, recording in results.items():
        assert [(c.invocation, c.sequence) for c in recording.commands] == [
            (i, j) for i in range(3) for j in range(2)
        ]
        assert all(command.argv[-1] == unit for command in recording.commands)
